"""Execute registered RDF reads and retain bounded typed results."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from rdfsolve.client_api import Client, Results, _title
from rdfsolve.hydration import HydrationLimitError
from rdfsolve.rdf_operations import ARGUMENTS, Find, Get, Paths, Related, Select
from rdfsolve.registry import Contract


class ClientSession:
    """Keep results until released or this session is discarded. Calls are sequential."""

    def __init__(
        self,
        client: Client,
        *,
        source_id: str,
        preview_rows: int = 20,
        max_results: int = 50,
        max_records: int = 1000,
    ) -> None:
        """Bind the registry to a caller-owned client without making requests."""
        if any(
            type(value) is not int or value < 1
            for value in (preview_rows, max_results, max_records)
        ):
            raise ValueError("Use positive integer session budgets")
        self.client = client
        self.registry = client.registry(source_id=source_id)
        self._revision = self.registry.revision
        client._registries[self._revision] = self.registry.model_dump(mode="json")
        self.preview_rows = min(preview_rows, 100)
        self.max_results = max_results
        self.max_records = max_records
        self.results: dict[str, Results] = {}

    def release(self, reference: str) -> None:
        """Remove a retained result. Its execution record remains in the client log."""
        self._result(reference)
        del self.results[reference]

    def _result(self, reference: str) -> Results:
        if reference not in self.results:
            raise ValueError("Unknown result reference; use a reference from this session")
        return self.results[reference]

    def _reference(self, value: str) -> str:
        """Accept a result handle or an IRI already retrieved in this session."""
        if value in self.results:
            return value
        records = {}
        for reference, result in self.results.items():
            if result.records and all(vars(record)["uri"] == value for record in result):
                return reference
            for record in result:
                if vars(record)["uri"] == value:
                    records[type(record)] = record
        if not records:
            raise ValueError("Unknown result reference or record IRI; use a retrieved result")
        return self._retain(Results(self.client, list(records.values())))

    def result(self, reference: str) -> Results:
        """Read retained typed records without sending another query."""
        return self._result(reference)

    def export_result(self, reference: str) -> dict[str, Any]:
        """Copy retained records and session evidence without fetching unread fields."""
        records = self._result(reference).records
        metadata = self.client.session_metadata()
        return deepcopy(
            {
                "reference": reference,
                "registry_revision": self._revision,
                "schema": self.registry.evidence,
                "labels": {item.id: item.label for item in self.registry.types},
                "records": [
                    {
                        "type": str(getattr(type(record), "rdf_class_iri", "")),
                        "data": record.model_dump(mode="json"),
                    }
                    for record in records
                ],
                "queries": metadata["queries"],
                "links": metadata["links"],
                "operations": metadata["operations"],
                "scope": self.registry.binding,
            }
        )

    def call(self, operation: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Validate and execute an allowlisted operation; retain failed attempts too."""
        start = len(self.client._records())
        execution: dict[str, Any] = {
            "id": uuid4().hex,
            "operation": operation,
            "registry_revision": self._revision,
            "source_id": self.registry.source_id,
            "scope": dict(self.registry.binding),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
        }
        self.client._operations.append(execution)
        try:
            if self.client.graph_uris != self.registry.binding["graph_uris"]:
                raise ValueError("Create a new session after changing graph scope")
            if operation not in ARGUMENTS:
                raise ValueError(f"Unknown operation: {operation}")
            parsed = ARGUMENTS[operation].model_validate(arguments)
            execution["arguments"] = parsed.model_dump(mode="json")
            if isinstance(parsed, (Find, Get, Related)) and (
                len(self.results) >= self.max_results
                or sum(len(result) for result in self.results.values()) >= self.max_records
            ):
                raise HydrationLimitError("Result budget reached; release a retained result")
            with self.client.step(operation):
                result = self._execute(parsed)
            execution.update(status="complete", result=deepcopy(result))
            return {**result, "execution": execution["id"]}
        except Exception as error:
            execution["error"] = {"category": type(error).__name__, "message": str(error)}
            raise
        finally:
            execution["query_ids"] = list(range(start + 1, len(self.client._records()) + 1))
            execution["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _execute(self, arguments: Contract) -> dict[str, Any]:
        if isinstance(arguments, Find):
            return {
                **self._keep(
                    self.client.find(arguments.text, kind=arguments.kind, field=arguments.field)
                ),
                "basis": "Name or identifier matches; shared names do not establish identity or links",
            }
        if isinstance(arguments, Get):
            model = self.client.model(arguments.kind)
            fields = [self.client.field_name(model, name) for name in arguments.fields]
            record = self.client.get(model, arguments.iri, fields=fields)
            return self._keep(Results(self.client, [record]), fields=arguments.fields)
        if isinstance(arguments, Related):
            if arguments.kind is not None:
                self.client.model(arguments.kind)
            result = self._result(self._reference(arguments.reference))
            start = len(self.client._matches)
            page = self._keep(
                result.related(
                    kind=arguments.kind,
                    value=arguments.value,
                    via=arguments.via,
                    incoming=arguments.incoming,
                )
            )
            return {**page, "links": deepcopy(self.client._matches[start:])}
        if isinstance(arguments, Select):
            reference = self._reference(arguments.reference)
            return self._page(arguments.model_copy(update={"reference": reference}))
        if isinstance(arguments, Paths):
            table = self.client.paths_between(
                arguments.source,
                arguments.target,
                max_hops=arguments.max_hops,
                max_paths=100,
            )
            selected = table[table["Path"].isin(table["Path"].unique()[: self.preview_rows])]
            return {
                "rows": json.loads(selected.to_json(orient="records")),
                "matched_paths": int(table["Path"].nunique()),
                "preview_only": len(selected) < len(table),
                "status": "complete",
                "basis": "possible class routes, not observed connections",
            }
        raise ValueError("Unsupported operation arguments")

    def _keep(self, result: Results, *, fields: list[str] | None = None) -> dict[str, Any]:
        reference = self._retain(result)
        return self._page(Select(reference=reference, fields=fields or [], limit=self.preview_rows))

    def _retain(self, result: Results) -> str:
        count = sum(len(item) for item in self.results.values()) + len(result)
        if count > self.max_records or len(self.results) >= self.max_results:
            raise HydrationLimitError(
                "Result exceeds the retained-record budget; narrow the search"
            )
        reference = uuid4().hex
        self.results[reference] = result
        return reference

    def _page(self, arguments: Select) -> dict[str, Any]:
        result = self._result(arguments.reference)
        names = {
            model: [self.client.field_name(model, name) for name in arguments.fields]
            for model in {type(record) for record in result.records}
        }
        for model, fields in names.items():
            for name in fields:
                extra = model.model_fields[name].json_schema_extra
                if not isinstance(extra, dict) or not extra.get("rdf_path"):
                    raise ValueError(f"No RDF field: {name}")
        records = result.records[
            arguments.offset : arguments.offset + min(arguments.limit, self.preview_rows)
        ]
        page = Results(self.client, records)
        page._load(*arguments.fields)
        result.records[arguments.offset : arguments.offset + len(records)] = page.records
        rows = []
        for record in page.records:
            payload = record.model_dump(mode="json")
            requested_type = str(getattr(type(record), "rdf_class_iri", ""))
            observed_types = payload.get("rdf_type", [])
            rows.append(
                {
                    "id": payload["uri"],
                    "type": requested_type,
                    "type_label": self.client.type_name(type(record)),
                    "label": _title(record),
                    "observed_types": observed_types,
                    "type_evidence": "observed"
                    if requested_type in observed_types
                    else "not established",
                    "fields": {
                        name: payload["rdf_terms"].get(name, []) for name in names[type(record)]
                    },
                    "blank_node_scope": payload["rdf_source"].get("blank_node_scope"),
                }
            )
        end = arguments.offset + len(rows)
        return {
            "reference": arguments.reference,
            "rows": rows,
            "retained_records": len(result),
            "total_matches": None,
            "next_offset": end if end < len(result) else None,
            "preview_only": arguments.offset > 0 or len(rows) < len(result),
            "status": "complete",
            "scope": dict(self.registry.binding),
        }
