"""Explore generated RDF models through named fields."""

from __future__ import annotations

from typing import Any, TypeVar

import pandas as pd
from pydantic import BaseModel
from rdflib import Literal

from rdfsolve.hydration import HydrationLimitError, Hydrator, _iri, _term
from rdfsolve.schema_models.enrichment import LABEL_PREDICATES
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.sparql_helper import EndpointError

Model = TypeVar("Model", bound=BaseModel)
SEARCH_PREDICATES = set(LABEL_PREDICATES) | {
    "http://purl.org/dc/elements/1.1/identifier",
    "http://purl.org/dc/terms/identifier",
    "http://purl.org/dc/terms/alternative",
    "http://www.w3.org/2004/02/skos/core#altLabel",
    "http://www.w3.org/2004/02/skos/core#notation",
}


def _path(model: type[BaseModel], field: str) -> PropertyPath:
    info = model.model_fields.get(field)
    extra = info.json_schema_extra if info is not None else None
    if not isinstance(extra, dict) or not extra.get("rdf_path"):
        raise ValueError(f"No RDF path for field {field}")
    return PropertyPath.model_validate(extra["rdf_path"])


class DatasetClient(Hydrator):
    """Search typed records and follow recorded links without recursive loading."""

    def links(self, model: type[BaseModel]) -> pd.DataFrame:
        """List outgoing typed links in the schema, not proven instance routes."""
        rows = []
        names = {getattr(cls, "rdf_class_iri", ""): name for name, cls in self.models.items()}
        fields = {}
        for field, info in model.model_fields.items():
            extra = info.json_schema_extra
            if isinstance(extra, dict) and extra.get("rdf_property_iri"):
                fields[extra["rdf_property_iri"]] = field
        for pattern in self._schema.patterns:
            if (
                pattern.subject_class == getattr(model, "rdf_class_iri", "")
                and pattern.object_class in names
                and pattern.property_uri in fields
            ):
                rows.append(
                    {
                        "field": fields[pattern.property_uri],
                        "target": names[pattern.object_class],
                        "predicate": pattern.property_uri,
                    }
                )
        return pd.DataFrame(rows, columns=["field", "target", "predicate"]).drop_duplicates()

    def search_names(
        self,
        model: type[Model],
        text: str,
        *,
        fields: list[str] | None = None,
        limit: int = 100,
    ) -> list[Model]:
        """Find typed IRIs whose label or title contains text, ignoring case.

        Raise if the match count exceeds the limit. This is a text search,
        not a search for related biological concepts.
        """
        if not text.strip():
            raise ValueError("Supply search text")
        if type(limit) is not int or not 1 <= limit <= self.max_subjects:
            raise ValueError(f"Use a limit between 1 and {self.max_subjects}")
        predicates = " ".join(_iri(p) for p in LABEL_PREDICATES)
        body = self._scope(
            f"?s a {_iri(getattr(model, 'rdf_class_iri', ''))} . "
            f"VALUES ?labelProperty {{ {predicates} }} ?s ?labelProperty ?label . "
            f"FILTER(isIRI(?s) && isLiteral(?label) && "
            f"CONTAINS(LCASE(STR(?label)), LCASE({Literal(text).n3()})))"
        )
        rows = self._select(f"SELECT DISTINCT ?s WHERE {{ {body} }} ORDER BY ?s LIMIT {limit + 1}")
        if len(rows) > limit:
            raise HydrationLimitError("Search limit exceeded; narrow the text or raise the limit")
        iris = []
        for row in rows:
            term = _term(row.get("s", {}))
            if term.kind != "uri":
                raise EndpointError("Expected an IRI search result")
            iris.append(term.value)
        return self.get_many(model, iris, fields=fields)

    def follow(
        self,
        records: list[BaseModel],
        field: str,
        target: type[Model],
        *,
        fields: list[str] | None = None,
        inverse: bool = False,
        value: str | None = None,
    ) -> list[Model]:
        """Follow a named field and retrieve distinct typed targets.

        In inverse mode the field belongs to the target model. Keep matched
        source/target pairs in session metadata. A path match is not a claim
        of causation. Blank nodes cannot be followed between requests.
        """
        if not records:
            return []
        if value is not None and not value.strip():
            raise ValueError("Enter a word or name to find")
        text_filter = ""
        if value is not None:
            predicates = " ".join(_iri(p) for p in sorted(SEARCH_PREDICATES))
            text_filter = (
                f"FILTER EXISTS {{ VALUES ?_nameProperty {{ {predicates} }} "
                f"?target ?_nameProperty ?_nameValue . FILTER(!isBlank(?_nameValue) && "
                f"CONTAINS(LCASE(STR(?_nameValue)), LCASE({Literal(value).n3()}))) }}"
            )
        source_model = type(records[0])
        if any(type(record) is not source_model for record in records):
            raise ValueError("Use records of one generated model")
        path = _path(target if inverse else source_model, field)
        if inverse:
            path = PropertyPath(operator="inverse", items=[path])
        path_text = path_to_sparql(path)
        subjects = list(dict.fromkeys(str(vars(record)["uri"]) for record in records))
        if len(subjects) > self.max_subjects:
            raise HydrationLimitError("Subject budget exceeded")
        targets: set[str] = set()
        for start in range(0, len(subjects), self.batch_size):
            batch = subjects[start : start + self.batch_size]
            values = " ".join(_iri(iri) for iri in batch)
            body = self._scope(
                f"VALUES ?source {{ {values} }} "
                f"?source a {_iri(getattr(source_model, 'rdf_class_iri', ''))} ; "
                f"{path_text} ?target . "
                f"?target a {_iri(getattr(target, 'rdf_class_iri', ''))} . FILTER(isIRI(?target)) {text_filter}"
            )
            rows = self._select(
                f"SELECT DISTINCT ?source ?target ?_graph WHERE {{ {body} }} "
                f"LIMIT {self.max_rows + 1}"
            )
            if len(rows) > self.max_rows:
                raise HydrationLimitError("Link budget exceeded; use fewer source records")
            for row in rows:
                source_term, target_term = (
                    _term(row.get("source", {})),
                    _term(row.get("target", {})),
                )
                if (
                    source_term.kind != "uri"
                    or source_term.value not in batch
                    or target_term.kind != "uri"
                ):
                    raise EndpointError("Unexpected source or target in link results")
                targets.add(target_term.value)
                self._matches.append(
                    {
                        "source": source_term.value,
                        "target": target_term.value,
                        "field": field,
                        "path": path.model_dump(mode="json"),
                        "graph": row.get("_graph", {}).get("value"),
                        "query_id": len(self._records()),
                    }
                )
            if len(targets) > self.max_subjects:
                raise HydrationLimitError("Target budget exceeded; use fewer source records")
        return self.get_many(target, sorted(targets), fields=fields)

    @property
    def _matches(self) -> list[dict[str, Any]]:
        if not hasattr(self, "_link_matches"):
            self._link_matches: list[dict[str, Any]] = []
        return self._link_matches

    def steps(self) -> pd.DataFrame:
        """Summarize named steps; the full queries remain in session metadata."""
        records = self._records()
        rows = []
        for step in self._steps:
            ids = set(step.get("query_ids", []))
            rows.append(
                {
                    "Step": step["name"],
                    "Queries": len(ids),
                    "Records read": sum(
                        len(item["subjects"])
                        for item in self._retrievals
                        if ids.intersection(item["query_ids"])
                    ),
                    "HTTP fallbacks": sum(records[index - 1].fallback_used for index in ids),
                    "Result": step["status"],
                }
            )
        return pd.DataFrame(rows)

    def evidence(self) -> pd.DataFrame:
        """Show recorded path matches, with the query that returned each match."""
        return pd.DataFrame(
            self._matches, columns=["source", "field", "target", "graph", "query_id", "path"]
        )

    def session_metadata(self) -> dict[str, Any]:
        """Include the source/target matches with the saved query record."""
        return {**super().session_metadata(), "links": list(self._matches)}

    @staticmethod
    def table(records: list[BaseModel], fields: list[str]) -> pd.DataFrame:
        """Display selected fields. Original records retain lists and RDF terms."""
        rows = []
        for record in records:
            unknown = set(fields) - type(record).model_fields.keys()
            if unknown:
                raise ValueError(f"Unknown fields: {sorted(unknown)}")
            row: dict[str, Any] = {"uri": vars(record)["uri"]}
            for field in fields:
                value = getattr(record, field)
                row[field] = (
                    " | ".join(str(item) for item in value) if isinstance(value, list) else value
                )
            rows.append(row)
        return pd.DataFrame(rows, columns=["uri", *fields])
