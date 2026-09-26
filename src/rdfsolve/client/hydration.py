"""Retrieve bounded RDF views into generated Pydantic models."""

from __future__ import annotations

import json
import keyword
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from platform import python_version
from typing import Any, TypeVar
from uuid import uuid4

import pyoxigraph as ox
from pydantic import BaseModel, Field, create_model
from pydantic.fields import FieldInfo
from rdflib import Graph, Literal
from typing_extensions import Self

from rdfsolve.local_rdf import LocalBackend, LocalRdf
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.exporters.pydantic import build_pydantic_classes
from rdfsolve.schema_models.paths import PropertyPath, absolute_iri
from rdfsolve.sparql_helper import EndpointError, QueryRecord, SparqlHelper

logger = logging.getLogger(__name__)
Model = TypeVar("Model", bound=BaseModel)
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


class HydrationLimitError(ValueError):
    """The result exceeded a budget. No partial object is returned."""


def class_iri(model: type[BaseModel] | BaseModel) -> str:
    """Return the RDF class IRI of a generated model or of one of its records."""
    value = getattr(model, "rdf_class_iri", None)
    if not isinstance(value, str):
        raise TypeError(f"{model!r} is not a generated RDF model")
    return value


def field_metadata(info: FieldInfo | None) -> dict[str, Any]:
    """Return the RDF metadata that the exporter stored on a generated field."""
    extra = info.json_schema_extra if info is not None else None
    return dict(extra) if isinstance(extra, dict) else {}


def _iri(value: str) -> str:
    """Validate an absolute IRI before placing it in a query."""
    return "<" + absolute_iri(value) + ">"


def _term(binding: dict[str, Any]) -> RdfTerm:
    """Read a SPARQL JSON value without dropping RDF term information."""
    kind = binding.get("type")
    if kind == "typed-literal":
        kind = "literal"
    if kind not in ("uri", "literal", "bnode") or not isinstance(binding.get("value"), str):
        raise EndpointError("Invalid RDF term in hydration results")
    return RdfTerm.model_validate(
        {
            "kind": kind,
            "value": binding["value"],
            "datatype": binding.get("datatype"),
            "language": binding.get("xml:lang"),
        }
    )


def _value(term: RdfTerm) -> Any:
    """Use native literal values while retaining their RDF form separately."""
    if term.kind != "literal":
        return term.json_value()
    literal = term.to_rdf()
    if not isinstance(literal, Literal):
        raise TypeError("Expected a literal")
    native = literal.toPython()
    return str(literal) if isinstance(native, Literal) else native


class Hydrator:
    """Read model fields from an endpoint or a local RDF graph.

    Fields are views, not SHACL validation results. References remain IRIs.
    Named-graph paths join across the selected data graphs.
    """

    def __init__(
        self,
        schema: MinedSchema,
        source: str | SparqlHelper | Graph | ox.Dataset | ox.Store | None = None,
        *,
        graph_uris: list[str] | None = None,
        timeout: float = 30,
        batch_size: int = 20,
        max_rows: int = 5000,
        max_subjects: int = 100,
        contract: bool = False,
        local_backend: LocalBackend = "oxigraph",
    ) -> None:
        """Create models and budgets without making requests.

        None graph scope uses the schema scope. Pass [] for the default graph.
        A supplied helper remains owned by the caller. Without a source or a schema
        endpoint, the client writes records but cannot query.
        """
        if any(type(v) is not int or v < 1 for v in (batch_size, max_rows, max_subjects)):
            raise ValueError("Use positive integer hydration budgets")
        self.graph_uris = (
            list(schema.about.graph_uris or []) if graph_uris is None else list(graph_uris)
        )
        for graph in self.graph_uris:
            _iri(graph)
        if source is None:
            source = schema.about.endpoint
        # Without a data source, records can be written but nothing can be queried.
        self.queryable = source not in (None, "")
        if not self.queryable:
            source = Graph()
        self._owns_helper = isinstance(source, str)
        self.source = (
            SparqlHelper(source, timeout=timeout, max_retries=2, inter_request_delay=0.5)
            if isinstance(source, str)
            else source
        )
        self._local_rdf = (
            LocalRdf(self.source, backend=local_backend)
            if isinstance(self.source, (Graph, ox.Dataset, ox.Store))
            else None
        )
        self.models = build_pydantic_classes(schema, contract=contract)
        self.batch_size = batch_size
        self.max_rows = max_rows
        self.max_subjects = max_subjects
        self.queries: list[str] = []
        self.last_query_execution: dict[str, Any] = {}
        self._schema = schema
        self._local_records: list[QueryRecord] = []
        self._steps: list[dict[str, Any]] = []
        self._retrievals: list[dict[str, Any]] = []
        self._operations: list[dict[str, Any]] = []
        self._tool_calls: list[dict[str, Any]] = []
        self._registries: dict[str, dict[str, Any]] = {}
        self._query_logs: dict[Path, tuple[Path, int]] = {}
        if isinstance(self.source, SparqlHelper):
            self.source.enable_query_collection(clear=False, include_results=True)

    def _records(self) -> list[QueryRecord]:
        if isinstance(self.source, SparqlHelper):
            return self.source.get_collected_queries()
        return self._local_records

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        """Name a research step and retain its queries, even when it fails."""
        if not name.strip():
            raise ValueError("Supply a step name")
        start = len(self._records())
        started = time.perf_counter()
        item: dict[str, Any] = {
            "name": name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
        }
        self._steps.append(item)
        try:
            yield
            item["status"] = "complete"
        except Exception as error:
            item["error"] = type(error).__name__
            item["message"] = str(error)
            raise
        finally:
            item["query_ids"] = list(range(start + 1, len(self._records()) + 1))
            item["finished_at"] = datetime.now(timezone.utc).isoformat()
            item["seconds"] = round(time.perf_counter() - started, 4)
            logger.info(
                "%s: %s; queries=%s; %.3fs",
                name,
                item["status"],
                item["query_ids"],
                item["seconds"],
            )

    def session_metadata(self, *, include_results: bool = True) -> dict[str, Any]:
        """Return all retained helper queries and named steps.

        Query IDs identify executions, including repeated texts. Queries outside
        a named step remain in the session. HTTP retries are one execution.
        Use this client and its helper sequentially while recording steps.
        """
        return {
            "schema": self._schema.to_dict(),
            "environment": {
                "python": python_version(),
                **{name: version(name) for name in ("rdfsolve", "pydantic", "rdflib")},
            },
            "graph_uris": list(self.graph_uris),
            "local_backend": self._local_rdf.metadata() if self._local_rdf else None,
            "budgets": {
                "batch_size": self.batch_size,
                "max_rows": self.max_rows,
                "max_subjects": self.max_subjects,
            },
            "queries": [
                {
                    "id": index,
                    **(
                        asdict(record)
                        if include_results
                        else {
                            **{
                                key: value for key, value in vars(record).items() if key != "result"
                            },
                            "result_retained": False,
                        }
                    ),
                }
                for index, record in enumerate(self._records(), 1)
            ],
            "steps": [dict(step) for step in self._steps],
            "retrievals": list(self._retrievals),
            "operations": list(self._operations),
            "tool_calls": list(self._tool_calls),
            "registries": dict(self._registries),
        }

    def save_session(self, path: str | Path, *, incremental: bool = False) -> None:
        """Save session JSON. Incremental mode appends query results to a sidecar.

        Keep both files together. QueryLog.read loads either representation.
        """
        path = Path(path).resolve()
        metadata = self.session_metadata(include_results=not incremental)
        if incremental:
            journal, written = self._query_logs.get(
                path, (path.with_name(f"{path.stem}-{uuid4().hex}.queries.jsonl"), 0)
            )
            records = self._records()
            with journal.open("a", encoding="utf-8") as stream:
                for index, record in enumerate(records[written:], written + 1):
                    json.dump({"id": index, **vars(record)}, stream, ensure_ascii=False)
                    stream.write("\n")
            self._query_logs[path] = (journal, len(records))
            metadata["queries_file"] = journal.name
            context = {key: metadata.pop(key) for key in ("schema", "registries")}
            context_path = journal.with_suffix(".context.json")
            temporary = context_path.with_name(f".{context_path.name}.{uuid4().hex}.tmp")
            temporary.write_text(json.dumps(context, ensure_ascii=False), encoding="utf-8")
            temporary.replace(context_path)
            metadata["context_file"] = context_path.name
            metadata["queries"] = [
                {key: row[key] for key in ("id", "query_type", "purpose", "success")}
                for row in metadata["queries"]
            ]
            calls = {call["id"] for call in metadata["tool_calls"]}
            metadata["operations"] = [
                {"id": operation["id"]} if operation["id"] in calls else operation
                for operation in metadata["operations"]
            ]
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(metadata, stream, ensure_ascii=False)
            stream.write("\n")
        temporary.replace(path)

    def model(self, name_or_iri: str) -> type[BaseModel]:
        """Find a generated class by its Python name or full RDF class IRI."""
        if name_or_iri in self.models:
            return self.models[name_or_iri]
        for model in self.models.values():
            if getattr(model, "rdf_class_iri", None) == name_or_iri:
                return model
        raise KeyError(name_or_iri)

    def _check_model_scope(self, model: type[BaseModel]) -> None:
        """Require the model's declared data and typing scope to match this schema."""
        metadata = model.model_config.get("json_schema_extra")
        if not isinstance(metadata, dict):
            raise ValueError("Model graph scope is missing")
        source = metadata.get("rdf_dataset")
        if not isinstance(source, dict):
            raise ValueError("Model graph scope is missing")
        for field in ("graph_uris", "type_graph_uris", "type_context_graph_uris"):
            graphs = source.get(field) or []
            if not isinstance(graphs, list) or any(not isinstance(graph, str) for graph in graphs):
                raise ValueError("Model graph scope must contain graph IRIs")
            if {str(graph) for graph in graphs} != set(getattr(self._schema.about, field) or []):
                raise ValueError("Model graph scope differs from this client's schema")
        if metadata.get("endpoint") != self._schema.about.endpoint:
            raise ValueError("Model graph scope belongs to a different source endpoint")

    def with_paths(
        self, model: type[Model], **paths: str | list[str] | PropertyPath
    ) -> type[Model]:
        """Extend a view with named predicates or paths, without changing its schema.

        A string means one predicate. A list means a sequence of predicates.
        PropertyPath supports the remaining SHACL Core path operators.
        """
        self._check_model_scope(model)
        fields: dict[str, Any] = {}
        for name, value in paths.items():
            if (
                not name.isidentifier()
                or keyword.iskeyword(name)
                or name.startswith(("_", "model_"))
                or hasattr(model, name)
                or name in model.model_fields
            ):
                raise ValueError(f"Use a new public field name: {name}")
            path = (
                value
                if isinstance(value, PropertyPath)
                else (
                    PropertyPath(
                        operator="sequence",
                        items=[PropertyPath(operator="predicate", iri=item) for item in value],
                    )
                    if isinstance(value, list)
                    else PropertyPath(operator="predicate", iri=value)
                )
            )
            fields[name] = (
                Any,
                Field(None, json_schema_extra={"rdf_path": path.model_dump(mode="json")}),
            )
        return create_model(model.__name__, __base__=model, **fields)

    def _type_pattern(self, node: str, cls: str) -> str:
        """Read types from the selected data and declared type context."""
        from rdfsolve.mining.query_builders import _type_pattern

        context = list(
            dict.fromkeys(
                (self._schema.about.type_graph_uris or [])
                + (self._schema.about.type_context_graph_uris or [])
            )
        )
        return _type_pattern(node, cls, context)

    def _subject_type(self, node: str, cls: str) -> str:
        """Require a typed subject to have an outgoing data edge."""
        pattern = self._type_pattern(node, cls)
        if self._schema.about.type_graph_uris or self._schema.about.type_context_graph_uris:
            pattern += f" FILTER EXISTS {{ {node} ?_dataP ?_dataO }}"
        return pattern

    def _scope(self, body: str) -> str:
        """Scope one graph directly; multiple data graphs use a query dataset."""
        if len(self.graph_uris) != 1:
            return body
        if self._schema.about.type_context_graph_uris or self._schema.about.type_graph_uris:
            return f"VALUES ?_graph {{ {_iri(self.graph_uris[0])} }} {{ {body} }}"
        graphs = " ".join(_iri(graph) for graph in self.graph_uris)
        return f"VALUES ?_graph {{ {graphs} }} GRAPH ?_graph {{ {body} }}"

    def _scope_query(self, query: str) -> str:
        """Apply the selected data-graph union to a complete query."""
        context = (self._schema.about.type_graph_uris or []) + (
            self._schema.about.type_context_graph_uris or []
        )
        if len(self.graph_uris) < 2 and not context:
            return query
        if not self.graph_uris:
            return query
        import re

        from rdflib.plugins.sparql import prepareQuery

        from rdfsolve.client.query_fragments import PROTECTED

        if prepareQuery(query).algebra.datasetClause:
            return query
        masked = PROTECTED.sub(lambda match: " " * len(match.group()), query)
        start = masked.find("{")
        if start < 0:
            raise ValueError("A scoped query requires a graph pattern")
        where = re.search(r"\bWHERE\s*$", masked[:start], re.IGNORECASE)
        position = where.start() if where else start
        dataset = " ".join(f"FROM {_iri(graph)}" for graph in dict.fromkeys(self.graph_uris))
        named = " ".join(
            f"FROM NAMED {_iri(graph)}" for graph in dict.fromkeys(self.graph_uris + context)
        )
        return query[:position] + dataset + " " + named + " " + query[position:]

    @property
    def is_local(self) -> bool:
        """Return True when the source is local RDF, not an endpoint."""
        return self._local_rdf is not None

    def _select(self, query: str, *, exhaustive: bool = False) -> list[dict[str, Any]]:
        """Execute one bounded query and retain its text for inspection."""
        if not self.queryable:
            raise ValueError(
                "No data source to query. Give the client an endpoint, a SPARQL helper or an "
                "RDF graph."
            )
        query = self._scope_query(query)
        self.queries.append(query)
        self.last_query_execution = {
            "strategy": "local_graph" if self.is_local else "single_response"
        }
        if self.is_local:
            if self._local_rdf is None:
                raise RuntimeError("Local RDF backend is not initialized")
            record = QueryRecord(query, "SELECT", "", success=False, purpose="hydrate")
            self._local_records.append(record)
            started = time.monotonic()
            try:
                raw = self._local_rdf.query(query).serialize(format="json")
                if raw is None:
                    raise EndpointError("Local query returned no results document")
                result = json.loads(raw)
                record.result = json.loads(raw)
                record.result_retained = True
                record.success = True
            except Exception as error:
                record.error = type(error).__name__
                raise
            finally:
                record.elapsed_seconds = time.monotonic() - started
        elif isinstance(self.source, SparqlHelper):
            try:
                result = self.source.select_with_fallback(
                    query, purpose="hydrate", **({"exhaustive": True} if exhaustive else {})
                )
            finally:
                self.last_query_execution = deepcopy(
                    getattr(self.source, "last_select_execution", {})
                )
        else:
            raise TypeError(f"Unknown data source: {type(self.source).__name__}")
        try:
            rows = result["results"]["bindings"]
        except (KeyError, TypeError) as error:
            raise EndpointError("Invalid hydration SELECT response") from error
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise EndpointError("Invalid hydration result rows")
        return rows

    def sample(
        self, model: type[Model], *, limit: int = 3, fields: list[str] | None = None
    ) -> list[Model]:
        """Retrieve a small ordered sample of IRI subjects with the requested type."""
        self._check_model_scope(model)
        if type(limit) is not int or not 1 <= limit <= self.max_subjects:
            raise ValueError(f"Use a sample limit between 1 and {self.max_subjects}")
        class_iri = _iri(getattr(model, "rdf_class_iri", ""))
        body = self._scope(self._subject_type("?s", class_iri) + " FILTER(isIRI(?s))")
        rows = self._select(f"SELECT DISTINCT ?s WHERE {{ {body} }} ORDER BY ?s LIMIT {limit}")
        iris = []
        for row in rows:
            term = _term(row.get("s", {}))
            if term.kind != "uri":
                raise EndpointError("Expected an IRI sample subject")
            iris.append(term.value)
        return self.get_many(model, iris, fields=fields)

    def get(self, model: type[Model], iri: str, *, fields: list[str] | None = None) -> Model:
        """Retrieve one IRI. Raise when no requested values or types are found."""
        return self.get_many(model, [iri], fields=fields)[0]

    def get_many(
        self, model: type[Model], iris: list[str], *, fields: list[str] | None = None
    ) -> list[Model]:
        """Retrieve fields in sequential subject batches, without cross products.

        Requested fields are lists, including [] when no values were returned.
        Unrequested fields stay None. Raw terms keep language and datatype.
        Blank-node labels are scoped to their response and cannot be followed.
        """
        self._check_model_scope(model)
        if len(iris) > self.max_subjects:
            raise HydrationLimitError("Too many subjects; use smaller calls")
        for iri in iris:
            _iri(iri)
        paths = {}
        for name, field in model.model_fields.items():
            extra = field.json_schema_extra
            if isinstance(extra, dict):
                if "rdf_path" in extra:
                    paths[name] = PropertyPath.model_validate(extra["rdf_path"])
                elif "rdf_property_iri" in extra:
                    paths[name] = PropertyPath(
                        operator="predicate", iri=str(extra["rdf_property_iri"])
                    )
        selected = list(paths) if fields is None else list(dict.fromkeys(fields))
        for name in selected:
            if name not in paths:
                raise ValueError(f"No RDF path for field {name}")
        branches = [f'{self._subject_type("?s", "?value")} BIND("@type" AS ?field)']
        branches += [
            f"?s {path_to_sparql(paths[name])} ?value . BIND({Literal(name).n3()} AS ?field)"
            for name in selected
        ]
        values: dict[str, dict[str, list[RdfTerm]]] = {iri: {} for iri in iris}
        unique = list(values)
        response_scopes: dict[str, str] = {}
        query_ids: dict[str, int] = {}
        for start in range(0, len(unique), self.batch_size):
            batch = unique[start : start + self.batch_size]
            scope = uuid4().hex
            response_scopes.update(dict.fromkeys(batch, scope))
            bindings = f"VALUES ?s {{ {' '.join(_iri(iri) for iri in batch)} }}"
            body = self._scope(" UNION ".join(f"{{ {bindings} {branch} }}" for branch in branches))
            query = f"SELECT DISTINCT ?s ?field ?value WHERE {{ {body} }} LIMIT {self.max_rows + 1}"
            rows = self._select(query)
            query_ids.update(dict.fromkeys(batch, len(self._records())))
            if len(rows) > self.max_rows:
                raise HydrationLimitError("Value budget exceeded; select fewer fields or subjects")
            for row in rows:
                subject = _term(row.get("s", {}))
                field_term = _term(row.get("field", {}))
                if (
                    subject.kind != "uri"
                    or subject.value not in batch
                    or field_term.kind != "literal"
                    or field_term.value not in [*selected, "@type"]
                ):
                    raise EndpointError("Unexpected subject or field in hydration results")
                term = _term(row.get("value", {}))
                terms = values[subject.value].setdefault(field_term.value, [])
                if term not in terms:
                    terms.append(term)
        objects = {}
        for iri, found in values.items():
            if not found:
                raise LookupError(f"No requested values or types returned for {iri}")
            payload: dict[str, Any] = {
                "@id": iri,
                "@type": [term.value for term in found.get("@type", []) if term.kind == "uri"],
                "rdf_terms": {
                    name: [term.model_dump(mode="json") for term in terms]
                    for name, terms in found.items()
                },
                "rdf_loaded_fields": selected,
                "rdf_source": {
                    "endpoint": self.source.endpoint_url
                    if isinstance(self.source, SparqlHelper)
                    else None,
                    "graph_uris": self.graph_uris,
                    "blank_node_scope": response_scopes[iri],
                    "query_id": query_ids[iri],
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                },
            }
            for name in selected:
                terms = found.get(name, [])
                converted = [_value(term) for term in terms]
                payload[name] = converted
            objects[iri] = model.model_validate(payload)
        self._retrievals.append(
            {
                "model": model.__name__,
                "class_iri": getattr(model, "rdf_class_iri", None),
                "subjects": list(objects),
                "fields": selected,
                "query_ids": sorted(set(query_ids.values())),
            }
        )
        logger.info(
            "Retrieved %d %s objects; %d fields per object",
            len(objects),
            model.__name__,
            len(selected),
        )
        return [objects[iri] for iri in iris]

    def close(self) -> None:
        """Close only the helper created by this client."""
        if self._owns_helper and isinstance(self.source, SparqlHelper):
            self.source.close()

    def __enter__(self) -> Self:
        """Use the client as a context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Release the owned HTTP session."""
        self.close()
