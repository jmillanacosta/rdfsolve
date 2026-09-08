"""Retrieve bounded RDF views into generated Pydantic models."""

from __future__ import annotations

import json
import keyword
import logging
from datetime import datetime, timezone
from typing import Any, TypeVar

from pydantic import BaseModel, Field, create_model
from rdflib import Graph, Literal
from typing_extensions import Self

from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.exporters.pydantic import build_pydantic_classes
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.sparql_helper import EndpointError, SparqlHelper

logger = logging.getLogger(__name__)
Model = TypeVar("Model", bound=BaseModel)
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


class HydrationLimitError(ValueError):
    """The result exceeded a budget. No partial object is returned."""


def _iri(value: str) -> str:
    """Validate an absolute IRI before placing it in a query."""
    return path_to_sparql(PropertyPath(operator="predicate", iri=value))


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
    Named-graph paths stay within each graph; graphs are not merged.
    """

    def __init__(
        self,
        schema: MinedSchema,
        source: str | SparqlHelper | Graph | None = None,
        *,
        graph_uris: list[str] | None = None,
        timeout: float = 30,
        batch_size: int = 20,
        max_rows: int = 5000,
        max_subjects: int = 100,
    ) -> None:
        """Create models and budgets without making requests.

        None graph scope uses the schema scope. Pass [] for the default graph.
        A supplied helper remains owned by the caller.
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
        if source is None or source == "":
            raise ValueError("Supply an endpoint, helper, or local RDF graph")
        self._owns_helper = isinstance(source, str)
        self.source = (
            SparqlHelper(source, timeout=timeout, max_retries=2, inter_request_delay=0.5)
            if isinstance(source, str)
            else source
        )
        self.models = build_pydantic_classes(schema)
        self.batch_size = batch_size
        self.max_rows = max_rows
        self.max_subjects = max_subjects
        self.queries: list[str] = []

    def model(self, name_or_iri: str) -> type[BaseModel]:
        """Find a generated class by its Python name or full RDF class IRI."""
        if name_or_iri in self.models:
            return self.models[name_or_iri]
        for model in self.models.values():
            if getattr(model, "rdf_class_iri", None) == name_or_iri:
                return model
        raise KeyError(name_or_iri)

    def with_paths(
        self, model: type[Model], **paths: str | list[str] | PropertyPath
    ) -> type[Model]:
        """Extend a view with named predicates or paths, without changing its schema.

        A string means one predicate. A list means a sequence of predicates.
        PropertyPath supports the remaining SHACL Core path operators.
        """
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

    def _scope(self, body: str) -> str:
        """Keep an entire path inside the selected named graph."""
        if not self.graph_uris:
            return body
        graphs = " ".join(_iri(graph) for graph in self.graph_uris)
        return f"VALUES ?_graph {{ {graphs} }} GRAPH ?_graph {{ {body} }}"

    def _select(self, query: str) -> list[dict[str, Any]]:
        """Execute one bounded query and retain its text for inspection."""
        self.queries.append(query)
        if isinstance(self.source, Graph):
            raw = self.source.query(query).serialize(format="json")
            if raw is None:
                raise EndpointError("Local query returned no results document")
            result = json.loads(raw)
        else:
            result = self.source.select(query, purpose="hydrate")
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
        if type(limit) is not int or not 1 <= limit <= self.max_subjects:
            raise ValueError(f"Use a sample limit between 1 and {self.max_subjects}")
        class_iri = _iri(getattr(model, "rdf_class_iri", ""))
        body = self._scope(f"?s a {class_iri} . FILTER(isIRI(?s))")
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
        branches = [f'{{ ?s <{RDF_TYPE}> ?value . BIND("@type" AS ?field) }}']
        branches += [
            f"{{ ?s {path_to_sparql(paths[name])} ?value . BIND({Literal(name).n3()} AS ?field) }}"
            for name in selected
        ]
        values: dict[str, dict[str, list[RdfTerm]]] = {iri: {} for iri in iris}
        unique = list(values)
        for start in range(0, len(unique), self.batch_size):
            batch = unique[start : start + self.batch_size]
            body = self._scope(" UNION ".join(branches))
            query = (
                f"SELECT DISTINCT ?s ?field ?value WHERE {{ VALUES ?s {{ {' '.join(_iri(iri) for iri in batch)} }} "
                f"{body} }} LIMIT {self.max_rows + 1}"
            )
            rows = self._select(query)
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
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                },
            }
            for name in selected:
                terms = found.get(name, [])
                converted = [_value(term) for term in terms]
                payload[name] = converted
            objects[iri] = model.model_validate(payload)
        logger.info(
            "Hydrated %d %s objects; %d fields per object",
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
