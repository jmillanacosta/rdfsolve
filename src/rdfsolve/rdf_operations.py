"""Declare the read operations provided by the RDF client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from pydantic import Field, TypeAdapter, model_validator
from rdflib import Graph

from rdfsolve.registry import Contract, FieldDescription, Operation, Registry, TypeDescription
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.version import VERSION

if TYPE_CHECKING:
    from rdfsolve.client_api import Client

PageSize = Annotated[int, Field(ge=1, le=30)]
ReadSize = Annotated[int, Field(ge=1, le=100)]
HopLimit = Annotated[int, Field(ge=1, le=3)]


class Schema(Contract):
    """Find classes and fields using their names and source definitions."""

    text: str = ""
    kind: str | None = None
    offset: int = Field(default=0, ge=0)
    limit: PageSize = 10


class Search(Contract):
    """Find candidate records in names, identifiers and descriptive text."""

    terms: list[str] = Field(min_length=1, max_length=12)
    kind: str | None = None
    fields: list[str] = Field(default_factory=list, max_length=12)

    @model_validator(mode="after")
    def nonempty_terms(self) -> Search:
        """Reject empty phrases before querying."""
        if any(not term.strip() or len(term) > 200 for term in self.terms):
            raise ValueError("Use nonempty search phrases of at most 200 characters")
        return self


class Read(Contract):
    """Read fields or follow selected paths from retained records."""

    reference: str | None = None
    iri: str | None = None
    kind: str | None = None
    paths: list[str] = Field(default_factory=list, max_length=20)
    fields: list[str] = Field(default_factory=list, max_length=12)
    offset: int = Field(default=0, ge=0)
    limit: ReadSize = 20
    evidence_offset: int = Field(default=0, ge=0)
    detail: bool = False

    @model_validator(mode="after")
    def source_required(self) -> Read:
        """Require one source and a type for a new IRI."""
        if (self.reference is None) == (self.iri is None):
            raise ValueError("Supply a result reference or an iri with kind")
        if self.iri is not None and self.kind is None:
            raise ValueError("Supply kind with iri")
        return self


class Paths(Contract):
    """List possible class routes, not observed record connections."""

    source: str
    target: str
    max_hops: HopLimit = 2
    text: str = ""
    offset: int = Field(default=0, ge=0)
    limit: PageSize = 10


class Plan(Contract):
    """Choose the answer's classes and topic values before querying records."""

    source: str
    terms: list[str] = Field(min_length=1, max_length=12)
    targets: list[str] = Field(default_factory=list, max_length=5)
    selection: str = Field(min_length=1, max_length=600)
    evidence: str = ""
    max_hops: HopLimit = 3


ARGUMENTS: dict[str, type[Contract]] = {
    "plan": Plan,
    "schema": Schema,
    "search": Search,
    "paths": Paths,
    "read": Read,
}


def build_registry(client: Client, source_id: str) -> Registry:
    """Describe implemented RDF operations without probing the source."""
    schema = client._schema
    types = []
    for model in client.models.values():
        fields = []
        for name, field in model.model_fields.items():
            extra = field.json_schema_extra
            if isinstance(extra, dict) and extra.get("rdf_path"):
                patterns = TypeAdapter(list[SchemaPattern]).validate_python(
                    extra.get("rdf_patterns", [])
                )
                fields.append(
                    FieldDescription(
                        name=name,
                        label=client.link_name(model, name),
                        binding={"path": extra["rdf_path"]},
                        description=schema.enrichment.description(
                            str(extra.get("rdf_property_iri", ""))
                        ),
                        examples=list(field.examples or [])[:1],
                        node_kinds=sorted(
                            {
                                "literal"
                                if p.object_class == "Literal"
                                else "bnode"
                                if p.object_class == "BlankNode"
                                else "iri"
                                for p in patterns
                            }
                        ),
                        targets=sorted(
                            {
                                p.object_class
                                for p in patterns
                                if p.object_class not in _SENTINEL_OBJECTS
                            }
                        ),
                        datatypes=sorted({p.datatype for p in patterns if p.datatype}),
                    )
                )
        types.append(
            TypeDescription(
                id=str(getattr(model, "rdf_class_iri", "")),
                label=client.type_name(model),
                fields=fields,
                description=schema.enrichment.description(str(getattr(model, "rdf_class_iri", ""))),
            )
        )
    return Registry(
        producer=f"rdfsolve {VERSION}",
        source_id=source_id,
        source_version=schema.about.source_version_iri or schema.about.source_version,
        binding={
            "protocol": "rdf",
            "endpoint": None if isinstance(client.source, Graph) else client.source.endpoint_url,
            "graph_uris": list(client.graph_uris),
            "scope": "named graphs separately" if client.graph_uris else "default graph",
        },
        types=sorted(types, key=lambda item: item.id),
        operations=[
            Operation(
                id=key,
                action=key,
                description=(model.__doc__ or "").strip(),
                arguments=model.model_json_schema(),
                returns="answer plan"
                if model is Plan
                else "class routes"
                if model is Paths
                else "schema fields"
                if model is Schema
                else "record reference",
            )
            for key, model in ARGUMENTS.items()
        ],
        evidence=schema.to_dict(),
    )
