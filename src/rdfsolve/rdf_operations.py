"""Declare the read operations provided by the RDF client."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field, TypeAdapter
from rdflib import Graph

from rdfsolve.registry import Contract, FieldDescription, Operation, Registry, TypeDescription
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.version import VERSION

if TYPE_CHECKING:
    from rdfsolve.client_api import Client


class Find(Contract):
    """Find names or identifiers; return all candidates within client budgets."""

    text: str = Field(min_length=1)
    kind: str | None = None
    field: str | None = None


class Get(Contract):
    """Read one identifier using a source type and selected fields."""

    iri: str
    kind: str
    fields: list[str] = Field(default_factory=list)


class Related(Contract):
    """Follow links from a retained result."""

    reference: str
    kind: str | None = None
    value: str | None = None
    via: str | None = None
    incoming: bool = False


class Select(Contract):
    """Read a page of fields from a retained result."""

    reference: str
    fields: list[str] = Field(default_factory=list)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)


class Paths(Contract):
    """List possible class routes, not observed record connections."""

    source: str
    target: str
    max_hops: int = Field(default=2, ge=1, le=3)


ARGUMENTS: dict[str, type[Contract]] = {
    "records.find": Find,
    "records.get": Get,
    "records.related": Related,
    "records.select": Select,
    "schema.paths": Paths,
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
                action=key.split(".")[1],
                description=(model.__doc__ or "").strip(),
                arguments=model.model_json_schema(),
                returns="class routes"
                if model is Paths
                else "record page"
                if model is Select
                else "record reference",
            )
            for key, model in ARGUMENTS.items()
        ],
        evidence=schema.to_dict(),
    )
