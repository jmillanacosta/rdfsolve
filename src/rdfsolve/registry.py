"""Describe generated RDF types and serialize their source evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from rdflib import Graph

from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.version import VERSION

if TYPE_CHECKING:
    from rdfsolve.client_api import Client
from typing_extensions import Self


class Contract(BaseModel):
    """Reject unknown fields and implicit argument conversions."""

    model_config = ConfigDict(extra="forbid", strict=True)


class FieldDescription(Contract):
    """Name a field and retain its source binding."""

    name: str
    label: str
    binding: dict[str, Any]
    description: str | None = None
    examples: list[Any] = Field(default_factory=list)
    node_kinds: list[str] = Field(default_factory=list)
    targets: list[str] = Field(default_factory=list)
    datatypes: list[str] = Field(default_factory=list)


class TypeDescription(Contract):
    """Describe a source type without claiming that records were validated."""

    id: str
    label: str
    fields: list[FieldDescription]
    description: str | None = None


class Registry(Contract):
    """A source-scoped snapshot of types and their evidence."""

    format: Literal["rdfsolve.registry"] = "rdfsolve.registry"
    format_version: Literal[3] = 3
    producer: str
    source_id: str = Field(min_length=1)
    source_version: str | None = None
    binding: dict[str, Any]
    types: list[TypeDescription]
    evidence: dict[str, Any]

    @model_validator(mode="after")
    def unique_ids(self) -> Self:
        """Reject duplicate type and field keys."""
        for ids in (
            [item.id for item in self.types],
            *[[field.name for field in item.fields] for item in self.types],
        ):
            if len(ids) != len(set(ids)):
                raise ValueError("Registry identifiers must be unique within each collection")
        return self

    @property
    def revision(self) -> str:
        """Identify the saved content, including bindings and source evidence."""
        content = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(content.encode()).hexdigest()

    def write(self, path: str | Path) -> None:
        """Save a JSON snapshot and its content revision."""
        document = {**self.model_dump(mode="json"), "revision": self.revision}
        Path(path).write_text(
            json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    @classmethod
    def read(cls, path: str | Path) -> Self:
        """Read a snapshot; do not import code, resolve references or make requests."""
        document = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise ValueError("Expected a registry document")
        revision = document.pop("revision", None)
        registry = cls.model_validate(document)
        if revision != registry.revision:
            raise ValueError("Registry revision does not match its content")
        return registry


def build_registry(client: Client, source_id: str) -> Registry:
    """Describe generated RDF types and fields from the retained schema."""
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
        evidence=schema.to_dict(),
    )
