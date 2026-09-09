"""Save and inspect operation contracts without loading an execution backend."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self


class Contract(BaseModel):
    """Reject unknown fields and implicit argument conversions."""

    model_config = ConfigDict(extra="forbid", strict=True)


class FieldDescription(Contract):
    """Name a field and retain its source binding."""

    name: str
    label: str
    binding: dict[str, Any]


class TypeDescription(Contract):
    """Describe a source type without claiming that records were validated."""

    id: str
    label: str
    fields: list[FieldDescription]


class Operation(Contract):
    """Describe one implemented read operation, not current source availability."""

    id: str
    action: str
    description: str
    arguments: dict[str, Any]
    returns: str
    status: Literal["supported"] = "supported"


class Registry(Contract):
    """A source-scoped snapshot of types, operations and their evidence."""

    format: Literal["rdfsolve.operations"] = "rdfsolve.operations"
    format_version: Literal[1] = 1
    producer: str
    source_id: str = Field(min_length=1)
    source_version: str | None = None
    binding: dict[str, Any]
    types: list[TypeDescription]
    operations: list[Operation]
    evidence: dict[str, Any]

    @model_validator(mode="after")
    def unique_ids(self) -> Self:
        """Reject duplicate operation, type and field keys."""
        for ids in (
            [item.id for item in self.operations],
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

    def find(self, text: str = "", *, limit: int = 5) -> list[dict[str, str]]:
        """Find operation cards by words, without making source requests."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Use a limit from 1 to 100")
        words = text.casefold().split()
        return [
            {
                "id": item.id,
                "action": item.action,
                "description": item.description,
                "returns": item.returns,
            }
            for item in self.operations
            if all(
                word in f"{item.id} {item.description} {item.action}".casefold() for word in words
            )
        ][:limit]

    def describe(self, identifier: str, *, evidence: bool = False) -> dict[str, Any]:
        """Read a callable contract or a type; include source evidence on request."""
        entries: list[Operation | TypeDescription] = [*self.operations, *self.types]
        matches = [item for item in entries if item.id == identifier]
        if len(matches) != 1:
            raise ValueError(f"Unknown or ambiguous registry identifier: {identifier}")
        result = matches[0].model_dump(mode="json")
        if evidence:
            result.update(binding=self.binding, evidence=self.evidence)
        return result

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
