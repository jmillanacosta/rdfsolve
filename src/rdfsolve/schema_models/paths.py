"""RDF property-path expressions and IRI validation."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PropertyPath(BaseModel):
    """A SHACL Core property-path expression."""

    model_config = ConfigDict(extra="forbid")
    operator: Literal[
        "predicate",
        "sequence",
        "alternative",
        "inverse",
        "zero_or_more",
        "one_or_more",
        "zero_or_one",
    ]
    iri: str | None = None
    items: list[PropertyPath] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_expression(self) -> PropertyPath:
        """Check path arity and reject characters that change SPARQL syntax."""
        if self.operator == "predicate":
            if self.items:
                raise ValueError("A predicate path has no child paths")
            absolute_iri(self.iri)
        elif self.iri is not None:
            raise ValueError("Only predicate paths have an IRI")
        elif self.operator in ("sequence", "alternative"):
            if len(self.items) < 2:
                raise ValueError("Sequence and alternative paths need at least two items")
        elif len(self.items) != 1:
            raise ValueError("Unary paths need exactly one item")
        return self


def absolute_iri(value: str) -> str:
    """Validate an RDF IRI before serializing it into SPARQL."""
    if not isinstance(value, str) or not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", value):
        raise ValueError(
            "Expected an absolute RDF IRI; relative or blank-node identifiers need an anchored query."
        )
    if any(char.isspace() or char in '<>"{}|^`\\' or ord(char) < 32 for char in value):
        raise ValueError("Invalid character in an RDF IRI")
    return value
