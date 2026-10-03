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

    @classmethod
    def from_sparql(cls, text: str, prefixes: dict[str, str] | None = None) -> PropertyPath:
        """Read SPARQL property path text, for example ``^schema:author/schema:name``."""
        from rdfsolve.schema_models.readers.paths import read_sparql_path

        return read_sparql_path(text, prefixes)

    @model_validator(mode="after")
    def check_expression(self) -> PropertyPath:
        """Check path arity and reject characters that change SPARQL syntax."""
        if self.operator == "predicate":
            if self.items:
                raise ValueError("A predicate path has no child paths")
            absolute_iri(self.iri or "")
        elif self.iri is not None:
            raise ValueError("Only predicate paths have an IRI")
        elif self.operator in ("sequence", "alternative"):
            if len(self.items) < 2:
                raise ValueError("Sequence and alternative paths need at least two items")
        elif len(self.items) != 1:
            raise ValueError("Unary paths need exactly one item")
        return self


_INVALID_IRI_CHAR = re.compile(r'[\s<>"{}|^`\\\x00-\x1f]')


def absolute_iri(value: str) -> str:
    """Validate an RDF IRI before serializing it into SPARQL."""
    if not isinstance(value, str) or not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", value):
        raise ValueError(
            "Expected an absolute RDF IRI; relative or blank-node identifiers need an anchored query."
        )
    if _INVALID_IRI_CHAR.search(value):
        raise ValueError("Invalid character in an RDF IRI")
    return value
