"""Store property paths without treating them as RDF predicates."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PropertyPath(BaseModel):
    """A SHACL Core path expression. This model does not execute queries."""

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
            if self.items or not self.iri or not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", self.iri):
                raise ValueError("A predicate path needs one absolute IRI")
            if any(
                char.isspace() or char in '<>"{}|^\x60\\' or ord(char) < 32 for char in self.iri
            ):
                raise ValueError("Invalid character in a path IRI")
        elif self.iri is not None:
            raise ValueError("Only predicate paths have an IRI")
        elif self.operator in ("sequence", "alternative"):
            if len(self.items) < 2:
                raise ValueError("Sequence and alternative paths need at least two items")
        elif len(self.items) != 1:
            raise ValueError("Unary paths need exactly one item")
        return self
