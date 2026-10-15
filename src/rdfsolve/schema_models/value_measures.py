"""Measures of the values of each (class, property) of a schema, from which SHACL constraints
are written (rdfsolve.mining.value_measures).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from rdfsolve.schema_models.class_parts import ClassPart

__all__ = ["ValueCount", "ValueParts"]


class ValueCount(BaseModel):
    """The members of a class that have a property, and the most distinct values of one."""

    subject_class: str
    property_uri: str
    subjects: int = Field(ge=0, description="Members with at least one value")
    most: int = Field(ge=0, description="Most distinct values of one member")


class ValueParts(BaseModel):
    """The parts of the IRI values without a type of a (class, property), counted over every
    distinct value (*values*): how they are identified, as the parts of a class.
    """

    subject_class: str
    property_uri: str
    parts: list[ClassPart] = Field(default_factory=list)
    values: int = Field(ge=0, description="Distinct IRI values without a type")
