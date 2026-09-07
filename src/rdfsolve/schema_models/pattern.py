"""Observed RDF schema patterns."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS, _URI_SCHEMES


class PatternType(str, Enum):
    """Semantic type of an RDF pattern.

    Distinguishes what kind of RDF construct a pattern represents,
    which is essential for proper schema interpretation and
    downstream code generation.
    """

    OBJECT_PROPERTY = "object_property"
    """Links subject to another resource (typed or untyped URI)."""

    DATATYPE_PROPERTY = "datatype_property"
    """Links subject to a literal value (string, integer, date, etc)."""

    ANNOTATION_PROPERTY = "annotation"
    """Metadata property (rdfs:label, rdfs:comment, dcterms prefix, etc)."""

    BLANK_NODE_PROPERTY = "blank_node_property"
    """Links subject to a blank node (structural/anonymous)."""

    UNKNOWN = "unknown"
    """Could not determine the pattern type."""


class SchemaPattern(BaseModel):
    """A single schema pattern: subject_class -> property -> object.

    Captures four kinds of relationships:

    - **typed-object**:
      ``?s a ?sc . ?s ?p ?o . ?o a ?oc``
    - **literal**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isLiteral(?o))``
    - **untyped-uri** (unconstrained URI):
      ``?s a ?sc . ?s ?p ?o . FILTER(isURI(?o) && NOT EXISTS { ?o a ?any })``
    - **blank-node**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isBlank(?o))``

    This model is shared between SchemaMiner (direct SPARQL)
    and VoidParser (RDF triples VoID catalog-based extraction).
    """

    subject_class: str = Field(
        ...,
        description="URI of the subject class",
    )
    property_uri: str = Field(
        ...,
        description="URI of the property",
    )
    object_class: str = Field(
        ...,
        description=(
            "URI of the object class. Special values: "
            "'Literal' for literal objects (use datatype for XSD type), "
            "'Resource' for untyped URI objects (rdfs:Resource), "
            "'BlankNode' for blank node objects."
        ),
    )
    count: int | None = Field(
        None,
        ge=0,
        description="Number of triples matching this pattern",
    )
    datatype: str | None = Field(
        None,
        description="XSD datatype URI for literal objects (only when object_class == 'Literal')",
    )
    blank_node_predicates: list[str] | None = Field(
        None,
        description=(
            "Predicates that blank node objects have (structural signature). "
            "Only set when object_class == 'BlankNode'. Common patterns: "
            "rdf:first/rdf:rest (lists), owl:onProperty (restrictions)."
        ),
    )

    # Semantic type
    pattern_type: PatternType = Field(
        default=PatternType.UNKNOWN,
        description="Semantic type of this pattern (object/datatype/annotation/blank_node)",
    )

    # Evidence metrics
    distinct_subjects: int | None = Field(
        None,
        ge=0,
        description="Number of distinct subjects using this pattern",
    )
    distinct_objects: int | None = Field(
        None,
        ge=0,
        description="Number of distinct objects in this pattern",
    )
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Confidence score (0.0-1.0) for this pattern",
    )
    evidence_source: str = Field(
        default="mined",
        description="How this pattern was discovered: 'mined', 'inferred', 'imported'",
    )

    # Labels
    subject_label: str | None = Field(
        None,
        description="Human-readable label for the subject class",
    )
    property_label: str | None = Field(
        None,
        description="Human-readable label for the property",
    )
    object_label: str | None = Field(
        None,
        description="Human-readable label for the object class",
    )

    @field_validator("subject_class", "property_uri")
    @classmethod
    def _validate_uri(cls, v: str) -> str:
        if not v.startswith(_URI_SCHEMES):
            msg = f"Invalid URI: {v}"
            raise ValueError(msg)
        return v

    @field_validator("object_class")
    @classmethod
    def _validate_object(cls, v: str) -> str:
        if v not in _SENTINEL_OBJECTS and not v.startswith(
            _URI_SCHEMES,
        ):
            msg = f"Invalid object class: {v}"
            raise ValueError(msg)
        return v
