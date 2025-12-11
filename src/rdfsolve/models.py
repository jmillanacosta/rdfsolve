"""
Pydantic models for VoID schema representation.

Provides type-safe data structures with validation for VoID schema elements.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class SchemaTriple(BaseModel):
    """A single schema relationship triple."""

    subject_class: str = Field(..., description="Subject class name")
    subject_uri: str = Field(..., description="Subject class URI")
    property: str = Field(..., description="Property name")
    property_uri: str = Field(..., description="Property URI")
    object_class: str = Field(..., description="Object class name")
    object_uri: str = Field(..., description="Object URI")

    @field_validator("subject_uri", "property_uri", "object_uri")
    @classmethod
    def validate_uri(cls, v: str) -> str:
        """Validate that URIs are properly formatted."""
        if not v.startswith(("http://", "https://", "urn:")):
            if v not in ["Literal", "Resource"]:
                raise ValueError(f"Invalid URI format: {v}")
        return v


class SchemaMetadata(BaseModel):
    """Metadata about the extracted schema."""

    total_triples: int = Field(..., ge=0, description="Total number of triples")
    total_classes: int = Field(..., ge=0, description="Total number of classes")
    total_properties: int = Field(..., ge=0, description="Total number of properties")
    dataset_name: Optional[str] = Field(None, description="Name of the dataset")
    extraction_date: Optional[str] = Field(None, description="Date of extraction")
    source_endpoint: Optional[HttpUrl] = Field(None, description="Source SPARQL endpoint")

    model_config = ConfigDict(extra="forbid")


class VoidSchema(BaseModel):
    """Complete VoID-extracted schema with triples and metadata."""

    triples: List[SchemaTriple] = Field(..., description="Schema triples")
    metadata: SchemaMetadata = Field(..., description="Schema metadata")

    @field_validator("triples")
    @classmethod
    def validate_triples(cls, v: List[SchemaTriple]) -> List[SchemaTriple]:
        """Ensure we have at least some triples for a valid schema."""
        if not v:
            raise ValueError("Schema must contain at least one triple")
        return v

    def get_classes(self) -> List[str]:
        """Get all unique class names."""
        classes = set()
        for triple in self.triples:
            classes.add(triple.subject_class)
            if triple.object_class not in ["Literal", "Resource"]:
                classes.add(triple.object_class)
        return sorted(classes)

    def get_properties(self) -> List[str]:
        """Get all unique property names."""
        return sorted({t.property for t in self.triples})

    def get_class_properties(self, class_name: str) -> List[str]:
        """Get all properties used by a specific class."""
        return sorted({t.property for t in self.triples if t.subject_class == class_name})

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "triples": [[t.subject_uri, t.property_uri, t.object_uri] for t in self.triples],
            "metadata": self.metadata.dict(),
            "classes": self.get_classes(),
            "properties": self.get_properties(),
        }


class LinkMLClass(BaseModel):
    """Represents a LinkML class definition."""

    name: str = Field(..., description="Class name")
    description: Optional[str] = Field(None, description="Class description")
    slots: List[str] = Field(default_factory=list, description="Slot names")
    class_uri: Optional[str] = Field(None, description="Class URI")


class LinkMLSlot(BaseModel):
    """Represents a LinkML slot definition."""

    name: str = Field(..., description="Slot name")
    description: Optional[str] = Field(None, description="Slot description")
    range: str = Field(..., description="Slot range type")
    domain_of: List[str] = Field(default_factory=list, description="Classes using this slot")
    required: bool = Field(False, description="Whether slot is required")
    multivalued: bool = Field(False, description="Whether slot accepts multiple values")
    slot_uri: Optional[str] = Field(None, description="Slot URI")


class LinkMLSchema(BaseModel):
    """Represents a complete LinkML schema.

    LinkML schemas can be exported to multiple formats including:
    - YAML: Human-readable schema definition
    - JSON Schema: For JSON validation
    - SHACL: For RDF data validation (Shapes Constraint Language)
    - Python: Pydantic models for data validation
    - And more...
    """

    id: str = Field(..., description="Schema ID")
    name: str = Field(..., description="Schema name")
    description: Optional[str] = Field(None, description="Schema description")
    classes: Dict[str, LinkMLClass] = Field(default_factory=dict, description="Class definitions")
    slots: Dict[str, LinkMLSlot] = Field(default_factory=dict, description="Slot definitions")

    def get_class_count(self) -> int:
        """Get number of classes."""
        return len(self.classes)

    def get_slot_count(self) -> int:
        """Get number of slots."""
        return len(self.slots)

    def get_object_properties(self) -> List[str]:
        """Get slots that reference other classes (object properties)."""
        object_props = []
        for slot_name, slot in self.slots.items():
            if slot.range in self.classes:
                object_props.append(slot_name)
        return sorted(object_props)
