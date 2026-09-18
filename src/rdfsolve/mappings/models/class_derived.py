"""ClassDerivedMapping - class mappings derived from instance evidence."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from rdfsolve.mappings.models.core import Mapping


class ClassDerivedMapping(Mapping):
    """Store class mappings derived from instance-level mappings and their derivation metadata.

    The mapping predicate is the relation between the supporting instances. It is
    not a class-level assertion.
    """

    mapping_type: str = Field(default="class_derived")
    source_mapping_type: str = Field(
        ...,
        description="Original mapping type (sssom_import or semra_import)",
    )
    source_mapping_files: list[str] = Field(
        default_factory=list,
        description="Paths to the source instance-level mapping files",
    )
    derivation_stats: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Statistics about the derivation process. "
            "Keys: input_edges, class_pairs_found, class_pairs_after_filter, "
            "output_edges, min_instance_count, min_confidence, "
            "confidence_mean, confidence_median, confidence_max, "
            "supporting_entity_predicates, top_class_pairs."
        ),
    )
    enrichment_stats: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Statistics from the JSON-LD class enrichment step. "
            "Keys: entities_total, entities_enriched, entities_not_found, "
            "entities_not_found_pct, classes_added, distinct_classes, "
            "graphs_referenced, classes_per_entity_mean, "
            "classes_per_entity_max, "
            "not_found_iris, not_found_prefixes."
        ),
    )
    class_index_endpoint: str | None = Field(
        None,
        description="SPARQL endpoint used for class index queries",
    )

    def to_jsonld(self) -> dict[str, Any]:
        """Extend base JSON-LD with class-derivation provenance."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["strategy"] = self.mapping_type
        about["source_mapping_type"] = self.source_mapping_type
        about["source_files"] = self.source_mapping_files
        if self.derivation_stats:
            about["derivation_stats"] = self.derivation_stats
        if self.enrichment_stats:
            about["enrichment_stats"] = self.enrichment_stats
        if self.class_index_endpoint:
            about["class_index_endpoint"] = self.class_index_endpoint
        doc["@about"] = about
        return doc
