"""ClassDerivedMapping - class mappings derived from instance evidence."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from rdfsolve.mapping_models.core import Mapping


class ClassDerivedMapping(Mapping):
    """Mapping derived by aggregating instance-level evidence to class level.

    Each edge represents a class-to-class mapping supported by N
    instance-level edges from SSSOM or SeMRA sources.

    The derivation process:
    1. Loads instance-level mapping edges (SSSOM or SeMRA format).
    2. Expands entity IRIs to all known URI forms via bioregistry.
    3. Queries the LSLOD QLever endpoint for rdf:type classes of each entity.
    4. Aggregates instance evidence per (source_class, target_class) pair.
    5. Computes confidence and filters by threshold.
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
            "predicates_distribution, top_class_pairs."
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
