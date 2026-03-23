"""InferencedMapping - inference pipeline mappings."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from rdfsolve.mapping_models.core import Mapping


class InferencedMapping(Mapping):
    """Mapping produced by the rdfsolve/SeMRA inference pipeline.

    Carries the set of inference types applied, source mapping
    files, evidence chain, and optional aggregate stats.
    """

    mapping_type: str = Field(default="inferenced")
    inference_types: list[str] = Field(
        default_factory=list,
        description="Inference operations applied",
    )
    source_mapping_files: list[str] = Field(
        default_factory=list,
        description=("Paths to the input mapping JSON-LD files"),
    )
    evidence_chain: list[dict[str, Any]] = Field(
        default_factory=list,
        description=("Serialised semra evidence objects for inferred edges"),
    )
    stats: dict[str, Any] = Field(
        default_factory=dict,
        description=("Aggregate inference stats (edge counts, etc.)"),
    )

    def to_jsonld(self) -> dict[str, Any]:
        """Extend base JSON-LD with inference provenance."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["strategy"] = self.mapping_type
        about["inference_types"] = self.inference_types
        about["source_files"] = self.source_mapping_files
        if self.evidence_chain:
            about["evidence"] = self.evidence_chain
        if self.stats:
            about["stats"] = self.stats
        doc["@about"] = about
        return doc
