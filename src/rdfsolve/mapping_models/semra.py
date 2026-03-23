"""SemraMapping - SeMRA-derived mappings."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from rdfsolve.mapping_models.core import Mapping


class SemraMapping(Mapping):
    """Mapping imported from a SeMRA external source.

    Carries the semra source key (e.g. ``"biomappings"``) and,
    for per-prefix sources, the bioregistry prefix.
    """

    mapping_type: str = Field(default="semra_import")
    source_name: str = Field(
        ...,
        description=("SeMRA source key, e.g. 'biomappings'"),
    )
    source_prefix: str | None = Field(
        None,
        description=("Bioregistry prefix for per-prefix sources"),
    )
    evidence_chain: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Serialised semra evidence objects",
    )

    def to_jsonld(self) -> dict[str, Any]:
        """Extend base JSON-LD with SeMRA provenance."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["strategy"] = self.mapping_type
        about["semra_source"] = self.source_name
        if self.source_prefix:
            about["semra_prefix"] = self.source_prefix
        if self.evidence_chain:
            about["evidence"] = self.evidence_chain
        doc["@about"] = about
        return doc
