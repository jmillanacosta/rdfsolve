"""SsomMapping - SSSOM-derived mappings."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from rdfsolve.mapping_models.core import Mapping


class SsomMapping(Mapping):
    """Mapping imported from an SSSOM source.

    Each instance corresponds to one ``.sssom.tsv`` file extracted
    from an SSSOM bundle (e.g. the EBI OLS SSSOM archive).
    """

    mapping_type: str = Field(default="sssom_import")
    source_name: str = Field(
        ...,
        description="Name of the SSSOM source bundle",
    )
    sssom_file: str = Field(
        ...,
        description=("Original filename of the .sssom.tsv file"),
    )
    mapping_set_id: str | None = Field(
        None,
        description=("SSSOM mapping_set_id from the file header (URI)"),
    )
    mapping_set_title: str | None = Field(
        None,
        description=("SSSOM mapping_set_title from the file header"),
    )
    license: str | None = Field(
        None,
        description=("License URI from the SSSOM file header"),
    )
    curie_map: dict[str, str] = Field(
        default_factory=dict,
        description=("CURIE prefix map from the SSSOM file header"),
    )

    def to_jsonld(self) -> dict[str, Any]:
        """Extend base JSON-LD with SSSOM provenance."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["strategy"] = self.mapping_type
        about["sssom_source"] = self.source_name
        about["sssom_file"] = self.sssom_file
        if self.mapping_set_id:
            about["mapping_set_id"] = self.mapping_set_id
        if self.mapping_set_title:
            about["mapping_set_title"] = self.mapping_set_title
        if self.license:
            about["license"] = self.license
        if self.curie_map:
            about["curie_map"] = self.curie_map
            doc["@context"].update(self.curie_map)
        doc["@about"] = about
        return doc
