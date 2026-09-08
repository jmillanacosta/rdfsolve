"""Read metadata evidence for the optional pipeline phase."""

from __future__ import annotations

from rdfsolve.metadata import query_metadata_document
from rdfsolve.schema_models.metadata import MetadataDocument
from rdfsolve.sparql_helper import SparqlHelper


class MetadataMiner:
    """Retrieve descriptions without rebuilding them as VoID."""

    def __init__(self, helper: SparqlHelper, graph_uris: list[str] | None = None) -> None:
        """Use the caller's helper and graph scope."""
        self.helper = helper
        self.graph_uris = graph_uris

    def mine(self) -> MetadataDocument:
        """Return evidence, including an empty graph when no roots match."""
        return query_metadata_document(self.helper, graph_uris=self.graph_uris)
