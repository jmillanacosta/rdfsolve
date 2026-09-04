"""Infrastructure metadata mining (DCAT/VoID)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rdfsolve.schema_models.metadata import (
    DatasetDescription,
    MetadataPatterns,
    ServiceDescription,
)

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


class MetadataMiner:
    """Extract infrastructure metadata patterns."""

    def __init__(
        self,
        helper: SparqlHelper,
        graph_uris: list[str] | None = None,
    ) -> None:
        self.helper = helper
        self.graph_uris = graph_uris

    def _graph_clause(self) -> tuple[str, str]:
        """Return (open, close) for GRAPH clause."""
        if not self.graph_uris:
            return "", ""
        if len(self.graph_uris) == 1:
            return f"GRAPH <{self.graph_uris[0]}> {{", "}"
        values = " ".join(f"(<{u}>)" for u in self.graph_uris)
        return f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{", "}"

    def mine(self) -> MetadataPatterns | None:
        """Extract metadata patterns."""
        logger.info("Mining infrastructure metadata")

        datasets = self.query_datasets()
        services = self.query_services()

        if not datasets and not services:
            logger.info("No infrastructure metadata found")
            return None

        logger.info(f"Found {len(datasets)} datasets, {len(services)} services")

        return MetadataPatterns(datasets=datasets, services=services)

    def query_datasets(self) -> list[DatasetDescription]:
        """Query void:Dataset descriptions."""
        g_open, g_close = self._graph_clause()
        query = f"""\
SELECT DISTINCT ?ds ?title ?desc ?homepage ?endpoint ?license
WHERE {{
  {g_open}
    ?ds a <http://rdfs.org/ns/void#Dataset> .
    OPTIONAL {{ ?ds <http://purl.org/dc/terms/title> ?title . }}
    OPTIONAL {{ ?ds <http://purl.org/dc/terms/description> ?desc . }}
    OPTIONAL {{ ?ds <http://xmlns.com/foaf/0.1/homepage> ?homepage . }}
    OPTIONAL {{ ?ds <http://rdfs.org/ns/void#sparqlEndpoint> ?endpoint . }}
    OPTIONAL {{ ?ds <http://purl.org/dc/terms/license> ?license . }}
  {g_close}
}}"""

        try:
            result = self.helper.select(query, purpose="metadata/datasets")
            bindings = result.get("results", {}).get("bindings", [])
            datasets = []
            for row in bindings:
                ds_uri = row.get("ds", {}).get("value")
                if ds_uri:
                    datasets.append(
                        DatasetDescription(
                            uri=ds_uri,
                            title=row.get("title", {}).get("value"),
                            description=row.get("desc", {}).get("value"),
                            homepage=row.get("homepage", {}).get("value"),
                            sparql_endpoint=row.get("endpoint", {}).get("value"),
                            license=row.get("license", {}).get("value"),
                        )
                    )
            return datasets
        except Exception as e:
            logger.warning(f"Failed to query datasets: {e}")
            return []

    def query_services(self) -> list[ServiceDescription]:
        """Query SPARQL service descriptions."""
        g_open, g_close = self._graph_clause()
        query = f"""\
SELECT DISTINCT ?endpoint
WHERE {{
  {g_open}
    ?service a <http://www.w3.org/ns/sparql-service-description#Service> .
    ?service <http://www.w3.org/ns/sparql-service-description#endpoint> ?endpoint .
  {g_close}
}}"""

        try:
            result = self.helper.select(query, purpose="metadata/services")
            bindings = result.get("results", {}).get("bindings", [])
            return [
                ServiceDescription(
                    endpoint=row.get("endpoint", {}).get("value", ""),
                    supported_language=[],
                    result_format=[],
                    feature=[],
                )
                for row in bindings
                if row.get("endpoint", {}).get("value")
            ]
        except Exception as e:
            logger.warning(f"Failed to query services: {e}")
            return []


__all__ = ["MetadataMiner"]
