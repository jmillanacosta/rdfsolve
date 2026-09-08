"""VoID parser for converting RDF graphs to schemas and discovering VoID catalogs from SPARQL endpoints."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rdfsolve.schema_models.metadata import MetadataDocument

import pandas as pd
from linkml_runtime.linkml_model import SchemaDefinition
from rdflib import Graph, URIRef

from rdfsolve.schema_models.core import MinedSchema

# Create logger with NullHandler by default , no output unless user configures
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class VoidParser:
    """Parser for VoID (Vocabulary of Interlinked Datasets) files."""

    def __init__(
        self,
        void_source: str | Graph | None = None,
        graph_uris: str | list[str] | None = None,
        exclude_graphs: bool = True,
    ):
        """Initialize the VoID parser.

        Args:
            void_source: File path (str) or RDF Graph object
            graph_uris: Graph URI(s) to analyze, or None for all non-system graphs
            exclude_graphs: Exclude Virtuoso system graphs
        """
        self.void_file_path: str | None = None
        self.graph: Graph = Graph()
        self.graph_uris = self._normalize_graph_uris(graph_uris)
        self.exclude_graphs = exclude_graphs
        self.exclude_graph_patterns: list[str] | None = None

        if void_source:
            if isinstance(void_source, str):
                self.void_file_path = void_source
                self._load_graph()
            elif isinstance(void_source, Graph):
                self.graph = void_source

    def _normalize_graph_uris(self, graph_uris: str | list[str] | None) -> list[str] | None:
        """Normalize graph URIs input to a list."""
        if graph_uris is None:
            return None
        elif isinstance(graph_uris, str):
            return [graph_uris]
        elif isinstance(graph_uris, list):
            return graph_uris
        else:
            raise ValueError("graph_uris must be str, list of str, or None")

    def _load_graph(self) -> None:
        """Load the VoID file into an RDF graph.

        Automatically detects format from file extension:
        - .ttl -> turtle
        - .jsonld, .json -> json-ld
        - .xml, .rdf -> xml
        - .nt -> ntriples
        """
        from pathlib import Path

        if not self.void_file_path:
            return

        # Detect format from extension
        path = Path(self.void_file_path)
        suffix = path.suffix.lower()

        format_map = {
            ".ttl": "turtle",
            ".jsonld": "json-ld",
            ".json": "json-ld",
            ".xml": "xml",
            ".rdf": "xml",
            ".nt": "ntriples",
            ".nq": "nquads",
        }

        fmt = format_map.get(suffix, "turtle")  # default to turtle
        self.graph.parse(self.void_file_path, format=fmt)

    def get_metadata(self) -> MetadataDocument:
        """Return retained RDF metadata without querying an endpoint."""
        from rdfsolve.schema_models.metadata import MetadataDocument

        return MetadataDocument(
            graph=self.graph + Graph(), graph_uris=self.graph_uris, scope="retained VoID RDF"
        )

    def to_mined_schema(self) -> MinedSchema:
        """Read the supported VoID profile through the canonical reader."""
        from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

        schema = void_graph_to_minedschema(self.graph)
        if self.graph_uris is not None:
            schema.about.graph_uris = self.graph_uris
        return schema

    def to_jsonld(
        self,
        endpoint_url: str | None = None,
        dataset_name: str | None = None,
        graph_uris: list[str] | None = None,
    ) -> dict[str, Any]:
        """Export VoID JSON-LD, not an adjacency dictionary."""
        schema = self.to_mined_schema()
        if endpoint_url is not None:
            schema.about.endpoint = endpoint_url
        if dataset_name is not None:
            schema.about.dataset_name = dataset_name
        if graph_uris is not None:
            schema.about.graph_uris = graph_uris
        return schema.to_jsonld()

    def to_schema(self) -> pd.DataFrame:
        """Return canonical pattern fields as a table."""
        return pd.DataFrame([pattern.model_dump() for pattern in self.to_mined_schema().patterns])

    def to_rdfconfig(
        self,
        endpoint_url: str | None = None,
        endpoint_name: str | None = None,
        graph_uri: str | None = None,
    ) -> dict[str, str]:
        """Export RDF-config from the parsed schema."""
        return self.to_mined_schema().to_rdfconfig(
            endpoint_url=endpoint_url,
            endpoint_name=endpoint_name,
            graph_uri=graph_uri,
        )

    # VoID catalog discovery

    def discover_void_graphs(
        self,
        endpoint_url: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 1,
        batch_size: int = 100,
        graph_batch_size: int = 8,
        max_pages: int = 1000,
    ) -> dict[str, Any]:
        """Retrieve published VoID and parse it through the canonical reader.

        Named graphs containing 'void' are candidates, not proof of VoID.
        Inspect every named graph and the default graph; retain their boundaries.
        Explicit graph scope never expands. Failures raise.
        """
        from rdfsolve.schema_models._constants import SERVICE_NAMESPACE_PREFIXES
        from rdfsolve.sparql_helper import SparqlHelper
        from rdfsolve.void_retrieval import discover_description

        with SparqlHelper(endpoint_url, timeout=timeout, max_retries=max_retries) as helper:
            dataset, found, default_graph = discover_description(
                helper,
                self.graph_uris,
                batch_size=batch_size,
                graph_batch_size=graph_batch_size,
                max_pages=max_pages,
                excluded_prefixes=tuple(SERVICE_NAMESPACE_PREFIXES) if self.exclude_graphs else (),
            )
        graph = Graph()
        for context in dataset.contexts():
            graph += context
        self.graph = graph
        schema = self.to_mined_schema()
        partitions = [
            {
                "subjectClass": p.subject_class,
                "prop": p.property_uri,
                "objectClass": p.object_class,
                "objectDatatype": p.datatype,
                "count": p.count,
            }
            for p in schema.patterns
        ]
        logger.info("Retrieved %d VoID triples and %d patterns", len(graph), len(partitions))
        return {
            "has_void_descriptions": bool(len(graph)),
            "found_graphs": found,
            "total_graphs": len(found),
            "default_graph": default_graph,
            "partitions": partitions,
            "graph": graph,
            "rdf_dataset": dataset,
            "schema": schema,
            "state": "complete" if len(graph) else "empty",
        }

    def discover_all_graphs(
        self,
        endpoint_url: str,
        *,
        include_counts: bool = False,
        timeout: float = 30.0,
        max_retries: int = 1,
        batch_size: int = 100,
        max_pages: int = 1000,
    ) -> dict[str, Any]:
        """List named graphs. Counts are optional and can be expensive."""
        from rdfsolve.sparql_helper import SparqlHelper
        from rdfsolve.void_retrieval import discover_graph_names

        with SparqlHelper(endpoint_url, timeout=timeout, max_retries=max_retries) as helper:
            names = discover_graph_names(helper, batch_size=batch_size, max_pages=max_pages)
            graphs: list[dict[str, Any]] = [{"uri": uri, "count": None} for uri in names]
            if include_counts:
                for graph in graphs:
                    query = (
                        "SELECT (COUNT(*) AS ?count) WHERE { GRAPH "
                        + URIRef(graph["uri"]).n3()
                        + " { ?s ?p ?o } }"
                    )
                    rows = helper.select(query, purpose="graph/count")["results"]["bindings"]
                    graph["count"] = int(rows[0]["count"]["value"])
        return {
            "graphs": graphs,
            "total_graphs": len(graphs),
            "ontology_graphs": [],
            "void_graphs": [uri for uri in names if "void" in uri.lower()],
        }

    def extract_metadata_from_void_graphs(
        self, endpoint_url: str, void_graph_uris: list[str]
    ) -> dict[str, Any]:
        """Extract metadata from graphs identified as containing VoID descriptions.

        Queries all triples from the specified VoID metadata graphs to extract
        dataset metadata like title, description, license, publisher, etc.

        Args:
            endpoint_url: SPARQL endpoint URL.
            void_graph_uris: List of graph URIs identified as containing metadata.

        Returns:
            Dict with keys:
            - ``metadata_by_graph``: dict mapping graph URI to extracted metadata
            - ``total_triples``: total triples extracted from all metadata graphs
            - ``error``: error message (only present on failure)
        """
        from rdfsolve.sparql_helper import SparqlHelper

        if not void_graph_uris:
            return {
                "metadata_by_graph": {},
                "total_triples": 0,
            }

        metadata_by_graph = {}
        total_triples = 0

        try:
            helper = SparqlHelper(endpoint_url)

            for graph_uri in void_graph_uris:
                # Query all triples from this metadata graph
                # Try one-shot first (no LIMIT), fall back to pagination if needed
                query = f"""
                SELECT ?s ?p ?o
                WHERE {{
                  GRAPH <{graph_uri}> {{
                    ?s ?p ?o
                  }}
                }}
                """

                try:
                    # SparqlHelper will automatically handle pagination if one-shot fails
                    results = helper.select(query, purpose=f"metadata-extraction/{graph_uri}")
                    triples = results["results"]["bindings"]

                    metadata_by_graph[graph_uri] = {
                        "triple_count": len(triples),
                        "triples": triples,
                    }
                    total_triples += len(triples)

                    logger.info(
                        f"Extracted {len(triples)} triples from metadata graph: {graph_uri}"
                    )
                except Exception as exc:
                    logger.warning(f"Failed to extract metadata from {graph_uri}: {exc}")
                    metadata_by_graph[graph_uri] = {
                        "triple_count": 0,
                        "triples": [],
                        "error": str(exc),
                    }

            return {
                "metadata_by_graph": metadata_by_graph,
                "total_triples": total_triples,
            }
        except Exception as exc:
            logger.error(f"Metadata extraction failed: {exc}")
            return {
                "metadata_by_graph": {},
                "total_triples": 0,
                "error": str(exc),
            }

    def to_linkml(
        self,
        schema_name: str | None = None,
        schema_description: str | None = None,
        schema_base_uri: str | None = None,
    ) -> SchemaDefinition:
        """Generate LinkML SchemaDefinition from VoID triples.

        Args:
            schema_name: Name for the LinkML schema (used as prefix)
            schema_description: Human-readable description
            schema_base_uri: Base URI for the schema

        Returns:
            LinkML SchemaDefinition object
        """
        from rdfsolve.schema_models.exporters.linkml import to_linkml
        from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

        schema = void_graph_to_minedschema(self.graph)
        return to_linkml(
            schema,
            schema_name=schema_name,
            schema_description=schema_description,
            schema_base_uri=schema_base_uri,
        )

    def to_linkml_yaml(
        self,
        schema_name: str | None = None,
        schema_description: str | None = None,
        schema_base_uri: str | None = None,
    ) -> str:
        """Generate LinkML YAML schema from VoID triples.

        Args:
            schema_name: Name for the LinkML schema
            schema_description: Human-readable description
            schema_base_uri: Base URI for the schema

        Returns:
            LinkML schema as YAML string
        """
        from rdfsolve.schema_models.exporters.linkml import to_linkml_yaml
        from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

        schema = void_graph_to_minedschema(self.graph)
        return to_linkml_yaml(
            schema,
            schema_name=schema_name,
            schema_description=schema_description,
            schema_base_uri=schema_base_uri,
        )

    def to_shacl(
        self,
        schema_base_uri: str = "http://example.org/shapes/",
    ) -> str:
        """Generate SHACL shapes (Turtle) from VoID triples.

        SHACL shapes define constraints on RDF data and can be used
        for validation.

        Args:
            schema_base_uri: Base URI for the SHACL shapes (default: http://example.org/shapes/)

        Returns:
            SHACL shapes as Turtle string
        """
        mined_schema = self.to_mined_schema()
        result: str = mined_schema.to_shacl(base_uri=schema_base_uri)
        return result
