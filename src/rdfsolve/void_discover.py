"""VoID parser for converting RDF graphs to schemas and discovering VoID catalogs from SPARQL endpoints."""

import logging
from hashlib import md5
from typing import Any

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
        self, endpoint_url: str, *, timeout: float = 30.0, max_retries: int = 1
    ) -> dict[str, Any]:
        """Discover VoID graphs at *endpoint_url* via a SELECT query.

        Queries for VoID partitions across all named graphs.  Returns a
        dict describing found graphs and their raw partition records,
        which can be passed directly to
        :meth:`build_void_graph_from_partitions`.

        Args:
            endpoint_url: SPARQL endpoint URL.

        Returns:
            Dict with keys ``has_void_descriptions``, ``found_graphs``,
            ``total_graphs``, ``void_content``, ``partitions``.
            Query failures raise; they are not empty results.
        """
        from rdfsolve.sparql_helper import SparqlHelper

        query = """
        PREFIX void: <http://rdfs.org/ns/void#>
        PREFIX void-ext: <http://ldf.fi/void-ext#>
        SELECT DISTINCT ?subjectClass ?prop ?objectClass ?objectDatatype ?g
        WHERE {
          GRAPH ?g {
            {
              ?cp void:class ?subjectClass ;
                  void:propertyPartition ?pp .
              ?pp void:property ?prop .
              OPTIONAL {
                {
                  ?pp void:classPartition [ void:class ?objectClass ] .
                } UNION {
                  ?pp void-ext:datatypePartition
                      [ void-ext:datatype ?objectDatatype ] .
                }
              }
            } UNION {
              ?ls void:subjectsTarget [ void:class ?subjectClass ] ;
                  void:linkPredicate ?prop ;
                  void:objectsTarget [ void:class ?objectClass ] .
            }
          }
        }
        """
        if self.graph_uris == []:
            raise ValueError("Use graph_uris=None for all named graphs, or select at least one graph")
        if self.graph_uris is not None:
            values = " ".join(URIRef(iri).n3() for iri in self.graph_uris)
            query = query.replace("GRAPH ?g {", "VALUES ?g { " + values + " } GRAPH ?g {", 1)
        if self.exclude_graphs:
            from rdflib import Literal

            from rdfsolve.schema_models._constants import SERVICE_NAMESPACE_PREFIXES

            filters = " ".join(
                f"FILTER(!STRSTARTS(STR(?g), {Literal(prefix).n3()}))"
                for prefix in SERVICE_NAMESPACE_PREFIXES
            )
            query = query.replace("GRAPH ?g {", filters + " GRAPH ?g {", 1)
        logger.info(
            "Querying VoID partitions at %s; graph scope: %s",
            endpoint_url,
            self.graph_uris if self.graph_uris is not None else "all named graphs",
        )
        try:
            helper = SparqlHelper(endpoint_url, timeout=timeout, max_retries=max_retries)
            results = helper.select(query, purpose="void/partition-discovery")

            found_graphs: list[str] = []
            void_content: dict[str, dict[str, Any]] = {}
            partitions: list[dict[str, str]] = []

            for row in results["results"]["bindings"]:
                g = row.get("g", {}).get("value")
                if not g:
                    continue
                if g not in void_content:
                    void_content[g] = {
                        "partition_count": 0,
                        "has_any_partitions": True,
                    }
                    found_graphs.append(g)
                void_content[g]["partition_count"] += 1

                p: dict[str, str] = {
                    "graph": g,
                    "subjectClass": row.get("subjectClass", {}).get("value", ""),
                    "prop": row.get("prop", {}).get("value", ""),
                }
                if row.get("objectClass", {}).get("value"):
                    p["objectClass"] = row["objectClass"]["value"]
                if row.get("objectDatatype", {}).get("value"):
                    p["objectDatatype"] = row["objectDatatype"]["value"]
                partitions.append(p)

            logger.info(
                "Found %d VoID partition records in %d graphs", len(partitions), len(found_graphs)
            )
            return {
                "has_void_descriptions": bool(found_graphs),
                "found_graphs": found_graphs,
                "total_graphs": len(found_graphs),
                "void_content": void_content,
                "partitions": partitions,
            }
        except Exception as exc:
            logger.warning("VoID discovery failed at %s: %s", endpoint_url, exc)
            raise

    def discover_all_graphs(self, endpoint_url: str) -> dict[str, Any]:
        """Discover all named graphs at endpoint with triple counts.

        Executes: SELECT ?g (COUNT(*) AS ?count) WHERE {GRAPH ?g {?s ?p ?o}}
        GROUP BY ?g ORDER BY DESC(?count)

        Args:
            endpoint_url: SPARQL endpoint URL.

        Returns:
            Dict with keys:
            - ``graphs``: list of dicts with 'uri' and 'count' keys
            - ``total_graphs``: number of graphs found
            - ``ontology_graphs``: list of graph URIs with .owl extension
            - ``void_graphs``: list of graph URIs containing 'void' in name
            - ``error``: error message (only present on failure)
        """
        from rdfsolve.sparql_helper import SparqlHelper

        query = """
        SELECT ?g (COUNT(*) AS ?count)
        WHERE {
          GRAPH ?g { ?s ?p ?o }
        }
        GROUP BY ?g
        ORDER BY DESC(?count)
        """

        try:
            helper = SparqlHelper(endpoint_url)
            results = helper.select(query, purpose="graph-discovery")

            graphs = []
            ontology_graphs = []
            void_graphs = []

            for row in results["results"]["bindings"]:
                g_uri = row.get("g", {}).get("value")
                count = int(row.get("count", {}).get("value", 0))

                if not g_uri:
                    continue

                graphs.append({"uri": g_uri, "count": count})

                # Identify ontology graphs (.owl extension)
                if g_uri.endswith(".owl") or ".owl/" in g_uri or ".owl#" in g_uri:
                    ontology_graphs.append(g_uri)
                    logger.info(f"Found ontology graph: {g_uri} ({count} triples)")

                # Identify potential metadata graphs (containing 'void')
                if "void" in g_uri.lower():
                    void_graphs.append(g_uri)
                    logger.info(f"Found VoID metadata graph: {g_uri} ({count} triples)")

            return {
                "graphs": graphs,
                "total_graphs": len(graphs),
                "ontology_graphs": ontology_graphs,
                "void_graphs": void_graphs,
            }
        except Exception as exc:
            logger.warning(f"Graph discovery failed: {exc}")
            return {
                "graphs": [],
                "total_graphs": 0,
                "ontology_graphs": [],
                "void_graphs": [],
                "error": str(exc),
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

    def build_void_graph_from_partitions(
        self,
        partitions: list[dict[str, str]],
        base_uri: str | None = None,
    ) -> Graph:
        """Convert raw partition records into an RDF VoID graph.

        Partition records are the dicts produced by
        :meth:`discover_void_graphs` (keys: ``subjectClass``, ``prop``,
        optionally ``objectClass`` / ``objectDatatype``).

        Args:
            partitions: Partition records from :meth:`discover_void_graphs`.
            base_uri: Base URI for generated blank-node-replacement IRIs.

        Returns:
            RDF :class:`~rdflib.Graph` with VoID partition triples.
        """
        VOID = "http://rdfs.org/ns/void#"
        VOID_EXT = "http://ldf.fi/void-ext#"
        base = base_uri or "urn:void:partition:"

        void_graph = Graph()
        void_graph.bind("void", URIRef(VOID))
        void_graph.bind("void-ext", URIRef(VOID_EXT))

        class_partitions: dict[str, URIRef] = {}

        for part in partitions:
            sc = part.get("subjectClass", "")
            prop = part.get("prop", "")
            if not sc or not prop:
                continue

            if sc not in class_partitions:
                cp_uri = URIRef(
                    f"{base}class_{md5(sc.encode(), usedforsecurity=False).hexdigest()[:12]}"
                )
                class_partitions[sc] = cp_uri
                void_graph.add((cp_uri, URIRef(f"{VOID}class"), URIRef(sc)))

            cp_uri = class_partitions[sc]
            oc = part.get("objectClass", "")
            dt = part.get("objectDatatype", "")
            pp_key = f"{sc}_{prop}_{oc or dt}"
            pp_uri = URIRef(
                f"{base}prop_{md5(pp_key.encode(), usedforsecurity=False).hexdigest()[:12]}"
            )

            void_graph.add((cp_uri, URIRef(f"{VOID}propertyPartition"), pp_uri))
            void_graph.add((pp_uri, URIRef(f"{VOID}property"), URIRef(prop)))
            void_graph.add(
                (
                    pp_uri,
                    URIRef(f"{VOID_EXT}subjectClass"),
                    URIRef(sc),
                )
            )

            if oc:
                if oc not in class_partitions:
                    oc_uri = URIRef(
                        f"{base}class_{md5(oc.encode(), usedforsecurity=False).hexdigest()[:12]}"
                    )
                    class_partitions[oc] = oc_uri
                    void_graph.add((oc_uri, URIRef(f"{VOID}class"), URIRef(oc)))
                oc_uri = class_partitions[oc]
                void_graph.add(
                    (
                        pp_uri,
                        URIRef(f"{VOID}classPartition"),
                        oc_uri,
                    )
                )
                void_graph.add(
                    (
                        pp_uri,
                        URIRef(f"{VOID_EXT}objectClass"),
                        URIRef(oc),
                    )
                )
            elif dt:
                dt_uri = URIRef(
                    f"{base}dtype_{md5(dt.encode(), usedforsecurity=False).hexdigest()[:12]}"
                )
                void_graph.add(
                    (
                        pp_uri,
                        URIRef(f"{VOID_EXT}datatypePartition"),
                        dt_uri,
                    )
                )
                void_graph.add((dt_uri, URIRef(f"{VOID_EXT}datatype"), URIRef(dt)))

        logger.debug(
            "Built VoID graph: %d triples from %d partitions",
            len(void_graph),
            len(partitions),
        )
        return void_graph

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
