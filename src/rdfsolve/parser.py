"""
VoID (Vocabulary of Interlinked Datasets) Parser.

This module provides functionality to parse VoID descriptions and extract
the underlying schema structure of RDF datasets. It can also generate VoID
descriptions from SPARQL endpoints using CONSTRUCT queries.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Union, cast

import pandas as pd
from bioregistry import curie_from_iri
from linkml.generators.shaclgen import ShaclGenerator
from linkml.generators.yamlgen import YAMLGenerator
from linkml_runtime.linkml_model import (
    ClassDefinition,
    SchemaDefinition,
    SlotDefinition,
    TypeDefinition,
)
from rdflib import Graph, Literal, URIRef

from rdfsolve.sparql_helper import SparqlHelper

# Create logger with NullHandler by default , no output unless user configures
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class VoidParser:
    """Parser for VoID (Vocabulary of Interlinked Datasets) files."""

    def __init__(
        self,
        void_source: Optional[Union[str, Graph]] = None,
        graph_uris: Optional[Union[str, List[str]]] = None,
        exclude_graphs: bool = True,
    ):
        """Initialize the VoID parser.

        Args:
            void_source: File path (str) or RDF Graph object
            graph_uris: Graph URI(s) to analyze, or None for all non-system graphs
            exclude_graphs: Exclude Virtuoso system graphs
        """
        self.void_file_path: Optional[str] = None
        self.graph: Graph = Graph()
        self.schema_triples: List[Any] = []
        self.classes: Dict[str, Any] = {}
        self.properties: Dict[str, Any] = {}
        self.graph_uris = self._normalize_graph_uris(graph_uris)
        self.exclude_graphs = exclude_graphs
        self.exclude_graph_patterns: Optional[List[str]] = None

        # VoID namespace URIs
        self.void_class = URIRef("http://rdfs.org/ns/void#class")
        self.void_property = URIRef("http://rdfs.org/ns/void#property")
        self.void_propertyPartition = URIRef("http://rdfs.org/ns/void#propertyPartition")
        self.void_classPartition = URIRef("http://rdfs.org/ns/void#classPartition")
        self.void_datatypePartition = URIRef("http://ldf.fi/void-ext#datatypePartition")

        # Bind common namespace prefixes
        self.void_ns = "http://rdfs.org/ns/void#"
        self.void_ext_ns = "http://ldf.fi/void-ext#"
        # Extended VoID properties for schema
        self.void_subjectClass = URIRef("http://ldf.fi/void-ext#subjectClass")
        self.void_objectClass = URIRef("http://ldf.fi/void-ext#objectClass")

        if void_source:
            if isinstance(void_source, str):
                self.void_file_path = void_source
                self._load_graph()
            elif isinstance(void_source, Graph):
                self.graph = void_source

    def _normalize_graph_uris(
        self, graph_uris: Optional[Union[str, List[str]]]
    ) -> Optional[List[str]]:
        """Normalize graph URIs input to a list."""
        if graph_uris is None:
            return None
        elif isinstance(graph_uris, str):
            return [graph_uris]
        elif isinstance(graph_uris, list):
            return graph_uris
        else:
            raise ValueError("graph_uris must be str, list of str, or None")

    def _generate_graph_clause(self, graph_uris: Optional[List[str]] = None) -> str:
        """
        Generate appropriate GRAPH clause for SPARQL queries.

        Args:
            graph_uris: List of specific graph URIs. If None, instance URIs

        Returns:
            String containing the graph clause or filter clause
        """
        # Use instance graph_uris if none provided
        if graph_uris is None:
            graph_uris = self.graph_uris

        # If no specific graphs, query all graphs with optional filtering
        if not graph_uris:
            if self.exclude_graphs:
                return """
                GRAPH ?g {
                    # Query content will be here
                }
                # Filter out Virtuoso system graphs and others
                FILTER(!REGEX(STR(?g), "http://www.openlinksw.com/"))
                FILTER(!REGEX(STR(?g), "well-known"))
                FILTER(!REGEX(STR(?g), "virtuoso"))
                FILTER(?g != <http://www.w3.org/2002/07/owl#>)"""
            else:
                return "GRAPH ?g { # Query content will be here }"

        # Single graph
        elif len(graph_uris) == 1:
            return f"GRAPH <{graph_uris[0]}> {{ # Query content will be here }}"

        # Multiple graphs using VALUES clause for efficiency
        else:
            values_clause = " ".join([f"<{uri}>" for uri in graph_uris])
            return f"""
            VALUES ?g {{ {values_clause} }}
            GRAPH ?g {{ # Query content will be here }}"""

    def _replace_graph_clause_placeholder(
        self, query: str, graph_uris: Optional[List[str]] = None
    ) -> str:
        """
        Replace #GRAPH_CLAUSE placeholder with appropriate graph clause.

        Args:
            query: SPARQL query string with #GRAPH_CLAUSE placeholder
            graph_uris: Optional list of specific graph URIs

        Returns:
            Query with graph clause properly inserted
        """
        # Use instance graph_uris if none provided
        if graph_uris is None:
            graph_uris = self.graph_uris

        logger.debug(f"Replacing graph clause, graph_uris={graph_uris}")
        logger.debug(f"Query has #GRAPH_CLAUSE: {'#GRAPH_CLAUSE' in query}")

        # Replace graph clause markers
        if not graph_uris:
            # No specific graphs - query default graph (remove GRAPH clause, keep structure)
            # Just remove the placeholder markers, keeping the content and braces
            result = query.replace("#GRAPH_CLAUSE", "")
            result = result.replace("#END_GRAPH_CLAUSE", "")
            logger.debug("Removed markers for default graph")
        elif len(graph_uris) == 1:
            # Single specific graph
            graph_clause = f"GRAPH <{graph_uris[0]}> {{"
            result = query.replace("#GRAPH_CLAUSE", graph_clause)
            result = result.replace("#END_GRAPH_CLAUSE", "}")
            logger.debug(f"Single graph: {graph_clause}")
        else:
            # Multiple specific graphs - use VALUES clause
            values_clause = " ".join([f"<{uri}>" for uri in graph_uris])
            graph_clause = f"VALUES ?g {{ {values_clause} }}\n            GRAPH ?g {{"
            result = query.replace("#GRAPH_CLAUSE", graph_clause)
            result = result.replace("#END_GRAPH_CLAUSE", "}")
            logger.debug("Multiple graphs, VALUES clause")

        logger.debug(f"After replacement has #GRAPH_CLAUSE: {'#GRAPH_CLAUSE' in result}")

        return result

    def _load_graph(self) -> None:
        """Load the VoID file into an RDF graph."""
        self.graph.parse(self.void_file_path, format="turtle")

    def discover_void_graphs(self, endpoint_url: str) -> Dict[str, Any]:
        """
        Discover existing VoID graphs in the endpoint by querying for VoID partitions.

        Query pattern inspired by: https://github.com/sib-swiss/sparql-editor/

        Uses SparqlHelper for automatic GET-->POST fallback on endpoints that
        require POST. The SELECT query returns partition data
        directly, which is then converted to an RDF graph, no additional
        CONSTRUCT query needed.

        Args:
            endpoint_url: SPARQL endpoint URL

        Returns:
            Dictionary with discovery results including found graphs and partition data
        """
        # Query directly for VoID partitions - ?g tells us which graphs have them
        partition_discovery_query = """
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
                      ?pp  void:classPartition [ void:class ?objectClass ] .
                  } UNION {
                      ?pp void-ext:datatypePartition [ void-ext:datatype ?objectDatatype ] .
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

        try:
            logger.debug(f"Starting VoID partition discovery for {endpoint_url}")
            logger.info("Discovering VoID partitions across all graphs")

            # Use SparqlHelper - automatic GET-->POST fallback
            helper = SparqlHelper(endpoint_url)
            results = helper.select(partition_discovery_query, purpose="void/partition-discovery")

            # Group results by graph URI and collect partition data
            found_graphs: List[str] = []
            void_content: Dict[str, Dict[str, Any]] = {}
            partitions: List[Dict[str, str]] = []  # Store partition data for graph building

            for result in results["results"]["bindings"]:
                graph_uri = result.get("g", {}).get("value")
                if not graph_uri:
                    continue

                # Track unique graphs and count partitions
                if graph_uri not in void_content:
                    void_content[graph_uri] = {
                        "partition_count": 0,
                        "has_any_partitions": True,
                    }
                    found_graphs.append(graph_uri)
                    logger.info("Found VoID graph: %s", graph_uri)

                void_content[graph_uri]["partition_count"] += 1

                # Collect partition data for graph building
                partition: Dict[str, str] = {
                    "graph": graph_uri,
                    "subjectClass": result.get("subjectClass", {}).get("value", ""),
                    "prop": result.get("prop", {}).get("value", ""),
                }
                if result.get("objectClass", {}).get("value"):
                    partition["objectClass"] = result["objectClass"]["value"]
                if result.get("objectDatatype", {}).get("value"):
                    partition["objectDatatype"] = result["objectDatatype"]["value"]

                partitions.append(partition)

            logger.debug(
                f"Discovery complete: found {len(found_graphs)} graphs with VoID partitions"
            )
            logger.debug(f"Collected {len(partitions)} partition records")

            for graph_uri in found_graphs:
                logger.debug(
                    f"   • {graph_uri}: {void_content[graph_uri]['partition_count']} partitions"
                )

            return {
                "has_void_descriptions": len(found_graphs) > 0,
                "found_graphs": found_graphs,
                "total_graphs": len(found_graphs),
                "void_content": void_content,
                "partitions": partitions,  # Include partition data for direct graph building
            }

        except Exception as e:
            logger.info(f"VoID discovery failed: {e}")
            logger.debug(f"Discovery exception: {type(e).__name__}: {e!s}")
            return {
                "has_void_descriptions": False,
                "found_graphs": [],
                "total_graphs": 0,
                "void_content": {},
                "partitions": [],
                "error": str(e),
            }

    def build_void_graph_from_partitions(
        self, partitions: List[Dict[str, str]], base_uri: Optional[str] = None
    ) -> Graph:
        """
        Build an RDF graph from partition data (from SELECT query results).

        This converts the partition records directly to VoID RDF triples,
        avoiding the need for an additional CONSTRUCT query.

        Args:
            partitions: List of partition dicts with subjectClass, prop, objectClass/objectDatatype
            base_uri: Base URI for generating partition node IRIs

        Returns:
            RDF Graph containing VoID partition descriptions
        """
        from hashlib import md5

        void_graph = Graph()

        # Bind common prefixes
        void_graph.bind("void", URIRef("http://rdfs.org/ns/void#"))
        void_graph.bind("void-ext", URIRef("http://ldf.fi/void-ext#"))
        void_graph.bind("rdf", URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))

        # VoID namespace URIs
        VOID = "http://rdfs.org/ns/void#"
        VOID_EXT = "http://ldf.fi/void-ext#"

        if not base_uri:
            base_uri = "urn:void:partition:"

        logger.debug(f"Building VoID graph from {len(partitions)} partition records")

        # Track class partitions we've created
        class_partitions: Dict[str, URIRef] = {}

        for partition in partitions:
            subject_class = partition.get("subjectClass", "")
            prop = partition.get("prop", "")
            object_class = partition.get("objectClass", "")
            object_datatype = partition.get("objectDatatype", "")

            if not subject_class or not prop:
                continue

            # Create or get class partition node for subject class
            if subject_class not in class_partitions:
                cp_id = md5(subject_class.encode()).hexdigest()[:12]
                cp_uri = URIRef(f"{base_uri}class_{cp_id}")
                class_partitions[subject_class] = cp_uri

                # Add class partition triple
                void_graph.add((cp_uri, URIRef(f"{VOID}class"), URIRef(subject_class)))

            cp_uri = class_partitions[subject_class]

            # Create property partition node
            pp_id = md5(
                f"{subject_class}_{prop}_{object_class or object_datatype}".encode()
            ).hexdigest()[:12]
            pp_uri = URIRef(f"{base_uri}prop_{pp_id}")

            # Link class partition to property partition
            void_graph.add((cp_uri, URIRef(f"{VOID}propertyPartition"), pp_uri))

            # Add property triple
            void_graph.add((pp_uri, URIRef(f"{VOID}property"), URIRef(prop)))

            # Add subject class info using void-ext
            void_graph.add((pp_uri, URIRef(f"{VOID_EXT}subjectClass"), URIRef(subject_class)))

            # Add object class or datatype
            if object_class:
                # Create class partition for object if not exists
                if object_class not in class_partitions:
                    oc_id = md5(object_class.encode()).hexdigest()[:12]
                    oc_uri = URIRef(f"{base_uri}class_{oc_id}")
                    class_partitions[object_class] = oc_uri
                    void_graph.add((oc_uri, URIRef(f"{VOID}class"), URIRef(object_class)))

                oc_uri = class_partitions[object_class]
                void_graph.add((pp_uri, URIRef(f"{VOID}classPartition"), oc_uri))
                void_graph.add((pp_uri, URIRef(f"{VOID_EXT}objectClass"), URIRef(object_class)))

            elif object_datatype:
                # Create datatype partition node
                dt_id = md5(object_datatype.encode()).hexdigest()[:12]
                dt_uri = URIRef(f"{base_uri}dtype_{dt_id}")
                void_graph.add((pp_uri, URIRef(f"{VOID_EXT}datatypePartition"), dt_uri))
                void_graph.add((dt_uri, URIRef(f"{VOID_EXT}datatype"), URIRef(object_datatype)))

        logger.info(f"Built VoID graph with {len(void_graph)} triples from partition data")

        # Enrich with bioregistry prefixes before returning
        void_graph = VoidParser._enrich_void_with_bioregistry_prefixes(void_graph)

        return void_graph

    def retrieve_partitions_from_void(
        self, endpoint_url: str, graph_uris: List[str]
    ) -> List[Dict[str, str]]:
        """
        Retrieve partition data (class-property-datatype triples) from VoID graphs.

        Queries to extract partition information.
        This is called AFTER discovering that VoID graphs exist and have partitions.
        Uses SparqlHelper for automatic GET-->POST fallback.

        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uris: List of graph URIs containing VoID partitions

        Returns:
            List of partition records with subject_class, property, object_class/object_datatype
        """
        logger.debug(f"Retrieving partitions from {len(graph_uris)} VoID graphs")
        all_partitions: List[Dict[str, str]] = []

        # Create one helper - it will learn POST requirement if needed
        helper = SparqlHelper(endpoint_url)

        for i, graph_uri in enumerate(graph_uris, 1):
            logger.debug(f"Querying graph {i}/{len(graph_uris)}: {graph_uri}")
            try:
                # Escape the graph URI properly for SPARQL
                escaped_uri = graph_uri.replace("\\", "\\\\").replace('"', '\\"')

                # Lightweight unified query for partition data
                partition_query = f"""
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX void-ext: <http://ldf.fi/void-ext#>
                SELECT DISTINCT ?subjectClass ?prop ?objectClass ?objectDatatype
                WHERE {{
                  GRAPH <{escaped_uri}> {{
                    {{
                      ?cp void:class ?subjectClass ;
                          void:propertyPartition ?pp .
                      ?pp void:property ?prop .
                      OPTIONAL {{
                          {{
                              ?pp  void:classPartition [ void:class ?objectClass ] .
                          }} UNION {{
                              ?pp void-ext:datatypePartition [ void-ext:datatype ?objectDatatype ] .
                          }}
                      }}
                    }} UNION {{
                      ?ls void:subjectsTarget [ void:class ?subjectClass ] ;
                          void:linkPredicate ?prop ;
                          void:objectsTarget [ void:class ?objectClass ] .
                    }}
                  }}
                }}
                """

                logger.debug("Executing partition query...")

                # SparqlHelper handles GET-->POST fallback automatically
                results = helper.select(partition_query, purpose="void/partition-detail")

                bindings = results.get("results", {}).get("bindings", [])
                logger.debug(f"Retrieved {len(bindings)} partition records from {graph_uri}")

                # Process each partition record
                for binding in bindings:
                    partition_record: Dict[str, str] = {
                        "subject_class": binding.get("subjectClass", {}).get("value", ""),
                        "property": binding.get("prop", {}).get("value", ""),
                    }

                    # Object is either a class or datatype
                    if "objectClass" in binding:
                        partition_record["object_class"] = binding["objectClass"]["value"]
                    elif "objectDatatype" in binding:
                        partition_record["object_datatype"] = binding["objectDatatype"]["value"]

                    all_partitions.append(partition_record)

            except Exception as e:
                logger.warning(f"Failed to retrieve partitions from {graph_uri}: {e}")
                logger.debug(f"Exception: {type(e).__name__}: {e!s}")
                continue

        logger.info(f"Retrieved {len(all_partitions)} total partition records")
        return all_partitions

    def void_querier(self, endpoint_url: str, graph_uris: List[str]) -> Graph:
        """
        Query existing VoID descriptions from specific graphs using CONSTRUCT.

        Uses SparqlHelper for automatic GET-->POST fallback on endpoints that
        require POST.

        Note: For most cases, prefer using build_void_graph_from_partitions()
        with partition data from discover_void_graphs() - it's more efficient
        as it avoids additional queries.

        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uris: List of graph URIs containing VoID descriptions

        Returns:
            RDF Graph containing the retrieved VoID descriptions
        """
        logger.debug(f"Starting VoID querier for {len(graph_uris)} graphs")
        merged_graph = Graph()

        # Create one helper - it will learn POST requirement if needed
        helper = SparqlHelper(endpoint_url)

        for i, graph_uri in enumerate(graph_uris, 1):
            logger.debug(f"Processing graph {i}/{len(graph_uris)}: {graph_uri}")
            try:
                # Query to retrieve all VoID content from the graph
                logger.debug("Building CONSTRUCT query for VoID data...")
                void_query = f"""
                CONSTRUCT {{
                    ?s ?p ?o
                }}
                WHERE {{
                    GRAPH <{graph_uri}> {{
                        ?s ?p ?o .
                        {{
                            ?s a <http://rdfs.org/ns/void#Dataset> .
                        }}
                        UNION
                        {{
                            ?s <http://rdfs.org/ns/void#class> ?class .
                        }}
                        UNION
                        {{
                            ?s <http://rdfs.org/ns/void#property> ?property .
                        }}
                        UNION
                        {{
                            ?s <http://ldf.fi/void-ext#datatypePartition> ?datatype .
                        }}
                        UNION
                        {{
                            ?s <http://ldf.fi/void-ext#subjectClass> ?subjectClass .
                        }}
                        UNION
                        {{
                            ?s <http://ldf.fi/void-ext#objectClass> ?objectClass .
                        }}
                    }}
                }}
                """

                logger.debug("Executing VoID CONSTRUCT query...")

                # SparqlHelper handles GET-->POST fallback automatically
                graph_data = helper.construct_graph(void_query)

                logger.debug("VoID CONSTRUCT query completed")

                if len(graph_data) > 0:
                    # Merge into combined graph
                    for triple in graph_data:
                        merged_graph.add(triple)
                    logger.info(f"Retrieved VoID from: {graph_uri}")
                    logger.debug(f"Added {len(graph_data)} triples from this graph")
                else:
                    logger.debug("Empty result from CONSTRUCT query")

            except Exception as e:
                logger.info(f"Failed to retrieve VoID from {graph_uri}: {e}")
                logger.debug(f"VoID retrieval exception: {type(e).__name__}")
                continue

        logger.debug("VoID querier complete")
        logger.debug(f"Merged graph size: {len(merged_graph)} triples")

        if len(merged_graph) > 0:
            logger.info(f"Successfully retrieved VoID data: {len(merged_graph)} triples")
        else:
            logger.warning(" No VoID triples retrieved from any graph")

        return merged_graph

    def _extract_classes(self) -> None:
        """Extract class information from VoID description."""
        self.classes = {}
        for s, _p, o in self.graph.triples((None, self.void_class, None)):
            self.classes[s] = o

    def _extract_properties(self) -> None:
        """Extract property information from VoID description."""
        self.properties = {}
        for s, _p, o in self.graph.triples((None, self.void_property, None)):
            self.properties[s] = o

    def _extract_schema_triples(self) -> None:
        """Extract schema triples by analyzing property partitions."""
        self.schema_triples = []

        # Try new ty extraction first (with subjectClass/objectClass)
        triples = self._extract_schema()
        if triples:
            self.schema_triples = triples
            return

    def _extract_schema(self) -> List[Any]:
        """Extract schema from property partitions with type info."""
        triples: List[Any] = []

        # Find all property partitions with subject/object class info
        for partition, _, property_uri in self.graph.triples((None, self.void_property, None)):
            # Get subject class
            subject_classes = list(self.graph.triples((partition, self.void_subjectClass, None)))
            # Get object class
            object_classes = list(self.graph.triples((partition, self.void_objectClass, None)))

            if subject_classes and object_classes:
                for _, _, subject_class in subject_classes:
                    for _, _, object_class in object_classes:
                        triples.append((subject_class, property_uri, object_class))
            elif subject_classes:
                # Check for datatype partitions (literal objects)
                datatype_partitions = list(
                    self.graph.triples((partition, self.void_datatypePartition, None))
                )
                if datatype_partitions:
                    for _, _, subject_class in subject_classes:
                        triples.append((subject_class, property_uri, "Literal"))
                else:
                    # No explicit datatype or object class - assume Resource
                    for _, _, subject_class in subject_classes:
                        triples.append((subject_class, property_uri, "Resource"))

        return triples

    def _filter_void_admin_nodes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out VoID-related triples."""
        mask = (
            ~df["subject_uri"].str.contains("void", case=False, na=False)
            & ~df["property_uri"].str.contains("void", case=False, na=False)
            & ~df["object_uri"].str.contains("void", case=False, na=False)
            & ~df["subject_uri"].str.contains("well-known", case=False, na=False)
            & ~df["property_uri"].str.contains("well-known", case=False, na=False)
            & ~df["object_uri"].str.contains("well-known", case=False, na=False)
            & ~df["subject_uri"].str.contains("openlink", case=False, na=False)
            & ~df["property_uri"].str.contains("openlink", case=False, na=False)
            & ~df["object_uri"].str.contains("openlink", case=False, na=False)
        )
        return df[mask].copy()

    def _extract_about_metadata(
        self,
        endpoint_url: Optional[str] = None,
        dataset_name: Optional[str] = None,
        graph_uris: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Extract metadata from the VoID graph for the @about section.

        Pulls metadata from the VoID graph (endpoint, title, graph URIs)
        and merges with any explicitly provided values.

        Args:
            endpoint_url: SPARQL endpoint URL (overrides graph value)
            dataset_name: Dataset name (overrides graph value)
            graph_uris: Graph URIs (overrides graph value)

        Returns:
            Dictionary with metadata for the @about section
        """
        from datetime import datetime, timezone

        from rdfsolve.version import VERSION

        about: Dict[str, Any] = {
            "generatedBy": f"rdfsolve {VERSION}",
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }

        # Try to extract metadata from the VoID graph
        void_dataset_type = URIRef("http://rdfs.org/ns/void#Dataset")
        void_sparql_endpoint = URIRef("http://rdfs.org/ns/void#sparqlEndpoint")
        dcterms_title = URIRef("http://purl.org/dc/terms/title")

        graph_endpoint = None
        graph_title = None
        graph_graph_uris: List[str] = []

        for s, p, o in self.graph:
            if p == URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") and o == void_dataset_type:
                # Found a void:Dataset — extract its properties
                for _, pred, obj in self.graph.triples((s, None, None)):
                    if pred == void_sparql_endpoint:
                        graph_endpoint = str(obj)
                    elif pred == dcterms_title:
                        graph_title = str(obj)

        # Collect graph URIs from the parser
        if self.graph_uris:
            graph_graph_uris = list(self.graph_uris)

        # Use explicit values, fall back to graph values
        if endpoint_url:
            about["endpoint"] = endpoint_url
        elif graph_endpoint:
            about["endpoint"] = graph_endpoint

        if dataset_name:
            about["datasetName"] = dataset_name
        elif graph_title:
            about["datasetName"] = graph_title

        effective_graph_uris = graph_uris if graph_uris else graph_graph_uris
        if effective_graph_uris:
            about["graphURIs"] = effective_graph_uris

        if self.void_file_path:
            about["voidFile"] = self.void_file_path

        about["tripleCount"] = len(self.schema_triples) if self.schema_triples else 0

        return about

    def to_jsonld(
        self,
        filter_void_admin_nodes: bool = True,
        endpoint_url: Optional[str] = None,
        dataset_name: Optional[str] = None,
        graph_uris: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Parse VoID file and return simple JSON-LD with the schema triples.

        Args:
            filter_void_admin_nodes: Whether to filter out VoID-specific nodes
            endpoint_url: SPARQL endpoint URL for the @about section
            dataset_name: Dataset name for the @about section
            graph_uris: Graph URIs for the @about section

        Returns:
            Simple JSON-LD with @context, @graph, and @about sections
        """
        # Extract schema triples
        self._extract_schema_triples()

        if not self.schema_triples:
            about = self._extract_about_metadata(
                endpoint_url=endpoint_url,
                dataset_name=dataset_name,
                graph_uris=graph_uris,
            )
            return {"@context": {}, "@graph": [], "@about": about}

        # Create minimal context for the namespaces we find
        context: Dict[str, str] = {}
        triples: List[Dict[str, Any]] = []

        for s, p, o in self.schema_triples:
            # Convert to CURIEs and collect namespaces
            s_curie, s_prefix, s_namespace = self._get_curie_and_namespace(str(s))
            p_curie, p_prefix, p_namespace = self._get_curie_and_namespace(str(p))

            # Add prefixes to context
            if s_prefix and s_namespace:
                context[s_prefix] = s_namespace
            if p_prefix and p_namespace:
                context[p_prefix] = p_namespace

            # Handle object
            o_value: Union[str, Dict[str, str]]
            if isinstance(o, Literal):
                # It's a literal value
                if o.datatype:
                    o_value = {"@value": str(o), "@type": str(o.datatype)}
                else:
                    o_value = str(o)
            else:
                # It's a URI/Resource
                o_curie, o_prefix, o_namespace = self._get_curie_and_namespace(str(o))
                if o_prefix and o_namespace:
                    context[o_prefix] = o_namespace
                o_value = {"@id": o_curie if o_curie else str(o)}

            # Create simple triple as JSON-LD
            triple = {
                "@id": s_curie if s_curie else str(s),
                p_curie if p_curie else str(p): o_value,
            }
            triples.append(triple)

        # Group triples by subject
        grouped: Dict[str, Dict[str, Any]] = {}
        for triple in triples:
            subject_id: str = cast(str, triple["@id"])
            if subject_id not in grouped:
                grouped[subject_id] = {"@id": subject_id}

            # Merge properties
            for key, value in triple.items():
                if key != "@id":
                    if key in grouped[subject_id]:
                        # Convert to array if not already
                        if not isinstance(grouped[subject_id][key], list):
                            grouped[subject_id][key] = [grouped[subject_id][key]]
                        # Add new value if not duplicate
                        if value not in grouped[subject_id][key]:
                            grouped[subject_id][key].append(value)
                    else:
                        grouped[subject_id][key] = value

        # Build @about metadata section
        about = self._extract_about_metadata(
            endpoint_url=endpoint_url,
            dataset_name=dataset_name,
            graph_uris=graph_uris,
        )

        # Return simple JSON-LD
        return {"@context": context, "@graph": list(grouped.values()), "@about": about}

    def _create_context(self) -> Dict[str, str]:
        """Create JSON-LD @context."""
        # Start with standard W3C vocabularies
        context = {
            # Core RDF vocabularies
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            # Metadata vocabularies
            "dcterms": "http://purl.org/dc/terms/",
            "dc": "http://purl.org/dc/elements/1.1/",
            "prov": "http://www.w3.org/ns/prov#",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "schema": "https://schema.org/",
            # VoID and SHACL for schema description
            "void": "http://rdfs.org/ns/void#",
            "sh": "http://www.w3.org/ns/shacl#",
            # Common biological/chemical ontologies (clean URIs)
            "go": "http://purl.obolibrary.org/obo/GO_",
            "chebi": "http://purl.obolibrary.org/obo/CHEBI_",
            "pato": "http://purl.obolibrary.org/obo/PATO_",
            "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
            "cheminf": "http://semanticscience.org/resource/CHEMINF_",
        }

        # Add prefixes from VoID graph namespace manager
        if self.graph and hasattr(self.graph, "namespace_manager"):
            for prefix, namespace in self.graph.namespace_manager.namespaces():
                if prefix and namespace and str(prefix) not in context:
                    # Only add if it's a valid URI and not already present
                    ns_str = str(namespace)
                    if ns_str.startswith(("http://", "https://", "urn:")):
                        context[str(prefix)] = ns_str

        return context

    def _extract_context(self) -> Dict[str, str]:
        """Extract @context from VoID graph and common namespaces."""
        return self._create_context()

    def _filter_jsonld_void_admin_nodes(self, jsonld: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out VoID administrative nodes from JSON-LD structure."""
        void_patterns = [
            "void",
            "rdfs",
            "rdf",
            "owl",
            "skos",
            "foaf",
            "dcterms",
            "dc",
            "prov",
            "schema",
        ]

        # Handle @graph structure
        if "@graph" in jsonld:
            filtered_graph = []

            for item in jsonld["@graph"]:
                # Keep dataset description (first item)
                if item.get("@type") == "void:Dataset":
                    filtered_graph.append(item)
                    continue

                # Keep schema pattern statements, S-P-O relationships
                if "void:SchemaPattern" in item.get("@type", []):
                    filtered_graph.append(item)
                    continue

                # Filter other items based on @id patterns
                item_id = item.get("@id", "").lower()
                if not any(void_pat in item_id for void_pat in void_patterns):
                    filtered_graph.append(item)

            jsonld_filtered = jsonld.copy()
            jsonld_filtered["@graph"] = filtered_graph
            return jsonld_filtered

        # Return as-is if no recognized structure
        return jsonld

    def _extract_schema_patterns_from_triples(self) -> List[Dict[str, str]]:
        """
        Extract schema patterns from the internal schema triples.
        This creates the schema_patterns structure expected by other methods.

        Returns:
            List of schema pattern dictionaries
        """
        if not hasattr(self, "schema_triples") or not self.schema_triples:
            return []

        patterns = []
        for subject_uri, property_uri, object_uri in self.schema_triples:
            # Convert URIs to CURIEs for display
            subject_curie, _, _ = self._get_curie_and_namespace(str(subject_uri))
            property_curie, _, _ = self._get_curie_and_namespace(str(property_uri))
            object_curie, _, _ = self._get_curie_and_namespace(str(object_uri))

            patterns.append(
                {
                    "subject_class": subject_curie,
                    "subject_uri": str(subject_uri),
                    "property": property_curie,
                    "property_uri": str(property_uri),
                    "object_class": object_curie,
                    "object_uri": str(object_uri),
                }
            )

        return patterns

    def to_schema(self, filter_void_admin_nodes: bool = True) -> pd.DataFrame:
        """
        Parse VoID file and return schema as pandas DataFrame.
        This method now uses the JSON-LD generation as the source of truth.

        Args:
            filter_void_admin_nodes: Whether to filter out VoID-specific nodes

        Returns:
            DataFrame with schema information including CURIEs
        """
        # Ensure schema is extracted (populates self.schema_triples)
        self._extract_schema_triples()

        # Get schema patterns from the internal triples
        schema_patterns = self._extract_schema_patterns_from_triples()

        if not schema_patterns:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(schema_patterns)

        # Apply filtering if requested
        if filter_void_admin_nodes:
            df = self._filter_void_admin_nodes(df)

        return df

    def to_dataframe(
        self,
        endpoint_url: Optional[str] = None,
        graph_uri: Optional[str] = None,
        use_linkml: bool = False,
        datatype_partitions: bool = True,
        offset_limit_steps: int = 10000,
    ) -> pd.DataFrame:
        """Extract schema to pandas DataFrame.

        Returns:
            DataFrame with schema analysis including coverage statistics.
        """
        # Use to_schema() as the single source of truth for consistent data extraction
        schema_df = self.to_schema(filter_void_admin_nodes=True)

        # Return the full DataFrame with all columns for backward compatibility
        # This ensures all methods use the same CURIE-converted, data
        return schema_df

    def to_linkml(
        self,
        filter_void_nodes: bool = True,
        schema_name: Optional[str] = None,
        schema_description: Optional[str] = None,
        schema_base_uri: Optional[str] = None,
    ) -> SchemaDefinition:
        """
        Generate LinkML schema directly from JSON-LD representation.

        This method works directly with the JSON-LD @graph structure to create
        a valid LinkML schema with proper identifier naming conventions.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes
            schema_name: Name for the LinkML schema (used as prefix and default prefix)
            schema_description: Description for the LinkML schema
            schema_base_uri: Base URI for the schema (default: https://w3id.org/{schema_name}/)

        Returns:
            LinkML SchemaDefinition with classes based on subjects
        """
        import re

        # Get JSON-LD as source of truth
        jsonld = self.to_jsonld(filter_void_nodes)

        # Generate schema name from dataset if not provided
        if not schema_name:
            schema_name = getattr(self, "_dataset_name", "rdf_schema")
            schema_name = re.sub(r"[^a-zA-Z0-9_]", "_", schema_name)

        # Set schema base URI
        if not schema_base_uri:
            schema_uri = f"https://w3id.org/{schema_name}/"
        else:
            # Ensure base URI ends with /
            schema_uri = schema_base_uri.rstrip("/") + "/"

        # Base prefixes required by LinkML
        base_prefixes = {
            schema_name: schema_uri,
            "linkml": "https://w3id.org/linkml/",
            "schema": "http://schema.org/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
        }

        # Merge with extracted prefixes from JSON-LD
        all_prefixes = {**base_prefixes, **jsonld.get("@context", {})}

        schema = SchemaDefinition(
            id=schema_uri,
            name=schema_name,
            description=(
                schema_description or f"LinkML schema generated from JSON-LD for {schema_name}"
            ),
            default_prefix=schema_name,
            prefixes=all_prefixes,
            types={
                "string": TypeDefinition(name="string", uri="xsd:string", base="str"),
                "uriorcurie": TypeDefinition(
                    name="uriorcurie", uri="xsd:anyURI", base="URIorCURIE"
                ),
            },
        )

        # Extract triples from JSON-LD @graph
        if "@graph" not in jsonld:
            logger.warning("No @graph found in JSON-LD, returning empty schema")
            return schema

        graph_data = jsonld["@graph"]

        # Skip the first item if it's a VoID dataset description
        schema_items = []
        for item in graph_data:
            if item.get("@type") != "void:Dataset":
                schema_items.append(item)

        if not schema_items:
            logger.warning("No schema triples found in JSON-LD @graph")
            return schema

        # Label map (CURIE → human label) from miner
        label_map: Dict[str, str] = jsonld.get("_labels", {})

        # Collect all classes and properties from the JSON-LD triples
        all_class_names: set[str] = set()
        all_slot_names: set[str] = set()
        class_properties: Dict[str, List[str]] = {}  # class_name -> [property_names]
        property_ranges: Dict[str, str] = {}  # property_name -> range_type
        property_descriptions: Dict[str, str] = {}  # property_name -> description
        # Track original URIs/CURIEs for classes and slots
        original_class_uris: Dict[str, str] = {}  # class_name -> original CURIE/URI
        original_slot_uris: Dict[str, str] = {}  # slot_name -> original CURIE/URI

        for item in schema_items:
            if "@id" not in item:
                continue

            subject = item["@id"]
            subject_clean = self._make_valid_linkml_name(subject)
            all_class_names.add(subject_clean)

            # Store original URI/CURIE for this class
            if subject_clean not in original_class_uris:
                original_class_uris[subject_clean] = subject

            if subject_clean not in class_properties:
                class_properties[subject_clean] = []

            # Process all properties in this item
            for prop, value in item.items():
                if prop.startswith("@"):  # Skip JSON-LD keywords
                    continue

                prop_clean = self._make_valid_linkml_name(prop)
                all_slot_names.add(prop_clean)

                # Store original URI/CURIE for this property
                if prop_clean not in original_slot_uris:
                    original_slot_uris[prop_clean] = prop

                if prop_clean not in class_properties[subject_clean]:
                    class_properties[subject_clean].append(prop_clean)

                # Determine property range from value structure
                if isinstance(value, list):
                    # Handle arrays - use first item to determine type
                    value = value[0] if value else None

                if isinstance(value, dict):
                    if "@id" in value:
                        # It's a reference to another resource
                        target_clean = self._make_valid_linkml_name(value["@id"])
                        all_class_names.add(target_clean)
                        # Store original URI/CURIE for the target class
                        if target_clean not in original_class_uris:
                            original_class_uris[target_clean] = value["@id"]
                        property_ranges[prop_clean] = target_clean
                    elif "@value" in value:
                        # It's a literal value
                        property_ranges[prop_clean] = "string"
                elif isinstance(value, str):
                    # String value
                    property_ranges[prop_clean] = "string"
                else:
                    # Default to string
                    property_ranges[prop_clean] = "string"

                # Store property description (prefer label)
                if prop_clean not in property_descriptions:
                    lbl = label_map.get(prop)
                    if lbl:
                        property_descriptions[prop_clean] = lbl
                    else:
                        property_descriptions[prop_clean] = (
                            f"Property {prop}"
                        )

        # Detect naming conflicts between classes and slots
        conflicts = all_class_names.intersection(all_slot_names)
        slot_name_mapping = {}

        # Resolve conflicts by prefixing slot names
        for slot_name in all_slot_names:
            if slot_name in conflicts:
                final_name = f"has_{slot_name}"
            else:
                final_name = slot_name
            slot_name_mapping[slot_name] = final_name

        # Create class definitions with original URIs from source data
        classes = {}
        for class_name in all_class_names:
            # Get properties for this class, applying name mapping
            class_slots = []
            if class_name in class_properties:
                class_slots = [
                    slot_name_mapping.get(prop, prop) for prop in class_properties[class_name]
                ]

            # Use original CURIE/URI from the source data
            # Expand CURIE to full URI using context if needed
            original_uri = original_class_uris.get(class_name, class_name)
            if ":" in original_uri and not original_uri.startswith(("http://", "https://")):
                # It's a CURIE, try to expand it using the context
                prefix, local = original_uri.split(":", 1)
                if prefix in all_prefixes:
                    class_uri = all_prefixes[prefix] + local
                else:
                    class_uri = original_uri
            else:
                class_uri = original_uri

            class_def = ClassDefinition(
                name=class_name,
                description=label_map.get(
                    original_class_uris.get(class_name, ""),
                    f"Class representing {class_name}",
                ),
                slots=class_slots,
                class_uri=class_uri,
            )
            classes[class_name] = class_def

        # Create slot definitions
        slots = {}
        for original_slot_name in all_slot_names:
            final_slot_name = slot_name_mapping[original_slot_name]

            # Determine range, applying name mapping for class references
            range_type = property_ranges.get(original_slot_name, "string")
            if range_type in all_class_names and range_type != "string":
                range_type = range_type  # Class reference
            elif range_type not in ["string", "uriorcurie"]:
                range_type = "string"  # Default fallback

            # Use original CURIE/URI from the source data
            original_uri = original_slot_uris.get(original_slot_name, original_slot_name)
            if ":" in original_uri and not original_uri.startswith(("http://", "https://")):
                # It's a CURIE, try to expand it using the context
                prefix, local = original_uri.split(":", 1)
                if prefix in all_prefixes:
                    slot_uri = all_prefixes[prefix] + local
                else:
                    slot_uri = original_uri
            else:
                slot_uri = original_uri

            slot_def = SlotDefinition(
                name=final_slot_name,
                description=property_descriptions.get(
                    original_slot_name, f"Property {original_slot_name}"
                ),
                range=range_type,
                slot_uri=slot_uri,
            )

            # Set domain_of based on which classes use this property
            domain_classes = []
            for class_name, props in class_properties.items():
                if original_slot_name in props:
                    domain_classes.append(class_name)

            if domain_classes:
                slot_def.domain_of = domain_classes
                slot_def.owner = domain_classes[0]  # First class as owner

            slots[final_slot_name] = slot_def

        # Add classes and slots to schema
        schema.classes = classes
        schema.slots = slots

        return schema

    def _make_valid_linkml_name(self, uri_or_curie: str) -> str:
        """
        Convert a URI or CURIE to a valid LinkML identifier.

        LinkML identifiers must:
        - Start with a letter
        - Contain only letters, digits, and underscores
        , not contain dots, colons, or other special characters

        Examples:
        - "aopo:KeyEvent" -> "aopo_KeyEvent" (preserve prefix)
        - "edam.data1025" -> "edam_data_1025" (dots to underscores, separate numbers)
        - "http://example.org/Class" -> "example_Class"

        Args:
            uri_or_curie: URI or CURIE to convert

        Returns:
            Valid LinkML identifier
        """
        import re

        # Try to get a CURIE first if it's a URI
        if uri_or_curie.startswith(("http://", "https://")):
            curie = curie_from_iri(uri_or_curie)
            if curie:
                uri_or_curie = curie

        # Handle different CURIE/name formats
        if ":" in uri_or_curie:
            # Split on colon - standard CURIE format like "aopo:KeyEvent"
            parts = uri_or_curie.split(":", 1)  # Split only on first colon
            prefix = parts[0]
            local = parts[1]

            # Clean prefix (remove dots, special chars)
            prefix = re.sub(r"[^a-zA-Z0-9_]", "_", prefix)

            # Clean local part more carefully to preserve structure
            # Handle cases like "data1025" -> "data_1025"
            local = self._clean_local_part(local)

            # Combine with underscore
            name = f"{prefix}_{local}"

        else:
            # No colon - could be a local name with dots like "edam.data1025"
            name = self._clean_local_part(uri_or_curie)

        # Final cleanup
        name = self._finalize_linkml_name(name)

        return name

    def _clean_local_part(self, local: str) -> str:
        """
        Clean the local part of a name while preserving meaningful structure.

        Examples:
        - "KeyEvent" -> "KeyEvent"
        - "data1025" -> "data_1025"
        - "edam.data1025" -> "edam_data_1025"
        - "C123456" -> "C_123456"
        """
        import re

        # Replace dots with underscores first
        local = local.replace(".", "_")

        # Add underscore before numbers that follow letters (like data1025 -> data_1025)
        local = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", local)

        # Add underscore before uppercase letters that follow lowercase (camelCase -> camel_Case)
        local = re.sub(r"([a-z])([A-Z])", r"\1_\2", local)

        # Replace any remaining special characters with underscores
        local = re.sub(r"[^a-zA-Z0-9_]", "_", local)

        return local

    def _finalize_linkml_name(self, name: str) -> str:
        """Apply final cleanup rules to ensure valid LinkML identifier."""
        import re

        # Remove multiple consecutive underscores
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores
        name = name.strip("_")

        # Ensure it starts with a letter
        if name and name[0].isdigit():
            name = f"item_{name}"
        elif not name or not name[0].isalpha():
            name = f"item_{name}" if name else "unknown_item"

        # Ensure it's not empty
        if not name:
            name = "unknown_item"

        return name

    def _get_curie_and_namespace(self, uri: str) -> tuple[str, str, str]:
        """
        Get CURIE representation and extract prefix/namespace info for an URI.

        Args:
            uri: The URI to convert

        Returns:
            Tuple of (curie, prefix, namespace_uri) where:
            - curie: The CURIE representation (e.g., "aopo:KeyEvent")
            - prefix: The prefix part (e.g., "aopo")
            - namespace_uri: The namespace URI (e.g., "http://aopwiki.org/")
        """
        import re

        curie = None
        prefix = None
        namespace_uri = None

        # First try bioregistry conversion
        if uri.startswith(("http://", "https://")):
            try:
                from bioregistry import curie_from_iri, parse_iri

                # Parse IRI to get prefix and local ID
                parsed = parse_iri(uri)
                if parsed:
                    prefix, local_id = parsed
                    # Reconstruct namespace by removing local_id from URI
                    if local_id in uri:
                        idx = uri.rfind(local_id)
                        namespace_uri = uri[:idx]
                    else:
                        # Fallback: standard URI patterns
                        if "#" in uri:
                            namespace_uri = uri.rsplit("#", 1)[0] + "#"
                        else:
                            namespace_uri = uri.rsplit("/", 1)[0] + "/"

                    # Get CURIE representation
                    curie = curie_from_iri(uri)
                    if not curie and prefix and local_id:
                        curie = f"{prefix}:{local_id}"

            except Exception as e:
                # Fallback if bioregistry fails
                logger.debug(f"Bioregistry failed for {uri}: {e}")

        # Fallback to string manipulation if bioregistry fails
        if not curie:
            if "#" in uri:
                namespace_part, local_part = uri.rsplit("#", 1)
                namespace_uri = namespace_part + "#"
            elif "/" in uri:
                namespace_part, local_part = uri.rsplit("/", 1)
                namespace_uri = namespace_part + "/"
            else:
                local_part = uri

            # Generate a simple prefix from the URI if we don't have one
            if not prefix and namespace_uri:
                # Extract a reasonable prefix from the domain/path
                clean_uri = namespace_uri.replace("http://", "").replace("https://", "")
                clean_uri = clean_uri.replace("www.", "").strip("/").strip("#")
                if "/" in clean_uri:
                    parts = clean_uri.split("/")
                    prefix = parts[-1] if parts[-1] else parts[-2] if len(parts) > 1 else "ns"
                else:
                    prefix = clean_uri.split(".")[0] if "." in clean_uri else clean_uri
                # Clean the prefix
                prefix = re.sub(r"[^a-zA-Z0-9_]", "", prefix)[:10]  # Max 10 chars

            curie = f"{prefix}:{local_part}" if prefix and local_part else uri

        return curie or uri, prefix or "", namespace_uri or ""

    def _clean_identifier_name(self, identifier: str, is_class: bool = False) -> str:
        """
        Clean identifier name to be valid LinkML identifier.

        Args:
            identifier: The identifier (URI or CURIE)
            is_class: Whether this is for a class (vs slot/property)

        Returns:
            Clean LinkML identifier
        """
        import re

        # Get CURIE representation first
        curie, prefix, namespace_uri = self._get_curie_and_namespace(identifier)

        # Store the namespace info for later use in schema prefixes
        if prefix and namespace_uri and hasattr(self, "_discovered_prefixes"):
            self._discovered_prefixes[prefix] = namespace_uri

        # Convert CURIE to valid LinkML identifier
        if ":" in curie:
            prefix_part, local_part = curie.split(":", 1)
            # Clean both parts
            clean_prefix = re.sub(r"[^a-zA-Z0-9_]", "_", prefix_part)
            clean_local = re.sub(r"[^a-zA-Z0-9_]", "_", local_part)
            name = f"{clean_prefix}_{clean_local}"
        else:
            # Handle cases without prefix
            name = re.sub(r"[^a-zA-Z0-9_]", "_", curie)

        return name or ("unknown_class" if is_class else "unknown_property")

    def _clean_class_name(self, class_name: str) -> str:
        """Clean class name to be valid LinkML identifier."""
        return self._clean_identifier_name(class_name, is_class=True)

    def _clean_slot_name(self, slot_name: str) -> str:
        """Clean slot name to be valid LinkML identifier."""
        return self._clean_identifier_name(slot_name, is_class=False)

    def _extract_prefixes_from_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Extract prefixes from schema DataFrame by analyzing URIs."""
        prefixes = {}

        # Get prefixes from the VoID RDFlib graph first
        if self.graph and hasattr(self.graph, "namespace_manager"):
            for prefix, namespace in self.graph.namespace_manager.namespaces():
                if prefix and namespace:
                    prefixes[str(prefix)] = str(namespace)

        # Initialize prefix collection for discovered CURIEs
        self._discovered_prefixes: Dict[str, str] = {}

        # Extract prefixes from all URIs in the DataFrame
        all_uris: set[str] = set()
        for col in ["property_uri", "subject_uri", "object_uri"]:
            if col in df.columns:
                all_uris.update(df[col].dropna().unique())

        # Process each URI to discover prefixes
        for uri in all_uris:
            uri_str = str(uri)
            if uri_str not in ["Literal", "Resource"]:
                # This will populate self._discovered_prefixes
                _curie, prefix, _namespace_uri = self._get_curie_and_namespace(uri_str)

        # Merge discovered prefixes with existing ones
        prefixes.update(self._discovered_prefixes)

        # Clean up the temporary collection
        if hasattr(self, "_discovered_prefixes"):
            delattr(self, "_discovered_prefixes")

        return prefixes

    def to_linkml_yaml(
        self,
        filter_void_nodes: bool = True,
        schema_name: Optional[str] = None,
        schema_description: Optional[str] = None,
        schema_base_uri: Optional[str] = None,
    ) -> str:
        """
        Parse VoID file and return LinkML schema as YAML string.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes
            schema_name: Name for the LinkML schema
            schema_description: Description for the LinkML schema
            schema_base_uri: Base URI for the schema (default: https://w3id.org/{schema_name}/)

        Returns:
            LinkML schema as YAML string
        """
        linkml_schema = self.to_linkml(
            filter_void_nodes=filter_void_nodes,
            schema_name=schema_name,
            schema_description=schema_description,
            schema_base_uri=schema_base_uri,
        )

        return cast(str, YAMLGenerator(linkml_schema).serialize())

    def to_shacl(
        self,
        filter_void_nodes: bool = True,
        schema_name: Optional[str] = None,
        schema_description: Optional[str] = None,
        schema_base_uri: Optional[str] = None,
        closed: bool = True,
        suffix: Optional[str] = None,
        include_annotations: bool = False,
    ) -> str:
        """
        Generate SHACL (Shapes Constraint Language) shapes from VoID schema.

        This method converts the VoID schema to LinkML and then generates SHACL
        shapes using LinkML's ShaclGenerator. SHACL shapes can be used to validate
        RDF data against the extracted schema constraints.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes
            schema_name: Name for the LinkML schema (used as base for SHACL shapes)
            schema_description: Description for the LinkML schema
            schema_base_uri: Base URI for the schema
            closed: If True, generate closed SHACL shapes (sh:closed true).
            suffix: Optional suffix to append to shape names (e.g., "Shape" -> PersonShape)
            include_annotations: If True, include annotations from classes/slots in shapes.

        Returns:
            SHACL shapes as Turtle/RDF string

        Example:
            >>> parser = VoidParser("dataset_void.ttl")
            >>> shacl_ttl = parser.to_shacl(schema_name="my_dataset", closed=True, suffix="Shape")
            >>> # Save to file
            >>> with open("schema.shacl.ttl", "w") as f:
            ...     f.write(shacl_ttl)
        """
        # First generate LinkML schema
        linkml_schema = self.to_linkml(
            filter_void_nodes=filter_void_nodes,
            schema_name=schema_name,
            schema_description=schema_description,
            schema_base_uri=schema_base_uri,
        )

        # Create a StringIO stream from the LinkML YAML serialization
        linkml_yaml = YAMLGenerator(linkml_schema).serialize()

        # Generate SHACL from LinkML schema
        shacl_gen = ShaclGenerator(
            schema=linkml_yaml,
            closed=closed,
            # suffix=suffix,
            include_annotations=include_annotations,
        )

        return cast(str, shacl_gen.serialize())

    def to_rdfconfig(
        self,
        filter_void_nodes: bool = True,
        endpoint_url: Optional[str] = None,
        endpoint_name: Optional[str] = None,
        graph_uri: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Generate RDF-config YAML files (model.yaml, prefix.yaml, endpoint.yaml).

        RDF-config is a schema standard that describes RDF data models using
        YAML configuration files. This method converts the VoID schema to
        RDF-config format with three files:
        - model.yaml: Describes classes, properties, and their relationships
        - prefix.yaml: Namespace prefix definitions
        - endpoint.yaml: SPARQL endpoint configuration

        IMPORTANT: The rdf-config tool requires these files to be named
        exactly model.yaml, prefix.yaml, and endpoint.yaml, and placed in a
        directory named {dataset}_config. The CLI automatically creates
        this structure when using --format rdfconfig.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes
            endpoint_url: SPARQL endpoint URL for endpoint.yaml
            endpoint_name: Name for the endpoint (defaults to "endpoint")
            graph_uri: Optional graph URI to include in endpoint.yaml

        Returns:
            Dictionary with keys 'model', 'prefix', 'endpoint' containing
            YAML strings
        """
        # Get JSON-LD as source of truth
        jsonld = self.to_jsonld(filter_void_nodes)

        # Generate prefix.yaml from @context
        prefixes = jsonld.get("@context", {})
        prefix_yaml = self._generate_rdfconfig_prefix(prefixes)

        # Generate model.yaml from @graph
        graph_data = jsonld.get("@graph", [])
        model_yaml = self._generate_rdfconfig_model(graph_data, prefixes)

        # Generate endpoint.yaml
        endpoint_yaml = self._generate_rdfconfig_endpoint(endpoint_url, endpoint_name, graph_uri)

        return {
            "model": model_yaml,
            "prefix": prefix_yaml,
            "endpoint": endpoint_yaml,
        }

    def _generate_rdfconfig_prefix(self, prefixes: Dict[str, str]) -> str:
        """Generate prefix.yaml content for RDF-config."""
        lines = []
        for prefix, uri in sorted(prefixes.items()):
            # Format as: prefix: <uri>
            lines.append(f"{prefix}: <{uri}>")
        return "\n".join(lines) + "\n"

    def _generate_rdfconfig_endpoint(
        self,
        endpoint_url: Optional[str],
        endpoint_name: Optional[str],
        graph_uri: Optional[str],
    ) -> str:
        """Generate endpoint.yaml content for RDF-config."""
        if not endpoint_url:
            return ""

        name = endpoint_name or "endpoint"
        lines = [f"{name}:"]
        lines.append(f"  - {endpoint_url}")

        if graph_uri:
            lines.append("  - graph:")
            lines.append(f"    - {graph_uri}")

        return "\n".join(lines) + "\n"

    def _generate_rdfconfig_model(
        self, graph_data: List[Dict[str, Any]], prefixes: Dict[str, str]
    ) -> str:
        """
        Generate model.yaml content for RDF-config.

        The model describes classes and their properties in a structured
        format. Each class becomes a top-level entry with its properties
        as nested items.
        """
        # Group items by class (@id becomes the class subject)
        classes: Dict[str, List[Dict[str, Any]]] = {}

        # First pass: collect all class URIs
        class_uris = set()
        for item in graph_data:
            if "@id" in item:
                class_uris.add(item["@id"])

        # Build unique class name mapping to handle duplicates
        class_name_map = self._build_unique_class_names(class_uris, prefixes)

        # Second pass: process properties
        for item in graph_data:
            if "@id" not in item:
                continue

            subject = item["@id"]

            # Initialize class entry if not exists
            if subject not in classes:
                classes[subject] = []

            # Get snake_case version for variable prefixes from unique name
            unique_class_name = class_name_map.get(
                subject, self._make_rdfconfig_class_name(subject)
            )
            class_var = self._make_rdfconfig_variable(unique_class_name)

            # Process each property in the item
            for prop, value in item.items():
                if prop.startswith("@"):  # Skip JSON-LD keywords
                    continue

                # Determine if value is a reference or literal
                prop_info = self._analyze_rdfconfig_property(prop, value, class_var, class_name_map)
                if prop_info:
                    classes[subject].append(prop_info)

        # Format as RDF-config YAML
        return self._format_rdfconfig_classes_yaml(classes, class_name_map)

    def _analyze_rdfconfig_property(
        self,
        prop: str,
        value: Any,
        class_context: str,
        class_name_map: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze a property and return structured info for RDF-config.

        Args:
            prop: Property URI/CURIE
            value: Property value (dict, list, or literal)
            class_context: Class name for scoping variable names
            class_name_map: Mapping of class URIs to unique names

        Returns:
            Dictionary with property info or None
        """
        # Determine if value is a reference or literal
        is_reference = False
        target_class = None

        if isinstance(value, dict) and "@id" in value:
            is_reference = True
            target_class = value["@id"]
        elif isinstance(value, list) and len(value) > 0:
            first_val = value[0]
            if isinstance(first_val, dict) and "@id" in first_val:
                is_reference = True
                target_class = first_val["@id"]

        # Create variable name from property URI
        # Prefix with class context to ensure uniqueness
        prop_base = self._make_rdfconfig_variable(prop)
        prop_var = f"{class_context}_{prop_base}"

        if is_reference and target_class:
            # Object property pointing to another class
            # Use unique name from map
            target_name = class_name_map.get(
                target_class, self._make_rdfconfig_class_name(target_class)
            )
            return {
                "property": prop,
                "variable": prop_var,
                "range": target_name,
            }
        else:
            # Data property (literal)
            return {
                "property": prop,
                "variable": prop_var,
                "range": f'"{prop_var}_value"',
            }

    def _build_unique_class_names(
        self, class_uris: Any, prefixes: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Build a mapping of class URIs to unique class names.

        Handles collisions by including the prefix in the class name
        when the same local name appears in multiple namespaces.

        Args:
            class_uris: Iterable of class URIs
            prefixes: Dictionary of prefix to namespace URI mappings

        Returns:
            Dictionary mapping class URI to unique class name
        """
        import re

        # Reverse prefix mapping for lookup
        ns_to_prefix = {ns: prefix for prefix, ns in prefixes.items()}

        # First pass: collect all base names and their URIs
        name_to_uris: Dict[str, List[str]] = {}
        uri_to_base_name: Dict[str, str] = {}

        for class_uri in class_uris:
            base_name = self._make_rdfconfig_class_name(class_uri)
            uri_to_base_name[class_uri] = base_name

            if base_name not in name_to_uris:
                name_to_uris[base_name] = []
            name_to_uris[base_name].append(class_uri)

        # Second pass: resolve collisions
        class_name_map: Dict[str, str] = {}

        for base_name, uris in name_to_uris.items():
            if len(uris) == 1:
                # No collision, use base name
                class_name_map[uris[0]] = base_name
            else:
                # Collision detected, add prefix to disambiguate
                for uri in uris:
                    # Try to extract prefix from CURIE
                    prefix_part = None
                    if ":" in uri:
                        prefix_part = uri.split(":", 1)[0]
                    else:
                        # Try to find matching namespace
                        for ns_uri, prefix in ns_to_prefix.items():
                            if uri.startswith(ns_uri):
                                prefix_part = prefix
                                break

                    if prefix_part:
                        # Create name: PrefixBaseName (e.g., WpDataNode)
                        # Capitalize prefix and prepend to base name
                        prefix_clean = re.sub(r"[^a-zA-Z0-9]", "", prefix_part)
                        prefix_cap = (
                            prefix_clean[0].upper() + prefix_clean[1:] if prefix_clean else ""
                        )
                        unique_name = f"{prefix_cap}{base_name}"
                    else:
                        # Fallback: use hash of URI
                        uri_hash = str(abs(hash(uri)))[:6]
                        unique_name = f"{base_name}{uri_hash}"

                    class_name_map[uri] = unique_name

        return class_name_map

    def _format_rdfconfig_classes_yaml(
        self, classes: Dict[str, List[Dict[str, Any]]], class_name_map: Dict[str, str]
    ) -> str:
        """
        Format classes dictionary as RDF-config YAML.

        Args:
            classes: Dictionary mapping class URIs to their properties
            class_name_map: Dictionary mapping class URIs to unique names

        Returns:
            YAML string for model.yaml
        """
        lines = []

        for class_uri in sorted(classes.keys()):
            properties = classes[class_uri]

            # Get unique class name from map
            class_name = class_name_map.get(class_uri, self._make_rdfconfig_class_name(class_uri))
            lines.append(f"- {class_name} {class_uri}:")

            # Add properties
            for prop_info in properties:
                lines.append(f"  - {prop_info['property']}:")
                var_name = prop_info["variable"]
                range_val = prop_info["range"]
                lines.append(f"    - {var_name}: {range_val}")

        return "\n".join(lines) + "\n"

    def _make_rdfconfig_class_name(self, uri_or_curie: str) -> str:
        """
        Convert URI/CURIE to RDF-config class name.

        RDF-config requires class names to:
        - Start with a capital letter
        - Be alphanumeric only (no underscores or special chars)
        - Be in CamelCase format

        Args:
            uri_or_curie: URI or CURIE to convert

        Returns:
            Valid RDF-config class name in CamelCase
        """
        import re

        # Extract local part
        if ":" in uri_or_curie:
            local = uri_or_curie.split(":", 1)[1]
        elif "/" in uri_or_curie:
            local = uri_or_curie.split("/")[-1]
        elif "#" in uri_or_curie:
            local = uri_or_curie.split("#")[-1]
        else:
            local = uri_or_curie

        # Remove all non-alphanumeric characters
        local = re.sub(r"[^a-zA-Z0-9]", "", local)

        # If starts with number, prefix with letter
        if local and local[0].isdigit():
            local = "C" + local

        # Ensure first letter is capital
        if local:
            local = local[0].upper() + local[1:]
        else:
            local = "Class"

        return local

    def _make_rdfconfig_variable(self, uri_or_curie: str) -> str:
        """
        Convert URI/CURIE to RDF-config variable name.

        RDF-config uses snake_case variable names derived from the local
        part of the URI/CURIE.
        """
        import re

        # Extract local part
        if ":" in uri_or_curie:
            local = uri_or_curie.split(":", 1)[1]
        elif "/" in uri_or_curie:
            local = uri_or_curie.split("/")[-1]
        elif "#" in uri_or_curie:
            local = uri_or_curie.split("#")[-1]
        else:
            local = uri_or_curie

        # Convert to snake_case
        # Replace non-alphanumeric with underscore
        local = re.sub(r"[^a-zA-Z0-9_]", "_", local)
        # Add underscore before capitals (camelCase -> camel_Case)
        local = re.sub(r"([a-z])([A-Z])", r"\1_\2", local)
        # Lowercase
        local = local.lower()
        # Remove consecutive underscores
        local = re.sub(r"_+", "_", local)
        # Remove leading/trailing underscores
        local = local.strip("_")

        return local

    def to_json(self, filter_void_nodes: bool = True) -> Dict[str, Any]:
        """
        Parse VoID file and return schema as JSON structure.
        This method now uses the JSON-LD generation as the source of truth.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes

        Returns:
            Dictionary with schema information
        """
        # Get both JSON-LD and schema patterns
        jsonld = self.to_jsonld(filter_void_nodes)
        schema_patterns = self._extract_schema_patterns_from_triples()

        # Apply filtering if requested
        if filter_void_nodes:
            df = pd.DataFrame(schema_patterns)
            if not df.empty:
                df = self._filter_void_admin_nodes(df)
                schema_patterns = df.to_dict("records")

        schema_graph: Dict[str, Any] = {
            "triples": [],
            "metadata": {
                "total_triples": len(schema_patterns),
                "classes": len(self.classes) if hasattr(self, "classes") else 0,
                "properties": len(self.properties) if hasattr(self, "properties") else 0,
                "prefixes": jsonld["@context"],  # Use @context for prefixes
            },
            "jsonld": jsonld,  # Include the full JSON-LD structure
            "schema_patterns": schema_patterns,  # Add schema patterns for backward compatibility
        }

        # Add triples from schema patterns
        for pattern in schema_patterns:
            schema_graph["triples"].append(
                [pattern["subject_uri"], pattern["property_uri"], pattern["object_uri"]]
            )

        return schema_graph

    @staticmethod
    def _enrich_void_with_bioregistry_prefixes(void_graph: Graph) -> Graph:
        """
        Enrich VoID graph with bioregistry-resolved prefixes.

        Collects all URIs from the VoID graph, converts them to CURIEs using
        bioregistry, and binds the prefixes to the graph namespace manager.

        Args:
            void_graph: RDF Graph containing VoID description

        Returns:
            The same graph with enriched namespace bindings
        """
        from bioregistry import curie_from_iri, parse_iri

        logger.debug("Enriching VoID graph with bioregistry prefixes")

        # Collect all URIs from the graph (subjects, predicates, objects)
        all_uris = set()
        for s, p, o in void_graph:
            if isinstance(s, URIRef):
                all_uris.add(str(s))
            if isinstance(p, URIRef):
                all_uris.add(str(p))
            if isinstance(o, URIRef):
                all_uris.add(str(o))

        logger.debug(f"Collected {len(all_uris)} unique URIs from VoID graph")

        # Track discovered prefixes
        discovered_prefixes = {}

        # Process each URI with bioregistry
        for uri in all_uris:
            try:
                # Parse IRI to get prefix and local ID
                parsed = parse_iri(uri)
                if parsed:
                    prefix, local_id = parsed
                    # Reconstruct namespace by removing local_id from URI
                    # Get CURIE to understand the pattern
                    curie = curie_from_iri(uri)
                    if curie and ":" in curie:
                        # Extract namespace pattern from original URI
                        # Try to find where the local_id appears in the URI
                        if local_id in uri:
                            # Find the position and extract namespace
                            idx = uri.rfind(local_id)
                            namespace_uri = uri[:idx]
                        else:
                            # Fallback: use standard patterns
                            if "#" in uri:
                                namespace_uri = uri.rsplit("#", 1)[0] + "#"
                            else:
                                namespace_uri = uri.rsplit("/", 1)[0] + "/"

                        discovered_prefixes[prefix] = namespace_uri
                        logger.debug(f"Bioregistry: {prefix} -> {namespace_uri}")

            except Exception:
                # Fallback for URIs that bioregistry doesn't recognize
                try:
                    curie = curie_from_iri(uri)
                    if curie and ":" in curie:
                        prefix = curie.split(":", 1)[0]
                        # Extract namespace from URI structure
                        if "#" in uri:
                            namespace_uri = uri.rsplit("#", 1)[0] + "#"
                        else:
                            namespace_uri = uri.rsplit("/", 1)[0] + "/"
                        discovered_prefixes[prefix] = namespace_uri
                        logger.debug(f"Fallback: {prefix} -> {namespace_uri}")
                except Exception as e:
                    # Skip URIs that can't be processed
                    logger.debug(f"Could not process URI {uri}: {e}")

        logger.info(f"Discovered {len(discovered_prefixes)} prefixes")

        # Get existing namespace bindings to avoid overwriting
        existing_namespaces = {str(ns): prefix for prefix, ns in void_graph.namespaces()}

        # Bind all discovered prefixes to the graph
        for prefix, namespace_uri in discovered_prefixes.items():
            # Skip if this namespace is already bound with a different prefix
            if namespace_uri in existing_namespaces:
                logger.debug(
                    f"Skipping {prefix} -> {namespace_uri} "
                    f"(already bound as {existing_namespaces[namespace_uri]})"
                )
                continue

            try:
                void_graph.bind(prefix, namespace_uri, override=False)
            except Exception as e:
                logger.debug(f"Failed to bind {prefix}: {e}")

        return void_graph

    @staticmethod
    def get_void_queries(
        graph_uris: Optional[Union[str, List[str]]] = None,
        counts: bool = True,
        offset_limit_steps: Optional[int] = None,
        exclude_graphs: bool = True,
        exclude_graph_patterns: Optional[List[str]] = None,
        endpoint_url: Optional[str] = None,
        void_base_uri: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Get formatted CONSTRUCT queries for VoID generation.

        Args:
            graph_uris: Graph URI(s) to analyze. If None, queries all graphs
            counts: If True, include COUNT aggregations; else faster discovery
            offset_limit_steps: If provided, use this as both LIMIT and OFFSET step
            exclude_graphs: Whether to exclude system or any specific graphs
            exclude_graph_patterns: List of regex patterns to exclude specific graphs
            endpoint_url: SPARQL endpoint URL for generating meaningful base URIs
            void_base_uri: Custom base URI for VoID partition IRIs (e.g., 'http://example.org/void')

        Returns:
            Dictionary containing the formatted queries
        """
        # Create a temporary instance to use the graph clause methods
        temp_parser = VoidParser(graph_uris=graph_uris, exclude_graphs=exclude_graphs)
        # Store exclude patterns for later use
        temp_parser.exclude_graph_patterns = exclude_graph_patterns

        # Determine the base graph URI for VoID partition naming
        if void_base_uri:
            # Use custom void_base_uri if provided
            base_graph_uri = void_base_uri.rstrip("/")
        elif isinstance(graph_uris, str):
            base_graph_uri = graph_uris
        elif isinstance(graph_uris, list) and len(graph_uris) == 1:
            base_graph_uri = graph_uris[0]
        elif isinstance(graph_uris, list) and len(graph_uris) > 1:
            # Multiple graphs - use the first one as base
            base_graph_uri = graph_uris[0]
        else:
            # No graph URIs provided - derive from endpoint URL
            if endpoint_url:
                # Extract meaningful base from endpoint URL
                from urllib.parse import urlparse

                parsed = urlparse(endpoint_url)
                if parsed.netloc:
                    base_graph_uri = f"{parsed.scheme}://{parsed.netloc}/default-graph"
                else:
                    base_graph_uri = endpoint_url.rstrip("/") + "/default-graph"
            else:
                base_graph_uri = "urn:sparql:default:graph"

        # Build limit and offset clause
        limit_offset_clause = ""

        # Use offset_limit_steps if provided, or default LIMIT for discovery
        if offset_limit_steps is not None:
            limit_offset_clause += f"LIMIT {offset_limit_steps}"

        if counts:
            # Count-based queries (slower but complete)
            class_q_template = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
CONSTRUCT {{
    ?cp void:class ?class ;
        void:entities ?count .
}}
WHERE {{
    {{
        SELECT ?class (COUNT(?s) AS ?count)
        WHERE {{
            #GRAPH_CLAUSE
                ?s a ?class
            #END_GRAPH_CLAUSE
        }}
        GROUP BY ?class
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/class_partition_',
                   REPLACE(STR(?class), '[^a-zA-Z0-9_]', '_', 'g'))) AS ?cp)
}}"""

            prop_q_template = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT {{
    ?pp void:property ?property ;
        void:triples ?count ;
        void-ext:subjectClass ?subject_class ;
        void-ext:objectClass ?object_class .
}}
WHERE {{
    {{
        SELECT ?property ?subject_class ?object_class (COUNT(*) AS ?count)
        WHERE {{
            #GRAPH_CLAUSE
                ?subject ?property ?object .
                ?subject rdf:type ?subject_class .
            #END_GRAPH_CLAUSE

            # Determine object class based on object type
            {{
                BIND(rdfs:Literal AS ?object_class)
                FILTER(isLiteral(?object))
            }}
            UNION
            {{
                ?object rdf:type ?object_class .
                FILTER(isURI(?object))
            }}
            UNION
            {{
                BIND(rdfs:Resource AS ?object_class)
                FILTER(isURI(?object))
                FILTER NOT EXISTS {{ ?object rdf:type ?any_type }}
            }}
        }}
        GROUP BY ?property ?subject_class ?object_class
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/property_partition_',
                   REPLACE(CONCAT(STR(?property), '_', STR(?subject_class), '_', STR(?object_class)), '[^a-zA-Z0-9_]', '_', 'g'))) AS ?pp)
}}"""

            dtype_q_template = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
CONSTRUCT {{
    ?dp void-ext:datatypePartition ?datatype ;
        void:triples ?count .
}}
WHERE {{
    {{
        SELECT ?datatype (COUNT(?s) AS ?count)
        WHERE {{
            #GRAPH_CLAUSE
                ?s ?p ?o .
                FILTER(isLiteral(?o))
                BIND(datatype(?o) AS ?datatype)
            #END_GRAPH_CLAUSE
        }}
        GROUP BY ?datatype
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/datatype_partition_',
                   REPLACE(STR(?datatype), '[^a-zA-Z0-9_]', '_', 'g'))) AS ?dp)
}}"""
        else:
            # Discovery-only queries (faster)
            class_q_template = f"""PREFIX void: <http://rdfs.org/ns/void#>
CONSTRUCT {{
    ?cp void:class ?class .
}}
WHERE {{
    {{
        SELECT DISTINCT ?class
        WHERE {{
            #GRAPH_CLAUSE
                ?s a ?class
            #END_GRAPH_CLAUSE
        }}
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/class_partition_',
                   REPLACE(STR(?class), '[^a-zA-Z0-9_]', '_', 'g'))) AS ?cp)
}}"""

            prop_q_template = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT {{
    ?pp void:property ?property ;
        void-ext:subjectClass ?subject_class ;
        void-ext:objectClass ?object_class .
}}
WHERE {{
    {{
        SELECT DISTINCT ?property ?subject_class ?object_class
        WHERE {{
            #GRAPH_CLAUSE
                ?subject ?property ?object .
                ?subject rdf:type ?subject_class .
            #END_GRAPH_CLAUSE

            # Determine object class based on object type
            {{
                BIND(rdfs:Literal AS ?object_class)
                FILTER(isLiteral(?object))
            }}
            UNION
            {{
                ?object rdf:type ?object_class .
                FILTER(isURI(?object))
            }}
            UNION
            {{
                BIND(rdfs:Resource AS ?object_class)
                FILTER(isURI(?object))
                FILTER NOT EXISTS {{ ?object rdf:type ?any_type }}
            }}
        }}
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/property_partition_',
                   REPLACE(CONCAT(STR(?property), '_', STR(?subject_class), '_', STR(?object_class)), '[^a-zA-Z0-9_]', '_', 'g'))) AS ?pp)
}}"""

            dtype_q_template = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
CONSTRUCT {{
    ?dp void-ext:datatypePartition ?datatype .
}}
WHERE {{
    {{
        SELECT DISTINCT ?datatype
        WHERE {{
            #GRAPH_CLAUSE
                ?s ?p ?o .
                FILTER(isLiteral(?o))
                BIND(datatype(?o) AS ?datatype)
            #END_GRAPH_CLAUSE
        }}
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/datatype_partition_',
                   REPLACE(STR(?datatype), '[^a-zA-Z0-9_]', '_', 'g'))) AS ?dp)
}}"""

        # Replace the graph clause placeholders in all queries
        class_q = temp_parser._replace_graph_clause_placeholder(class_q_template)
        prop_q = temp_parser._replace_graph_clause_placeholder(prop_q_template)
        dtype_q = temp_parser._replace_graph_clause_placeholder(dtype_q_template)

        return {
            "class_partitions": class_q,
            "property_partitions": prop_q,
            "datatype_partitions": dtype_q,
        }

    @staticmethod
    def generate_void_from_sparql(
        endpoint_url: str,
        graph_uris: Optional[Union[str, List[str]]] = None,
        output_file: Optional[str] = None,
        counts: bool = True,
        offset_limit_steps: Optional[int] = None,
        exclude_graphs: bool = True,
        exclude_graph_patterns: Optional[List[str]] = None,
        void_base_uri: Optional[str] = None,
    ) -> Graph:
        """
        Generate VoID description from SPARQL endpoint using CONSTRUCT queries.

        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uris: Graph URI(s) for the dataset. If None, queries all graphs
            output_file: Optional output file path for TTL
            counts: If True, include COUNT aggregations; else faster discovery
            offset_limit_steps: If provided, use this as both LIMIT and OFFSET step
            exclude_graphs: Whether to exclude Virtuoso system graphs
            void_base_uri: Custom base URI for VoID partition IRIs

        Returns:
            RDF Graph containing the VoID description
        """
        queries = VoidParser.get_void_queries(
            graph_uris,
            counts,
            offset_limit_steps,
            exclude_graphs,
            exclude_graph_patterns,
            endpoint_url,
            void_base_uri,
        )

        # Use SparqlHelper which handles GET-->POST fallback automatically
        helper = SparqlHelper(endpoint_url, timeout=120.0)

        merged_graph = Graph()

        # Bind common prefixes for VoID generation
        merged_graph.bind("void", "http://rdfs.org/ns/void#")
        merged_graph.bind("void-ext", "http://ldf.fi/void-ext#")
        merged_graph.bind("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        merged_graph.bind("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

        # Ensure we're in a valid working directory for RDFLib parsing
        import os
        import tempfile

        from rdflib import RDF, URIRef

        try:
            current_dir = os.getcwd()
            # Check if current directory exists and is accessible
            if not os.path.exists(current_dir) or not os.access(current_dir, os.R_OK):
                # Use temporary directory as fallback
                temp_dir = tempfile.gettempdir()
                os.chdir(temp_dir)

        except (FileNotFoundError, OSError, PermissionError):
            # If we can't get cwd or it doesn't exist, use temp directory
            temp_dir = tempfile.gettempdir()
            os.chdir(temp_dir)

        def run_construct(
            query_text: str,
            name: str,
            is_optional: bool = False,
            public_id: str = "http://jmillanacosta.github.io/",
        ) -> None:
            """Execute CONSTRUCT query and add results to graph.

            Args:
                query_text: SPARQL CONSTRUCT query to execute
                name: Name identifier for this query
                is_optional: Whether query failure should be logged as warning
                public_id: Base URI for VoID dataset identifier
            """
            public_id = f"{public_id}/{name}/void"

            # Log the query being executed
            logger.info(f"Executing CONSTRUCT query: {name}")
            logger.debug(f"Query text:\n{query_text}")

            t0 = time.monotonic()

            try:
                # Use SparqlHelper which handles GET-->POST fallback automatically
                turtle_data = helper.construct(query_text)
                results = (
                    turtle_data.encode("utf-8") if isinstance(turtle_data, str) else turtle_data
                )
                dt = time.monotonic() - t0

                # Parse result
                if results:
                    result_str = (
                        results.decode("utf-8") if isinstance(results, bytes) else str(results)
                    )

                    if result_str.strip():
                        try:
                            merged_graph.parse(data=result_str, format="turtle", publicID=public_id)
                            logger.info(f"Query {name} completed in {dt:.2f}s")
                        except Exception as parse_e:
                            logger.warning(f"Failed to parse {name} results: {parse_e}")
                            if not is_optional:
                                raise
                    else:
                        logger.info(f"Empty results for {name}")
                else:
                    logger.warning(f"No results for {name}")

            except Exception as e:
                dt = time.monotonic() - t0
                logger.error(f"Query {name} failed after {dt:.2f}s: {e}")
                logger.error(f"Failed query text:\n{query_text}")

                # Check for timeout conditions
                timeout_keywords = ["timeout", "timed out"]
                if any(keyword in str(e).lower() for keyword in timeout_keywords):
                    logger.warning(f"Query {name} timed out")
                    if is_optional:
                        return
                if not is_optional:
                    raise

        # Execute partition queries - all are optional since endpoints vary widely
        # The goal is to collect as much schema info as possible, not fail on any single query

        query_type = queries["class_partitions"]
        run_construct(query_type, "class_partitions", is_optional=True)

        query_type = queries["property_partitions"]
        run_construct(query_type, "property_partitions", is_optional=True)

        query_type = queries["datatype_partitions"]
        run_construct(query_type, "datatype_partitions", is_optional=True)
        # Check if we got any useful data
        if len(merged_graph) == 0:
            logger.warning("No RDF data was successfully extracted from endpoint")
            logger.info("This could indicate endpoint compatibility issues - creating minimal VoID")
            # Create a minimal VoID dataset description
            from urllib.parse import urlparse

            from rdflib import Literal, Namespace

            VOID = Namespace("http://rdfs.org/ns/void#")
            DCTERMS = Namespace("http://purl.org/dc/terms/")

            # Create dataset URI from endpoint
            parsed = urlparse(endpoint_url)
            dataset_uri = (
                f"{parsed.scheme}://{parsed.netloc}/dataset"
                if parsed.netloc
                else endpoint_url.rstrip("/") + "/dataset"
            )

            merged_graph.add((URIRef(dataset_uri), RDF.type, VOID.Dataset))
            merged_graph.add((URIRef(dataset_uri), VOID.sparqlEndpoint, URIRef(endpoint_url)))
            merged_graph.add(
                (
                    URIRef(dataset_uri),
                    DCTERMS.title,
                    Literal("Dataset discovered from SPARQL endpoint"),
                )
            )
        else:
            logger.info(f"Successfully extracted {len(merged_graph)} RDF triples")

        # Enrich VoID graph with bioregistry prefixes before returning
        merged_graph = VoidParser._enrich_void_with_bioregistry_prefixes(merged_graph)

        # Save to file if specified
        if output_file:
            try:
                merged_graph.serialize(destination=output_file, format="turtle")
                logger.info(f"VoID description saved to {output_file}")
            except Exception as e:
                logger.error(f"Failed to save VoID file: {e}")
                # Don't raise here - we still have the graph in memory
        try:
            return merged_graph
        except Exception as e:
            raise RuntimeError(f"""
Failed to return VoID from SPARQL endpoint: {e}
Last query: {query_type}
""")

    @classmethod
    def generate_void_alternative_method(
        cls,
        endpoint_url: str,
        dataset_prefix: str,
        graph_uri: Optional[str] = None,
        output_file: Optional[str] = None,
    ) -> Graph:
        """
        Alternative VoID generation method using single non-paginated CONSTRUCT query.

        This method uses a unified query approach to extract all VoID partition data
        in one request, without pagination. Adapted from void-generator project but
        using read-only CONSTRUCT instead of UPDATE operations.

        Uses SparqlHelper for automatic GET-->POST fallback on endpoints that
        require POST.

        Source: https://github.com/sib-swiss/void-generator/issues/30

        Args:
            endpoint_url: SPARQL endpoint URL
            dataset_prefix: Prefix for partition node IRIs
            graph_uri: Optional graph URI to restrict queries
            output_file: Optional output file path for TTL

        Returns:
            RDF Graph containing the VoID description
        """
        logger.info("Executing alternative VoID generation method (CONSTRUCT)...")
        logger.debug(f"Dataset prefix: {dataset_prefix}")
        if graph_uri:
            logger.debug(f"Restricted to graph: {graph_uri}")

        # Build unified CONSTRUCT query that generates all VoID partitions
        graph_clause_start = f"GRAPH <{graph_uri}> {{" if graph_uri else ""
        graph_clause_end = "}" if graph_uri else ""

        construct_query = f"""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>

CONSTRUCT {{
    # Class partitions
    ?cp void:class ?class ;
        void:entities ?class_count .

    # Property partitions with class objects
    ?pp void:property ?prop ;
        void:triples ?prop_count ;
        void-ext:subjectClass ?c_s ;
        void-ext:objectClass ?c_o .

    # Property partitions with datatype objects
    ?pp_dt void:property ?prop_dt ;
           void:triples ?dt_count ;
           void-ext:subjectClass ?c_s_dt ;
           void-ext:datatypePartition ?dtp .
    ?dtp void-ext:datatype ?dt .
}}
WHERE {{
    # Class partitions
    {{
        SELECT ?class (COUNT(*) AS ?class_count)
        WHERE {{
            {graph_clause_start}
            [] a ?class .
            {graph_clause_end}
        }}
        GROUP BY ?class
    }}
    BIND(IRI(CONCAT("{dataset_prefix}:class_partition_", MD5(STR(?class)))) AS ?cp)

    # Property partitions with class objects
    OPTIONAL {{
        {{
            SELECT ?c_s ?prop ?c_o (COUNT(*) AS ?prop_count)
            WHERE {{
                {graph_clause_start}
                ?s ?prop ?o .
                ?s a ?c_s .
                ?o a ?c_o .
                {graph_clause_end}
            }}
            GROUP BY ?prop ?c_s ?c_o
        }}
        BIND(IRI(CONCAT("{dataset_prefix}:class_partition_", MD5(STR(?c_s)))) AS ?cps)
        BIND(IRI(CONCAT("{dataset_prefix}:class_partition_", MD5(STR(?c_o)))) AS ?cpo)
        BIND(IRI(CONCAT("{dataset_prefix}:property_partition_", MD5(CONCAT(STR(?c_s), STR(?prop), STR(?c_o))))) AS ?pp)
    }}

    # Property partitions with datatype objects
    OPTIONAL {{
        {{
            SELECT ?c_s_dt ?prop_dt ?dt (COUNT(*) AS ?dt_count)
            WHERE {{
                {graph_clause_start}
                ?s ?prop_dt ?o .
                ?s a ?c_s_dt .
                FILTER(isLITERAL(?o))
                BIND(DATATYPE(?o) AS ?dt)
                {graph_clause_end}
            }}
            GROUP BY ?prop_dt ?c_s_dt ?dt
        }}
        BIND(IRI(CONCAT("{dataset_prefix}:class_partition_", MD5(STR(?c_s_dt)))) AS ?cps_dt)
        BIND(IRI(CONCAT("{dataset_prefix}:datatype_partition_", MD5(STR(?dt)))) AS ?dtp)
        BIND(IRI(CONCAT("{dataset_prefix}:property_partition_", MD5(CONCAT(STR(?c_s_dt), STR(?prop_dt), STR(?dt))))) AS ?pp_dt)
    }}
}}
"""

        try:
            import time

            t0 = time.monotonic()

            logger.debug("Executing unified CONSTRUCT query...")

            # Use SparqlHelper for automatic GET-->POST fallback
            helper = SparqlHelper(endpoint_url)
            void_graph = helper.construct_graph(construct_query)

            dt = time.monotonic() - t0
            logger.info(f"Alternative method completed in {dt:.2f}s")

            if len(void_graph) > 0:
                logger.info(f"Generated {len(void_graph)} VoID triples")
            else:
                logger.warning("No VoID data generated")

        except Exception as e:
            logger.error(f"Alternative method failed: {e}")
            raise RuntimeError(f"Failed to execute alternative VoID generation: {e}")

        # Save to file if specified
        if output_file:
            try:
                void_graph.serialize(destination=output_file, format="turtle")
                logger.info(f"VoID description saved to {output_file}")
            except Exception as e:
                logger.error(f"Failed to save VoID file: {e}")

        return void_graph

    @classmethod
    def from_sparql(
        cls,
        endpoint_url: str,
        graph_uris: Optional[Union[str, List[str]]] = None,
        output_file: Optional[str] = None,
        exclude_graphs: bool = True,
    ) -> "VoidParser":
        """
        Create a VoidParser instance from a SPARQL endpoint.

        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uris: Graph URI(s) for the dataset. If None, queries all
            output_file: Optional output file path for TTL
            exclude_graphs: Whether to exclude Virtuoso system graphs

        Returns:
            VoidParser instance with generated VoID
        """
        void_graph = cls.generate_void_from_sparql(
            endpoint_url,
            graph_uris,
            output_file,
            exclude_graphs=exclude_graphs,
        )
        # Pass graph_uris to preserve context for subsequent operations
        parser = cls(void_graph, graph_uris=graph_uris, exclude_graphs=exclude_graphs)
        return parser

    @classmethod
    def from_endpoint_with_discovery(
        cls,
        endpoint_url: str,
        dataset_name: str,
        exports_path: str,
        prefer_existing: bool = True,
        counts: bool = True,
        offset_limit_steps: Optional[int] = None,
        exclude_graphs: bool = True,
        exclude_graph_patterns: Optional[List[str]] = None,
        graph_uris: Optional[Union[str, List[str]]] = None,
    ) -> "VoidParser":
        """
        Attempt to create VoidParser by discovering existing VoID,
        then generating if needed.

        Args:
            endpoint_url: SPARQL endpoint URL
            dataset_name: Name for the dataset (used for file naming)
            exports_path: Path where VoID files should be saved
            prefer_existing: If True, prefer existing VoID over generation
            sample_limit: Optional LIMIT for sampling (speeds up discovery)
            offset_limit_steps: If provided, use this as both LIMIT and OFFSET step
            exclude_graphs: Whether to exclude Virtuoso system graphs
            graph_uris: Graph URI(s) to analyze. If None, queries all graphs
        Returns:
            VoidParser instance with the best available VoID
        """
        import os

        logger.debug("Starting endpoint discovery process...")
        logger.debug(f"Endpoint: {endpoint_url}")
        logger.debug(f"Dataset: {dataset_name}")
        logger.debug(f" Provided graph URIs: {graph_uris}")
        logger.debug(
            f"Settings: prefer_existing={prefer_existing}, exclude_graphs={exclude_graphs}"
        )

        # If the user provided specific graph URIs, validate they exist.
        # If none exist, log an error and fall back to discovery.
        if graph_uris:
            logger.debug("Validating provided graph URIs...")
            try:
                # Use SparqlHelper for automatic GET-->POST fallback
                helper = SparqlHelper(endpoint_url)

                # Normalize to list
                if isinstance(graph_uris, list):
                    candidate_uris = graph_uris
                else:
                    candidate_uris = [graph_uris]
                valid_uris = []
                for uri in candidate_uris:
                    try:
                        ask_q = f"ASK {{ GRAPH <{uri}> {{ ?s ?p ?o }} }}"
                        exists = helper.ask(ask_q)

                        if exists:
                            valid_uris.append(uri)
                        else:
                            logger.error(
                                "Provided graph URI '%s' not found at '%s'.",
                                uri,
                                endpoint_url,
                            )
                    except Exception as e:
                        logger.error(
                            "Error checking graph URI '%s' at '%s': %s",
                            uri,
                            endpoint_url,
                            e,
                        )

                # If at least one provided graph URI is valid, keep only those.
                if valid_uris:
                    graph_uris = valid_uris
                else:
                    # No valid provided URIs; proceed with discovery
                    logger.info("No provided graph URIs were valid; proceeding with discovery.")
                    graph_uris = None

            except Exception:
                # If validation tooling fails, fall back to discovery
                logger.debug("Graph URI validation failed; continuing.")

        # Step 1: Discover existing VoID graphs
        logger.debug("Step 1: Discovering existing VoID graphs...")
        temp_parser = cls(graph_uris=graph_uris, exclude_graphs=exclude_graphs)
        discovery_result = temp_parser.discover_void_graphs(endpoint_url)

        logger.debug(
            f"Discovery result: found={discovery_result.get('has_void_descriptions', False)}"
        )
        logger.debug(f"Found {len(discovery_result.get('found_graphs', []))} graphs with VoID data")

        existing_void_graph = None
        existing_parser = None

        if discovery_result.get("has_void_descriptions", False):
            logger.debug("Found existing VoID descriptions")
            valid_void_graphs = [
                graph_uri
                for graph_uri, content in discovery_result.get("void_content", {}).items()
                if content.get("has_any_partitions", False)
            ]

            logger.debug(f" Valid VoID graphs: {len(valid_void_graphs)}")

            if valid_void_graphs:
                valid_str = ", ".join(valid_void_graphs)
                logger.info("Retrieving VoID from %s", valid_str)
                logger.debug("Starting VoID retrieval process...")
                existing_void_graph = temp_parser.void_querier(endpoint_url, valid_void_graphs)

                if len(existing_void_graph) > 0:
                    existing_parser = cls(
                        existing_void_graph, graph_uris=graph_uris, exclude_graphs=exclude_graphs
                    )
                    existing_schema_df = existing_parser.to_schema(filter_void_admin_nodes=True)

                    # Check if existing VoID has sufficient content
                    if prefer_existing and len(existing_schema_df) > 3:
                        len_df = len(existing_schema_df)
                        logger.info("Using existing VoID with \n%d schema triples", len_df)

                        # Save existing VoID
                        existing_void_path = os.path.join(
                            exports_path, f"{dataset_name}_existing_void.ttl"
                        )
                        existing_void_graph.serialize(
                            destination=existing_void_path, format="turtle"
                        )

                        return existing_parser

        # Step 2: Generate new VoID if no suitable existing VoID found
        logger.info("No suitable VoID found; generating from queries...")
        logger.debug("Step 2: Generating new VoID from SPARQL queries...")
        logger.debug(
            f"Generation settings: counts={counts}, offset_limit_steps={offset_limit_steps}"
        )

        output_path = os.path.join(exports_path, f"{dataset_name}_generated_void.ttl")
        logger.debug(f"Output path: {output_path}")

        logger.debug("Starting VoID generation process...")
        generated_void_graph = cls.generate_void_from_sparql(
            endpoint_url=endpoint_url,
            graph_uris=graph_uris,
            output_file=output_path,
            counts=counts,
            offset_limit_steps=offset_limit_steps,
            exclude_graphs=exclude_graphs,
            exclude_graph_patterns=exclude_graph_patterns,
        )
        logger.debug("VoID generation completed")

        return cls(
            generated_void_graph,
            graph_uris=graph_uris,
            exclude_graphs=exclude_graphs,
        )

    def count_instances_per_class(
        self,
        endpoint_url: str,
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
        chunk_size: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
        delay_between_chunks: float = 20.0,
        streaming: bool = False,
    ) -> Union[Dict[str, int], Any]:
        """
        Count instances for each class in the dataset.

        Uses SparqlHelper for automatic GET-->POST fallback on endpoints that
        require POST.

        Args:
            endpoint_url: SPARQL endpoint URL
            sample_limit: Optional limit for total results (None = all results)
            sample_offset: Optional starting offset for pagination
            chunk_size: Optional size for chunked querying (enables pagination)
            offset_limit_steps: If provided, use this as both LIMIT and OFFSET step
            delay_between_chunks: Seconds to wait between chunk queries (default: 20.0)
            streaming: If True, return generator yielding (class_uri, count) tuples

        Returns:
            Dictionary mapping class URIs to instance counts,
            or generator if streaming=True
        """
        # Create SparqlHelper - handles GET-->POST fallback automatically
        helper = SparqlHelper(endpoint_url)

        # If offset_limit_steps is provided, use it for chunked querying
        if offset_limit_steps is not None:
            offset_value = sample_offset if sample_offset is not None else 0
            if streaming:
                return self._count_instances_chunked_streaming(
                    helper,
                    sample_limit,
                    offset_value,
                    offset_limit_steps,
                    delay_between_chunks,
                )
            else:
                return self._count_instances_chunked(
                    helper,
                    sample_limit,
                    offset_value,
                    offset_limit_steps,
                    delay_between_chunks,
                )
        # If chunk_size is provided, use chunked querying
        elif chunk_size is not None:
            offset_value = sample_offset if sample_offset is not None else 0
            if streaming:
                return self._count_instances_chunked_streaming(
                    helper, sample_limit, offset_value, chunk_size, delay_between_chunks
                )
            else:
                return self._count_instances_chunked(
                    helper, sample_limit, offset_value, chunk_size, delay_between_chunks
                )

        # Otherwise, use single query with limit/offset
        # Build limit and offset clause
        limit_offset_clause = ""
        if sample_offset is not None:
            limit_offset_clause += f"OFFSET {sample_offset}\n        "
        if sample_limit is not None:
            limit_offset_clause += f"LIMIT {sample_limit}"

        query_template = f"""
        SELECT ?class (COUNT(DISTINCT ?instance) AS ?count) WHERE {{
            #GRAPH_CLAUSE
                ?instance a ?class .
            #END_GRAPH_CLAUSE
        }}
        GROUP BY ?class
        ORDER BY DESC(?count)
        {limit_offset_clause}
        """

        query = self._replace_graph_clause_placeholder(query_template)

        try:
            results = helper.select(query, purpose="coverage/class")

            if streaming:
                # Return generator for single query case
                def _stream_results() -> Any:
                    for result in results["results"]["bindings"]:
                        class_uri = result["class"]["value"]
                        count = int(result["count"]["value"])
                        yield (class_uri, count)

                return _stream_results()
            else:
                # Return dictionary as before
                instance_counts = {}
                for result in results["results"]["bindings"]:
                    class_uri = result["class"]["value"]
                    count = int(result["count"]["value"])
                    instance_counts[class_uri] = count
                return instance_counts

        except Exception:
            if streaming:
                return iter([])  # Empty generator
            else:
                return {}

    def _count_instances_chunked(
        self,
        helper: SparqlHelper,
        total_limit: Optional[int],
        start_offset: int,
        chunk_size: int,
        delay_between_chunks: float = 1.0,
    ) -> Dict[str, int]:
        """Helper method for chunked instance counting using SparqlHelper."""
        return dict(
            self._count_instances_chunked_streaming(
                helper, total_limit, start_offset, chunk_size, delay_between_chunks
            )
        )

    def _count_instances_chunked_streaming(
        self,
        helper: SparqlHelper,
        total_limit: Optional[int],
        start_offset: int,
        chunk_size: int,
        delay_between_chunks: float = 1.0,
    ) -> Any:
        """
        Streaming version of chunked instance counting that yields results
        as they arrive. Uses SparqlHelper for automatic GET-->POST fallback.

        Yields:
            Tuple[str, int]: (class_uri, count) pairs for immediate processing
        """
        import time

        current_offset = start_offset or 0
        total_classes_fetched = 0

        while True:
            # Calculate how many classes to fetch in this chunk
            if total_limit is not None:
                remaining = total_limit - total_classes_fetched
                if remaining <= 0:
                    break
                current_chunk_size = min(chunk_size, remaining)
            else:
                current_chunk_size = chunk_size

            query_template = f"""
            SELECT ?class (COUNT(DISTINCT ?instance) AS ?count) WHERE {{
                #GRAPH_CLAUSE
                    ?instance a ?class .
                #END_GRAPH_CLAUSE
            }}
            GROUP BY ?class
            ORDER BY DESC(?count)
            OFFSET {current_offset}
            LIMIT {current_chunk_size}
            """

            query = self._replace_graph_clause_placeholder(query_template)

            try:
                # Use SparqlHelper - handles GET-->POST fallback automatically
                results = helper.select(query, purpose="coverage/class-chunked")
                chunk_results = results["results"]["bindings"]

                # If no results, we've reached the end
                if not chunk_results:
                    break

                # Yield results immediately as they're processed
                classes_in_chunk = 0
                for result in chunk_results:
                    class_uri = result["class"]["value"]
                    count = int(result["count"]["value"])
                    yield (class_uri, count)
                    classes_in_chunk += 1

                total_classes_fetched += classes_in_chunk
                current_offset += classes_in_chunk

                # If we got fewer results than requested, we've reached the end
                if classes_in_chunk < current_chunk_size:
                    break

                # Add delay between chunks to be respectful to the endpoint
                if delay_between_chunks > 0:
                    time.sleep(delay_between_chunks)

            except Exception as e:
                logger.info(f"Failed to fetch chunk at offset {current_offset}: {e}")
                break

    def _execute_chunked_query(
        self,
        helper: SparqlHelper,
        query_template: str,
        chunk_size: int = 100,
        max_total_results: Optional[int] = None,
        delay_between_chunks: float = 0.5,
    ) -> Any:
        """
        Execute a SPARQL query in chunks using OFFSET/LIMIT pagination.

        Uses SparqlHelper for automatic GET-->POST fallback on endpoints that
        require POST.

        Args:
            helper: SparqlHelper instance for executing queries
            query_template: SPARQL query with {offset} and {limit} placeholders
            chunk_size: Number of results per chunk
            max_total_results: Maximum total results to fetch
            delay_between_chunks: Sleep time between chunks (seconds)

        Yields:
            Results from each chunk
        """
        current_offset = 0
        total_fetched = 0
        max_iterations = 1000  # Prevent infinite loops
        iteration_count = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                logger.error(f"Chunked query exceeded maximum iterations ({max_iterations})")
                raise RuntimeError(f"Chunked query exceeded maximum iterations: {max_iterations}")
            # Calculate chunk size for this iteration
            if max_total_results is not None:
                remaining = max_total_results - total_fetched
                if remaining <= 0:
                    break
                current_chunk_size = min(chunk_size, remaining)
            else:
                current_chunk_size = chunk_size

            # Format query with current pagination
            query = query_template.format(offset=current_offset, limit=current_chunk_size)

            try:
                logger.debug(
                    "Executing chunked query: offset=%d, limit=%d",
                    current_offset,
                    current_chunk_size,
                )
                # SparqlHelper handles GET-->POST fallback and retries
                results = helper.select(query, purpose="coverage/property-chunked")

                # Extract bindings
                bindings = results.get("results", {}).get("bindings", [])

                if not bindings:
                    logger.debug(" No more results, pagination complete")
                    break

                # Yield this chunk's results
                yield bindings

                # Update counters
                chunk_count = len(bindings)
                total_fetched += chunk_count
                current_offset += chunk_count

                logger.debug(f"Fetched chunk: {chunk_count} results (total: {total_fetched})")

                # If chunk was smaller than requested, we've reached the end
                if chunk_count < current_chunk_size:
                    logger.debug("Partial chunk received, pagination complete")
                    break

                # Respectful delay between chunks
                if delay_between_chunks > 0:
                    time.sleep(delay_between_chunks)

            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)

                logger.warning(f"Chunk query failed at offset {current_offset}")
                logger.debug("Query error details:")
                logger.debug(f"   • Error type: {error_type}")
                logger.debug(f"   • Error message: {error_msg}")
                logger.debug(f"   • Query: \n{query}")

                # Check for common SPARQL issues
                if "syntax" in error_msg.lower() or "parse" in error_msg.lower():
                    logger.debug("Likely SPARQL syntax error - check query formatting")
                elif "timeout" in error_msg.lower():
                    logger.debug("Query timeout - consider reducing chunk size")
                elif "connection" in error_msg.lower():
                    logger.debug("Network/connection issue with SPARQL endpoint")

                break

    @staticmethod
    def _safe_query(
        sparql: Any,
        query: str,
        max_retries: int = 4,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ) -> Any:
        """
        DEPRECATED: This method is only kept for backward compatibility.

        All internal code now uses SparqlHelper which handles automatic
        GET-->POST fallback, retries, and proper error handling.

        Execute a SPARQL query with retries and exponential backoff.
        Handles transient failures (connection resets, RemoteDisconnected,
        temporary endpoint failures). The function will raise the last
        exception if all retries fail.
        """
        import random
        import time

        attempt = 0
        while True:
            attempt += 1
            try:
                logger.debug(f"Setting query (attempt {attempt}/{max_retries})")
                if attempt == 1:
                    # Log full query on first attempt only
                    logger.info(f"Executing query:\n{query}")
                    logger.info(f"Using method: {sparql.method}")
                sparql.setQuery(query)
                results = sparql.query().convert()
                return results
            except Exception as e:
                err_text = str(e)
                logger.warning("Query attempt %d failed: %s", attempt, err_text)
                logger.debug("Query:\n%s", query)
                if attempt >= max_retries:
                    logger.error("Query failed after %d attempts", attempt)
                    raise
                # exponential backoff with jitter
                backoff = min(initial_backoff * (2 ** (attempt - 1)), max_backoff)
                jitter = random.uniform(0, backoff * 0.1)
                sleep_time = backoff + jitter
                logger.info(
                    "Retrying after %.1fs (attempt %d/%d)", sleep_time, attempt + 1, max_retries
                )
                time.sleep(sleep_time)

    def analyze_class_partition_usage(
        self,
        endpoint_url: str,
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
    ) -> tuple[Any, ...]:
        """
        Complete analysis of class partition usage and coverage.

        Args:
            endpoint_url: SPARQL endpoint URL
            sample_limit: Optional limit for sampling
            sample_offset: Optional offset for pagination
            offset_limit_steps: If provided, use this as both LIMIT and OFFSET step

        Returns:
            Tuple of (instance_counts, class_mappings, coverage_stats)
        """
        instance_counts = self.count_instances_per_class(
            endpoint_url,
            sample_limit=sample_limit,
            sample_offset=sample_offset,
            offset_limit_steps=offset_limit_steps,
        )

        # For chunked queries, skip class mappings as they can be too large
        if offset_limit_steps is not None:
            class_mappings: Dict[str, Any] = {}
            # Create simplified coverage stats without detailed mappings
            coverage_stats: Dict[str, Any] = {}
            for class_uri, count in instance_counts.items():
                coverage_stats[class_uri] = {
                    "total_instances": count,
                    "instances_in_partitions": count,  # Assume full coverage
                    "partition_occurrences": count,
                    "occurrence_coverage_percent": 100.0,
                    "avg_occurrences_per_instance": 1.0,
                }
        else:
            # Use placeholder for detailed mappings (implementation can be added later)
            class_mappings = {}
            coverage_stats = {}
            for class_uri, count in instance_counts.items():
                coverage_stats[class_uri] = {
                    "total_instances": count,
                    "instances_in_partitions": count,
                    "partition_occurrences": count,
                    "occurrence_coverage_percent": 100.0,
                    "avg_occurrences_per_instance": 1.0,
                }

        return instance_counts, class_mappings, coverage_stats

    def export_coverage_analysis(
        self, coverage_stats: Dict[str, Any], output_file: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Export coverage analysis to CSV format.

        Args:
            coverage_stats: Dictionary with coverage statistics
            output_file: Optional output CSV file path

        Returns:
            DataFrame with coverage analysis
        """
        coverage_data = []

        for class_uri, stats in coverage_stats.items():
            # Extract readable class name
            class_name = (
                class_uri.split("#")[-1].split("/")[-1]
                if "#" in class_uri or "/" in class_uri
                else class_uri
            )

            coverage_data.append(
                {
                    "class_name": class_name,
                    "class_uri": class_uri,
                    "total_instances": stats["total_instances"],
                    "instances_in_partitions": stats["instances_in_partitions"],
                    "partition_occurrences": stats["partition_occurrences"],
                    "occurrence_coverage_percent": stats["occurrence_coverage_percent"],
                    "avg_occurrences_per_instance": stats["avg_occurrences_per_instance"],
                }
            )

        df = pd.DataFrame(coverage_data)
        df = df.sort_values("total_instances", ascending=False)

        if output_file:
            df.to_csv(output_file, index=False)

        return df

    def count_schema_shape_frequencies(
        self,
        endpoint_url: str,
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
        collect_instances: bool = False,
        batch_size: int = 20,
        track_queries: bool = False,
        delay_between_chunks: float = 10.0,
    ) -> tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Calculate schema pattern coverage: for each subject class, how many entities
        actually use each s,p,o pattern divided by total entities of that class.

        This gives coverage ratios showing what percentage of entities of each class
        type actually participate in each schema relationship pattern.

        Args:
            endpoint_url: SPARQL endpoint URL
            sample_limit: Optional limit for entity counting only
            sample_offset: Optional offset for pagination
            offset_limit_steps: If provided, use chunked pagination
            collect_instances: If True, store IRIs in instances_df
            batch_size: Size of batches for VALUES queries (default: 20)
            track_queries: If True, collect detailed query statistics
            delay_between_chunks: Seconds between chunked queries (10.0)

        Returns:
            Tuple of (frequencies_df, instances_df):
            - frequencies_df: DataFrame with schema pattern coverage ratios and counts
            - instances_df: Optional DataFrame with actual subject/object IRIs linked by shape_id
        """
        # Initialize query tracking (only if requested)
        total_queries_sent: int = 0
        total_rows_collected: int = 0
        query_details: Optional[List[Dict[str, Any]]] = [] if track_queries else None

        # Get the schema triples first
        schema_df = self.to_schema(filter_void_admin_nodes=True)

        if schema_df.empty:
            return pd.DataFrame(), None

        # First, get total entity counts using batched VALUES queries
        class_entity_counts: Dict[str, int] = {}

        # Use SparqlHelper for automatic GET-->POST fallback
        helper = SparqlHelper(endpoint_url)

        # Get unique subject class URIs and batch them
        subject_class_uris = list(schema_df["subject_uri"].unique())

        # Use chunked pagination for entity counts if offset_limit_steps is provided
        if offset_limit_steps is not None:
            logger.info(
                f"Using chunked pagination for entity counts (step size: {offset_limit_steps})"
            )
            class_entity_counts = self._count_entity_classes_chunked(
                helper,
                subject_class_uris,
                offset_limit_steps,
                sample_limit,
                delay_between_chunks,
                track_queries,
                query_details,
            )
            if track_queries:
                total_queries_sent += len(subject_class_uris)  # Approximation
        else:
            # Original batched VALUES approach for non-chunked queries
            class_entity_counts = self._count_entity_classes_batched(
                helper,
                subject_class_uris,
                batch_size,
                sample_limit,
                track_queries,
                query_details,
            )
            if track_queries and query_details:
                total_queries_sent += len(
                    [q for q in query_details if q["query_type"] == "entity_count_batch"]
                )

        # Now calculate coverage for each schema pattern
        coverage_results: List[Dict[str, Any]] = []
        instances_data: Optional[List[Dict[str, Any]]] = [] if collect_instances else None

        for idx, row in schema_df.iterrows():
            # Generate unique shape ID for linking DataFrames
            shape_id = f"shape_{idx:06d}"
            subject_class_uri = row["subject_uri"]
            property_uri = row["property_uri"]
            object_class_uri = row["object_uri"]

            total_entities = class_entity_counts.get(subject_class_uri, 0)

            if total_entities == 0:
                coverage_results.append(
                    {
                        "shape_id": shape_id,
                        "subject_class": row["subject_class"],
                        "subject_uri": subject_class_uri,
                        "property": row["property"],
                        "property_uri": property_uri,
                        "object_class": row["object_class"],
                        "object_uri": object_class_uri,
                        "total_entities": 0,
                        # "participating_entities": 0,
                        "occurrence_count": 0,
                        "coverage_ratio": 0.0,
                        "coverage_percent": 0.0,
                        "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                    }
                )
                continue

            # Count entities that participate in this pattern
            literal_classes = ["Literal", "http://www.w3.org/2000/01/rdf-schema#Literal"]
            resource_classes = ["Resource", "http://www.w3.org/2000/01/rdf-schema#Resource"]

            if object_class_uri in literal_classes + resource_classes:
                # For literals and untyped resources
                if object_class_uri in literal_classes:
                    # Specifically for literals
                    object_filter = "FILTER(isLiteral(?o))"
                else:
                    # For untyped resources (rdfs:Resource)
                    object_filter = "FILTER(isURI(?o) && !EXISTS { ?o a ?any_type })"

                # Always use SELECT DISTINCT (not COUNT) - it's more reliable
                pattern_query = f"""
                SELECT DISTINCT ?s ?o WHERE {{
                    #GRAPH_CLAUSE
                        ?s <{property_uri}> ?o .
                        ?s a <{subject_class_uri}> .
                        {object_filter}
                    #END_GRAPH_CLAUSE
                }}
                """
            else:
                # For object properties: ?s ?p ?o where ?s a ?s_shape and ?o a ?o_shape
                # Always use SELECT DISTINCT (not COUNT) - it's more reliable
                pattern_query = f"""
                SELECT DISTINCT ?s ?o WHERE {{
                    #GRAPH_CLAUSE
                        ?s <{property_uri}> ?o .
                        ?s a <{subject_class_uri}> .
                        ?o a <{object_class_uri}> .
                    #END_GRAPH_CLAUSE
                }}
                """

            # NOTE: sample_limit is ONLY used for entity counting, not for pattern queries
            # Pattern queries must run without LIMIT to get accurate occurrence counts
            query = self._replace_graph_clause_placeholder(pattern_query)

            try:
                results = helper.select(query, purpose="coverage/pattern")
                if track_queries:
                    total_queries_sent += 1

                participating = 0
                rows_in_pattern = 0
                # Check if bindings key exists (not if it's truthy - empty list is valid)
                has_bindings = results and "results" in results and "bindings" in results["results"]
                if has_bindings:
                    rows_in_pattern = len(results["results"]["bindings"])
                    if track_queries:
                        total_rows_collected += rows_in_pattern

                    # Always count from actual results (works for both modes)
                    unique_subjects = set()
                    for binding in results["results"]["bindings"]:
                        subject_iri = binding["s"]["value"]
                        object_iri = binding["o"]["value"]
                        unique_subjects.add(subject_iri)

                        # Store instances if requested
                        if collect_instances and instances_data is not None:
                            instances_data.append(
                                {
                                    "shape_id": shape_id,
                                    "subject_iri": subject_iri,
                                    "object_iri": object_iri,
                                }
                            )
                    participating = len(unique_subjects)

                    # Debug logging
                    if participating == 0 and rows_in_pattern > 0:
                        logger.warning(
                            "Pattern has %d rows but 0 participating: %s",
                            rows_in_pattern,
                            shape_id,
                        )

                if track_queries and query_details is not None:
                    query_details.append(
                        {
                            "query_type": "pattern_query",
                            "shape_id": shape_id,
                            "pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                            "collect_instances": collect_instances,
                            "rows_returned": rows_in_pattern,
                            "participating_entities": participating,
                        }
                    )

                # Ensure participating count cannot exceed total entities
                # This handles edge cases in sampling or query inconsistencies
                if participating > total_entities and total_entities > 0:
                    participating = min(participating, total_entities)

                # Calculate coverage ratio and percentage
                coverage_ratio = participating / total_entities if total_entities > 0 else 0
                coverage_percent = coverage_ratio * 100

                coverage_results.append(
                    {
                        "shape_id": shape_id,
                        "subject_class": row["subject_class"],
                        "subject_uri": subject_class_uri,
                        "property": row["property"],
                        "property_uri": property_uri,
                        "object_class": row["object_class"],
                        "object_uri": object_class_uri,
                        "total_entities": total_entities,
                        "participating_entities": participating,
                        "occurrence_count": participating,
                        "coverage_ratio": round(coverage_ratio, 4),
                        "coverage_percent": round(coverage_percent, 2),
                        "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                    }
                )

            except Exception as e:
                # Query execution failed - log and record error
                logger.error(
                    "Pattern query failed for %s: %s",
                    shape_id,
                    str(e),
                )

                if track_queries:
                    total_queries_sent += 1
                    if query_details is not None:
                        query_details.append(
                            {
                                "query_type": "pattern_query",
                                "shape_id": shape_id,
                                "pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                                "collect_instances": collect_instances,
                                "rows_returned": 0,
                                "participating_entities": 0,
                                "error": str(e),
                            }
                        )

                coverage_results.append(
                    {
                        "shape_id": shape_id,
                        "subject_class": row["subject_class"],
                        "subject_uri": subject_class_uri,
                        "property": row["property"],
                        "property_uri": property_uri,
                        "object_class": row["object_class"],
                        "object_uri": object_class_uri,
                        "total_entities": total_entities,
                        # "participating_entities": 0,
                        "occurrence_count": 0,
                        "coverage_ratio": 0.0,
                        "coverage_percent": 0.0,
                        "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                        "error": str(e),
                    }
                )

        # Convert to DataFrame and sort by coverage ratio
        frequencies_df = pd.DataFrame(coverage_results)
        if not frequencies_df.empty:
            frequencies_df = frequencies_df.sort_values("coverage_ratio", ascending=False)

        # Create instances DataFrame if collected
        instances_df = None
        if collect_instances and instances_data:
            instances_df = pd.DataFrame(instances_data)
            # Optimize memory by using categorical data types for shape_id
            # No need to store subject_class, property, object_class as they're in frequencies_df
            instances_df = instances_df.astype({"shape_id": "category"})

        # Report query statistics (only if tracking was enabled)
        if track_queries:
            logger.info("Query Statistics:")
            logger.info("Total queries sent: %s", total_queries_sent)
            logger.info("Total rows collected: %s", total_rows_collected)

            if query_details:
                # Break down by query type
                entity_count_queries = [
                    q for q in query_details if q["query_type"] == "entity_count_batch"
                ]
                pattern_queries = [q for q in query_details if q["query_type"] == "pattern_query"]
                error_queries = [q for q in query_details if q.get("error")]

                logger.info("Entity count batch queries: %s", len(entity_count_queries))
                if entity_count_queries:
                    entity_count_rows = sum(q["rows_returned"] for q in entity_count_queries)
                    logger.info("Rows from entity counts: %s", entity_count_rows)

                logger.info("Pattern queries: %s", len(pattern_queries))
                if pattern_queries:
                    pattern_rows = sum(q["rows_returned"] for q in pattern_queries)
                    logger.info("Rows from patterns: %s", pattern_rows)

                if error_queries:
                    logger.warning("Queries with errors: %s", len(error_queries))

                # Detailed per-query report (limit to first 200 to avoid flooding)
                logger.info("Per-query details (first 200 shown):")
                for qi, q in enumerate(query_details[:200], start=1):
                    qtype = q.get("query_type")
                    rows = q.get("rows_returned", 0)
                    err = q.get("error")
                    if qtype == "entity_count_batch":
                        batch = q.get("batch_number")
                        classes = q.get("classes_in_batch")
                        logger.info(
                            "[%03d] entity_count_batch batch=%s classes=%s rows=%s%s",
                            qi,
                            batch,
                            classes,
                            rows,
                            " ERROR" if err else "",
                        )
                    else:
                        shape = q.get("shape_id", q.get("pattern", "unknown"))
                        part = q.get("participating_entities", 0)
                        coll = q.get("collect_instances", False)
                        logger.info(
                            "[%03d] pattern_query shape=%s "
                            "collect_instances=%s rows=%s "
                            "participating=%s%s",
                            qi,
                            shape,
                            coll,
                            rows,
                            part,
                            " ERROR" if err else "",
                        )

                if len(query_details) > 200:
                    remaining = len(query_details) - 200
                    logger.info("... (and %s more queries not shown)", remaining)

        if collect_instances and instances_data:
            logger.info("Total instances collected: %s", len(instances_data))

        return frequencies_df, instances_df

    def _count_entity_classes_batched(
        self,
        helper: SparqlHelper,
        subject_class_uris: List[str],
        batch_size: int,
        sample_limit: Optional[int],
        track_queries: bool,
        query_details: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, int]:
        """
        Count entities per class using batched VALUES queries.

        This is efficient for most endpoints when pagination isn't needed.

        Args:
            helper: SparqlHelper instance
            subject_class_uris: List of class URIs to count
            batch_size: Number of classes per batch
            sample_limit: Optional LIMIT clause
            track_queries: Whether to track query statistics
            query_details: List to append query details to

        Returns:
            Dictionary mapping class URIs to instance counts
        """
        class_entity_counts: Dict[str, int] = {}

        for i in range(0, len(subject_class_uris), batch_size):
            batch_uris = subject_class_uris[i : i + batch_size]

            # Create VALUES clause for this batch
            values_clause = " ".join([f"<{uri}>" for uri in batch_uris])

            # Build the base query
            count_query = f"""
            SELECT ?class (COUNT(DISTINCT ?s) AS ?total) WHERE {{
                VALUES ?class {{ {values_clause} }}
                #GRAPH_CLAUSE
                    ?s a ?class .
                #END_GRAPH_CLAUSE
            }}
            GROUP BY ?class
            """

            # Add LIMIT clause if sample_limit is specified
            if sample_limit is not None:
                count_query += f"LIMIT {sample_limit}"

            query = self._replace_graph_clause_placeholder(count_query)

            try:
                results = helper.select(query, purpose="coverage/entity-count")

                # Initialize all classes in this batch to 0 first
                for class_uri in batch_uris:
                    class_entity_counts[class_uri] = 0

                # Update with actual counts from results
                rows_in_batch = 0
                if results and results.get("results", {}).get("bindings"):
                    rows_in_batch = len(results["results"]["bindings"])
                    for binding in results["results"]["bindings"]:
                        class_uri = binding["class"]["value"]
                        total = int(binding["total"]["value"])
                        class_entity_counts[class_uri] = total

                if track_queries and query_details is not None:
                    query_details.append(
                        {
                            "query_type": "entity_count_batch",
                            "batch_number": (i // batch_size) + 1,
                            "classes_in_batch": len(batch_uris),
                            "rows_returned": rows_in_batch,
                        }
                    )

            except Exception as e:
                logger.warning(f"Batch entity count failed: {e}")
                if track_queries and query_details is not None:
                    query_details.append(
                        {
                            "query_type": "entity_count_batch",
                            "batch_number": (i // batch_size) + 1,
                            "classes_in_batch": len(batch_uris),
                            "rows_returned": 0,
                            "error": True,
                        }
                    )
                # If batch fails, initialize all classes in batch to 0
                for class_uri in batch_uris:
                    class_entity_counts[class_uri] = 0

        return class_entity_counts

    def _count_entity_classes_chunked(
        self,
        helper: SparqlHelper,
        subject_class_uris: List[str],
        chunk_size: int,
        sample_limit: Optional[int],
        delay_between_chunks: float,
        track_queries: bool,
        query_details: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, int]:
        """
        Count entities per class using chunked OFFSET/LIMIT pagination.

        This is useful for endpoints that have result size limits or timeouts.

        Args:
            helper: SparqlHelper instance
            subject_class_uris: List of class URIs to count
            chunk_size: OFFSET/LIMIT step size for pagination
            sample_limit: Maximum total results to fetch
            delay_between_chunks: Seconds to wait between chunks
            track_queries: Whether to track query statistics
            query_details: List to append query details to

        Returns:
            Dictionary mapping class URIs to instance counts
        """
        class_entity_counts: Dict[str, int] = {}

        # Initialize all classes to 0
        for class_uri in subject_class_uris:
            class_entity_counts[class_uri] = 0

        # Build base query and replace graph clause markers
        base_query = """
        SELECT ?class (COUNT(DISTINCT ?s) AS ?total) WHERE {
            #GRAPH_CLAUSE
                ?s a ?class .
            #END_GRAPH_CLAUSE
        }
        GROUP BY ?class
        ORDER BY DESC(?total)
        """

        base_query = self._replace_graph_clause_placeholder(base_query)

        # Use SparqlHelper's utility to prepare for pagination
        # This escapes SPARQL braces and adds OFFSET/LIMIT placeholders
        query_template = SparqlHelper.prepare_paginated_query(base_query)

        total_fetched = 0
        chunk_number = 0

        for bindings in helper.select_chunked(
            query_template,
            chunk_size=chunk_size,
            max_total_results=sample_limit,
            delay_between_chunks=delay_between_chunks,
        ):
            chunk_number += 1
            rows_in_chunk = len(bindings)
            total_fetched += rows_in_chunk

            for binding in bindings:
                class_uri = binding["class"]["value"]
                total = int(binding["total"]["value"])
                # Only update if this class is in our list of interest
                if class_uri in class_entity_counts:
                    class_entity_counts[class_uri] = total
                else:
                    # Add classes we discover that weren't in the schema
                    class_entity_counts[class_uri] = total

            if track_queries and query_details is not None:
                query_details.append(
                    {
                        "query_type": "entity_count_chunked",
                        "chunk_number": chunk_number,
                        "rows_returned": rows_in_chunk,
                        "total_fetched": total_fetched,
                    }
                )

            logger.debug(
                f"Chunked entity count: chunk {chunk_number}, "
                f"rows={rows_in_chunk}, total={total_fetched}"
            )

        logger.info(
            f"Chunked entity counting complete: {chunk_number} chunks, "
            f"{total_fetched} total results"
        )

        return class_entity_counts

    def export_schema_shape_frequencies(
        self, frequencies_df: pd.DataFrame, output_file: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Export schema shape frequency analysis to CSV format.

        Args:
            frequencies_df: DataFrame with shape frequencies from count_schema_shape_frequencies
            output_file: Optional output CSV file path

        Returns:
            DataFrame with formatted frequency analysis
        """
        if frequencies_df.empty:
            return pd.DataFrame()

        # Save to file if specified
        if output_file:
            frequencies_df.to_csv(output_file, index=False)

        return frequencies_df

    def get_shape_instances(
        self,
        frequencies_df: pd.DataFrame,
        instances_df: pd.DataFrame,
        shape_pattern: Optional[str] = None,
        min_coverage: Optional[float] = None,
    ) -> pd.DataFrame:
        """
        Efficiently link and filter shape frequencies with their actual instances.

        Args:
            frequencies_df: Shape frequency DataFrame from count_schema_shape_frequencies
            instances_df: Instances DataFrame from count_schema_shape_frequencies
            shape_pattern: Optional pattern to filter (e.g., "Gene -> hasFunction -> Function")
            min_coverage: Optional minimum coverage ratio filter

        Returns:
            Combined DataFrame with shape info and instance IRIs
        """
        if frequencies_df.empty or instances_df is None or instances_df.empty:
            return pd.DataFrame()

        # Apply filters to frequencies first (more efficient)
        filtered_freq = frequencies_df.copy()

        if shape_pattern:
            filtered_freq = filtered_freq[
                filtered_freq["shape_pattern"].str.contains(shape_pattern, case=False, na=False)
            ]

        if min_coverage is not None:
            filtered_freq = filtered_freq[filtered_freq["coverage_ratio"] >= min_coverage]

        if filtered_freq.empty:
            return pd.DataFrame()

        # Get shape_ids of interest
        shape_ids = set(filtered_freq["shape_id"])

        # Filter instances to matching shapes (using categorical index for speed)
        filtered_instances = instances_df[instances_df["shape_id"].isin(shape_ids)]

        # Merge on shape_id to get combined view
        # Use left join to keep all frequency data even if no instances
        result = filtered_freq.merge(
            filtered_instances, on="shape_id", how="left", suffixes=("", "_instance")
        )

        return result

    def analyze_shape_distribution(
        self, frequencies_df: pd.DataFrame, instances_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Analyze the distribution of instances across shapes for memory optimization insights.

        Returns:
            Dictionary with distribution statistics
        """
        if frequencies_df.empty or instances_df is None or instances_df.empty:
            return {}

        # Count instances per shape
        instance_counts = instances_df.groupby("shape_id").size()

        # Get shape info
        shape_info = frequencies_df.set_index("shape_id")[["shape_pattern", "coverage_ratio"]]

        # Combine for analysis
        combined = pd.concat([instance_counts.rename("actual_instances"), shape_info], axis=1)

        return {
            "total_shapes": len(frequencies_df),
            "shapes_with_instances": len(combined),
            "total_instance_records": len(instances_df),
            "avg_instances_per_shape": instance_counts.mean(),
            "max_instances_per_shape": instance_counts.max(),
            "memory_usage_mb": {
                "frequencies_df": frequencies_df.memory_usage(deep=True).sum() / 1024 / 1024,
                "instances_df": instances_df.memory_usage(deep=True).sum() / 1024 / 1024,
            },
            "top_shapes_by_instances": combined.nlargest(10, "actual_instances").to_dict("index"),
        }

    def create_instances_index(
        self, instances_df: pd.DataFrame
    ) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """
        Create optimized hash-based index structures for instances.

        This creates two inverted index structures:
        1. subject_index: Maps each unique subject IRI to list of shape_ids where it appears as subject
        2. object_index: Maps each unique object IRI to list of shape_ids where it appears as object

        This structure is much more memory-efficient than the full DataFrame when
        you need to query "which shapes does this IRI participate in?"

        Args:
            instances_df: DataFrame with shape_id, subject_iri, object_iri columns

        Returns:
            Tuple of (subject_index, object_index) dictionaries
        """
        if instances_df is None or instances_df.empty:
            return {}, {}

        subject_index: Dict[str, List[str]] = {}
        object_index: Dict[str, List[str]] = {}

        # Group by IRI to build inverted index
        for _, row in instances_df.iterrows():
            shape_id = row["shape_id"]
            subject_iri = row["subject_iri"]
            object_iri = row["object_iri"]

            # Add to subject index
            if subject_iri not in subject_index:
                subject_index[subject_iri] = []
            subject_index[subject_iri].append(shape_id)

            # Add to object index
            if object_iri not in object_index:
                object_index[object_iri] = []
            object_index[object_iri].append(shape_id)

        return subject_index, object_index

    def export_instances_compact(
        self,
        instances_df: pd.DataFrame,
        output_prefix: str,
    ) -> Dict[str, Any]:
        """
        Export instances data in a compact, memory-efficient format.

        Creates three files:
        1. {prefix}_subject_index.json: Maps subject IRIs to shape_ids
        2. {prefix}_object_index.json: Maps object IRIs to shape_ids
        3. {prefix}_instances_minimal.jsonl: JSON Lines file with core data

        All files are text-based for portability and easy inspection.

        Args:
            instances_df: DataFrame with instance data
            output_prefix: Path prefix for output files (without extension)

        Returns:
            Dictionary with paths to created files and their sizes
        """
        import json
        import os

        if instances_df is None or instances_df.empty:
            return {}

        # Create indices
        subject_index, object_index = self.create_instances_index(instances_df)

        # Export subject index
        subject_path = f"{output_prefix}_subject_index.json"
        with open(subject_path, "w") as f:
            json.dump(subject_index, f, indent=2)

        # Export object index
        object_path = f"{output_prefix}_object_index.json"
        with open(object_path, "w") as f:
            json.dump(object_index, f, indent=2)

        # Export minimal instances data as JSON Lines
        jsonl_path = f"{output_prefix}_instances_minimal.jsonl"
        with open(jsonl_path, "w") as f:
            for _, row in instances_df.iterrows():
                json.dump(
                    {
                        "shape_id": row["shape_id"],
                        "subject_iri": row["subject_iri"],
                        "object_iri": row["object_iri"],
                    },
                    f,
                )
                f.write("\n")

        # Get file sizes for reporting
        result = {
            "subject_index": subject_path,
            "subject_index_size_mb": os.path.getsize(subject_path) / 1024 / 1024,
            "object_index": object_path,
            "object_index_size_mb": os.path.getsize(object_path) / 1024 / 1024,
            "instances_jsonl": jsonl_path,
            "instances_jsonl_size_mb": os.path.getsize(jsonl_path) / 1024 / 1024,
            "total_subjects": len(subject_index),
            "total_objects": len(object_index),
            "total_relationships": len(instances_df),
        }

        return result

    def load_instances_compact(
        self, output_prefix: str
    ) -> tuple[pd.DataFrame, Dict[str, List[str]], Dict[str, List[str]]]:
        """
        Load instances data that was exported in compact format.

        Args:
            output_prefix: Path prefix used during export (without extension)

        Returns:
            Tuple of (instances_df, subject_index, object_index)
        """
        import json

        # Load indices
        with open(f"{output_prefix}_subject_index.json") as f:
            subject_index = json.load(f)

        with open(f"{output_prefix}_object_index.json") as f:
            object_index = json.load(f)

        # Load instances DataFrame from JSON Lines
        instances_data = []
        with open(f"{output_prefix}_instances_minimal.jsonl") as f:
            for line in f:
                instances_data.append(json.loads(line))

        instances_df = pd.DataFrame(instances_data)

        return instances_df, subject_index, object_index

    def diagnose_object_classes(self, endpoint_url: str) -> Dict[str, Any]:
        """
        Diagnostic method to analyze what object classes are found in schema vs
        what literal properties might be missing.

        Returns:
            Dictionary with diagnostic information about object types
        """
        # Get raw schema
        schema_df = self.to_schema(filter_void_admin_nodes=True)

        # Use SparqlHelper for automatic GET-->POST fallback
        helper = SparqlHelper(endpoint_url)

        # Query for properties with literal values
        literal_props_query = """
        SELECT DISTINCT ?property ?subject_class WHERE {
            #GRAPH_CLAUSE
                ?s ?property ?o .
                ?s a ?subject_class .
                FILTER(isLiteral(?o))
            #END_GRAPH_CLAUSE
        }
        ORDER BY ?subject_class ?property
        """

        query = self._replace_graph_clause_placeholder(literal_props_query)

        try:
            results = helper.select(query, purpose="coverage/literal-props")
            literal_props = []
            for result in results["results"]["bindings"]:
                prop_uri = result["property"]["value"]
                subj_class = result["subject_class"]["value"]
                literal_props.append(
                    {
                        "property_uri": prop_uri,
                        "property": prop_uri.split("#")[-1].split("/")[-1],
                        "subject_class_uri": subj_class,
                        "subject_class": subj_class.split("#")[-1].split("/")[-1],
                    }
                )
        except Exception:
            literal_props = []

        # Query for properties with untyped resource objects
        untyped_resource_query = """
        SELECT DISTINCT ?property ?subject_class WHERE {
            #GRAPH_CLAUSE
                ?s ?property ?o .
                ?s a ?subject_class .
                FILTER(isURI(?o) && !EXISTS { ?o a ?any_type })
            #END_GRAPH_CLAUSE
        }
        ORDER BY ?subject_class ?property
        """

        query = self._replace_graph_clause_placeholder(untyped_resource_query)

        try:
            results = helper.select(query, purpose="coverage/untyped-resource-props")
            untyped_props = []
            for result in results["results"]["bindings"]:
                prop_uri = result["property"]["value"]
                subj_class = result["subject_class"]["value"]
                untyped_props.append(
                    {
                        "property_uri": prop_uri,
                        "property": prop_uri.split("#")[-1].split("/")[-1],
                        "subject_class_uri": subj_class,
                        "subject_class": subj_class.split("#")[-1].split("/")[-1],
                    }
                )
        except Exception:
            untyped_props = []

        return {
            "schema_object_classes": schema_df["object_class"].value_counts().to_dict()
            if not schema_df.empty
            else {},
            "schema_object_uris": schema_df["object_uri"].value_counts().to_dict()
            if not schema_df.empty
            else {},
            "literal_properties_found": literal_props,
            "untyped_resource_properties_found": untyped_props,
            "total_literal_props": len(literal_props),
            "total_untyped_resource_props": len(untyped_props),
            "schema_has_literals": any(
                "Literal" in str(obj) for obj in schema_df["object_uri"].unique()
            )
            if not schema_df.empty
            else False,
            "schema_has_resources": any(
                "Resource" in str(obj) for obj in schema_df["object_uri"].unique()
            )
            if not schema_df.empty
            else False,
        }


def parse_void_file(void_file_path: str, filter_void_nodes: bool = True) -> pd.DataFrame:
    """
    Convenience function to parse a VoID file and return schema DataFrame.

    Args:
        void_file_path: Path to the VoID turtle file
        filter_void_nodes: Whether to filter out VoID-specific nodes

    Returns:
        DataFrame with schema information
    """
    parser = VoidParser(void_file_path)
    return parser.to_schema(filter_void_admin_nodes=filter_void_nodes)


def generate_void_from_endpoint(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    output_file: Optional[str] = None,
    exclude_graphs: bool = True,
) -> "VoidParser":
    """
    Generate VoID from SPARQL endpoint and create parser.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URI(s) for the dataset. If None, queries all graphs
        output_file: Optional output file path for TTL
        exclude_graphs: Whether to exclude Virtuoso system graphs

    Returns:
        VoidParser instance with generated VoID
    """
    return VoidParser.from_sparql(endpoint_url, graph_uris, output_file, exclude_graphs)
