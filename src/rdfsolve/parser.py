"""
VoID (Vocabulary of Interlinked Datasets) Parser

This module provides functionality to parse VoID descriptions and extract
the underlying schema structure of RDF datasets. It can also generate VoID
descriptions from SPARQL endpoints using CONSTRUCT queries.
"""

import logging
import time

from rdflib import Graph, URIRef, Literal
import pandas as pd
from typing import Dict, Union, Optional, List
from SPARQLWrapper import SPARQLWrapper, TURTLE
from bioregistry import curie_from_iri


from linkml_runtime.utils.schemaview import SchemaView
from linkml.generators.yamlgen import YAMLGenerator
from linkml_runtime.linkml_model import (
    SchemaDefinition,
    ClassDefinition,
    SlotDefinition,
    TypeDefinition,
    Annotation,
)

# Create logger with NullHandler by default - no output unless user configures
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
        """
        Initialize the VoID parser.

        Args:
            void_source: Either a file path (str) or an RDF Graph object
            graph_uris: Single graph URI (str) or list of graph URIs to analyze.
                        If None, queries all graphs except Virtuoso system graphs
            exclude_graphs: Whether to exclude Virtuoso system graphs by default
        """
        self.void_file_path = None
        self.graph = Graph()
        self.schema_triples = []
        self.classes = {}
        self.properties = {}
        self.graph_uris = self._normalize_graph_uris(graph_uris)
        self.exclude_graphs = exclude_graphs

        # VoID namespace URIs
        self.void_class = URIRef("http://rdfs.org/ns/void#class")
        self.void_property = URIRef("http://rdfs.org/ns/void#property")
        self.void_propertyPartition = URIRef(
            "http://rdfs.org/ns/void#propertyPartition"
        )
        self.void_classPartition = URIRef("http://rdfs.org/ns/void#classPartition")
        self.void_datatypePartition = URIRef("http://ldf.fi/void-ext#datatypePartition")
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
            graph_uris: List of specific graph URIs. If None, uses instance URIs

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

        # Replace graph clause markers
        if not graph_uris:
            # No specific graphs - query default graph (remove GRAPH clause)
            # Simply remove the markers and keep the content
            result = query.replace("#GRAPH_CLAUSE\n", "")
            result = result.replace("#GRAPH_CLAUSE", "")
            result = result.replace("#END_GRAPH_CLAUSE\n", "")
            result = result.replace("#END_GRAPH_CLAUSE", "")
        elif len(graph_uris) == 1:
            # Single specific graph
            graph_clause = f"GRAPH <{graph_uris[0]}> {{"
            result = query.replace("#GRAPH_CLAUSE", graph_clause)
            result = result.replace("#END_GRAPH_CLAUSE", "}")
        else:
            # Multiple specific graphs - for now, just use the first one
            graph_clause = f"GRAPH <{graph_uris[0]}> {{"
            result = query.replace("#GRAPH_CLAUSE", graph_clause)
            result = result.replace("#END_GRAPH_CLAUSE", "}")

        return result

    def _load_graph(self):
        """Load the VoID file into an RDF graph."""
        self.graph.parse(self.void_file_path, format="turtle")

    def discover_void_graphs(self, endpoint_url: str) -> Dict:
        """
        Discover existing VoID graphs in the endpoint using chunked queries.

        Args:
            endpoint_url: SPARQL endpoint URL

        Returns:
            Dictionary with discovery results
        """
        from SPARQLWrapper import SPARQLWrapper, JSON

        try:
            sparql = SPARQLWrapper(endpoint_url)
            sparql.setReturnFormat(JSON)
            
            logger.debug(f" Starting chunked VoID graph discovery for {endpoint_url}")

            # Use chunked query to discover graphs that might contain VoID content
            logger.debug("Starting VoID graph discovery with pagination...")
            graph_discovery_query_template = """
            SELECT DISTINCT ?g WHERE {{
                GRAPH ?g {{
                    ?s ?p ?o
                }}
                FILTER(
                    REGEX(STR(?g), "void") || 
                    REGEX(STR(?g), "well-known") ||
                    REGEX(STR(?g), "service")
                )
            }}
            ORDER BY ?g
            OFFSET {offset}
            LIMIT {limit}
            """
            
            logger.info("Discovering VoID candidate graphs with chunked approach")
            candidate_graphs = []
            
            # Use smaller chunks for graph discovery (typically few results)
            for chunk_bindings in self._execute_chunked_query(
                sparql, graph_discovery_query_template, 
                chunk_size=50, max_total_results=500
            ):
                for result in chunk_bindings:
                    candidate = result["g"]["value"]
                    candidate_graphs.append(candidate)
                    logger.info("Candidate VoID graph at %s", candidate)
                
            logger.debug(f"Found {len(candidate_graphs)} candidate VoID graphs")
            
            if not candidate_graphs:
                logger.debug("No candidate VoID graphs found")
                return {
                    "has_void_descriptions": False,
                    "found_graphs": [],
                    "total_graphs": 0,
                    "void_content": {},
                }

            # Now check each candidate graph for actual VoID content
            logger.debug("Starting detailed VoID content analysis...")
            found_graphs = []
            void_content = {}

            for i, graph_uri in enumerate(candidate_graphs, 1):
                logger.debug(f"Analyzing graph {i}/{len(candidate_graphs)}: {graph_uri}")

                try:
                    # Use chunked queries to discover partitions in this graph
                    logger.debug(f"Discovering partitions in {graph_uri}")
                    
                    # Escape the graph URI properly for SPARQL
                    escaped_uri = graph_uri.replace('\\', '\\\\').replace('"', '\\"')
                    
                    # Check for class partitions - use f-string with proper escaping
                    class_query_template = f"""
                    SELECT DISTINCT ?cp WHERE {{
                        GRAPH <{escaped_uri}> {{
                            ?cp <http://rdfs.org/ns/void#class> ?class .
                        }}
                    }}
                    ORDER BY ?cp
                    OFFSET {{offset}}
                    LIMIT {{limit}}
                    """
                    
                    logger.debug(f"📝 Class partition query template: {class_query_template[:200]}...")
                    
                    class_partitions = 0
                    for chunk_bindings in self._execute_chunked_query(
                        sparql, class_query_template, chunk_size=100, max_total_results=1000
                    ):
                        class_partitions += len(chunk_bindings)
                        
                    # Check for property partitions
                    property_query_template = f"""
                    SELECT DISTINCT ?pp WHERE {{
                        GRAPH <{escaped_uri}> {{
                            ?pp <http://rdfs.org/ns/void#property> ?property .
                        }}
                    }}
                    ORDER BY ?pp
                    OFFSET {{offset}}
                    LIMIT {{limit}}
                    """
                    
                    property_partitions = 0
                    for chunk_bindings in self._execute_chunked_query(
                        sparql, property_query_template, chunk_size=100, max_total_results=1000
                    ):
                        property_partitions += len(chunk_bindings)
                        
                    # Check for datatype partitions
                    datatype_query_template = f"""
                    SELECT DISTINCT ?dp WHERE {{
                        GRAPH <{escaped_uri}> {{
                            ?dp <http://ldf.fi/void-ext#datatypePartition> ?datatype .
                        }}
                    }}
                    ORDER BY ?dp
                    OFFSET {{offset}}
                    LIMIT {{limit}}
                    """
                    
                    datatype_partitions = 0
                    for chunk_bindings in self._execute_chunked_query(
                        sparql, datatype_query_template, chunk_size=100, max_total_results=1000
                    ):
                        datatype_partitions += len(chunk_bindings)

                    logger.debug(f"Partition counts for {graph_uri}:")
                    logger.debug(f"   • Class partitions: {class_partitions}")
                    logger.debug(f"   • Property partitions: {property_partitions}")
                    logger.debug(f"   • Datatype partitions: {datatype_partitions}")

                    has_partitions = (
                        class_partitions + property_partitions + datatype_partitions
                    ) > 0

                    void_content[graph_uri] = {
                        "class_partition_count": class_partitions,
                        "property_partition_count": property_partitions,
                        "datatype_partition_count": datatype_partitions,
                        "has_any_partitions": has_partitions,
                    }

                    if has_partitions:
                        logger.info("Graph %s has partition data", graph_uri)
                        logger.debug(f"Adding {graph_uri} to found_graphs")
                        found_graphs.append(graph_uri)
                    else:
                        logger.debug(f"No partition data found in {graph_uri}")

                except Exception as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    # Log detailed error information for debugging
                    logger.warning(f"Error analyzing VoID partitions in {graph_uri}")
                    logger.debug(f"Full error details:")
                    logger.debug(f"   • Error type: {error_type}")
                    logger.debug(f"   • Error message: {error_msg}")
                    logger.debug(f"   • Graph URI: {graph_uri}")
                    if hasattr(e, '__cause__') and e.__cause__:
                        logger.debug(f"   • Root cause: {e.__cause__}")
                    
                    # Check if this is likely a query syntax error
                    if any(keyword in error_msg.lower() for keyword in 
                           ['syntax', 'parse', 'malformed', 'unexpected']):
                        logger.warning(f"Possible SPARQL syntax issue with URI: {graph_uri}")
                    
                    void_content[graph_uri] = {
                        "class_partition_count": 0,
                        "property_partition_count": 0,
                        "datatype_partition_count": 0,
                        "has_any_partitions": False,
                        "error": f"{error_type}: {error_msg}",
                        "escaped_uri": escaped_uri,  # Include for debugging
                    }

            logger.debug("VoID discovery analysis complete")
            logger.debug(f" Results summary:")
            logger.debug(f"   • Total candidate graphs: {len(candidate_graphs)}")
            logger.debug(f"   • Graphs with VoID data: {len(found_graphs)}")
            logger.debug(f"   • Has VoID descriptions: {len(found_graphs) > 0}")
            
            if found_graphs:
                logger.info(f"Found VoID data in {len(found_graphs)} graphs")
                for graph in found_graphs:
                    logger.debug(f"    {graph}")
            else:
                logger.info("No VoID data found in any candidate graphs")

            return {
                "has_void_descriptions": len(found_graphs) > 0,
                "found_graphs": found_graphs,
                "total_graphs": len(candidate_graphs),
                "void_content": void_content,
            }

        except Exception as e:
            logger.info(f"VoID discovery failed: {e}")
            logger.debug(f"Discovery exception: {type(e).__name__}: {str(e)}")
            return {
                "has_void_descriptions": False,
                "found_graphs": [],
                "total_graphs": 0,
                "void_content": {},
                "error": str(e),
            }

    def void_querier(self, endpoint_url: str, graph_uris: List[str]) -> Graph:
        """
        Query existing VoID descriptions from specific graphs.

        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uris: List of graph URIs containing VoID descriptions

        Returns:
            RDF Graph containing the retrieved VoID descriptions
        """
        from SPARQLWrapper import SPARQLWrapper, TURTLE

        logger.debug(f"Starting VoID querier for {len(graph_uris)} graphs")
        merged_graph = Graph()

        for i, graph_uri in enumerate(graph_uris, 1):
            logger.debug(f"Processing graph {i}/{len(graph_uris)}: {graph_uri}")
            try:
                sparql = SPARQLWrapper(endpoint_url)
                sparql.setReturnFormat(TURTLE)

                # Query to retrieve all VoID content from the graph
                logger.debug(f"Building CONSTRUCT query for VoID data...")
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
                results = self._safe_query(sparql, void_query)
                logger.debug("VoID CONSTRUCT query completed")

                if results and isinstance(results, bytes):
                    result_str = results.decode("utf-8")
                    logger.debug(f"📄 Got bytes result, size: {len(results)} bytes")
                elif results:
                    result_str = str(results)
                    logger.debug(f"📄 Got string result, size: {len(result_str)} chars")
                else:
                    logger.debug(f"No results from CONSTRUCT query")
                    continue

                if result_str.strip():
                    logger.debug(f"📝 Parsing VoID data (size: {len(result_str)} chars)")
                    merged_graph.parse(data=result_str, format="turtle")
                    logger.info(f"Retrieved VoID from: {graph_uri}")
                    logger.debug(f"Successfully parsed VoID data")
                else:
                    logger.debug(f"Empty result string, skipping")

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

    def _extract_classes(self):
        """Extract class information from VoID description."""
        self.classes = {}
        for s, p, o in self.graph.triples((None, self.void_class, None)):
            self.classes[s] = o

    def _extract_properties(self):
        """Extract property information from VoID description."""
        self.properties = {}
        for s, p, o in self.graph.triples((None, self.void_property, None)):
            self.properties[s] = o

    def _extract_schema_triples(self):
        """Extract schema triples by analyzing property partitions."""
        self.schema_triples = []

        # Try new ty extraction first (with subjectClass/objectClass)
        triples = self._extract_schema()
        if triples:
            self.schema_triples = triples
            return

    def _extract_schema(self):
        """Extract schema from property partitions with type info."""
        triples = []

        # Find all property partitions with subject/object class info
        for partition, _, property_uri in self.graph.triples(
            (None, self.void_property, None)
        ):
            # Get subject class
            subject_classes = list(
                self.graph.triples((partition, self.void_subjectClass, None))
            )
            # Get object class
            object_classes = list(
                self.graph.triples((partition, self.void_objectClass, None))
            )

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

    def to_jsonld(self, filter_void_admin_nodes: bool = True) -> dict:
        """
        Parse VoID file and return simple JSON-LD with the schema triples.
        Just normal RDF triples in JSON-LD format - no metadata, no VoID, no SHACL.

        Args:
            filter_void_admin_nodes: Whether to filter out VoID-specific nodes

        Returns:
            Simple JSON-LD with the schema triples as normal RDF statements
        """
        # Extract schema triples
        self._extract_schema_triples()

        if not self.schema_triples:
            return {"@context": {}, "@graph": []}

        # Create minimal context for the namespaces we find
        context = {}
        triples = []

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
                p_curie if p_curie else str(p): o_value
            }
            triples.append(triple)

        # Group triples by subject
        grouped = {}
        for triple in triples:
            subject_id = triple["@id"]
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

        # Return simple JSON-LD
        return {
            "@context": context,
            "@graph": list(grouped.values())
        }

    def _create_context(self) -> dict:
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
            
            # Application specific (Eg, shouldn't be needed)
            "aopo": "http://aopkb.org/aop_ontology#",
        }
        
        # Add prefixes from VoID graph namespace manager
        if self.graph and hasattr(self.graph, 'namespace_manager'):
            for prefix, namespace in self.graph.namespace_manager.namespaces():
                if prefix and namespace and str(prefix) not in context:
                    # Only add if it's a valid URI and not already present
                    ns_str = str(namespace)
                    if ns_str.startswith(('http://', 'https://', 'urn:')):
                        context[str(prefix)] = ns_str
        
        return context

    def _extract_context(self) -> dict:
        """Extract @context from VoID graph and common namespaces."""
        return self._create_context()

    def _filter_jsonld_void_admin_nodes(self, jsonld: dict) -> dict:
        """Filter out VoID administrative nodes from JSON-LD structure."""
        void_patterns = [
            "void", "rdfs", "rdf", "owl", "skos", "foaf", 
            "dcterms", "dc", "prov", "schema"
        ]
        
        # Handle new @graph structure
        if "@graph" in jsonld:
            filtered_graph = []
            
            for item in jsonld["@graph"]:
                # Keep dataset description (first item)
                if item.get("@type") == "void:Dataset":
                    filtered_graph.append(item)
                    continue
                
                # Keep schema pattern statements - these are our S-P-O relationships
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
        
        # Handle legacy structure (backwards compatibility) 
        elif "schema_patterns" in jsonld:
            filtered_patterns = []
            for pattern in jsonld["schema_patterns"]:
                subject_lower = pattern["subject_class"].lower()
                if not any(void_pat in subject_lower for void_pat in void_patterns):
                    filtered_patterns.append(pattern)
            
            jsonld["schema_patterns"] = filtered_patterns
            return jsonld
        
        # Return as-is if no recognized structure
        return jsonld

    def _extract_schema_patterns_from_triples(self) -> List[Dict[str, str]]:
        """
        Extract schema patterns from the internal schema triples.
        This creates the schema_patterns structure expected by other methods.
        
        Returns:
            List of schema pattern dictionaries
        """
        if not hasattr(self, 'schema_triples') or not self.schema_triples:
            return []
        
        patterns = []
        for subject_uri, property_uri, object_uri in self.schema_triples:
            # Convert URIs to CURIEs for display
            subject_curie, _, _ = self._get_curie_and_namespace(str(subject_uri))
            property_curie, _, _ = self._get_curie_and_namespace(str(property_uri))
            object_curie, _, _ = self._get_curie_and_namespace(str(object_uri))
            
            patterns.append({
                "subject_class": subject_curie,
                "subject_uri": str(subject_uri),
                "property": property_curie,
                "property_uri": str(property_uri),
                "object_class": object_curie,
                "object_uri": str(object_uri)
            })
        
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
        # Ensure schema is extracted
        self._extract_schema()
        
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

    def to_dataframe(self, endpoint_url: Optional[str] = None, graph_uri: Optional[str] = None, 
                     use_linkml: bool = False, datatype_partitions: bool = True, 
                     offset_limit_steps: int = 10000) -> pd.DataFrame:
        """
        Extract schema to pandas DataFrame.            
        Returns:
            DataFrame with schema analysis including coverage statistics
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
    ):
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
            schema_name = re.sub(r'[^a-zA-Z0-9_]', '_', schema_name)
        
        # Set schema base URI
        if not schema_base_uri:
            schema_uri = f"https://w3id.org/{schema_name}/"
        else:
            # Ensure base URI ends with /
            schema_uri = schema_base_uri.rstrip('/') + '/'
        
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
                schema_description
                or f"LinkML schema generated from JSON-LD for {schema_name}"
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
        
        # Collect all classes and properties from the JSON-LD triples
        all_class_names = set()
        all_slot_names = set()
        class_properties = {}  # class_name -> [property_names]
        property_ranges = {}   # property_name -> range_type
        property_descriptions = {}  # property_name -> description
        
        for item in schema_items:
            if "@id" not in item:
                continue
                
            subject = item["@id"]
            subject_clean = self._make_valid_linkml_name(subject)
            all_class_names.add(subject_clean)
            
            if subject_clean not in class_properties:
                class_properties[subject_clean] = []
            
            # Process all properties in this item
            for prop, value in item.items():
                if prop.startswith("@"):  # Skip JSON-LD keywords
                    continue
                    
                prop_clean = self._make_valid_linkml_name(prop)
                all_slot_names.add(prop_clean)
                
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
                
                # Store property description
                if prop_clean not in property_descriptions:
                    property_descriptions[prop_clean] = f"Property {prop}"
        
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
        
        # Create class definitions with URIs using original prefixes
        classes = {}
        for class_name in all_class_names:
            # Get properties for this class, applying name mapping
            class_slots = []
            if class_name in class_properties:
                class_slots = [slot_name_mapping.get(prop, prop) 
                             for prop in class_properties[class_name]]
            
            # Generate class URI using original prefix format: schema_uri/prefix_class
            class_uri = f"{schema_uri}{class_name}"
            
            class_def = ClassDefinition(
                name=class_name,
                description=f"Class representing {class_name}",
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
                
            # Generate slot URI using original prefix format: schema_uri/prefix_property  
            slot_uri = f"{schema_uri}{final_slot_name}"
            
            slot_def = SlotDefinition(
                name=final_slot_name,
                description=property_descriptions.get(original_slot_name, f"Property {original_slot_name}"),
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
        - Not contain dots, colons, or other special characters
        
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
        from bioregistry import curie_from_iri
        
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
            prefix = re.sub(r'[^a-zA-Z0-9_]', '_', prefix)
            
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
        local = local.replace('.', '_')
        
        # Add underscore before numbers that follow letters (like data1025 -> data_1025)
        local = re.sub(r'([a-zA-Z])(\d)', r'\1_\2', local)
        
        # Add underscore before uppercase letters that follow lowercase (camelCase -> camel_Case)
        local = re.sub(r'([a-z])([A-Z])', r'\1_\2', local)
        
        # Replace any remaining special characters with underscores
        local = re.sub(r'[^a-zA-Z0-9_]', '_', local)
        
        return local
    
    def _finalize_linkml_name(self, name: str) -> str:
        """
        Apply final cleanup rules to ensure valid LinkML identifier.
        """
        import re
        
        # Remove multiple consecutive underscores
        name = re.sub(r'_+', '_', name)
        
        # Remove leading/trailing underscores
        name = name.strip('_')
        
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
        from bioregistry import curie_from_iri
        
        curie = None
        prefix = None
        namespace_uri = None
        
        # First try bioregistry conversion
        if uri.startswith(("http://", "https://")):
            try:
                curie = curie_from_iri(uri)
                if curie and ":" in curie:
                    prefix = curie.split(":", 1)[0]
                    # Try to get the namespace from bioregistry
                    try:
                        from bioregistry import get_namespace_uri
                        namespace_uri = get_namespace_uri(prefix)
                    except:
                        # Fallback: extract namespace from original URI
                        if "#" in uri:
                            namespace_uri = uri.rsplit("#", 1)[0] + "#"
                        else:
                            namespace_uri = uri.rsplit("/", 1)[0] + "/"
            except curie is None:
                logger.warn("Couldn't validate %s", curie)
        
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
        if prefix and namespace_uri and hasattr(self, '_discovered_prefixes'):
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

    def _extract_prefixes_from_schema(self, df):
        """Extract prefixes from schema DataFrame by analyzing URIs."""
        prefixes = {}

        # Get prefixes from the VoID RDFlib graph first
        if self.graph and hasattr(self.graph, 'namespace_manager'):
            for prefix, namespace in self.graph.namespace_manager.namespaces():
                if prefix and namespace:
                    prefixes[str(prefix)] = str(namespace)

        # Initialize prefix collection for discovered CURIEs
        self._discovered_prefixes = {}

        # Extract prefixes from all URIs in the DataFrame
        all_uris = set()
        for col in ["property_uri", "subject_uri", "object_uri"]:
            if col in df.columns:
                all_uris.update(df[col].dropna().unique())

        # Process each URI to discover prefixes
        for uri in all_uris:
            uri_str = str(uri)
            if uri_str not in ["Literal", "Resource"]:
                # This will populate self._discovered_prefixes
                curie, prefix, namespace_uri = self._get_curie_and_namespace(uri_str)

        # Merge discovered prefixes with existing ones
        prefixes.update(self._discovered_prefixes)

        # Clean up the temporary collection
        if hasattr(self, '_discovered_prefixes'):
            delattr(self, '_discovered_prefixes')

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

        return YAMLGenerator(linkml_schema).serialize()

    def to_json(self, filter_void_nodes: bool = True) -> Dict:
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
                schema_patterns = df.to_dict('records')
        
        schema_graph = {
            "triples": [],
            "metadata": {
                "total_triples": len(schema_patterns),
                "classes": len(self.classes) if hasattr(self, 'classes') else 0,
                "properties": len(self.properties) if hasattr(self, 'properties') else 0,
                "prefixes": jsonld["@context"]  # Use @context for prefixes
            },
            "jsonld": jsonld,  # Include the full JSON-LD structure
            "schema_patterns": schema_patterns  # Add schema patterns for backward compatibility
        }

        # Add triples from schema patterns
        for pattern in schema_patterns:
            schema_graph["triples"].append([
                pattern["subject_uri"], 
                pattern["property_uri"], 
                pattern["object_uri"]
            ])

        return schema_graph

    @staticmethod
    def get_void_queries(
        graph_uris: Optional[Union[str, List[str]]] = None,
        counts: bool = True,        
        offset_limit_steps: Optional[int] = None,
        exclude_graphs: bool = True,
        exclude_graph_patterns: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        Get formatted CONSTRUCT queries for VoID generation.

        Args:
            graph_uris: Graph URI(s) to analyze. If None, queries all graphs
            counts: If True, include COUNT aggregations; else faster discovery
            offset_limit_steps: If provided, use this as both LIMIT and OFFSET step
            exclude_graphs: Whether to exclude system or any specific graphs

        Returns:
            Dictionary containing the formatted queries
        """
        # Create a temporary instance to use the graph clause methods
        temp_parser = VoidParser(
            graph_uris=graph_uris, exclude_graphs=exclude_graphs
        )
        # Store exclude patterns for later use
        temp_parser.exclude_graph_patterns = exclude_graph_patterns

        # Determine the base graph URI for VoID partition naming
        if isinstance(graph_uris, str):
            base_graph_uri = graph_uris
        elif isinstance(graph_uris, list) and len(graph_uris) == 1:
            base_graph_uri = graph_uris[0]
        else:
            base_graph_uri = "http://example.org/dataset"

        # Build limit and offset clause
        limit_offset_clause = ""

        # Use offset_limit_steps if provided
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
        SELECT ?class (COUNT(*) AS ?count)
        WHERE {{
            #GRAPH_CLAUSE
                [] a ?class
            #END_GRAPH_CLAUSE
        }}
        GROUP BY ?class
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/class_partition_',
                   MD5(STR(?class)))) AS ?cp)
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
                   MD5(CONCAT(STR(?property), STR(?subject_class))))) AS ?pp)
}}"""

            dtype_q_template = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
CONSTRUCT {{
    ?dp void-ext:datatypePartition ?datatype ;
        void:triples ?count .
}}
WHERE {{
    {{
        SELECT ?datatype (COUNT(*) AS ?count)
        WHERE {{
            #GRAPH_CLAUSE
                [] ?p ?o .
                FILTER(isLiteral(?o))
                BIND(datatype(?o) AS ?datatype)
            #END_GRAPH_CLAUSE
        }}
        GROUP BY ?datatype
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/datatype_partition_',
                   MD5(STR(?datatype)))) AS ?dp)
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
                [] a ?class
            #END_GRAPH_CLAUSE
        }}
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/class_partition_',
                   MD5(STR(?class)))) AS ?cp)
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
                   MD5(CONCAT(STR(?property), STR(?subject_class))))) AS ?pp)
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
                [] ?p ?o .
                FILTER(isLiteral(?o))
                BIND(datatype(?o) AS ?datatype)
            #END_GRAPH_CLAUSE
        }}
        {limit_offset_clause}
    }}
    BIND(IRI(CONCAT('{base_graph_uri}/void/datatype_partition_',
                   MD5(STR(?datatype)))) AS ?dp)
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

        Returns:
            RDF Graph containing the VoID description
        """
        queries = VoidParser.get_void_queries(
            graph_uris, counts, offset_limit_steps, exclude_graphs, exclude_graph_patterns
        )

        sparql = SPARQLWrapper(endpoint_url)

        merged_graph = Graph()

        # Ensure we're in a valid working directory for RDFLib parsing
        import os
        import tempfile

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
                public_id: str = "http://jmillanacosta.github.io/"):
            public_id = f"{public_id}/{name}/void"
            sparql.setReturnFormat(TURTLE)

            t0 = time.monotonic()

            try:
                results = VoidParser._safe_query(sparql, query_text)
                dt = time.monotonic() - t0

                # Parse result - handle bytes properly
                try:
                    # Handle both bytes and string results
                    if results:
                        if isinstance(results, bytes):
                            result_str = results.decode("utf-8")
                        else:
                            result_str = str(results)

                        if result_str.strip():
                            merged_graph.parse(
                                data=result_str,
                                format="turtle",
                                publicID=public_id
                            )
                        else:
                            logger.info(f"Empty results for {name}")
                    else:
                        logger.warning(f"No results for {name}")
                except Exception as e:
                    logger.warning(f"Failed to parse results for {name}: {e}")
                    if not is_optional:
                        raise

            except Exception as e:
                dt = time.monotonic() - t0
                logger.warning(f"Query {name} failed after {dt:.2f}s: {e}")

                # Check for timeout conditions
                timeout_keywords = ["timeout", "timed out"]
                if any(keyword in str(e).lower() for keyword in timeout_keywords):
                    logger.warning(f"Query {name} timed out")
                    if is_optional:
                        return
                if not is_optional:
                    raise

        try:
            # Execute queries for partitions
            query_type = queries["class_partitions"]
            run_construct(query_type, "class_partitions")
        except Exception as e:
            raise RuntimeError(
                f"""
Failed to generate VoID from SPARQL endpoint: {e}
Last query: {query_type}
"""
            )
        try:
            query_type = queries["property_partitions"]
            run_construct(
                query_type, "property_partitions", is_optional=False
            )
        except Exception as e:
            raise RuntimeError(f"""
Failed to generate VoID from SPARQL endpoint: {e}
Last query: {query_type}
""")
        try:
            query_type = queries["datatype_partitions"]
            run_construct(
                query_type, "datatype_partitions", is_optional=False
            )
        except Exception as e:
            raise RuntimeError(
                f"""
Failed to generate VoID from SPARQL endpoint: {e}
Last query: {query_type}
"""
            )
        try:
            # Save to file if specified
            if output_file:
                merged_graph.serialize(destination=output_file, format="turtle")
        except Exception as e:
            raise RuntimeError(f"""
Failed to generate VoID from SPARQL endpoint: {e}
Last query: {query_type}
""")
        try:
            return merged_graph 
        except Exception as e:
            raise RuntimeError(f"""
Failed to return VoID from SPARQL endpoint: {e}
Last query: {query_type}
""")

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
        return cls(void_graph)

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
        logger.debug(f"Settings: prefer_existing={prefer_existing}, "
                    f"exclude_graphs={exclude_graphs}")

    # If the user provided specific graph URIs, validate they exist.
    # If none exist, log an error and fall back to discovery.
        if graph_uris:
            logger.debug("Validating provided graph URIs...")
            try:
                from SPARQLWrapper import SPARQLWrapper, JSON

                sparql_checker = SPARQLWrapper(endpoint_url)
                sparql_checker.setReturnFormat(JSON)

                # Normalize to list
                if isinstance(graph_uris, list):
                    candidate_uris = graph_uris
                else:
                    candidate_uris = [graph_uris]
                valid_uris = []
                for uri in candidate_uris:
                    try:
                        ask_q = f"ASK {{ GRAPH <{uri}> {{ ?s ?p ?o }} }}"
                        # use safe query to retry transient failures
                        resp = cls._safe_query(sparql_checker, ask_q)
                        exists = False
                        if isinstance(resp, dict):
                            # ASK returns {'boolean': True/False} in JSON
                            exists = bool(resp.get("boolean", False))
                        else:
                            # Fallback: truthiness of response
                            exists = bool(resp)

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
        
        logger.debug(f"Discovery result: "
                    f"found={discovery_result.get('has_void_descriptions', False)}")
        logger.debug(f"Found {len(discovery_result.get('found_graphs', []))} "
                    f"graphs with VoID data")

        existing_void_graph = None
        existing_parser = None

        if discovery_result.get("has_void_descriptions", False):
            logger.debug("Found existing VoID descriptions")
            valid_void_graphs = [
                graph_uri
                for graph_uri, content in discovery_result.get(
                    "void_content", {}
                ).items()
                if content.get("has_any_partitions", False)
            ]
            
            logger.debug(f" Valid VoID graphs: {len(valid_void_graphs)}")

            if valid_void_graphs:
                valid_str = ", ".join(valid_void_graphs)
                logger.info("Retrieving VoID from %s", valid_str)
                logger.debug("Starting VoID retrieval process...")
                existing_void_graph = temp_parser.void_querier(
                    endpoint_url, valid_void_graphs
                )

                if len(existing_void_graph) > 0:
                    existing_parser = cls(existing_void_graph)
                    existing_schema_df = existing_parser.to_schema(
                        filter_void_admin_nodes=True
                    )

                    # Check if existing VoID has sufficient content
                    if prefer_existing and len(existing_schema_df) > 3:
                        len_df = len(existing_schema_df)
                        logger.info(
                            "Using existing VoID with \n%d schema triples",
                            len_df 
                        )

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
        logger.debug(f"Generation settings: counts={counts}, "
                    f"offset_limit_steps={offset_limit_steps}")
        
        output_path = os.path.join(
            exports_path, f"{dataset_name}_generated_void.ttl"
        )
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
        logger.debug(f"VoID generation completed")

        return cls(
            generated_void_graph,
            graph_uris=graph_uris,
            exclude_graphs=exclude_graphs,
        )

    def count_instances_per_class(
        self, endpoint_url: str, sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None, chunk_size: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
        delay_between_chunks: float = 20.0, streaming: bool = False
    ):
        """
        Count instances for each class in the dataset.

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
        from SPARQLWrapper import SPARQLWrapper, JSON

        sparql = SPARQLWrapper(endpoint_url)
        sparql.setReturnFormat(JSON)

        # If offset_limit_steps is provided, use it for chunked querying
        if offset_limit_steps is not None:
            if streaming:
                return self._count_instances_chunked_streaming(
                    sparql, sample_limit, sample_offset or 0, offset_limit_steps,
                    delay_between_chunks
                )
            else:
                return self._count_instances_chunked(
                    sparql, sample_limit, sample_offset or 0, offset_limit_steps,
                    delay_between_chunks
                )
        # If chunk_size is provided, use chunked querying
        elif chunk_size is not None:
            if streaming:
                return self._count_instances_chunked_streaming(
                    sparql, sample_limit, sample_offset, chunk_size,
                    delay_between_chunks
                )
            else:
                return self._count_instances_chunked(
                    sparql, sample_limit, sample_offset, chunk_size,
                    delay_between_chunks
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
        sparql.setQuery(query)

        try:
            results = self._safe_query(sparql, query)
            
            if streaming:
                # Return generator for single query case
                def _stream_results():
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

    def _count_instances_chunked(self, sparql, total_limit: Optional[int], 
                                 start_offset: int, chunk_size: int, 
                                 delay_between_chunks: float = 1.0) -> Dict:
        """Helper method for chunked instance counting."""
        return dict(self._count_instances_chunked_streaming(
            sparql, total_limit, start_offset, chunk_size, delay_between_chunks
        ))

    def _count_instances_chunked_streaming(
        self, sparql, total_limit: Optional[int],
        start_offset: int, chunk_size: int,
        delay_between_chunks: float = 1.0
    ):
        """
        Streaming version of chunked instance counting that yields results
        as they arrive.

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
            query = self._replace_graph_clause_placeholder(query_template)

            try:
                results = self._safe_query(sparql, query)
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
        self, sparql, query_template: str, chunk_size: int = 100,
        max_total_results: Optional[int] = None,
        delay_between_chunks: float = 0.5
    ):
        """
        Execute a SPARQL query in chunks using OFFSET/LIMIT pagination.
        
        Args:
            sparql: Configured SPARQLWrapper instance
            query_template: SPARQL query with {offset} and {limit} placeholders
            chunk_size: Number of results per chunk
            max_total_results: Maximum total results to fetch
            delay_between_chunks: Sleep time between chunks (seconds)
            
        Yields:
            Results from each chunk
        """
        current_offset = 0
        total_fetched = 0
        
        while True:
            # Calculate chunk size for this iteration
            if max_total_results is not None:
                remaining = max_total_results - total_fetched
                if remaining <= 0:
                    break
                current_chunk_size = min(chunk_size, remaining)
            else:
                current_chunk_size = chunk_size
                
            # Format query with current pagination
            query = query_template.format(
                offset=current_offset,
                limit=current_chunk_size
            )
            
            try:
                logger.debug("Executing chunked query: offset=%d, limit=%d",
                             current_offset, current_chunk_size)
                # Use safe query with retries and backoff to be robust to transient
                # network/server errors (RemoteDisconnected etc.)
                results = self._safe_query(sparql, query)

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
                
                logger.debug(f"Fetched chunk: {chunk_count} results "
                           f"(total: {total_fetched})")
                
                # If chunk was smaller than requested, we've reached the end
                if chunk_count < current_chunk_size:
                    logger.debug("📄 Partial chunk received, pagination complete")
                    break
                    
                # Respectful delay between chunks
                if delay_between_chunks > 0:
                    time.sleep(delay_between_chunks)
                    
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
                logger.warning(f"Chunk query failed at offset {current_offset}")
                logger.debug(f"Query error details:")
                logger.debug(f"   • Error type: {error_type}")
                logger.debug(f"   • Error message: {error_msg}")
                logger.debug(f"   • Query (first 500 chars): {query[:500]}...")
                
                # Check for common SPARQL issues
                if 'syntax' in error_msg.lower() or 'parse' in error_msg.lower():
                    logger.debug("Likely SPARQL syntax error - check query formatting")
                elif 'timeout' in error_msg.lower():
                    logger.debug("Query timeout - consider reducing chunk size")
                elif 'connection' in error_msg.lower():
                    logger.debug("Network/connection issue with SPARQL endpoint")
                    
                break

    @staticmethod
    def _safe_query(
        sparql,
        query: str,
        max_retries: int = 4,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ):
        """
        Execute a SPARQL query with retries and exponential backoff.

        Transient failures (connection resets, RemoteDisconnected,
        temporary endpoint failures). The function will raise the last
        exception if all retries fail.
        """
        import random
        import time

        attempt = 0
        while True:
            attempt += 1
            try:
                sparql.setQuery(query)
                results = sparql.query().convert()
                return results
            except Exception as e:
                err_text = str(e)
                logger.warning("Query attempt %d failed: %s", attempt, err_text)
                logger.debug("Query (first 500 chars): %s...", query[:500])
                if attempt >= max_retries:
                    logger.error("Query failed after %d attempts", attempt)
                    raise
                # exponential backoff with jitter
                backoff = min(initial_backoff * (2 ** (attempt - 1)), max_backoff)
                jitter = random.uniform(0, backoff * 0.1)
                sleep_time = backoff + jitter
                logger.info("Retrying after %.1fs (attempt %d/%d)", sleep_time, attempt + 1, max_retries)
                time.sleep(sleep_time)

    def analyze_class_partition_usage(
        self,
        endpoint_url: str,
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
    ):
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
            endpoint_url, sample_limit=sample_limit, sample_offset=sample_offset,
            offset_limit_steps=offset_limit_steps
        )

        # For chunked queries, skip class mappings as they can be too large
        if offset_limit_steps is not None:
            class_mappings = {}
            # Create simplified coverage stats without detailed mappings
            coverage_stats = {}
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
        self, coverage_stats: Dict, output_file: Optional[str] = None
    ):
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
                    "avg_occurrences_per_instance": stats[
                        "avg_occurrences_per_instance"
                    ],
                }
            )

        df = pd.DataFrame(coverage_data)
        df = df.sort_values("total_instances", ascending=False)

        if output_file:
            df.to_csv(output_file, index=False)

        return df

    def count_schema_shape_frequencies(
        self, endpoint_url: str,
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None, 
        offset_limit_steps: Optional[int] = None,
        collect_instances: bool = False,
        batch_size: int = 20,
        track_queries: bool = False
    ) -> tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Calculate schema pattern coverage: for each subject class, how many entities
        actually use each s,p,o pattern divided by total entities of that class.
        
        This gives coverage ratios showing what percentage of entities of each class
        type actually participate in each schema relationship pattern.

        Args:
            endpoint_url: SPARQL endpoint URL
            sample_limit: Optional limit for sampling entities
            sample_offset: Optional offset for pagination 
            offset_limit_steps: Optional limit/offset combination (legacy parameter, not used)
            collect_instances: If True, also collect actual subject/object IRI instances
            batch_size: Size of batches for VALUES queries (default: 20)
            track_queries: If True, collect detailed query statistics and report them

        Returns:
            Tuple of (frequencies_df, instances_df):
            - frequencies_df: DataFrame with schema pattern coverage ratios and counts
            - instances_df: Optional DataFrame with actual subject/object IRIs linked by shape_id
        """
        from SPARQLWrapper import SPARQLWrapper, JSON

        # Initialize query tracking (only if requested)
        total_queries_sent = 0
        total_rows_collected = 0
        query_details = [] if track_queries else None

        # Get the schema triples first
        schema_df = self.to_schema(filter_void_admin_nodes=True)

        if schema_df.empty:
            return pd.DataFrame()

        # First, get total entity counts using batched VALUES queries
        class_entity_counts = {}
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setReturnFormat(JSON)

        # Get unique subject class URIs and batch them
        subject_class_uris = list(schema_df['subject_uri'].unique())
        
        for i in range(0, len(subject_class_uris), batch_size):
            batch_uris = subject_class_uris[i:i + batch_size]
            
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
                sparql.setQuery(query)
                results = self._safe_query(sparql, query)
                if track_queries:
                    total_queries_sent += 1
                
                # Initialize all classes in this batch to 0 first
                for class_uri in batch_uris:
                    class_entity_counts[class_uri] = 0
                    
                # Update with actual counts from results and count rows
                rows_in_batch = 0
                if results and results.get("results", {}).get("bindings"):
                    rows_in_batch = len(results["results"]["bindings"])
                    if track_queries:
                        total_rows_collected += rows_in_batch
                    for binding in results["results"]["bindings"]:
                        class_uri = binding["class"]["value"]
                        total = int(binding["total"]["value"])
                        class_entity_counts[class_uri] = total
                
                if track_queries and query_details is not None:
                    query_details.append({
                        'query_type': 'entity_count_batch',
                        'batch_number': (i // batch_size) + 1,
                        'classes_in_batch': len(batch_uris),
                        'rows_returned': rows_in_batch
                    })
                        
            except Exception:
                if track_queries:
                    total_queries_sent += 1
                    if query_details is not None:
                        query_details.append({
                            'query_type': 'entity_count_batch',
                            'batch_number': (i // batch_size) + 1,
                            'classes_in_batch': len(batch_uris),
                            'rows_returned': 0,
                            'error': True
                        })
                # If batch fails, initialize all classes in batch to 0
                for class_uri in batch_uris:
                    class_entity_counts[class_uri] = 0

        # Now calculate coverage for each schema pattern
        coverage_results = []
        instances_data = [] if collect_instances else None

        for idx, row in schema_df.iterrows():
            # Generate unique shape ID for linking DataFrames
            shape_id = f"shape_{idx:06d}"
            subject_class_uri = row['subject_uri']
            property_uri = row['property_uri']
            object_class_uri = row['object_uri']

            total_entities = class_entity_counts.get(subject_class_uri, 0)

            if total_entities == 0:
                coverage_results.append({
                    "shape_id": shape_id,
                    "subject_class": row['subject_class'],
                    "subject_uri": subject_class_uri,
                    "property": row['property'],
                    "property_uri": property_uri,
                    "object_class": row['object_class'],
                    "object_uri": object_class_uri,
                    "total_entities": 0,
                    #"participating_entities": 0,
                    "occurrence_count": 0,
                    "coverage_ratio": 0.0,
                    "coverage_percent": 0.0,
                    "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}"
                })
                continue

            # Count entities that participate in this pattern
            literal_classes = [
                'Literal',
                'http://www.w3.org/2000/01/rdf-schema#Literal'
            ]
            resource_classes = [
                'Resource',
                'http://www.w3.org/2000/01/rdf-schema#Resource'
            ]
            
            if object_class_uri in literal_classes + resource_classes:
                # For literals and untyped resources
                if object_class_uri in literal_classes:
                    # Specifically for literals
                    object_filter = "FILTER(isLiteral(?o))"
                else:
                    # For untyped resources (rdfs:Resource)
                    object_filter = ("FILTER(isURI(?o) && "
                                   "!EXISTS { ?o a ?any_type })")
                
                if collect_instances:
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
                    pattern_query = f"""
                    SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {{
                        #GRAPH_CLAUSE
                            ?s <{property_uri}> ?o .
                            ?s a <{subject_class_uri}> .
                            {object_filter}
                        #END_GRAPH_CLAUSE
                    }}
                    """
            else:
                # For object properties: ?s ?p ?o where ?s a ?s_shape and ?o a ?o_shape
                if collect_instances:
                    pattern_query = f"""
                    SELECT DISTINCT ?s ?o WHERE {{
                        #GRAPH_CLAUSE
                            ?s <{property_uri}> ?o .
                            ?s a <{subject_class_uri}> .
                            ?o a <{object_class_uri}> .
                        #END_GRAPH_CLAUSE
                    }}
                    """
                else:
                    pattern_query = f"""
                    SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {{
                        #GRAPH_CLAUSE
                            ?s <{property_uri}> ?o .
                            ?s a <{subject_class_uri}> .
                            ?o a <{object_class_uri}> .
                        #END_GRAPH_CLAUSE
                    }}
                    """

            # Apply consistent sampling when specified
            if sample_limit:
                if collect_instances:
                    # For instance collection: limit subjects, get their actual relationships
                    pattern_query = pattern_query.replace(
                        "SELECT DISTINCT ?s ?o WHERE {",
                        f"""SELECT DISTINCT ?s ?o WHERE {{
                        {{ SELECT DISTINCT ?s WHERE {{
                            #GRAPH_CLAUSE
                                ?s a <{subject_class_uri}> .
                            #END_GRAPH_CLAUSE
                        }} LIMIT {sample_limit} }}"""
                    )
                else:
                    # For counting: ensure we count within the same subject sample
                    pattern_query = pattern_query.replace(
                        "SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {",
                        f"""SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {{
                        {{ SELECT DISTINCT ?s WHERE {{
                            #GRAPH_CLAUSE
                                ?s a <{subject_class_uri}> .
                            #END_GRAPH_CLAUSE
                        }} LIMIT {sample_limit} }}"""
                    )

            query = self._replace_graph_clause_placeholder(pattern_query)

            try:
                sparql.setQuery(query)
                results = self._safe_query(sparql, query)
                if track_queries:
                    total_queries_sent += 1

                participating = 0
                rows_in_pattern = 0
                if results and results.get("results", {}).get("bindings"):
                    rows_in_pattern = len(results["results"]["bindings"])
                    if track_queries:
                        total_rows_collected += rows_in_pattern
                    
                    if collect_instances:
                        # Process instance data and count unique subjects
                        unique_subjects = set()
                        for binding in results["results"]["bindings"]:
                            subject_iri = binding["s"]["value"]
                            object_iri = binding["o"]["value"]
                            unique_subjects.add(subject_iri)
                            
                            # Add instance data for linking
                            if instances_data is not None:
                                instances_data.append({
                                    "shape_id": shape_id,
                                    "subject_iri": subject_iri,
                                    "object_iri": object_iri,
                                    "subject_class": row['subject_class'],
                                    "property": row['property'],
                                    "object_class": row['object_class']
                                })
                        participating = len(unique_subjects)
                    else:
                        participating = int(results["results"]["bindings"][0]["participating"]["value"])
                
                if track_queries and query_details is not None:
                    query_details.append({
                        'query_type': 'pattern_query',
                        'shape_id': shape_id,
                        'pattern': f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                        'collect_instances': collect_instances,
                        'rows_returned': rows_in_pattern,
                        'participating_entities': participating
                    })
                        
                # Ensure participating count cannot exceed total entities
                # This handles edge cases in sampling or query inconsistencies
                if participating > total_entities and total_entities > 0:
                    participating = min(participating, total_entities)

                # Calculate coverage ratio and percentage
                coverage_ratio = participating / total_entities if total_entities > 0 else 0
                coverage_percent = coverage_ratio * 100

                coverage_results.append({
                    "shape_id": shape_id,
                    "subject_class": row['subject_class'],
                    "subject_uri": subject_class_uri,
                    "property": row['property'],
                    "property_uri": property_uri,
                    "object_class": row['object_class'],
                    "object_uri": object_class_uri,
                    "total_entities": total_entities,
                    "participating_entities": participating,
                    "occurrence_count": participating,
                    "coverage_ratio": round(coverage_ratio, 4),
                    "coverage_percent": round(coverage_percent, 2),
                    "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}"
                })

            except Exception as e:
                if track_queries:
                    total_queries_sent += 1
                    if query_details is not None:
                        query_details.append({
                            'query_type': 'pattern_query',
                            'shape_id': shape_id,
                            'pattern': f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                            'collect_instances': collect_instances,
                            'rows_returned': 0,
                            'participating_entities': 0,
                            'error': str(e)
                        })
                
                coverage_results.append({
                    "shape_id": shape_id,
                    "subject_class": row['subject_class'],
                    "subject_uri": subject_class_uri,
                    "property": row['property'],
                    "property_uri": property_uri,
                    "object_class": row['object_class'],
                    "object_uri": object_class_uri,
                    "total_entities": total_entities,
                    #"participating_entities": 0,
                    "occurrence_count": 0,
                    "coverage_ratio": 0.0,
                    "coverage_percent": 0.0,
                    "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}",
                    "error": str(e)
                })

        # Convert to DataFrame and sort by coverage ratio
        frequencies_df = pd.DataFrame(coverage_results)
        if not frequencies_df.empty:
            frequencies_df = frequencies_df.sort_values('coverage_ratio', ascending=False)

        # Create instances DataFrame if collected
        instances_df = None
        if collect_instances and instances_data:
            instances_df = pd.DataFrame(instances_data)
            # Optimize memory by using categorical data types for repeated values
            instances_df = instances_df.astype({
                'shape_id': 'category',
                'subject_class': 'category', 
                'property': 'category',
                'object_class': 'category'
            })

        # Report query statistics (only if tracking was enabled)
        if track_queries:
            logger.info("Query Statistics:")
            logger.info("Total queries sent: %s", total_queries_sent)
            logger.info("Total rows collected: %s", total_rows_collected)
            
            if query_details:
                # Break down by query type
                entity_count_queries = [q for q in query_details 
                                       if q['query_type'] == 'entity_count_batch']
                pattern_queries = [q for q in query_details 
                                  if q['query_type'] == 'pattern_query']
                error_queries = [q for q in query_details if q.get('error')]
                
                logger.info("Entity count batch queries: %s", 
                           len(entity_count_queries))
                if entity_count_queries:
                    entity_count_rows = sum(q['rows_returned'] 
                                          for q in entity_count_queries)
                    logger.info("Rows from entity counts: %s", entity_count_rows)
                    
                logger.info("Pattern queries: %s", len(pattern_queries))
                if pattern_queries:
                    pattern_rows = sum(q['rows_returned'] for q in pattern_queries)
                    logger.info("Rows from patterns: %s", pattern_rows)
                    
                if error_queries:
                    logger.warning("Queries with errors: %s", len(error_queries))
                    
                # Detailed per-query report (limit to first 200 to avoid flooding)
                logger.info("Per-query details (first 200 shown):")
                for qi, q in enumerate(query_details[:200], start=1):
                    qtype = q.get('query_type')
                    rows = q.get('rows_returned', 0)
                    err = q.get('error')
                    if qtype == 'entity_count_batch':
                        batch = q.get('batch_number')
                        classes = q.get('classes_in_batch')
                        logger.info("[%03d] entity_count_batch batch=%s "
                                   "classes=%s rows=%s%s", qi, batch, classes, 
                                   rows, ' ERROR' if err else '')
                    else:
                        shape = q.get('shape_id', q.get('pattern', 'unknown'))
                        part = q.get('participating_entities', 0)
                        coll = q.get('collect_instances', False)
                        logger.info("[%03d] pattern_query shape=%s "
                                   "collect_instances=%s rows=%s "
                                   "participating=%s%s", qi, shape, coll, 
                                   rows, part, ' ERROR' if err else '')

                if len(query_details) > 200:
                    remaining = len(query_details) - 200
                    logger.info("... (and %s more queries not shown)", remaining)
            
        if collect_instances and instances_data:
            logger.info("Total instances collected: %s", len(instances_data))

        return frequencies_df, instances_df

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
        min_coverage: Optional[float] = None
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
                filtered_freq['shape_pattern'].str.contains(shape_pattern, case=False, na=False)
            ]
            
        if min_coverage is not None:
            filtered_freq = filtered_freq[filtered_freq['coverage_ratio'] >= min_coverage]
            
        if filtered_freq.empty:
            return pd.DataFrame()
            
        # Get shape_ids of interest
        shape_ids = set(filtered_freq['shape_id'])
        
        # Filter instances to matching shapes (using categorical index for speed)
        filtered_instances = instances_df[instances_df['shape_id'].isin(shape_ids)]
        
        # Merge on shape_id to get combined view
        # Use left join to keep all frequency data even if no instances
        result = filtered_freq.merge(
            filtered_instances, 
            on='shape_id', 
            how='left',
            suffixes=('', '_instance')
        )
        
        return result

    def analyze_shape_distribution(
        self, 
        frequencies_df: pd.DataFrame, 
        instances_df: pd.DataFrame
    ) -> dict:
        """
        Analyze the distribution of instances across shapes for memory optimization insights.
        
        Returns:
            Dictionary with distribution statistics
        """
        if frequencies_df.empty or instances_df is None or instances_df.empty:
            return {}
            
        # Count instances per shape
        instance_counts = instances_df.groupby('shape_id').size()
        
        # Get shape info
        shape_info = frequencies_df.set_index('shape_id')[['shape_pattern', 'coverage_ratio']]
        
        # Combine for analysis
        combined = pd.concat([instance_counts.rename('actual_instances'), shape_info], axis=1)
        
        return {
            'total_shapes': len(frequencies_df),
            'shapes_with_instances': len(combined),
            'total_instance_records': len(instances_df),
            'avg_instances_per_shape': instance_counts.mean(),
            'max_instances_per_shape': instance_counts.max(),
            'memory_usage_mb': {
                'frequencies_df': frequencies_df.memory_usage(deep=True).sum() / 1024 / 1024,
                'instances_df': instances_df.memory_usage(deep=True).sum() / 1024 / 1024,
            },
            'top_shapes_by_instances': combined.nlargest(10, 'actual_instances').to_dict('index')
        }

    def diagnose_object_classes(self, endpoint_url: str) -> dict:
        """
        Diagnostic method to analyze what object classes are found in schema vs
        what literal properties might be missing.
        
        Returns:
            Dictionary with diagnostic information about object types
        """
        from SPARQLWrapper import SPARQLWrapper, JSON
        
        # Get raw schema
        schema_df = self.to_schema(filter_void_admin_nodes=True)
        
        # Direct query to find literal properties
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setReturnFormat(JSON)
        
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
            sparql.setQuery(query)
            results = self._safe_query(sparql, query)
            literal_props = []
            for result in results["results"]["bindings"]:
                prop_uri = result["property"]["value"]
                subj_class = result["subject_class"]["value"]
                literal_props.append({
                    "property_uri": prop_uri,
                    "property": prop_uri.split("#")[-1].split("/")[-1],
                    "subject_class_uri": subj_class,
                    "subject_class": subj_class.split("#")[-1].split("/")[-1]
                })
        except Exception as e:
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
            sparql.setQuery(query)
            results = self._safe_query(sparql, query)
            untyped_props = []
            for result in results["results"]["bindings"]:
                prop_uri = result["property"]["value"]
                subj_class = result["subject_class"]["value"]
                untyped_props.append({
                    "property_uri": prop_uri,
                    "property": prop_uri.split("#")[-1].split("/")[-1],
                    "subject_class_uri": subj_class,
                    "subject_class": subj_class.split("#")[-1].split("/")[-1]
                })
        except Exception as e:
            untyped_props = []
        
        return {
            "schema_object_classes": schema_df["object_class"].value_counts().to_dict() if not schema_df.empty else {},
            "schema_object_uris": schema_df["object_uri"].value_counts().to_dict() if not schema_df.empty else {},
            "literal_properties_found": literal_props,
            "untyped_resource_properties_found": untyped_props,
            "total_literal_props": len(literal_props),
            "total_untyped_resource_props": len(untyped_props),
            "schema_has_literals": any("Literal" in str(obj) for obj in schema_df["object_uri"].unique()) if not schema_df.empty else False,
            "schema_has_resources": any("Resource" in str(obj) for obj in schema_df["object_uri"].unique()) if not schema_df.empty else False
        }


def parse_void_file(
    void_file_path: str, filter_void_nodes: bool = True
) -> pd.DataFrame:
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
    return VoidParser.from_sparql(
        endpoint_url, graph_uris, output_file, exclude_graphs
    )
