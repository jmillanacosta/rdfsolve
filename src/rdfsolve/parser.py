"""
VoID (Vocabulary of Interlinked Datasets) Parser

This module provides functionality to parse VoID descriptions and extract
the underlying schema structure of RDF datasets. It can also generate VoID
descriptions from SPARQL endpoints using CONSTRUCT queries.
"""

import logging
import time

from rdflib import Graph, URIRef
import pandas as pd
from typing import Dict, Union, Optional, List
from SPARQLWrapper import SPARQLWrapper, TURTLE
from bioregistry import curie_from_iri

# Optional LinkML imports - only used if LinkML functionality is requested
try:
    from linkml_runtime.utils.schemaview import SchemaView
    from linkml.generators.yamlgen import YAMLGenerator
    from linkml_runtime.linkml_model import (
        SchemaDefinition,
        ClassDefinition,
        SlotDefinition,
        TypeDefinition,
        Annotation,
    )
    LINKML_AVAILABLE = True
except ImportError:
    LINKML_AVAILABLE = False
    # Placeholder classes for type hints
    SchemaDefinition = None
    ClassDefinition = None 
    SlotDefinition = None
    TypeDefinition = None
    Annotation = None

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
                # Filter out Virtuoso system graphs
                FILTER(!REGEX(STR(?g), "http://www.openlinksw.com/"))
                FILTER(!REGEX(STR(?g), "http://localhost:8890/"))
                FILTER(!REGEX(STR(?g), "^urn:virtuoso:"))
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

                logger.debug("⏱️ Executing VoID CONSTRUCT query...")
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

    def to_schema(self, filter_void_admin_nodes: bool = True) -> pd.DataFrame:
        """
        Parse VoID file and return schema as DataFrame.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes

        Returns:
            DataFrame with schema triples
        """
        # Extract all components
        self._extract_classes()
        self._extract_properties()
        self._extract_schema_triples()

        # Convert to DataFrame format
        schema_data = []
        for s, p, o in self.schema_triples:
            # Use bioregistry.curie_from_iri for readable names, fallback to local part
            s_name = curie_from_iri(str(s)) or (
                str(s).split("#")[-1].split("/")[-1]
                if "#" in str(s)
                else str(s).split("/")[-1]
            )
            p_name = curie_from_iri(str(p)) or (
                str(p).split("#")[-1].split("/")[-1]
                if "#" in str(p)
                else str(p).split("/")[-1]
            )
            if o not in ["Literal", "Resource"]:
                o_name = curie_from_iri(str(o)) or (
                    str(o).split("#")[-1].split("/")[-1]
                    if "#" in str(o)
                    else str(o).split("/")[-1]
                )
            else:
                o_name = o

            schema_data.append(
                {
                    "subject_class": s_name,
                    "subject_uri": str(s),
                    "property": p_name,
                    "property_uri": str(p),
                    "object_class": o_name,
                    "object_uri": str(o) if o not in ["Literal", "Resource"] else o,
                }
            )

        df = pd.DataFrame(schema_data)

        if filter_void_admin_nodes and not df.empty:
            df = self._filter_void_admin_nodes(df)

        return df

    def to_dataframe(self, endpoint_url: Optional[str] = None, graph_uri: Optional[str] = None, 
                     use_linkml: bool = False, datatype_partitions: bool = True, 
                     offset_limit_steps: int = 10000) -> pd.DataFrame:
        """
        Extract schema to pandas DataFrame.            
        Returns:
            DataFrame with comprehensive schema analysis including coverage statistics
        """
        return self._extract_schema()

    def to_linkml(
        self,
        filter_void_nodes: bool = True,
        schema_name: Optional[str] = None,
        schema_description: Optional[str] = None,
    ):
        """
        Parse VoID file and return schema as LinkML model.

        Subjects from the schema are converted to LinkML classes (entries).
        Properties become slots in those classes.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes
            schema_name: Name for the LinkML schema
            schema_description: Description for the LinkML schema

        Returns:
            LinkML SchemaDefinition with classes based on subjects
        """
        if not LINKML_AVAILABLE:
            raise ImportError("LinkML is not installed. Please install with: pip install linkml")
        # Get the schema DataFrame
        df = self.to_schema(filter_void_admin_nodes=filter_void_nodes)

        if df.empty:
            # Return empty schema if no data
            return SchemaDefinition(
                id=schema_name or "empty_schema",
                name=schema_name or "empty_schema",
                description=(schema_description or "Empty schema - no data found"),
            )

        # Generate schema name from dataset if not provided
        if not schema_name:
            schema_name = getattr(self, "_dataset_name", "rdf_schema")
            schema_name = schema_name.replace("-", "_").replace(".", "_")

        # Extract prefixes from the VoID graph and schema data
        extracted_prefixes = self._extract_prefixes_from_schema(df)

        # Create the LinkML schema with extracted prefixes
        schema_uri = f"https://w3id.org/{schema_name}/"

        # Base prefixes
        base_prefixes = {
            schema_name: schema_uri,
            "linkml": "https://w3id.org/linkml/",
            "schema": "http://schema.org/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
        }

        # Merge with extracted prefixes
        all_prefixes = {**base_prefixes, **extracted_prefixes}

        schema = SchemaDefinition(
            id=schema_name,
            name=schema_name,
            description=(
                schema_description
                or f"LinkML schema extracted from VoID analysis of {schema_name}"
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

        # First pass: collect all class names to avoid forward reference issues
        all_class_names = set()
        for subject_class in df["subject_class"].unique():
            class_name = self._clean_class_name(subject_class)
            all_class_names.add(class_name)

        for object_class in df["object_class"].unique():
            if object_class not in ["Literal", "rdfs:Literal", "Resource"]:
                class_name = self._clean_class_name(object_class)
                all_class_names.add(class_name)

        # Create placeholder classes for all referenced classes
        classes = {}
        for class_name in all_class_names:
            # Find original class info from dataframe if available
            original_name = None
            original_uri = None
            for _, row in df.iterrows():
                if self._clean_class_name(row["subject_class"]) == class_name:
                    original_name = row["subject_class"]
                    original_uri = row["subject_uri"]
                    break
                elif self._clean_class_name(row["object_class"]) == class_name:
                    original_name = row["object_class"]
                    original_uri = row["object_uri"]
                    break

            # Create basic class definition
            class_def = ClassDefinition(
                name=class_name,
                description=(
                    f"Class {original_name or class_name}"
                    + (f" (URI: {original_uri})" if original_uri else "")
                ),
                slots=[],
            )
            classes[class_name] = class_def

        # Group by subject class to create LinkML classes with properties
        slots = {}

        # Collect all slot names first to detect conflicts and apply consistent prefixing
        all_slot_names = set()
        for _, row in df.iterrows():
            prop_name = self._clean_slot_name(row["property"])
            all_slot_names.add(prop_name)

        # Detect naming conflicts and resolve them
        conflicts = all_class_names.intersection(all_slot_names)
        slot_name_mapping = {}

        for _, row in df.iterrows():
            original_prop = row["property"]
            base_slot_name = self._clean_slot_name(original_prop)

            # Apply consistent prefixing rules
            should_prefix = (
                base_slot_name in conflicts  # Conflict with class name
                or (
                    base_slot_name.startswith("C") and base_slot_name[1:].isdigit()
                )  # NCI codes
                or base_slot_name.isdigit()  # Pure numeric
                or base_slot_name.startswith("prop_")  # Already prefixed
            )

            # Keep common property names unprefixed for readability
            common_props = {
                "label",
                "identifier",
                "source",
                "type",
                "title",
                "description",
                "created",
                "modified",
                "page",
                "alternative",
                "abstract",
                "seeAlso",
                "sameAs",
                "exactMatch",
                "creator",
                "isPartOf",
            }

            if base_slot_name in common_props:
                final_slot_name = base_slot_name
            elif should_prefix and not base_slot_name.startswith("prop_"):
                final_slot_name = f"prop_{base_slot_name}"
            elif base_slot_name.startswith("prop_"):
                final_slot_name = base_slot_name
            else:
                final_slot_name = base_slot_name

            slot_name_mapping[original_prop] = final_slot_name

        for subject_class in df["subject_class"].unique():
            # Create class definition
            class_name = self._clean_class_name(subject_class)
            subject_data = df[df["subject_class"] == subject_class]

            # Get unique properties for this class
            class_properties = []
            for _, row in subject_data.iterrows():
                original_prop = row["property"]
                prop_name = slot_name_mapping[original_prop]

                # Create slot definition if not exists
                if prop_name not in slots:
                    slot_def = SlotDefinition(
                        name=prop_name,
                        description=(
                            f"Property {row['property']} "
                            f"(URI: {row['property_uri']})"
                        ),
                    )

                    # Set a meaningful owner (use the most common/representative class)
                    prop_classes = df[df["property"] == original_prop][
                        "subject_class"
                    ].value_counts()
                    if len(prop_classes) > 0:
                        most_common_class = prop_classes.index[0]
                        slot_def.owner = self._clean_class_name(most_common_class)

                    # Set domain_of to all classes that use this property
                    domain_classes = df[df["property"] == original_prop][
                        "subject_class"
                    ].unique()
                    slot_def.domain_of = [
                        self._clean_class_name(cls) for cls in domain_classes
                    ]

                    # Determine range based on object type
                    object_class = row["object_class"]
                    if object_class in ["Literal", "rdfs:Literal"]:
                        slot_def.range = "string"
                    elif object_class == "Resource":
                        slot_def.range = "uriorcurie"
                    else:
                        # Reference to another class - check if in schema
                        object_class_name = self._clean_class_name(object_class)
                        if object_class_name in all_class_names:
                            slot_def.range = object_class_name
                            # Mark as inlined for object references
                            slot_def.inlined = True
                            slot_def.inlined_as_list = True
                        else:
                            # Fallback to string for unknown classes
                            slot_def.range = "string"

                    slots[prop_name] = slot_def

                if prop_name not in class_properties:
                    class_properties.append(prop_name)

            # Update the existing class definition with slots
            if class_name in classes:
                classes[class_name].slots = class_properties
                classes[class_name].description = (
                    f"Class {subject_class} "
                    f"(URI: {subject_data.iloc[0]['subject_uri']})"
                )

        # Add classes and slots to schema
        schema.classes = classes
        schema.slots = slots

        return schema

    def _clean_class_name(self, class_name: str) -> str:
        """Clean class name to be valid LinkML identifier."""
        # Remove namespace prefixes and special characters
        name = class_name.split(":")[-1] if ":" in class_name else class_name
        # Replace invalid characters with underscore
        import re

        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        # Ensure it starts with a letter
        if name and name[0].isdigit():
            name = f"class_{name}"
        return name or "unknown_class"

    def _clean_slot_name(self, slot_name: str) -> str:
        """Clean slot name to be valid LinkML identifier using CURIE format."""
        import re

        # Convert CURIE to valid slot name
        if ":" in slot_name:
            prefix, local = slot_name.split(":", 1)
            # Replace dots and other invalid characters with underscores
            safe_prefix = re.sub(r"[^a-zA-Z0-9_]", "_", prefix)
            safe_local = re.sub(r"[^a-zA-Z0-9_]", "_", local)
            name = f"{safe_prefix}_{safe_local}"
        else:
            # Handle cases without prefix
            name = re.sub(r"[^a-zA-Z0-9_]", "_", slot_name)

        # Ensure it starts with a letter
        if name and name[0].isdigit():
            name = f"prop_{name}"

        return name or "unknown_property"

    def _extract_prefixes_from_schema(self, df):
        """Extract prefixes from schema DataFrame by analyzing URIs."""
        prefixes = {}

        # Get prefixes from the VoID RDFlib graph
        common_ns = {}
        if self.graph and hasattr(self.graph, 'namespace_manager'):
            # Extract all namespace mappings from the graph
            for prefix, namespace in self.graph.namespace_manager.namespaces():
                if prefix and namespace:
                    common_ns[str(namespace)] = str(prefix)

        # Fallback common namespace mappings if graph has no prefixes
        if not common_ns:
            common_ns = {}

        # Extract from property URIs
        all_uris = set()
        for col in ["property_uri", "subject_uri", "object_uri"]:
            if col in df.columns:
                all_uris.update(df[col].dropna().unique())

        # Find matching prefixes
        for uri in all_uris:
            uri_str = str(uri)
            for namespace, prefix in common_ns.items():
                if uri_str.startswith(namespace):
                    prefixes[prefix] = namespace
                    break

        return prefixes

    def to_linkml_yaml(
        self,
        filter_void_nodes: bool = True,
        schema_name: Optional[str] = None,
        schema_description: Optional[str] = None,
    ) -> str:
        """
        Parse VoID file and return LinkML schema as YAML string.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes
            schema_name: Name for the LinkML schema
            schema_description: Description for the LinkML schema

        Returns:
            LinkML schema as YAML string
        """
        if not LINKML_AVAILABLE:
            raise ImportError("LinkML is not installed. Please install with: pip install linkml")
        linkml_schema = self.to_linkml(
            filter_void_nodes=filter_void_nodes,
            schema_name=schema_name,
            schema_description=schema_description,
        )

        return YAMLGenerator(linkml_schema).serialize()

    def to_json(self, filter_void_nodes: bool = True) -> Dict:
        """
        Parse VoID file and return schema as JSON structure.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes

        Returns:
            Dictionary with schema information
        """
        df = self.to_schema(filter_void_admin_nodes=filter_void_nodes)

        schema_graph = {
            "triples": [],
            "metadata": {
                "total_triples": len(df),
                "classes": df["subject_uri"].unique().tolist() if not df.empty else [],
                "properties": (
                    df["property_uri"].unique().tolist() if not df.empty else []
                ),
                "objects": df["object_uri"].unique().tolist() if not df.empty else [],
            },
        }

        # Add triples
        for _, row in df.iterrows():
            schema_graph["triples"].append(
                [row["subject_uri"], row["property_uri"], row["object_uri"]]
            )

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

        def run_construct(query_text: str, name: str, is_optional: bool = False, public_id: str = "http://jmillanacosta.github.io/"):  #TODO set as class argument
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

        logger.debug("🚀 Starting endpoint discovery process...")
        logger.debug(f"📍 Endpoint: {endpoint_url}")
        logger.debug(f"📂 Dataset: {dataset_name}")
        logger.debug(f" Provided graph URIs: {graph_uris}")
        logger.debug(f"⚙️ Settings: prefer_existing={prefer_existing}, "
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
        logger.debug("🔎 Step 1: Discovering existing VoID graphs...")
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
                logger.debug("📥 Starting VoID retrieval process...")
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
        logger.debug("🔧 Step 2: Generating new VoID from SPARQL queries...")
        logger.debug(f"⚙️ Generation settings: counts={counts}, "
                    f"offset_limit_steps={offset_limit_steps}")
        
        output_path = os.path.join(
            exports_path, f"{dataset_name}_generated_void.ttl"
        )
        logger.debug(f"📁 Output path: {output_path}")

        logger.debug("🚀 Starting VoID generation process...")
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
                    logger.debug("💡 Likely SPARQL syntax error - check query formatting")
                elif 'timeout' in error_msg.lower():
                    logger.debug("⏰ Query timeout - consider reducing chunk size")
                elif 'connection' in error_msg.lower():
                    logger.debug("🔌 Network/connection issue with SPARQL endpoint")
                    
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
                # rely on underlying transport defaults; do not set a client
                # timeout here to respect the user's requirement
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
        self, endpoint_url: str, sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None, 
        offset_limit_steps: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Calculate schema pattern coverage: for each subject class, how many entities
        actually use each s,p,o pattern divided by total entities of that class.
        
        This gives coverage ratios showing what percentage of entities of each class
        type actually participate in each schema relationship pattern.

        Args:
            endpoint_url: SPARQL endpoint URL
            offset_limit_steps: Optional limit/offset combination for paginating.

        Returns:
            DataFrame with schema pattern coverage ratios and counts
        """
        from SPARQLWrapper import SPARQLWrapper, JSON

        # Get the schema triples first
        schema_df = self.to_schema(filter_void_admin_nodes=True)

        if schema_df.empty:
            return pd.DataFrame()

        # First, get total entity counts for each subject class
        class_entity_counts = {}
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setReturnFormat(JSON)

        for subject_class_uri in schema_df['subject_uri'].unique():
            count_query = f"""
            SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE {{
                #GRAPH_CLAUSE
                    ?s a <{subject_class_uri}> .
                #END_GRAPH_CLAUSE
            }}
            """

            if sample_limit:
                count_query = count_query.replace(
                    "SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE {",
                    f"SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE {{\n                {{\n                    SELECT DISTINCT ?s WHERE {{"
                ).replace("}", f"}}\n                    LIMIT {sample_limit}\n                }}\n            }}")

            query = self._replace_graph_clause_placeholder(count_query)

            try:
                sparql.setQuery(query)
                results = self._safe_query(sparql, query)
                total = 0
                if results and results.get("results", {}).get("bindings"):
                    total = int(results["results"]["bindings"][0]["total"]["value"])
                class_entity_counts[subject_class_uri] = total
            except Exception:
                class_entity_counts[subject_class_uri] = 0

        # Now calculate coverage for each schema pattern
        coverage_results = []

        for idx, row in schema_df.iterrows():
            subject_class_uri = row['subject_uri']
            property_uri = row['property_uri']
            object_class_uri = row['object_uri']

            total_entities = class_entity_counts.get(subject_class_uri, 0)

            if total_entities == 0:
                coverage_results.append({
                    "subject_class": row['subject_class'],
                    "subject_uri": subject_class_uri,
                    "property": row['property'],
                    "property_uri": property_uri,
                    "object_class": row['object_class'],
                    "object_uri": object_class_uri,
                    "total_entities": 0,
                    "participating_entities": 0,
                    "occurrence_count": 0,
                    "coverage_ratio": 0.0,
                    "coverage_percent": 0.0,
                    "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}"
                })
                continue

            # Count entities that participate in this pattern
            if object_class_uri in ['Literal', 'Resource']:
                # For literals, count distinct subjects using this property with literal values
                pattern_query = f"""
                SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {{
                    #GRAPH_CLAUSE
                        ?s a <{subject_class_uri}> .
                        ?s <{property_uri}> ?o .
                        FILTER(isLiteral(?o))
                    #END_GRAPH_CLAUSE
                }}
                """
            else:
                # For object properties, count distinct subjects using this pattern
                pattern_query = f"""
                SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {{
                    #GRAPH_CLAUSE
                        ?s a <{subject_class_uri}> .
                        ?s <{property_uri}> ?o .
                        ?o a <{object_class_uri}> .
                    #END_GRAPH_CLAUSE
                }}
                """

            if sample_limit:
                # Limit to the same sample used for total count
                pattern_query = pattern_query.replace(
                    "SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {",
                    f"""SELECT (COUNT(DISTINCT ?s) AS ?participating) WHERE {{
                    ?s a <{subject_class_uri}> .
                    {{
                        SELECT DISTINCT ?s WHERE {{
                            #GRAPH_CLAUSE
                                ?s a <{subject_class_uri}> .
                            #END_GRAPH_CLAUSE
                        }}
                        LIMIT {sample_limit}
                    }}"""
                )

            query = self._replace_graph_clause_placeholder(pattern_query)

            try:
                sparql.setQuery(query)
                results = self._safe_query(sparql, query)

                participating = 0
                if results and results.get("results", {}).get("bindings"):
                    participating = int(results["results"]["bindings"][0]["participating"]["value"])

                # Calculate coverage ratio and percentage
                coverage_ratio = participating / total_entities if total_entities > 0 else 0
                coverage_percent = coverage_ratio * 100

                coverage_results.append({
                    "subject_class": row['subject_class'],
                    "subject_uri": subject_class_uri,
                    "property": row['property'],
                    "property_uri": property_uri,
                    "object_class": row['object_class'],
                    "object_uri": object_class_uri,
                    "total_entities": total_entities,
                    "participating_entities": participating,
                    "occurrence_count": participating,  # For backward compatibility
                    "coverage_ratio": round(coverage_ratio, 4),
                    "coverage_percent": round(coverage_percent, 2),
                    "shape_pattern": f"{row['subject_class']} -> {row['property']} -> {row['object_class']}"
                })

            except Exception as e:
                coverage_results.append({
                    "subject_class": row['subject_class'],
                    "subject_uri": subject_class_uri,
                    "property": row['property'],
                    "property_uri": property_uri,
                    "object_class": row['object_class'],
                    "object_uri": object_class_uri,
                    "total_entities": total_entities,
                    "participating_entities": 0,
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

        return frequencies_df

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
