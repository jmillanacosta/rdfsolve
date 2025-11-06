"""
VoID (Vocabulary of Interlinked Datasets) Parser

This module provides functionality to parse VoID descriptions and extract
the underlying schema structure of RDF datasets. It can also generate VoID
descriptions from SPARQL endpoints using CONSTRUCT queries.
Adapted from https://github.com/sib-swiss/kgsteward/blob/0d7ec07715c1fcdce19c8246bc824185e771bdc4/src/kgsteward/special.py#L8
"""

from rdflib import Graph, URIRef
import time
import pandas as pd
from typing import Dict, Union, Optional
from SPARQLWrapper import SPARQLWrapper, TURTLE


class VoidParser:
    """Parser for VoID (Vocabulary of Interlinked Datasets) files."""

    def __init__(self, void_source: Optional[Union[str, Graph]] = None):
        """
        Initialize the VoID parser.
        
        Args:
            void_source: Either a file path (str) or an RDF Graph object
        """
        self.void_file_path = None
        self.graph = Graph()
        self.schema_triples = []
        self.classes = {}
        self.properties = {}

        # VoID namespace URIs
        self.void_class = URIRef("http://rdfs.org/ns/void#class")
        self.void_property = URIRef("http://rdfs.org/ns/void#property")
        self.void_propertyPartition = URIRef(
            "http://rdfs.org/ns/void#propertyPartition")
        self.void_classPartition = URIRef(
            "http://rdfs.org/ns/void#classPartition")
        self.void_datatypePartition = URIRef(
            "http://ldf.fi/void-ext#datatypePartition")
        # Extended VoID properties for enhanced schema
        self.void_subjectClass = URIRef("http://ldf.fi/void-ext#subjectClass")
        self.void_objectClass = URIRef("http://ldf.fi/void-ext#objectClass")

        if void_source:
            if isinstance(void_source, str):
                self.void_file_path = void_source
                self._load_graph()
            elif isinstance(void_source, Graph):
                self.graph = void_source

    def _load_graph(self):
        """Load the VoID file into an RDF graph."""
        self.graph.parse(self.void_file_path, format="turtle")

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
        
        # Try enhanced extraction first (with subjectClass/objectClass)
        enhanced_triples = self._extract_enhanced_schema()
        if enhanced_triples:
            self.schema_triples = enhanced_triples
            return
            
        # Fall back to original method for legacy VoID files
        self._extract_legacy_schema()

    def _extract_enhanced_schema(self):
        """Extract schema from enhanced property partitions with type info."""
        triples = []
        
        # Find all property partitions with subject/object class info
        for partition, _, property_uri in self.graph.triples(
                (None, self.void_property, None)):
            # Get subject class
            subject_classes = list(self.graph.triples(
                (partition, self.void_subjectClass, None)))
            # Get object class
            object_classes = list(self.graph.triples(
                (partition, self.void_objectClass, None)))
            
            if subject_classes and object_classes:
                for _, _, subject_class in subject_classes:
                    for _, _, object_class in object_classes:
                        triples.append((subject_class, property_uri,
                                       object_class))
        
        return triples

    def _extract_legacy_schema(self):
        """Extract schema triples by analyzing class property partitions (legacy)."""
        for class_dataset, class_uri in self.classes.items():
            # Find property partitions for this class
            for s, p, o in self.graph.triples((class_dataset,
                                               self.void_propertyPartition,
                                               None)):
                property_partition = o

                # Find what property this partition describes
                prop_triples = self.graph.triples((property_partition,
                                                   self.void_property, None))
                for s2, p2, property_uri in prop_triples:

                    # Find object types for this property
                    object_classes = []
                    class_part_triples = self.graph.triples((
                        property_partition, self.void_classPartition, None))
                    for s3, p3, o3 in class_part_triples:
                        class_partition = o3
                        # Get the actual class from the partition
                        class_triples = self.graph.triples((class_partition,
                                                            self.void_class,
                                                            None))
                        for s4, p4, target_class in class_triples:
                            object_classes.append(target_class)

                    # Check for datatype partitions (literal objects)
                    datatype_partitions = []
                    dtype_triples = self.graph.triples((
                        property_partition, self.void_datatypePartition, None))
                    for s3, p3, o3 in dtype_triples:
                        datatype_partitions.append(o3)

                    # Create schema triple(s)
                    if object_classes:
                        for obj_class in object_classes:
                            triple = (class_uri, property_uri, obj_class)
                            self.schema_triples.append(triple)
                    elif datatype_partitions:
                        triple = (class_uri, property_uri, "Literal")
                        self.schema_triples.append(triple)
                    else:
                        triple = (class_uri, property_uri, "Resource")
                        self.schema_triples.append(triple)

    def _filter_void_nodes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out VoID-related triples."""
        mask = (
            ~df['subject_uri'].str.contains('void', case=False, na=False) &
            ~df['property_uri'].str.contains('void', case=False, na=False) &
            ~df['object_uri'].str.contains('void', case=False, na=False)
        )
        return df[mask].copy()

    def to_schema(self, filter_void_nodes: bool = True) -> pd.DataFrame:
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
            # Extract readable names TODO use bioregistry
            s_name = (str(s).split('#')[-1].split('/')[-1]
                      if '#' in str(s) else str(s).split('/')[-1])
            p_name = (str(p).split('#')[-1].split('/')[-1]
                      if '#' in str(p) else str(p).split('/')[-1])
            o_name = (str(o).split('#')[-1].split('/')[-1]
                      if '#' in str(o) else str(o).split('/')[-1]
                      if o not in ["Literal", "Resource"] else o)

            schema_data.append({
                'subject_class': s_name,
                'subject_uri': str(s),
                'property': p_name,
                'property_uri': str(p),
                'object_class': o_name,
                'object_uri': str(o) if o not in ["Literal", "Resource"] else o
            })

        df = pd.DataFrame(schema_data)

        if filter_void_nodes and not df.empty:
            df = self._filter_void_nodes(df)

        return df

    def to_json(self, filter_void_nodes: bool = True) -> Dict:
        """
        Parse VoID file and return schema as JSON structure.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes

        Returns:
            Dictionary with schema information
        """
        df = self.to_schema(filter_void_nodes=filter_void_nodes)

        schema_graph = {
            "triples": [],
            "metadata": {
                "total_triples": len(df),
                "classes": df['subject_uri'].unique().tolist()
                if not df.empty else [],
                "properties": df['property_uri'].unique().tolist()
                if not df.empty else [],
                "objects": df['object_uri'].unique().tolist()
                if not df.empty else []
            }
        }

        # Add triples
        for _, row in df.iterrows():
            schema_graph["triples"].append([
                row['subject_uri'],
                row['property_uri'],
                row['object_uri']
            ])

        return schema_graph

    @staticmethod
    def get_void_queries(graph_uri: str, counts: bool = True, 
                        sample_limit: Optional[int] = None) -> Dict[str, str]:
        """
        Get formatted CONSTRUCT queries for VoID generation.
        
        Args:
            graph_uri: Specific graph URI to analyze
            counts: If True, include COUNT aggregations; if False, faster discovery
            sample_limit: Optional LIMIT for sampling (speeds up discovery)
            
        Returns:
            Dictionary containing the formatted queries
        """
        limit_clause = f"LIMIT {sample_limit}" if sample_limit else ""
        
        if counts:
            # Count-based queries (slower but complete)
            class_q = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
CONSTRUCT {{
    ?cp void:class ?class ;
        void:entities ?count .
}}
WHERE {{
    {{
        SELECT ?class (COUNT(*) AS ?count)
        WHERE {{
            GRAPH <{graph_uri}> {{
                [] a ?class
            }}
        }}
        GROUP BY ?class
        {limit_clause}
    }}
    BIND(IRI(CONCAT('{graph_uri}/void/class_partition_',
                   MD5(STR(?class)))) AS ?cp)
}}"""
            
            prop_q = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
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
            GRAPH <{graph_uri}> {{
                ?subject ?property ?object .
                ?subject rdf:type ?subject_class .
                OPTIONAL {{
                    ?object rdf:type ?obj_type .
                    FILTER(isURI(?object))
                }}
                BIND(
                    IF(isLiteral(?object),
                       <http://www.w3.org/2000/01/rdf-schema#Literal>,
                       COALESCE(?obj_type,
                               <http://www.w3.org/2000/01/rdf-schema#Resource>)
                    ) AS ?object_class
                )
            }}
        }}
        GROUP BY ?property ?subject_class ?object_class
        {limit_clause}
    }}
    BIND(IRI(CONCAT('{graph_uri}/void/property_partition_',
                   MD5(CONCAT(STR(?property), STR(?subject_class))))) AS ?pp)
}}"""

            dtype_q = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
CONSTRUCT {{
    ?dp void-ext:datatypePartition ?datatype ;
        void:triples ?count .
}}
WHERE {{
    {{
        SELECT ?datatype (COUNT(*) AS ?count)
        WHERE {{
            GRAPH <{graph_uri}> {{
                [] ?p ?o .
                FILTER(isLiteral(?o))
                BIND(datatype(?o) AS ?datatype)
            }}
        }}
        GROUP BY ?datatype
        {limit_clause}
    }}
    BIND(IRI(CONCAT('{graph_uri}/void/datatype_partition_',
                   MD5(STR(?datatype)))) AS ?dp)
}}"""
        else:
            # Discovery-only queries (faster)
            class_q = f"""PREFIX void: <http://rdfs.org/ns/void#>
CONSTRUCT {{
    ?cp void:class ?class .
}}
WHERE {{
    {{
        SELECT DISTINCT ?class
        WHERE {{
            GRAPH <{graph_uri}> {{
                [] a ?class
            }}
        }}
        {limit_clause}
    }}
    BIND(IRI(CONCAT('{graph_uri}/void/class_partition_',
                   MD5(STR(?class)))) AS ?cp)
}}"""

            prop_q = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
CONSTRUCT {{
    ?pp void:property ?property ;
        void-ext:subjectClass ?subject_class ;
        void-ext:objectClass ?object_class .
}}
WHERE {{
    {{
        SELECT DISTINCT ?property ?subject_class ?object_class
        WHERE {{
            GRAPH <{graph_uri}> {{
                ?subject ?property ?object .
                ?subject rdf:type ?subject_class .
                OPTIONAL {{
                    ?object rdf:type ?obj_type .
                    FILTER(isURI(?object))
                }}
                BIND(
                    IF(isLiteral(?object),
                       <http://www.w3.org/2000/01/rdf-schema#Literal>,
                       COALESCE(?obj_type,
                               <http://www.w3.org/2000/01/rdf-schema#Resource>)
                    ) AS ?object_class
                )
            }}
        }}
        {limit_clause}
    }}
    BIND(IRI(CONCAT('{graph_uri}/void/property_partition_',
                   MD5(CONCAT(STR(?property), STR(?subject_class))))) AS ?pp)
}}"""

            dtype_q = f"""PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
CONSTRUCT {{
    ?dp void-ext:datatypePartition ?datatype .
}}
WHERE {{
    {{
        SELECT DISTINCT ?datatype
        WHERE {{
            GRAPH <{graph_uri}> {{
                [] ?p ?o .
                FILTER(isLiteral(?o))
                BIND(datatype(?o) AS ?datatype)
            }}
        }}
        {limit_clause}
    }}
    BIND(IRI(CONCAT('{graph_uri}/void/datatype_partition_',
                   MD5(STR(?datatype)))) AS ?dp)
}}"""
        
        return {
            'class_partitions': class_q,
            'property_partitions': prop_q,
            'datatype_partitions': dtype_q
        }

    @staticmethod
    def generate_void_from_sparql(endpoint_url: str, graph_uri: str,
                                  output_file: Optional[str] = None,
                                  counts: bool = True,
                                  sample_limit: Optional[int] = None) -> Graph:
        """
        Generate VoID description from SPARQL endpoint using CONSTRUCT queries.
        
        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uri: Graph URI for the dataset
            output_file: Optional output file path for TTL
            counts: If True, include COUNT aggregations; if False, faster discovery
            sample_limit: Optional LIMIT for sampling (speeds up discovery)
            
        Returns:
            RDF Graph containing the VoID description
        """
        queries = VoidParser.get_void_queries(graph_uri, counts, sample_limit)

        sparql = SPARQLWrapper(endpoint_url)
        # set a more aggressive timeout for fast queries
        timeout_seconds = 30 if not counts else 60
        try:
            sparql.setTimeout(timeout_seconds)
        except Exception:
            # some SPARQLWrapper versions may not have setTimeout; ignore
            pass

        merged_graph = Graph()

        def run_construct(query_text: str, name: str,
                          is_optional: bool = False):
            sparql.setQuery(query_text)
            sparql.setReturnFormat(TURTLE)
            print(f"Starting query: {name}")
            t0 = time.monotonic()
            
            try:
                results = sparql.query().convert()
                dt = time.monotonic() - t0
                print(f"Finished query: {name} (took {dt:.2f}s)")

                # Parse result - handle bytes properly
                try:
                    # Handle both bytes and string results
                    if results:
                        if isinstance(results, bytes):
                            result_str = results.decode('utf-8')
                        else:
                            result_str = str(results)
                        
                        if result_str.strip():
                            merged_graph.parse(data=result_str,
                                               format="turtle")
                        else:
                            print(f"Empty results for {name}")
                    else:
                        print(f"No results for {name}")
                except Exception as e:
                    print(f"Failed to parse results for {name}: {e}")
                    if not is_optional:
                        raise
                        
            except Exception as e:
                dt = time.monotonic() - t0
                print(f"Query {name} failed after {dt:.2f}s: {e}")
                
                # Check for timeout conditions
                timeout_keywords = ["timeout", "timed out"]
                if any(keyword in str(e).lower()
                       for keyword in timeout_keywords):
                    print(f"Query {name} timed out - common with complex "
                          "queries")
                    if is_optional:
                        print(f"Skipping optional query: {name}")
                        return
                if not is_optional:
                    raise

        try:
            # Execute queries with timing - property query is optional
            run_construct(queries['class_partitions'], "class_partitions")
            run_construct(queries['property_partitions'],
                          "property_partitions", is_optional=True)
            run_construct(queries['datatype_partitions'],
                          "datatype_partitions", is_optional=True)

            # Save to file if specified
            if output_file:
                merged_graph.serialize(destination=output_file,
                                       format="turtle")
                print(f"VoID description saved to {output_file}")

            return merged_graph

        except Exception as e:
            raise RuntimeError(
                f"Failed to generate VoID from SPARQL endpoint: {e}"
            )

    @classmethod
    def from_sparql(cls, endpoint_url: str, graph_uri: str,
                    output_file: Optional[str] = None) -> 'VoidParser':
        """
        Create a VoidParser instance from a SPARQL endpoint.
        
        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uri: Graph URI for the dataset
            output_file: Optional output file path for TTL
            
        Returns:
            VoidParser instance with generated VoID
        """
        void_graph = cls.generate_void_from_sparql(
            endpoint_url, graph_uri, output_file
        )
        return cls(void_graph)


def parse_void_file(void_file_path: str,
                    filter_void_nodes: bool = True) -> pd.DataFrame:
    """
    Convenience function to parse a VoID file and return schema DataFrame.

    Args:
        void_file_path: Path to the VoID turtle file
        filter_void_nodes: Whether to filter out VoID-specific nodes

    Returns:
        DataFrame with schema information
    """
    parser = VoidParser(void_file_path)
    return parser.to_schema(filter_void_nodes=filter_void_nodes)


def generate_void_from_endpoint(endpoint_url: str, graph_uri: str,
                                output_file: Optional[str] = None
                                ) -> 'VoidParser':
    """
    Generate VoID from SPARQL endpoint and create parser.
    
    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uri: Graph URI for the dataset
        output_file: Optional output file path for TTL
        
    Returns:
        VoidParser instance with generated VoID
    """
    return VoidParser.from_sparql(endpoint_url, graph_uri, output_file)
