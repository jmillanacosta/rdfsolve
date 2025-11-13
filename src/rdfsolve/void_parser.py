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
        # Extended VoID properties for enhanced schema
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
            # No specific graphs - query all graphs (no GRAPH clause)
            result = query.replace("#GRAPH_CLAUSE", "")
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
        Discover existing VoID graphs in the endpoint.

        Args:
            endpoint_url: SPARQL endpoint URL

        Returns:
            Dictionary with discovery results
        """
        from SPARQLWrapper import SPARQLWrapper, JSON

        try:
            sparql = SPARQLWrapper(endpoint_url)
            sparql.setReturnFormat(JSON)

            # First, discover graphs that might contain VoID content
            graph_discovery_query = """
            SELECT DISTINCT ?g WHERE {
                GRAPH ?g {
                    ?s ?p ?o
                }
                FILTER(
                    REGEX(STR(?g), "void", "i") || 
                    REGEX(STR(?g), "well-known", "i") ||
                    REGEX(STR(?g), "\\\\.well-known", "i")
                )
            }
            ORDER BY ?g
            LIMIT 100
            """

            sparql.setQuery(graph_discovery_query)
            graph_results = sparql.query().convert()

            candidate_graphs = []
            for result in graph_results["results"]["bindings"]:
                candidate_graphs.append(result["g"]["value"])

            if not candidate_graphs:
                return {
                    "has_void_descriptions": False,
                    "found_graphs": [],
                    "total_graphs": 0,
                    "void_content": {},
                }

            # Now check each candidate graph for actual VoID content
            found_graphs = []
            void_content = {}

            for graph_uri in candidate_graphs:
                print(f"Found graph: {graph_uri}")
                # Query to check VoID content in this specific graph
                void_check_query = f"""
                SELECT 
                (COUNT(DISTINCT ?cp) as ?class_partitions)
                (COUNT(DISTINCT ?pp) as ?property_partitions) 
                (COUNT(DISTINCT ?dp) as ?datatype_partitions)
                WHERE {{
                  OPTIONAL {{
                    GRAPH <{graph_uri}> {{
                      ?cp <http://rdfs.org/ns/void#class> ?class .
                    }}
                  }}
                  OPTIONAL {{
                    GRAPH <{graph_uri}> {{
                      ?pp <http://rdfs.org/ns/void#property> ?property .
                    }}
                  }}
                  OPTIONAL {{
                    GRAPH <{graph_uri}> {{
                      ?dp <http://ldf.fi/void-ext#datatypePartition> ?datatype .
                    }}
                  }}
                }}
                """

                try:
                    sparql.setQuery(void_check_query)
                    void_results = sparql.query().convert()

                    if void_results["results"]["bindings"]:
                        result = void_results["results"]["bindings"][0]
                        class_partitions = int(
                            result.get("class_partitions", {}).get("value", "0")
                        )
                        property_partitions = int(
                            result.get("property_partitions", {}).get("value", "0")
                        )
                        datatype_partitions = int(
                            result.get("datatype_partitions", {}).get("value", "0")
                        )

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
                            found_graphs.append(graph_uri)

                except Exception as e:
                    print(f"Error checking VoID content in {graph_uri}: {e}")
                    void_content[graph_uri] = {
                        "class_partition_count": 0,
                        "property_partition_count": 0,
                        "datatype_partition_count": 0,
                        "has_any_partitions": False,
                        "error": str(e),
                    }

            return {
                "has_void_descriptions": len(found_graphs) > 0,
                "found_graphs": found_graphs,
                "total_graphs": len(candidate_graphs),
                "void_content": void_content,
            }

        except Exception as e:
            print(f"VoID discovery failed: {e}")
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

        merged_graph = Graph()

        for graph_uri in graph_uris:
            try:
                sparql = SPARQLWrapper(endpoint_url)
                sparql.setReturnFormat(TURTLE)

                # Query to retrieve all VoID content from the graph
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

                sparql.setQuery(void_query)
                results = sparql.query().convert()

                if results and isinstance(results, bytes):
                    result_str = results.decode("utf-8")
                elif results:
                    result_str = str(results)
                else:
                    continue

                if result_str.strip():
                    merged_graph.parse(data=result_str, format="turtle")
                    print(f"Retrieved VoID from: {graph_uri}")

            except Exception as e:
                print(f"Failed to retrieve VoID from {graph_uri}: {e}")
                continue

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

        return triples

    def _extract_legacy_schema(self):
        """Extract schema triples by analyzing class property partitions (legacy)."""
        for class_dataset, class_uri in self.classes.items():
            # Find property partitions for this class
            for s, p, o in self.graph.triples(
                (class_dataset, self.void_propertyPartition, None)
            ):
                property_partition = o

                # Find what property this partition describes
                prop_triples = self.graph.triples(
                    (property_partition, self.void_property, None)
                )
                for s2, p2, property_uri in prop_triples:

                    # Find object types for this property
                    object_classes = []
                    class_part_triples = self.graph.triples(
                        (property_partition, self.void_classPartition, None)
                    )
                    for s3, p3, o3 in class_part_triples:
                        class_partition = o3
                        # Get the actual class from the partition
                        class_triples = self.graph.triples(
                            (class_partition, self.void_class, None)
                        )
                        for s4, p4, target_class in class_triples:
                            object_classes.append(target_class)

                    # Check for datatype partitions (literal objects)
                    datatype_partitions = []
                    dtype_triples = self.graph.triples(
                        (property_partition, self.void_datatypePartition, None)
                    )
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

    def to_linkml(
        self,
        filter_void_nodes: bool = True,
        schema_name: Optional[str] = None,
        schema_description: Optional[str] = None,
    ) -> SchemaDefinition:
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
                or f"LinkML schema extracted from VoID analysis of " f"{schema_name}"
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
            common_ns = {
            }

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
        linkml_schema = self.to_linkml(
            filter_void_nodes=filter_void_nodes,
            schema_name=schema_name,
            schema_description=schema_description,
        )

        from linkml.generators.yamlgen import YAMLGenerator

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
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
        exclude_graphs: bool = True,
    ) -> Dict[str, str]:
        """
        Get formatted CONSTRUCT queries for VoID generation.

        Args:
            graph_uris: Graph URI(s) to analyze. If None, queries all graphs
            counts: If True, include COUNT aggregations; else faster discovery
            sample_limit: Optional LIMIT for sampling (speeds up discovery)
            sample_offset: Optional OFFSET for pagination (starts from offset)
            offset_limit_steps: If provided, use this as both LIMIT and OFFSET step
            exclude_graphs: Whether to exclude system or any specific graphs

        Returns:
            Dictionary containing the formatted queries
        """
        # Create a temporary instance to use the graph clause methods
        temp_parser = VoidParser(
            graph_uris=graph_uris, exclude_graphs=exclude_graphs
        )

        # Determine the base graph URI for VoID partition naming
        if isinstance(graph_uris, str):
            base_graph_uri = graph_uris
        elif isinstance(graph_uris, list) and len(graph_uris) == 1:
            base_graph_uri = graph_uris[0]
        else:
            base_graph_uri = "http://example.org/dataset"
        
        # Build limit and offset clause
        limit_offset_clause = ""
        
        # Use offset_limit_steps if provided, otherwise fall back to individual params
        if offset_limit_steps is not None:
            limit_offset_clause += f"LIMIT {offset_limit_steps}"
        else:
            if sample_offset is not None:
                limit_offset_clause += f"OFFSET {sample_offset}\n        "
            if sample_limit is not None:
                limit_offset_clause += f"LIMIT {sample_limit}"

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
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
        exclude_graphs: bool = True,
    ) -> Graph:
        """
        Generate VoID description from SPARQL endpoint using CONSTRUCT queries.

        Args:
            endpoint_url: SPARQL endpoint URL
            graph_uris: Graph URI(s) for the dataset. If None, queries all graphs
            output_file: Optional output file path for TTL
            counts: If True, include COUNT aggregations; else faster discovery
            sample_limit: Optional LIMIT for sampling (speeds up discovery)
            sample_offset: Optional OFFSET for pagination (starts from offset)
            exclude_graphs: Whether to exclude Virtuoso system graphs

        Returns:
            RDF Graph containing the VoID description
        """
        queries = VoidParser.get_void_queries(
            graph_uris, counts, sample_limit, sample_offset,
            offset_limit_steps, exclude_graphs
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
                print(f"Changed working directory to: {temp_dir}")
        except (FileNotFoundError, OSError, PermissionError):
            # If we can't get cwd or it doesn't exist, use temp directory
            temp_dir = tempfile.gettempdir()
            os.chdir(temp_dir)
            print(f"Working directory issue resolved, using: {temp_dir}")

        def run_construct(query_text: str, name: str, is_optional: bool = False, public_id: str = "http://jmillanacosta.github.io/"):
            public_id = f"{public_id}/{name}/void"
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
                            result_str = results.decode("utf-8")
                        else:
                            result_str = str(results)

                        if result_str.strip():
                            merged_graph.parse(
                                data=result_str, format="turtle", publicID=public_id
                            )
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
                if any(keyword in str(e).lower() for keyword in timeout_keywords):
                    print(f"Query {name} timed out - common with complex " "queries")
                    if is_optional:
                        print(f"Skipping optional query: {name}")
                        return
                if not is_optional:
                    raise

        try:
            # Execute queries with timing - property query is optional
            run_construct(queries["class_partitions"], "class_partitions")
            run_construct(
                queries["property_partitions"], "property_partitions", is_optional=False
            )
            run_construct(
                queries["datatype_partitions"], "datatype_partitions", is_optional=False
            )

            # Save to file if specified
            if output_file:
                merged_graph.serialize(destination=output_file, format="turtle")
                print(f"VoID description saved to {output_file}")

            return merged_graph

        except Exception as e:
            raise RuntimeError(f"Failed to generate VoID from SPARQL endpoint: {e}")

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
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
        exclude_graphs: bool = True,
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
            sample_offset: Optional OFFSET for pagination (starts from offset)
            exclude_graphs: Whether to exclude Virtuoso system graphs
            graph_uris: Graph URI(s) to analyze. If None, queries all graphs
        Returns:
            VoidParser instance with the best available VoID
        """
        import os

        # Step 1: Discover existing VoID graphs
        temp_parser = cls(graph_uris=graph_uris, exclude_graphs=exclude_graphs)
        discovery_result = temp_parser.discover_void_graphs(endpoint_url)

        existing_void_graph = None
        existing_parser = None

        # Step 2: Try to retrieve existing VoID if available
        if discovery_result.get("has_void_descriptions", False):
            valid_void_graphs = [
                graph_uri
                for graph_uri, content in discovery_result.get(
                    "void_content", {}
                ).items()
                if content.get("has_any_partitions", False)
            ]

            if valid_void_graphs:
                existing_void_graph = temp_parser.void_querier(
                    endpoint_url, valid_void_graphs
                )

                if len(existing_void_graph) > 0:
                    existing_parser = cls(existing_void_graph)
                    existing_schema_df = existing_parser.to_schema(
                        filter_void_admin_nodes=True
                    )

                    # Check if existing VoID has sufficient content
                    if prefer_existing and len(existing_schema_df) > 10:
                        print(
                            f"Using existing VoID with "
                            f"{len(existing_schema_df):,} schema triples"
                        )

                        # Save existing VoID
                        existing_void_path = os.path.join(
                            exports_path, f"{dataset_name}_existing_void.ttl"
                        )
                        existing_void_graph.serialize(
                            destination=existing_void_path, format="turtle"
                        )
                        print(f"Existing VoID saved to: {existing_void_path}")

                        return existing_parser

        # Step 3: Generate new VoID if no suitable existing VoID found
        print("No suitable existing VoID found, generating new VoID...")
        output_path = os.path.join(exports_path, f"{dataset_name}_generated_void.ttl")

        generated_void_graph = cls.generate_void_from_sparql(
            endpoint_url=endpoint_url,
            graph_uris=graph_uris,
            output_file=output_path,
            counts=counts,
            sample_limit=sample_limit,
            sample_offset=sample_offset,
            offset_limit_steps=offset_limit_steps,
            exclude_graphs=exclude_graphs
        )

        return cls(
            generated_void_graph,
            graph_uris=graph_uris,
            exclude_graphs=exclude_graphs
        )

    def count_instances_per_class(
        self, endpoint_url: str, sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None, chunk_size: Optional[int] = None,
        offset_limit_steps: Optional[int] = None,
        delay_between_chunks: float = 1.0
    ) -> Dict:
        """
        Count instances for each class in the dataset.

        Args:
            endpoint_url: SPARQL endpoint URL
            sample_limit: Optional limit for total results (None = all results)
            sample_offset: Optional starting offset for pagination
            chunk_size: Optional size for chunked querying (enables pagination)
            delay_between_chunks: Seconds to wait between chunk queries (default: 1.0)

        Returns:
            Dictionary mapping class URIs to instance counts
        """
        from SPARQLWrapper import SPARQLWrapper, JSON

        sparql = SPARQLWrapper(endpoint_url)
        sparql.setReturnFormat(JSON)
        
        # If offset_limit_steps is provided, use it for chunked querying
        if offset_limit_steps is not None:
            return self._count_instances_chunked(
                sparql, sample_limit, sample_offset or 0, offset_limit_steps,
                delay_between_chunks
            )
        # If chunk_size is provided, use chunked querying
        elif chunk_size is not None:
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
            results = sparql.query().convert()
            instance_counts = {}

            for result in results["results"]["bindings"]:
                class_uri = result["class"]["value"]
                count = int(result["count"]["value"])
                instance_counts[class_uri] = count

            return instance_counts

        except Exception as e:
            print(f"Failed to count instances: {e}")
            return {}
    
    def _count_instances_chunked(self, sparql, total_limit: Optional[int], start_offset: int, chunk_size: int, delay_between_chunks: float = 1.0) -> Dict:
        """Helper method for chunked instance counting."""
        import time
        
        instance_counts = {}
        current_offset = start_offset or 0
        total_fetched = 0
        chunk_number = 0
        
        while True:
            # Calculate how many to fetch in this chunk
            if total_limit is not None:
                remaining = total_limit - total_fetched
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
            sparql.setQuery(query)
            
            try:
                results = sparql.query().convert()
                chunk_results = results["results"]["bindings"]
                
                # If no results, we've reached the end
                if not chunk_results:
                    break
                
                # Process chunk results
                for result in chunk_results:
                    class_uri = result["class"]["value"]
                    count = int(result["count"]["value"])
                    # Aggregate counts if class already exists
                    if class_uri in instance_counts:
                        instance_counts[class_uri] += count
                    else:
                        instance_counts[class_uri] = count
                
                fetched_in_chunk = len(chunk_results)
                total_fetched += fetched_in_chunk
                current_offset += fetched_in_chunk
                
                # If we got fewer results than requested, we've reached the end
                if fetched_in_chunk < current_chunk_size:
                    # Make one final query to get any remaining results with no limit
                    print(f"Final query for remaining results from offset {current_offset}...")
                    final_query_template = f"""
                    SELECT ?class (COUNT(DISTINCT ?instance) AS ?count) WHERE {{
                        #GRAPH_CLAUSE
                            ?instance a ?class .
                        #END_GRAPH_CLAUSE
                    }}
                    GROUP BY ?class
                    ORDER BY DESC(?count)
                    OFFSET {current_offset}
                    """
                    
                    final_query = self._replace_graph_clause_placeholder(final_query_template)
                    sparql.setQuery(final_query)
                    
                    try:
                        final_results = sparql.query().convert()
                        final_chunk_results = final_results["results"]["bindings"]
                        
                        if final_chunk_results:
                            for result in final_chunk_results:
                                class_uri = result["class"]["value"]
                                count = int(result["count"]["value"])
                                if class_uri in instance_counts:
                                    instance_counts[class_uri] += count
                                else:
                                    instance_counts[class_uri] = count
                            
                            print(f"Final query retrieved {len(final_chunk_results)} additional classes")
                    except Exception as e:
                        print(f"Final query failed: {e}")
                    
                    break
                
                # Add delay between chunks to be respectful to the endpoint
                if delay_between_chunks > 0:
                    time.sleep(delay_between_chunks)
                    
            except Exception as e:
                print(f"Failed to fetch chunk at offset {current_offset}: {e}")
                break
        
        return instance_counts

    def get_class_mappings(
        self, endpoint_url: str, sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None, chunk_size: Optional[int] = None,
        offset_limit_steps: Optional[int] = None, delay_between_chunks: float = 1.0
    ) -> Dict:
        """
        Get mapping of instances to their classes.

        Args:
            endpoint_url: SPARQL endpoint URL
            sample_limit: Optional limit for total results (None = all results)
            sample_offset: Optional starting offset for pagination
            chunk_size: Optional size for chunked querying (enables pagination)
            offset_limit_steps: If provided, use as both LIMIT and step size
            delay_between_chunks: Seconds to wait between chunks (default: 1.0)

        Returns:
            Dictionary mapping instance URIs to class URIs
        """
        from SPARQLWrapper import SPARQLWrapper, JSON

        sparql = SPARQLWrapper(endpoint_url)
        sparql.setReturnFormat(JSON)
        
        # If offset_limit_steps is provided, use it for chunked querying
        if offset_limit_steps is not None:
            return self._get_class_mappings_chunked(
                sparql, sample_limit, sample_offset or 0, offset_limit_steps,
                delay_between_chunks
            )
        # If chunk_size is provided, use chunked querying
        elif chunk_size is not None:
            return self._get_class_mappings_chunked(
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
        SELECT ?instance ?class WHERE {{
            #GRAPH_CLAUSE
                ?instance a ?class .
            #END_GRAPH_CLAUSE
        }}
        {limit_offset_clause}
        """

        query = self._replace_graph_clause_placeholder(query_template)
        sparql.setQuery(query)

        try:
            results = sparql.query().convert()
            class_mappings = {}

            for result in results["results"]["bindings"]:
                instance_uri = result["instance"]["value"]
                class_uri = result["class"]["value"]

                if instance_uri not in class_mappings:
                    class_mappings[instance_uri] = []
                class_mappings[instance_uri].append(class_uri)

            return class_mappings

        except Exception as e:
            print(f"Failed to get class mappings: {e}")
            return {}
    
    def _get_class_mappings_chunked(
        self, sparql, total_limit: Optional[int], start_offset: int,
        chunk_size: int, delay_between_chunks: float = 1.0
    ) -> Dict:
        """Helper method for chunked class mappings retrieval."""
        import time
        
        class_mappings = {}
        current_offset = start_offset or 0
        total_fetched = 0
        
        while True:
            # Calculate how many to fetch in this chunk
            if total_limit is not None:
                remaining = total_limit - total_fetched
                if remaining <= 0:
                    break
                current_chunk_size = min(chunk_size, remaining)
            else:
                current_chunk_size = chunk_size
            
            # Virtuoso limit: OFFSET + LIMIT cannot exceed 10,000 for ORDER BY
            virtuoso_limit = 10000
            if current_offset + current_chunk_size > virtuoso_limit:
                # Either reduce chunk size or remove ORDER BY
                if current_offset >= virtuoso_limit:
                    # Can't use ORDER BY anymore, switch to unordered
                    query_template = f"""
                    SELECT ?instance ?class WHERE {{
                        #GRAPH_CLAUSE
                            ?instance a ?class .
                        #END_GRAPH_CLAUSE
                    }}
                    OFFSET {current_offset}
                    LIMIT {current_chunk_size}
                    """
                else:
                    # Reduce chunk size to stay within limit
                    current_chunk_size = virtuoso_limit - current_offset
                    query_template = f"""
                    SELECT ?instance ?class WHERE {{
                        #GRAPH_CLAUSE
                            ?instance a ?class .
                        #END_GRAPH_CLAUSE
                    }}
                    ORDER BY ?instance ?class
                    OFFSET {current_offset}
                    LIMIT {current_chunk_size}
                    """
            else:
                # Normal case - we're within the limit
                query_template = f"""
                SELECT ?instance ?class WHERE {{
                    #GRAPH_CLAUSE
                        ?instance a ?class .
                    #END_GRAPH_CLAUSE
                }}
                ORDER BY ?instance ?class
                OFFSET {current_offset}
                LIMIT {current_chunk_size}
                """
            
            query = self._replace_graph_clause_placeholder(query_template)
            sparql.setQuery(query)
            
            try:
                results = sparql.query().convert()
                chunk_results = results["results"]["bindings"]
                
                # If no results, we've reached the end
                if not chunk_results:
                    break
                
                # Process chunk results
                for result in chunk_results:
                    instance_uri = result["instance"]["value"]
                    class_uri = result["class"]["value"]
                    
                    if instance_uri not in class_mappings:
                        class_mappings[instance_uri] = []
                    class_mappings[instance_uri].append(class_uri)
                
                fetched_in_chunk = len(chunk_results)
                total_fetched += fetched_in_chunk
                current_offset += fetched_in_chunk
                
                
                # If we got fewer results than requested, we've reached the end
                if fetched_in_chunk < current_chunk_size:
                    # Make one final query to get any remaining results with no limit
                    
                    # Check if we need to avoid ORDER BY due to Virtuoso limit
                    if current_offset >= virtuoso_limit:
                        final_query_template = f"""
                        SELECT ?instance ?class WHERE {{
                            #GRAPH_CLAUSE
                                ?instance a ?class .
                            #END_GRAPH_CLAUSE
                        }}
                        OFFSET {current_offset}
                        """
                    else:
                        final_query_template = f"""
                        SELECT ?instance ?class WHERE {{
                            #GRAPH_CLAUSE
                                ?instance a ?class .
                            #END_GRAPH_CLAUSE
                        }}
                        ORDER BY ?instance ?class
                        OFFSET {current_offset}
                        """
                    
                    final_query = self._replace_graph_clause_placeholder(final_query_template)
                    sparql.setQuery(final_query)
                    
                    try:
                        final_results = sparql.query().convert()
                        final_chunk_results = final_results["results"]["bindings"]
                        
                        if final_chunk_results:
                            for result in final_chunk_results:
                                instance_uri = result["instance"]["value"]
                                class_uri = result["class"]["value"]
                                
                                if instance_uri not in class_mappings:
                                    class_mappings[instance_uri] = []
                                class_mappings[instance_uri].append(class_uri)
                            
                    except Exception as e:
                        print(f"Final query failed: {e}")
                    
                    break
                
                # Add delay between chunks to be respectful to the endpoint
                if delay_between_chunks > 0:
                    time.sleep(delay_between_chunks)
                    
            except Exception as e:
                print(f"Failed to fetch chunk at offset {current_offset}: {e}")
                break
        
        return class_mappings

    def calculate_coverage_statistics(
        self, instance_counts: Dict, class_mappings: Dict
    ) -> Dict:
        """
        Calculate coverage statistics for class partitions.

        Args:
            instance_counts: Dictionary of class URIs to instance counts
            class_mappings: Dictionary of instance URIs to class lists

        Returns:
            Dictionary with coverage statistics per class
        """
        coverage_stats = {}

        for class_uri, total_instances in instance_counts.items():
            # Count how many instances of this class appear in partitions
            partition_occurrences = 0
            instances_in_partitions = set()

            for instance_uri, classes in class_mappings.items():
                if class_uri in classes:
                    partition_occurrences += 1
                    instances_in_partitions.add(instance_uri)

            # Calculate coverage percentages
            if total_instances > 0:
                coverage_percent = (
                    len(instances_in_partitions) / total_instances
                ) * 100
                avg_occurrences = (
                    partition_occurrences / len(instances_in_partitions)
                    if instances_in_partitions
                    else 0
                )
            else:
                coverage_percent = 0.0
                avg_occurrences = 0.0

            coverage_stats[class_uri] = {
                "total_instances": total_instances,
                "instances_in_partitions": len(instances_in_partitions),
                "partition_occurrences": partition_occurrences,
                "occurrence_coverage_percent": coverage_percent,
                "avg_occurrences_per_instance": avg_occurrences,
            }

        return coverage_stats

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

        Returns:
            Tuple of (instance_counts, class_mappings, coverage_stats)
        """
        print("Counting instances per class...")
        instance_counts = self.count_instances_per_class(
            endpoint_url, sample_limit=sample_limit, sample_offset=sample_offset,
            offset_limit_steps=offset_limit_steps
        )

        print("Getting class mappings...")
        class_mappings = self.get_class_mappings(
            endpoint_url, sample_limit=sample_limit, sample_offset=sample_offset,
            offset_limit_steps=offset_limit_steps
        )

        print("Calculating coverage statistics...")
        coverage_stats = self.calculate_coverage_statistics(
            instance_counts, class_mappings
        )

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
            print(f"Coverage analysis exported to: {output_file}")

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
            sample_limit: Optional limit for sampling
            sample_offset: Optional offset for pagination

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
                results = sparql.query().convert()
                total = 0
                if results["results"]["bindings"]:
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
                results = sparql.query().convert()
                
                participating = 0
                if results["results"]["bindings"]:
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

    def analyze_complete_schema_coverage(
        self,
        endpoint_url: str,
        sample_limit: Optional[int] = None,
        sample_offset: Optional[int] = None,
    ) -> Dict:
        """
        Complete analysis combining class coverage and schema shape frequencies.

        Args:
            endpoint_url: SPARQL endpoint URL
            sample_limit: Optional limit for sampling
            sample_offset: Optional offset for pagination

        Returns:
            Dictionary containing all analysis results
        """
        # Get class partition coverage
        instance_counts, class_mappings, coverage_stats = self.analyze_class_partition_usage(
            endpoint_url, sample_limit=sample_limit, sample_offset=sample_offset
        )
        
        # Get schema shape frequencies
        shape_frequencies = self.count_schema_shape_frequencies(
            endpoint_url, sample_limit=sample_limit, sample_offset=sample_offset
        )
        
        # Combine results
        analysis_results = {
            'instance_counts': instance_counts,
            'class_mappings': class_mappings,
            'coverage_statistics': coverage_stats,
            'shape_frequencies': shape_frequencies,
            'summary': {
                'total_classes': len(instance_counts),
                'total_schema_shapes': len(shape_frequencies),
                'total_shape_occurrences': shape_frequencies['occurrence_count'].sum() if not shape_frequencies.empty else 0,
                'sampling_limit': sample_limit
            }
        }
        
        return analysis_results


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
