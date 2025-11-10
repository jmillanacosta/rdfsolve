from rdflib import ConjunctiveGraph, Graph, URIRef
from rdflib.namespace import RDFS
import os
from typing import Optional

from rdfsolve.void_parser import VoidParser

class RDFSolver:
    """
    A class to manage RDF datasets and generate VoID descriptions.

    :param str endpoint: SPARQL endpoint URL.
    :param str path: Path for storing temporary files.
    :param str void_iri: VoID IRI.
    :param str dataset_name: Dataset name for VoID generation.
    """

    def __init__(
        self,
        endpoint=None,
        path=None,
        void_iri=None,
        dataset_name=None,
        void=None,
    ):
        """
        Initialize the RDFSolver class.

        :param str endpoint: SPARQL endpoint URL.
        :param str path: Path for storing temporary files.
        :param str void_iri: VoID IRI.
        :param str dataset_name: Dataset name for VoID generation.
        :param ConjunctiveGraph or str or None void: VoID graph or path.
        """
        self._endpoint = endpoint
        self._path = path
        self._void_iri = void_iri
        self._dataset_name = dataset_name
        self._void = None

        self.validate_initialization()

        if void:
            self.void = void  # Triggers void setter

    def validate_initialization(self):
        """
        Validate initialization parameters and raise exceptions.
        """
        if not self._endpoint:
            raise ValueError("Missing required attribute: 'endpoint'.")
        if not self._path:
            raise ValueError("Missing required attribute: 'path'.")
        if not self._void_iri:
            raise ValueError("Missing required attribute: 'void_iri'.")
        if not self._dataset_name:
            raise ValueError("Missing required attribute: 'dataset_name'.")

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        if not os.path.isdir(path):
            raise ValueError(
                f"Invalid path: {path}. Directory does not exist."
            )
        self._path = path

    @property
    def void(self):
        return self._void

    @void.setter
    def void(self, value):
        if not isinstance(value, ConjunctiveGraph):
            raise ValueError("The value for void must be a ConjunctiveGraph.")
        self._void = value

    def parse_void(self, file_path):
        if not os.path.isfile(file_path):
            raise ValueError(f"File does not exist: {file_path}")
        g = ConjunctiveGraph()
        g.parse(file_path)
        self._void = g

    @property
    def dataset_name(self):
        return self._dataset_name

    @dataset_name.setter
    def dataset_name(self, value):
        if not value or not isinstance(value, str):
            raise ValueError("The dataset_name must be a non-empty string.")
        self._dataset_name = value

    @property
    def void_iri(self):
        return self._void_iri

    @void_iri.setter
    def void_iri(self, value):
        if not value or not isinstance(value, str):
            raise ValueError("The void_iri must be a non-empty string.")
        self._void_iri = value

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        if not value or not isinstance(value, str):
            raise ValueError("The endpoint must be a non-empty string.")
        self._endpoint = value

    def get_void_queries(self, graph_uri: str) -> dict:
        """
        Get formatted CONSTRUCT queries for VoID generation without executing.

        Args:
            graph_uri: Specific graph URI to analyze

        Returns:
            Dictionary containing the formatted queries
        """
        return VoidParser.get_void_queries(graph_uri, counts=True)

    def void_generator(
        self,
        graph_uri: str,
        output_file: Optional[str] = None,
        counts: bool = True,
        sample_limit: Optional[int] = None,
    ) -> Graph:
        """
        Generate VoID description using CONSTRUCT queries.

        Args:
            graph_uri: Specific graph URI to analyze
            output_file: Optional output file path for TTL
            counts: If True, include COUNT aggregations; if False, faster
            sample_limit: Optional LIMIT for sampling (speeds up discovery)

        Returns:
            RDF Graph containing the VoID description
        """

        try:
            if not output_file:
                print("No output path specified, defaulting to current directory.")
                output_file = f"{self._dataset_name}_void.ttl"

            if not self._endpoint:
                raise ValueError("No endpoint configured")

            print(f"Generating VoID from endpoint: {self._endpoint}")
            print(f"Using graph URI: {graph_uri}")
            if not counts:
                print("Fast mode: Skipping COUNT aggregations")
            if sample_limit:
                print(f"Using sample limit: {sample_limit}")

            void_graph = VoidParser.generate_void_from_sparql(
                self._endpoint, graph_uri, output_file, counts, sample_limit
            )

            self._void = void_graph
            print("VoID generation completed successfully")
            return void_graph

        except Exception as e:
            print(f"\nVoID generation failed: {str(e)}")

            raise RuntimeError(
                f"VoID generation failed. "
                f"Original error: {str(e)}"
            ) from e

    def extract_schema(self, filter_void_nodes: bool = True):
        """
        Extract schema from the VoID description.

        Args:
            filter_void_nodes: Whether to filter out VoID-specific nodes

        Returns:
            VoidParser instance for schema extraction
        """
        if not self._void:
            raise ValueError(
                "No VoID description available. " "Run void_generator() first."
            )

        parser = VoidParser(self._void)
        return parser

    def _extract_prefixes_from_void(self) -> dict:  #TODO check bioregistry for functions?
        """
        Extract namespace prefixes from the VoID graph for JSON-LD context.
        
        Returns:
            Dictionary with prefix mappings suitable for JSON-LD @context
        """
        if not self._void:
            return {}
        
        # Get namespace manager from the graph
        prefixes = {}
        for prefix, namespace in self._void.namespace_manager.namespaces():
            if prefix:  # Skip empty prefix
                prefixes[prefix] = str(namespace)
        
        # Always include core vocabularies if not present
        core_prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "void": "http://rdfs.org/ns/void#",
            "void-ext": "http://ldf.fi/void-ext#"
        }
        
        # Add core prefixes if not already present
        for prefix, namespace in core_prefixes.items():
            if prefix not in prefixes:
                prefixes[prefix] = namespace
        
        return prefixes

    def export_void_jsonld(
        self,
        output_file: Optional[str] = None,
        context: Optional[dict] = None,
        indent: int = 2,
    ) -> str:
        """
        Export the VoID description as JSON-LD.

        Args:
            output_file: Optional path to write the JSON-LD output
            context: Optional JSON-LD context dictionary to include
            indent: Number of spaces for JSON indentation (default: 2)

        Returns:
            JSON-LD as a string

        Raises:
            ValueError: If no VoID description is available
            RuntimeError: If serialization fails
        """
        if not self._void:
            raise ValueError(
                "No VoID description available. Run void_generator() first."
            )

        try:
            # Use VoID prefixes as context if none provided
            if context is None:
                void_prefixes = self._extract_prefixes_from_void()
                if void_prefixes:
                    context = {"@context": void_prefixes}
            
            # Serialize to JSON-LD format
            jsonld_data = self._void.serialize(
                format="json-ld",
                context=context,
                indent=indent
            )
            
            # Handle bytes return from serialize
            if isinstance(jsonld_data, bytes):
                jsonld_data = jsonld_data.decode("utf-8")

            # Write to file if specified
            if output_file:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(jsonld_data)
                print(f"JSON-LD exported to: {output_file}")

            return jsonld_data

        except Exception as e:
            raise RuntimeError(
                f"Failed to serialize VoID to JSON-LD: {str(e)}"
            ) from e

    def export_schema_jsonld(
        self,
        output_file: Optional[str] = None,
        context: Optional[dict] = None,
        indent: int = 2,
        filter_void_nodes: bool = True,
    ) -> str:
        """
        Export the extracted schema (from the VoID description) as JSON-LD.

        Args:
            output_file: Optional path to write the JSON-LD output
            context: Optional JSON-LD context dictionary to include
            indent: Number of spaces for JSON indentation (default: 2)
            filter_void_nodes: Whether to filter out VoID-specific nodes

        Returns:
            JSON-LD as a string

        Raises:
            ValueError: If no VoID description is available
            RuntimeError: If serialization fails
        """
        if not self._void:
            raise ValueError(
                "No VoID description available. Run void_generator() first."
            )

        # Build a graph from the extracted schema triples and serialize
        try:
            # Use VoID prefixes as context if none provided
            if context is None:
                void_prefixes = self._extract_prefixes_from_void()
                if void_prefixes:
                    context = {"@context": void_prefixes}
            
            parser = self.extract_schema(filter_void_nodes=filter_void_nodes)
            schema_dict = parser.to_json(filter_void_nodes=filter_void_nodes)

            schema_graph = Graph()
            
            # Add namespace prefixes to the schema graph
            void_prefixes = self._extract_prefixes_from_void()
            for prefix, namespace in void_prefixes.items():
                schema_graph.namespace_manager.bind(prefix, namespace)

            # Map special tokens to RDF/RDFS terms
            literal_uri = URIRef(str(RDFS.Literal))
            resource_uri = URIRef(str(RDFS.Resource))

            for s, p, o in schema_dict.get('triples', []):
                subj = URIRef(s)
                pred = URIRef(p)
                if o in ["Literal", "Resource"]:
                    obj = literal_uri if o == "Literal" else resource_uri
                else:
                    obj = URIRef(o)

                schema_graph.add((subj, pred, obj))

            jsonld_data = schema_graph.serialize(
                format="json-ld",
                context=context,
                indent=indent,
            )

            if isinstance(jsonld_data, bytes):
                jsonld_data = jsonld_data.decode("utf-8")

            if output_file:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(jsonld_data)
                print(f"Schema JSON-LD exported to: {output_file}")

            return jsonld_data

        except Exception as e:
            raise RuntimeError(
                f"Failed to serialize schema to JSON-LD: {str(e)}"
            ) from e
