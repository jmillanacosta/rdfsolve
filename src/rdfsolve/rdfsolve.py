from rdflib import ConjunctiveGraph, Graph
import os
from typing import Optional


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
        from .void_parser import VoidParser
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
        from .void_parser import VoidParser

        try:
            if not output_file:
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

        from .void_parser import VoidParser

        parser = VoidParser(self._void)
        return parser
