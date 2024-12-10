from rdflib import ConjunctiveGraph
from rdfsolve.tools.utils import (
    make_rdf_config,
    get_void_jar,
    run_void_generator_endpoint,
    get_graph_iris,
    IGNORE_GRAPH_IRIS,
)
import os
import yaml

class RDFSolver:
    """
    A class to manage RDF schema/shape specifications and generate RDF models in YAML format.

    :param str endpoint: SPARQL endpoint URL.
    :param str path: Path for storing temporary files.
    :param str void_iri: VoID IRI.
    :param str dataset_name: Dataset name for VoID generation.
    """

    def __init__(
        self, endpoint=None, path=None, void_iri=None, dataset_name=None, void=None
    ):
        """
        Initialize the RDFSolver class.

        :param str endpoint: SPARQL endpoint URL.
        :param str path: Path for storing temporary files.
        :param str void_iri: VoID IRI.
        :param str dataset_name: Dataset name for VoID generation.
        :param ConjunctiveGraph or str or None void: VoID graph or path or 'get' to auto-generate.
        """
        self._endpoint = endpoint
        self._path = path
        self._void_iri = void_iri
        self._dataset_name = dataset_name
        self._void = None
        self._rdfconfig = None
        self._graph_iris = None

        self.validate_initialization()

        if self._endpoint:
            self.graph_iris = endpoint  # Triggers graph_iris setter

        if void:
            self.void = void  # Triggers void setter

    def validate_initialization(self):
        """
        Validate initialization parameters and raise exceptions for missing values.
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
            raise ValueError(f"Invalid path: {path}. Directory does not exist.")
        self._path = path

    @property
    def graph_iris(self):
        return self._graph_iris

    @graph_iris.setter
    def graph_iris(self, endpoint):
        if not endpoint or not isinstance(endpoint, str):
            raise ValueError("The endpoint must be a non-empty string.")
        self._graph_iris = [
            iri
            for iri in get_graph_iris(endpoint)
            if iri.startswith("http") and iri not in IGNORE_GRAPH_IRIS
        ]

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

    @property
    def rdfconfig(self):
        if self._rdfconfig:
            return self._rdfconfig
        else:
            raise ValueError("Use self.rdfconfig_generator to set the rdfconfig")

    @rdfconfig.setter
    def rdfconfig(self, value):
        if not value or not isinstance(yaml, str):
            raise ValueError("The endpoint must be a YAML.")
        self._endpoint = value


    def rdfconfig_generator(self):
        if not self.endpoint:
            raise ValueError(
                "SPARQL endpoint is not set. Set it using the 'endpoint' property."
            )
        if not self._void:
            raise ValueError(
                "The VoID description is needed first. Set it via the 'void' property."
            )
        else:
            self._rdfconfig = make_rdf_config(self._void)
            return self._rdfconfig
        

    def void_generator(self):
        jar_path = get_void_jar(path=self._path)
        void_ttl = run_void_generator_endpoint(
            jar_path,
            endpoint=self._endpoint,
            void_file=f"{self._dataset_name}_void.ttl",
            void_iri=self._void_iri,
            graph_iris=self._graph_iris,
        )
        g = ConjunctiveGraph()
        self._void = g.parse(void_ttl)
