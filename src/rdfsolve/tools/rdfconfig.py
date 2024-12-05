import requests

from rdfsolve.tools.utils import (
    make_rdf_config,
    get_void_jar,
    run_void_generator_endpoint,
    get_graph_iris,
    IGNORE_GRAPH_IRIS
)


class RDFConfigr:
    """
    A class to manage RDF configurations by interacting with a SPARQL endpoint and
    generating RDF models in YAML format.

    :param str endpoint: SPARQL endpoint URL.
    :param str path: Path for storing temporary files.
    """

    def __init__(self, endpoint, path, void_iri, dataset_name):
        """
        Initialize the RDFConfigr class.

        :param str endpoint: SPARQL endpoint URL.
        :param str path: Path for storing temporary files.
        :param str void_iri: VoID IRI.
        """
        self._endpoint = endpoint
        self._path = path
        self._config = None
        self._void_iri = void_iri
        self._graph_iris = [iri for iri in get_graph_iris(endpoint) if iri.startswith("http") and iri not in IGNORE_GRAPH_IRIS]
        self._dataset_name = dataset_name

    @property
    def dataset_name(self):
        """
        Get the VoID IRI.

        :return: The VoID IRI.
        :rtype: str
        """
        return self._dataset_name

    @dataset_name.setter
    def dataset_name(self, value):
        """
        Set the _dataset_name.

        :param str value: The dataset name.
        :raises ValueError: If the value is not a non-empty string.
        """
        if not value or not isinstance(value, str):
            raise ValueError("The _dataset_name must be a non-empty string.")
        self._dataset_name = value

    @property
    def void_iri(self):
        """
        Get the VoID IRI.

        :return: The VoID IRI.
        :rtype: str
        """
        return self._void_iri

    @property
    def config(self):
        """
        Get the VoID IRI.

        :return: The VoID IRI.
        :rtype: str
        """
        return self._config

    @void_iri.setter
    def void_iri(self, value):
        """
        Set the VoID IRI.

        :param str value: The new VoID IRI.
        :raises ValueError: If the value is not a non-empty string.
        """
        if not value or not isinstance(value, str):
            raise ValueError("The VoID IRI must be a non-empty string.")
        self._void_iri = value

    @property
    def p(self):
        """
        Get the additional parameter predicates to include.

        :return: The additional parameter predicates to include.
        :rtype: str
        """
        return self._p

    @p.setter
    def p(self, value):
        """
        Set the additional parameter predicates to include.

        :param str value: The new value for predicates to include.
        """
        self._p = value

    @property
    def endpoint(self):
        """
        Get the SPARQL endpoint URL.

        :return: The SPARQL endpoint URL.
        :rtype: str
        """
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        """
        Set the SPARQL endpoint URL.

        :param str value: The new SPARQL endpoint URL.
        :raises ValueError: If the value is not a non-empty string.
        """
        if not value or not isinstance(value, str):
            raise ValueError("The endpoint must be a non-empty string.")
        self._endpoint = value

    @property
    def config(self):
        """
        Get the RDF configuration.

        :return: The RDF configuration in YAML format.
        :rtype: str
        :raises ValueError: If the configuration is not yet set.
        """
        if not self._config:
            raise ValueError("Configuration not set. Run get_config() first.")
        return self._config

    def get_config(self):
        """
        Retrieve RDF configuration using VoID generator and set up the triplestore.

        :return: The generated RDF configuration in YAML format.
        :rtype: str
        :raises ValueError: If the SPARQL endpoint is not set.
        :raises RuntimeError: If the triplestore setup fails.
        """
        if not self.endpoint:
            raise ValueError("SPARQL endpoint is not set. Set it using self.endpoint.")

        # Step 1: Generate VoID data
        jar_path = get_void_jar(path=self._path)
        print("JAR downloaded")
        void_ttl = run_void_generator_endpoint(
            jar_path,
            endpoint=self._endpoint,
            void_file=self._dataset_name + "_void.ttl",
            void_iri=self._void_iri,
            graph_iris=self._graph_iris

        )

        # Step 2: Generate RDF configuration (returns YAML)
        self._config = make_rdf_config(void_ttl)
        return self._config
