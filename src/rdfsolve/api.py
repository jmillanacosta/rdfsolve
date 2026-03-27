"""Main RDFSolve functionalities for extraction, conversion and solving."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rdfsolve.sources import SourceEntry

import pandas as pd
from rdflib import Graph

from .parser import VoidParser

logger = logging.getLogger(__name__)

__all__ = [
    "build_ontology_index",
    "compose_query_from_paths",
    "count_instances",
    "count_instances_per_class",
    "discover_void_graphs",
    "enrich_source_with_bioregistry",
    "execute_sparql",
    "extract_partitions_from_void",
    "generate_void_from_endpoint",
    "get_bioregistry_metadata",
    "graph_to_jsonld",
    "graph_to_linkml",
    "graph_to_schema",
    "graph_to_shacl",
    "import_semra_source",
    "import_sssom_source",
    "infer_mappings",
    "load_mapping_jsonld",
    "load_ontology_index",
    "load_parser_from_file",
    "load_parser_from_graph",
    "load_parser_from_jsonld",
    "mine_all_sources",
    "mine_schema",
    "probe_instance_mapping",
    "resolve_iris",
    "retrieve_void_from_graphs",
    "seed_inferenced_mappings",
    "seed_instance_mappings",
    "seed_semra_mappings",
    "seed_sssom_mappings",
    "sources_to_jsonld",
    "to_jsonld_from_file",
    "to_linkml_from_file",
    "to_rdfconfig_from_file",
    "to_shacl_from_file",
    "to_void_from_file",
]


def load_parser_from_file(
    void_file_path: str,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Load a VoID file and return a parser for schema extraction.

    Args:
        void_file_path: Path to VoID Turtle file
        graph_uris: Graph URIs to filter queries
        exclude_graphs: Exclude system graphs

    Returns:
        VoidParser instance
    """
    return VoidParser(
        void_source=void_file_path, graph_uris=graph_uris, exclude_graphs=exclude_graphs
    )


def load_parser_from_graph(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Load a VoID graph and return a parser for schema extraction.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter queries
        exclude_graphs: Exclude system graphs

    Returns:
        VoidParser instance
    """
    return VoidParser(void_source=graph, graph_uris=graph_uris, exclude_graphs=exclude_graphs)


def load_parser_from_jsonld(
    jsonld_path: str,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Load a mined-schema JSON-LD file and return a VoidParser.

    Reads the JSON-LD produced by ``rdfsolve mine``, reconstructs a
    :class:`~rdfsolve.schema_models.core.MinedSchema` via
    :meth:`MinedSchema.from_jsonld`, converts it to an in-memory VoID
    RDF graph, and wraps it in a :class:`~rdfsolve.parser.VoidParser`
    ready for export to CSV / LinkML / SHACL / RDF-config.

    Args:
        jsonld_path: Path to a ``*_schema.jsonld`` file produced by
            ``rdfsolve mine``.
        graph_uris: Graph URIs to filter (passed through to VoidParser).
        exclude_graphs: Exclude system graphs.

    Returns:
        VoidParser instance backed by the converted VoID graph.
    """
    from .models import MinedSchema

    schema = MinedSchema.from_jsonld(jsonld_path)
    return VoidParser(
        void_source=schema.to_void_graph(),
        graph_uris=graph_uris,
        exclude_graphs=exclude_graphs,
    )


def to_linkml_from_file(
    void_file_path: str,
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Convert a VoID file to LinkML YAML schema.

    Args:
        void_file_path: Path to VoID file
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema

    Returns:
        LinkML YAML schema string
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_linkml_yaml(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )


def to_shacl_from_file(
    void_file_path: str,
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
    include_annotations: bool = False,
) -> str:
    """Convert a VoID file to SHACL shapes.

    Generates SHACL (Shapes Constraint Language) shapes from a VoID
    description file. SHACL shapes define constraints on RDF data and
    can be used for validation.

    Args:
        void_file_path: Path to VoID file
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema
        closed: Generate closed shapes (only allow defined properties)
        suffix: Optional suffix for shape names (e.g., "Shape")
        include_annotations: Include class/slot annotations in shapes

    Returns:
        SHACL shapes as Turtle/RDF string

    Example:
        >>> from rdfsolve.api import to_shacl_from_file
        >>> shacl_ttl = to_shacl_from_file(
        ...     "dataset_void.ttl", schema_name="my_dataset", closed=True
        ... )
        >>> with open("schema.shacl.ttl", "w") as f:
        ...     f.write(shacl_ttl)
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_shacl(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
        closed=closed,
        suffix=suffix,
        include_annotations=include_annotations,
    )


def to_rdfconfig_from_file(
    void_file_path: str,
    filter_void_nodes: bool = True,
    endpoint_url: str | None = None,
    endpoint_name: str | None = None,
    graph_uri: str | None = None,
) -> dict[str, str]:
    """Convert a VoID file to RDF-config YAML files.

    RDF-config is a schema standard that describes RDF data models using
    YAML configuration files. This function generates three files:
    - model.yml: Class and property structure
    - prefix.yml: Namespace prefix definitions
    - endpoint.yml: SPARQL endpoint configuration

    Note: The rdf-config tool requires these files to be named exactly
    model.yml, prefix.yml, and endpoint.yml, and placed in a directory
    named {dataset}_config. The CLI automatically creates this structure.

    Args:
        void_file_path: Path to VoID file
        filter_void_nodes: Remove VoID-specific nodes
        endpoint_url: SPARQL endpoint URL (optional)
        endpoint_name: Name for endpoint (default: "endpoint")
        graph_uri: Named graph URI (optional)

    Returns:
        Dictionary with 'model', 'prefix', 'endpoint' keys containing
        YAML strings

    Example:
        >>> from rdfsolve.api import to_rdfconfig_from_file
        >>> rdfconfig = to_rdfconfig_from_file(
        ...     "dataset_void.ttl",
        ...     endpoint_url="https://example.org/sparql",
        ...     graph_uri="http://example.org/graph",
        ... )
        >>> # Save files
        >>> with open("model.yml", "w") as f:
        ...     f.write(rdfconfig["model"])
        >>> with open("prefix.yml", "w") as f:
        ...     f.write(rdfconfig["prefix"])
        >>> with open("endpoint.yml", "w") as f:
        ...     f.write(rdfconfig["endpoint"])
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_rdfconfig(
        filter_void_nodes=filter_void_nodes,
        endpoint_url=endpoint_url,
        endpoint_name=endpoint_name,
        graph_uri=graph_uri,
    )


def to_void_from_file(
    jsonld_path: str,
) -> Graph:
    """Convert a mined-schema JSON-LD file to a VoID RDF graph.

    Reads the JSON-LD, reconstructs a
    :class:`~rdfsolve.schema_models.core.MinedSchema`, and returns the
    equivalent VoID graph (rdflib ``Graph``).

    Args:
        jsonld_path: Path to a ``*_schema.jsonld`` file.

    Returns:
        rdflib ``Graph`` containing the VoID description.
    """
    from .models import MinedSchema

    schema = MinedSchema.from_jsonld(jsonld_path)
    return schema.to_void_graph()


def to_jsonld_from_file(
    void_file_path: str,
    filter_void_admin_nodes: bool = True,
    endpoint_url: str | None = None,
    dataset_name: str | None = None,
    graph_uris: str | list[str] | None = None,
) -> dict[str, Any]:
    """Convert a VoID file to JSON-LD format.

    Args:
        void_file_path: Path to VoID file
        filter_void_admin_nodes: Remove VoID and administrative nodes
        endpoint_url: SPARQL endpoint URL for the @about section
        dataset_name: Dataset name for the @about section
        graph_uris: Graph URIs for the @about section

    Returns:
        JSON-LD with @context, @graph, and @about
    """
    parser = load_parser_from_file(void_file_path)
    graph_uris_list = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    return parser.to_jsonld(
        filter_void_admin_nodes=filter_void_admin_nodes,
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_jsonld(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_admin_nodes: bool = True,
    endpoint_url: str | None = None,
    dataset_name: str | None = None,
) -> dict[str, Any]:
    """Convert a VoID graph to JSON-LD format.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        filter_void_admin_nodes: Remove VoID and administrative nodes
        endpoint_url: SPARQL endpoint URL for the @about section
        dataset_name: Dataset name for the @about section

    Returns:
        JSON-LD with @context, @graph, and @about
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    graph_uris_list = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    return parser.to_jsonld(
        filter_void_admin_nodes=filter_void_admin_nodes,
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_linkml(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Convert a VoID graph to LinkML YAML schema.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema

    Returns:
        LinkML YAML schema string
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    return parser.to_linkml_yaml(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )


def graph_to_shacl(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
    include_annotations: bool = False,
) -> str:
    """Convert a VoID graph to SHACL shapes.

    Generates SHACL (Shapes Constraint Language) shapes from a VoID
    graph. SHACL shapes define constraints on RDF data and can be used
    for validation.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema
        closed: Generate closed shapes (only allow defined properties)
        suffix: Optional suffix for shape names (e.g., "Shape")
        include_annotations: Include class/slot annotations in shapes

    Returns:
        SHACL shapes as Turtle/RDF string

    Example:
        >>> from rdflib import Graph
        >>> from rdfsolve.api import graph_to_shacl
        >>> void_graph = Graph()
        >>> void_graph.parse("dataset_void.ttl", format="turtle")
        >>> shacl_ttl = graph_to_shacl(void_graph, schema_name="my_dataset")
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    return parser.to_shacl(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
        closed=closed,
        suffix=suffix,
        include_annotations=include_annotations,
    )


def graph_to_schema(
    void_graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_admin_nodes: bool = True,
) -> pd.DataFrame:
    """Convert VoID graph to schema DataFrame.

    Args:
        void_graph: RDFLib graph with VoID data
        graph_uris: Graph URIs to extract
        filter_void_admin_nodes: Filter VoID or administrative nodes

    Returns:
        DataFrame with schema patterns (subject/property/object URIs)
    """
    parser = VoidParser(void_source=void_graph, graph_uris=graph_uris)
    return parser.to_schema(filter_void_admin_nodes=filter_void_admin_nodes)


def discover_void_graphs(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = False,
) -> dict[str, Any]:
    """Find VoID graphs at *endpoint_url*.

    Delegates to :meth:`~rdfsolve.parser.VoidParser.discover_void_graphs`.
    *graph_uris* and *exclude_graphs* are accepted for backwards-compatibility
    but the discovery query always searches all named graphs.
    """
    return VoidParser().discover_void_graphs(endpoint_url)


def count_instances(
    endpoint_url: str,
    sample_limit: int | None = None,
    sample_offset: int | None = None,
    chunk_size: int | None = None,
    offset_limit_steps: int | None = None,
    delay_between_chunks: float = 20.0,
    streaming: bool = False,
) -> dict[str, int] | Any:
    """Count instances per class at *endpoint_url*.

    Delegates to :func:`~rdfsolve.miner.count_instances`.
    """
    from rdfsolve.miner import count_instances as _count

    return _count(
        endpoint_url,
        sample_limit=sample_limit,
        sample_offset=sample_offset,
        chunk_size=chunk_size,
        offset_limit_steps=offset_limit_steps,
        delay_between_chunks=delay_between_chunks,
        streaming=streaming,
    )


def extract_partitions_from_void(
    endpoint_url: str,
    void_graph_uris: list[str],
) -> list[dict[str, str]]:
    """Extract partition records from VoID graphs.

    Delegates to :func:`~rdfsolve.miner.extract_partitions_from_void`.
    """
    from rdfsolve.miner import extract_partitions_from_void as _epv

    return _epv(endpoint_url, void_graph_uris)


def retrieve_void_from_graphs(
    endpoint_url: str,
    void_graph_uris: list[str],
    graph_uris: str | list[str] | None = None,
    partitions: list[dict[str, str]] | None = None,
) -> Graph:
    """Build a VoID RDF graph from partition data.

    Delegates to :func:`~rdfsolve.miner.retrieve_void_from_graphs`.
    """
    from rdfsolve.miner import retrieve_void_from_graphs as _rvfg

    return _rvfg(
        endpoint_url,
        void_graph_uris,
        graph_uris=graph_uris,
        partitions=partitions,
    )


def generate_void_from_endpoint(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    output_file: str | None = None,
    counts: bool = True,
    offset_limit_steps: int | None = None,
    exclude_graphs: bool = True,
    dataset_uri: str | None = None,
    void_base_uri: str | None = None,
) -> Graph:
    """Mine a VoID description from a SPARQL endpoint.

    .. deprecated:: Use :func:`mine_schema` instead.

    Delegates to :func:`~rdfsolve.miner.generate_void_from_endpoint`.
    """
    from rdfsolve.miner import generate_void_from_endpoint as _gvfe

    return _gvfe(
        endpoint_url,
        graph_uris=graph_uris,
        output_file=output_file,
        counts=counts,
        offset_limit_steps=offset_limit_steps,
        exclude_graphs=exclude_graphs,
        dataset_uri=dataset_uri,
        void_base_uri=void_base_uri,
    )


def count_instances_per_class(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    sample_limit: int | None = None,
    exclude_graphs: bool = True,
) -> dict[str, int]:
    """Return ``{class_uri: count}`` for *endpoint_url*.

    Delegates to :func:`~rdfsolve.miner.count_instances_per_class`.
    """
    from rdfsolve.miner import count_instances_per_class as _cipc

    return _cipc(
        endpoint_url,
        graph_uris=graph_uris,
        sample_limit=sample_limit,
        exclude_graphs=exclude_graphs,
    )


def mine_schema(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    dataset_name: str | None = None,
    chunk_size: int = 10_000,
    class_chunk_size: int | None = None,
    class_batch_size: int = 15,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
    two_phase: bool = True,
    report_path: str | None = None,
    filter_service_namespaces: bool = True,
    authors: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Mine RDF schema from a SPARQL endpoint using SELECT queries.

    This is a simpler, faster alternative to generate_void_from_endpoint
    that avoids heavy CONSTRUCT queries. Returns a MinedSchema which
    can export to JSON-LD or be converted to a VoID graph.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URI(s) to restrict queries
        dataset_name: Human-readable dataset name
        chunk_size: Pagination page size
        class_chunk_size: Page size for Phase-1 class discovery
            (``None`` = single query, no pagination)
        class_batch_size: Number of classes to group into one
            VALUES query in Phase-2 (default 15)
        delay: Delay between pages (seconds)
        timeout: HTTP timeout per request
        counts: Whether to fetch triple counts
        two_phase: Use two-phase mining (default ``True``).
            Pass ``False`` for the legacy single-pass strategy.
        report_path: If given, write analytics JSON to this path
        filter_service_namespaces: Strip service/system namespace
            patterns from the result (default ``True``)

    Returns:
        JSON-LD dict with @context, @graph, and @about
    """
    from .miner import mine_schema as _mine

    schema = _mine(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        dataset_name=dataset_name,
        chunk_size=chunk_size,
        class_chunk_size=class_chunk_size,
        class_batch_size=class_batch_size,
        delay=delay,
        timeout=timeout,
        counts=counts,
        two_phase=two_phase,
        report_path=report_path,
        filter_service_namespaces=filter_service_namespaces,
        authors=authors,
    )
    return schema.to_jsonld()


def mine_all_sources(
    sources_csv: str | None = None,
    *,
    sources: str | None = None,
    output_dir: str = ".",
    fmt: str = "all",
    chunk_size: int = 10_000,
    class_chunk_size: int | None = None,
    class_batch_size: int = 15,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
    reports: bool = True,
    filter_service_namespaces: bool = True,
    untyped_as_classes: bool = False,
    authors: list[dict[str, str]] | None = None,
    on_progress: Callable[[str, int, int, str | None], None] | None = None,
) -> dict[str, Any]:
    """Mine schemas for all sources in a JSON-LD or CSV file.

    Delegates to :func:`rdfsolve.miner.mine_all_sources`.
    """
    from rdfsolve.miner import mine_all_sources as _mas

    return _mas(
        sources_csv,
        sources=sources,
        output_dir=output_dir,
        fmt=fmt,
        chunk_size=chunk_size,
        class_chunk_size=class_chunk_size,
        class_batch_size=class_batch_size,
        delay=delay,
        timeout=timeout,
        counts=counts,
        reports=reports,
        filter_service_namespaces=filter_service_namespaces,
        untyped_as_classes=untyped_as_classes,
        authors=authors,
        on_progress=on_progress,
    )


# ── SPARQL / IRI / Compose API ───────────────────────────────────


def execute_sparql(
    query: str,
    endpoint: str,
    method: str = "GET",
    timeout: int = 30,
    variable_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Execute a SPARQL query against a remote endpoint.

    This is a pure-Python function- no Flask required.  It delegates to
    :func:`rdfsolve.query.execute_sparql` which uses the robust
    :class:`~rdfsolve.sparql_helper.SparqlHelper` under the hood.

    Args:
        query:        Full SPARQL query string.
        endpoint:     SPARQL endpoint URL.
        method:       HTTP method (``"GET"`` or ``"POST"``).
        timeout:      Timeout in seconds.
        variable_map: Optional mapping of SPARQL ?variable -> schema URI.

    Returns:
        Dict with keys ``query``, ``endpoint``, ``variables``, ``rows``,
        ``variable_map``, ``row_count``, ``duration_ms``, and optionally
        ``error``.

    Example::

        >>> from rdfsolve.api import execute_sparql
        >>> result = execute_sparql(
        ...     query="SELECT ?s WHERE { ?s a ?o } LIMIT 5",
        ...     endpoint="https://sparql.wikipathways.org/sparql/",
        ... )
        >>> result["row_count"]
        5
    """
    from rdfsolve.query import execute_sparql as _execute

    qr = _execute(
        query=query,
        endpoint=endpoint,
        method=method,
        timeout=timeout,
        variable_map=variable_map or {},
    )
    return qr.model_dump()


def resolve_iris(
    iris: list[str],
    endpoints: list[dict[str, Any]],
    timeout: int = 15,
) -> dict[str, Any]:
    """Resolve IRIs against SPARQL endpoints to discover their rdf:type.

    This is a pure-Python function- no Flask required.  It delegates to
    :func:`rdfsolve.iri.resolve_iris`.

    Args:
        iris: List of IRI strings to resolve.
        endpoints: List of endpoint dicts, each with keys
            ``name``, ``endpoint``, and optionally ``graph``.
        timeout: Per-endpoint timeout in seconds.

    Returns:
        Dict with keys ``resolved``, ``not_found``, ``errors``.

    Example::

        >>> from rdfsolve.api import resolve_iris
        >>> result = resolve_iris(
        ...     iris=["http://identifiers.org/ncbigene/1234"],
        ...     endpoints=[{
        ...         "name": "wikipathways",
        ...         "endpoint": "https://sparql.wikipathways.org/sparql/",
        ...     }],
        ... )
        >>> result["resolved"]
        {...}
    """
    from rdfsolve.iri import resolve_iris as _resolve

    return _resolve(iris=iris, endpoints=endpoints, timeout=timeout)


def compose_query_from_paths(
    paths: list[dict[str, Any]],
    prefixes: dict[str, str] | None = None,
    include_types: bool = False,
    include_labels: bool = True,
    limit: int = 100,
    value_bindings: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Generate a SPARQL query from diagram paths.

    This is a pure-Python function- no Flask required.  It delegates to
    :func:`rdfsolve.compose.compose_query_from_paths`.

    Args:
        paths: List of path dicts, each with an ``edges`` list.
            Each edge has ``source``, ``target``, ``predicate``,
            and ``is_forward``.
        prefixes: Namespace prefix map
            (e.g. ``{"wp": "http://..."}``).
        include_types: Add ``rdf:type`` assertions.
        include_labels: Add ``OPTIONAL rdfs:label`` clauses.
        limit: LIMIT for the generated query.
        value_bindings: VALUES clause bindings
            ``{var: [uri, ...]}``.

    Returns:
        Dict with ``query`` (SPARQL string), ``variable_map``
        (var -> schema URI), and ``jsonld``
        (SPARQLExecutable JSON-LD).

    Example::

        >>> from rdfsolve.api import compose_query_from_paths
        >>> result = compose_query_from_paths(
        ...     paths=[{"edges": [{
        ...         "source": "http://ex.org/Gene",
        ...         "target": "http://ex.org/Protein",
        ...         "predicate": "http://ex.org/encodes",
        ...         "is_forward": True,
        ...     }]}],
        ...     prefixes={"ex": "http://ex.org/"},
        ... )
        >>> print(result["query"])
        PREFIX ex: <http://ex.org/>
        ...
    """
    from rdfsolve.compose import compose_query_from_paths as _compose

    return _compose(
        paths=paths,
        prefixes=prefixes or {},
        options={
            "include_types": include_types,
            "include_labels": include_labels,
            "limit": limit,
            "value_bindings": value_bindings or {},
        },
    )


def probe_instance_mapping(
    prefix: str,
    sources_csv: str | None = None,
    *,
    sources: str | None = None,
    predicate: str = "http://www.w3.org/2004/02/skos/core#narrowMatch",
    dataset_names: list[str] | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Probe SPARQL endpoints for a bioregistry resource and return JSON-LD.

    For every dataset in *sources* (or the subset in *dataset_names*),
    queries the endpoint for RDF classes whose instances match the resource's
    known URI prefixes.  Generates pairwise ``skos:narrowMatch`` edges (or
    *predicate* override) between classes across different datasets and
    returns the result as a JSON-LD mapping document.

    The returned dict has the same structure as a mined schema JSON-LD
    (``@context`` + ``@graph`` + ``@about``) and can be saved directly
    to ``docker/schemas/`` for auto-import on Flask startup.

    Args:
        prefix: Bioregistry prefix, e.g. ``"ensembl"``.
        sources_csv: **Deprecated** - use *sources* instead.
        sources: Path to the sources file (JSON-LD or CSV).
            When ``None``, auto-detects the default file.
        predicate: Mapping predicate URI.  Defaults to
            ``skos:narrowMatch``.
        dataset_names: Restrict probing to these dataset names.
        timeout: SPARQL request timeout in seconds.

    Returns:
        JSON-LD ``dict`` with ``@context``, ``@graph``, ``@about``.

    Raises:
        ValueError: If *prefix* is unknown to bioregistry.
    """
    from rdfsolve.instance_matcher import probe_resource
    from rdfsolve.sources import load_sources_dataframe

    src_path = sources or sources_csv or None
    datasources = load_sources_dataframe(src_path)
    mapping = probe_resource(
        prefix=prefix,
        datasources=datasources,
        predicate=predicate,
        dataset_names=dataset_names,
        timeout=timeout,
    )
    return mapping.to_jsonld()


def _merge_instance_mapping_jsonld(
    existing: dict[str, Any],
    new: dict[str, Any],
) -> dict[str, Any]:
    """Merge *new* instance-mapping JSON-LD into *existing* in-place.

    Delegates to :func:`rdfsolve.mapping_models.instance.merge_instance_jsonld`.
    """
    from rdfsolve.mapping_models.instance import merge_instance_jsonld

    return merge_instance_jsonld(existing, new)


def seed_instance_mappings(
    prefixes: list[str],
    sources_csv: str | None = None,
    *,
    sources: str | None = None,
    output_dir: str = "docker/mappings/instance_matching",
    predicate: str = "http://www.w3.org/2004/02/skos/core#narrowMatch",
    dataset_names: list[str] | None = None,
    timeout: float = 60.0,
    skip_existing: bool = False,
) -> dict[str, Any]:
    """Probe multiple bioregistry resources and write mapping JSON-LD files.

    Delegates to :func:`rdfsolve.instance_matcher.seed_instance_mappings`.
    """
    from rdfsolve.instance_matcher import seed_instance_mappings as _sim

    return _sim(
        prefixes,
        sources_csv,
        sources=sources,
        output_dir=output_dir,
        predicate=predicate,
        dataset_names=dataset_names,
        timeout=timeout,
        skip_existing=skip_existing,
    )


# ── SeMRA import API ─────────────────────────────────────────────


def import_semra_source(
    source: str,
    keep_prefixes: list[str] | None = None,
    output_dir: str = "docker/mappings/semra",
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Import mappings from a SeMRA source and write one JSON-LD per prefix.

    Delegates to :func:`rdfsolve.semra_converter.import_source`.

    Args:
        source: SeMRA source key (e.g. ``"biomappings"``).
        keep_prefixes: Optional prefix filter.
        output_dir: Directory for output files.
        mapping_type: ``"instance"`` (default) or ``"class"``.  When
            ``"instance"``, the output JSON-LD contains instance-level
            edges that can be passed to
            :func:`derive_class_mappings_from_instances`.

    Returns:
        Summary dict ``{"succeeded", "failed", "skipped"}``.
    """
    from rdfsolve.semra_converter import import_source

    return import_source(
        source=source,
        keep_prefixes=keep_prefixes,
        output_dir=output_dir,
        mapping_type=mapping_type,
    )


def seed_semra_mappings(
    sources: list[str],
    keep_prefixes: list[str] | None = None,
    output_dir: str = "docker/mappings/semra",
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Seed semra mapping files for multiple sources.

    Delegates to :func:`rdfsolve.semra_converter.seed_semra_mappings`.
    """
    from rdfsolve.semra_converter import seed_semra_mappings as _ssm

    return _ssm(
        sources,
        keep_prefixes=keep_prefixes,
        output_dir=output_dir,
        mapping_type=mapping_type,
    )


def load_mapping_jsonld(path: str) -> dict[str, Any]:
    """Load a mapping JSON-LD file from disk.

    Args:
        path: Path to a ``.jsonld`` file.

    Returns:
        Parsed JSON dict.
    """
    result: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
    return result


def infer_mappings(
    input_paths: list[str],
    output_path: str,
    *,
    inversion: bool = True,
    transitivity: bool = True,
    generalisation: bool = False,
    chain_cutoff: int = 3,
    dataset_name: str | None = None,
) -> dict[str, Any]:
    """Run the SeMRA inference pipeline over mapping JSON-LD files.

    Thin wrapper around :func:`rdfsolve.inference.infer_mappings`.
    See that function for full documentation.

    Args:
        input_paths: Paths to input mapping JSON-LD files.
        output_path: Path to write the inferenced mapping JSON-LD.
        inversion: Apply symmetric inversion.
        transitivity: Apply transitive chain inference.
        generalisation: Apply generalisation.
        chain_cutoff: Max chain length for transitivity.
        dataset_name: Override for ``@about.dataset_name``.

    Returns:
        Summary dict with ``"input_edges"``, ``"output_edges"``,
        ``"inference_types"``, ``"output_path"``.
    """
    from rdfsolve.inference import infer_mappings as _infer

    return _infer(
        input_paths=input_paths,
        output_path=output_path,
        inversion=inversion,
        transitivity=transitivity,
        generalisation=generalisation,
        chain_cutoff=chain_cutoff,
        dataset_name=dataset_name,
    )


def seed_inferenced_mappings(
    input_dir: str = "docker/mappings",
    output_dir: str = "docker/mappings/inferenced",
    output_name: str = "inferenced_mappings",
    inversion: bool = True,
    transitivity: bool = True,
    generalisation: bool = False,
    chain_cutoff: int = 3,
) -> dict[str, Any]:
    """Infer over all mappings in *input_dir* and write to *output_dir*.

    Thin wrapper around
    :func:`rdfsolve.inference.seed_inferenced_mappings`.

    Args:
        input_dir: Directory containing mapping subdirs.
        output_dir: Directory for output.
        output_name: Stem for the output file.
        inversion: Apply inversion inference.
        transitivity: Apply transitivity inference.
        generalisation: Apply generalisation.
        chain_cutoff: Max chain length.

    Returns:
        Summary dict from :func:`infer_mappings`.
    """
    from rdfsolve.inference import (
        seed_inferenced_mappings as _seed,
    )

    return _seed(
        input_dir=input_dir,
        output_dir=output_dir,
        output_name=output_name,
        inversion=inversion,
        transitivity=transitivity,
        generalisation=generalisation,
        chain_cutoff=chain_cutoff,
    )


def import_sssom_source(
    entry: dict[str, Any],
    output_dir: str = "docker/mappings/sssom",
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Download and convert one SSSOM source entry to JSON-LD files.

    Thin wrapper around
    :func:`rdfsolve.sssom_importer.import_sssom_source`.

    For each ``.sssom.tsv`` file found inside the archive at
    ``entry["url"]``, one JSON-LD file is written to *output_dir*::

        {source_name}__{sssom_file_stem}.jsonld

    Args:
        entry: Dict with at least ``"name"`` and ``"url"`` keys, as found
               in ``data/sssom_sources.yaml``.
        output_dir: Directory to write output JSON-LD files.
        mapping_type: ``"instance"`` (default) or ``"class"``.
            Stored in the ``@about.mapping_type`` field of each output
            JSON-LD file.

    Returns:
        Summary dict with keys ``"succeeded"``, ``"failed"``,
        ``"skipped"``.
    """
    from rdfsolve.sssom_importer import import_sssom_source as _import

    return _import(
        entry=entry,
        output_dir=output_dir,
        mapping_type=mapping_type,
    )


def seed_sssom_mappings(
    sssom_sources_yaml: str = "data/sssom_sources.yaml",
    output_dir: str = "docker/mappings/sssom",
    names: list[str] | None = None,
    mapping_type: str = "instance",
) -> dict[str, Any]:
    """Seed SSSOM mapping files for all (or selected) sources.

    Thin wrapper around
    :func:`rdfsolve.sssom_importer.seed_sssom_mappings`.

    Reads *sssom_sources_yaml*, optionally filters to *names*, and calls
    :func:`import_sssom_source` for each entry.

    Args:
        sssom_sources_yaml: Path to the SSSOM sources YAML file
            (default: ``data/sssom_sources.yaml``).
        output_dir: Directory for output JSON-LD files
            (default: ``docker/mappings/sssom``).
        names: Optional list of source names to restrict processing;
               if ``None`` (default), all entries are processed.
        mapping_type: ``"instance"`` (default) or ``"class"``.
            Stored in the ``@about.mapping_type`` field of each output
            JSON-LD file.

    Returns:
        Aggregated summary with keys ``"succeeded"``, ``"failed"``,
        ``"skipped"``.
    """
    from rdfsolve.sssom_importer import seed_sssom_mappings as _seed

    return _seed(
        sssom_sources_yaml=sssom_sources_yaml,
        output_dir=output_dir,
        names=names,
        mapping_type=mapping_type,
    )


# ---------------------------------------------------------------------------
# Instance-to-class derivation
# ---------------------------------------------------------------------------


def build_class_index_from_endpoints(
    entity_iris: list[str],
    endpoint_url: str,
    *,
    batch_size: int = 50,
    timeout: float = 60.0,
    cache_path: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Build (or load) a :class:`~rdfsolve.class_index.ClassIndex`.

    Delegates to :func:`rdfsolve.class_index.build_class_index_from_endpoints`.
    """
    from rdfsolve.class_index import build_class_index_from_endpoints as _build

    return _build(
        entity_iris,
        endpoint_url,
        batch_size=batch_size,
        timeout=timeout,
        cache_path=cache_path,
    )


def enrich_instance_jsonld(
    jsonld_path: str,
    class_index: Any,
    *,
    output_path: str | None = None,
) -> dict[str, Any]:
    """Enrich an instance-mapping JSON-LD file with class annotations.

    Reads the JSON-LD document at *jsonld_path*, calls
    :func:`~rdfsolve.class_index.enrich_jsonld_with_classes`, and writes
    the enriched document.  The output location defaults to
    ``{stem}.enriched.jsonld`` next to the source file.

    Args:
        jsonld_path: Path to the source JSON-LD mapping file.
        class_index: A :class:`~rdfsolve.class_index.ClassIndex` instance
            returned by :func:`build_class_index_from_endpoints`.
        output_path: Explicit destination path.  Defaults to
            ``{jsonld_path_stem}.enriched.jsonld``.

    Returns:
        Enrichment statistics dict with keys
        ``"total_edges"``, ``"enriched_edges"``, ``"elapsed_s"``.
    """
    import json as _json
    from pathlib import Path as _Path

    from rdfsolve.class_index import enrich_jsonld_with_classes

    src = _Path(jsonld_path)
    doc = _json.loads(src.read_text(encoding="utf-8"))
    enriched_doc, stats = enrich_jsonld_with_classes(doc, class_index)

    dest = _Path(output_path) if output_path else src.with_suffix(".enriched.jsonld")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        _json.dumps(enriched_doc, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return stats


def derive_class_mappings_from_instances(
    input_paths: list[str],
    output_path: str,
    *,
    endpoint_url: str,
    timeout: float = 60.0,
    batch_size: int = 50,
    min_instance_count: int = 1,
    min_confidence: float = 0.0,
    cache_index: bool = False,
    index_cache_path: str | None = None,
    enrich_in_place: bool = False,
    source_name: str | None = None,
) -> dict[str, Any]:
    """Orchestrate the full instance-to-class derivation pipeline.

    Delegates to
    :func:`rdfsolve.class_derivation.derive_class_mappings_from_instances`.
    """
    from rdfsolve.class_derivation import (
        derive_class_mappings_from_instances as _dcmfi,
    )

    return _dcmfi(
        input_paths,
        output_path,
        endpoint_url=endpoint_url,
        timeout=timeout,
        batch_size=batch_size,
        min_instance_count=min_instance_count,
        min_confidence=min_confidence,
        cache_index=cache_index,
        index_cache_path=index_cache_path,
        enrich_in_place=enrich_in_place,
        source_name=source_name,
    )


# ── Bioregistry metadata ──────────────────────────────────────────


def get_bioregistry_metadata(br_prefix: str) -> dict[str, Any]:
    """Return a structured metadata dict for a Bioregistry prefix.

    Delegates to :func:`rdfsolve.sources.get_bioregistry_metadata`.

    Parameters
    ----------
    br_prefix:
        A valid Bioregistry prefix (e.g. ``"drugbank"``, ``"chebi"``).

    Returns
    -------
    dict
        Fields: ``prefix``, ``name``, ``description``, ``homepage``,
        ``license``, ``domain``, ``keywords``, ``publications``,
        ``uri_prefix``, ``uri_prefixes``, ``synonyms``, ``mappings``,
        ``logo``, ``extra_providers``.

    Raises
    ------
    ValueError
        If *br_prefix* is unknown to Bioregistry.
    """
    from rdfsolve.sources import get_bioregistry_metadata as _impl

    return _impl(br_prefix)


def enrich_source_with_bioregistry(
    entry: SourceEntry,
) -> str | None:
    """Populate ``bioregistry_*`` fields on a source entry in-place.

    Delegates to :func:`rdfsolve.sources.enrich_source_with_bioregistry`.

    Parameters
    ----------
    entry:
        A :class:`~rdfsolve.sources.SourceEntry` dict, modified in-place.

    Returns
    -------
    str or None
        The resolved Bioregistry prefix, or ``None`` if no match was found.
    """
    from rdfsolve.sources import enrich_source_with_bioregistry as _impl

    return _impl(entry)


def sources_to_jsonld(
    entries: list[SourceEntry],
    *,
    enrich: bool = False,
) -> dict[str, Any]:
    """Serialise source entries to a JSON-LD document.

    Delegates to :func:`rdfsolve.sources.sources_to_jsonld`.

    Parameters
    ----------
    entries:
        Source entries, typically returned by
        :func:`~rdfsolve.sources.load_sources`.
    enrich:
        When ``True``, resolve and embed Bioregistry metadata for each
        source before serialisation (entries are not modified in place).

    Returns
    -------
    dict
        JSON-LD document with ``@context`` and ``@graph`` keys.
    """
    from rdfsolve.sources import sources_to_jsonld as _impl

    return _impl(entries, enrich=enrich)


# ── Ontology Index ────────────────────────────────────────────────────────


def build_ontology_index(
    schema_class_uris: set[str] | None = None,
    *,
    cache_dir: str | None = None,
    ontology_ids: list[str] | None = None,
) -> Any:
    """Build an OntologyIndex from OLS4 metadata.

    Delegates to :func:`rdfsolve.ontology.index.build_ontology_index`.

    Parameters:
        schema_class_uris: Set of class IRIs from rdfsolve schemas.  When
            provided, only ontologies whose ``baseUri`` overlaps with the
            given URIs are fully indexed.
        cache_dir: Directory for diskcache (OLS HTTP-response cache).
            Pass ``None`` to disable caching.
        ontology_ids: Explicit list of OLS4 ontology IDs to index.  When
            provided, the OLS paginated ontology listing is skipped.

    Returns:
        OntologyIndex: Populated index ready for grounding tier 3 and
            path planning.
    """
    from rdfsolve.ontology.index import build_ontology_index as _impl

    return _impl(schema_class_uris, cache_dir=cache_dir, ontology_ids=ontology_ids)


def load_ontology_index(data_dir: str | Path = "data") -> Any:
    """Load a persisted OntologyIndex from *data_dir*.

    Delegates to :func:`rdfsolve.ontology.index.load_ontology_index`.

    Parameters:
        data_dir: Directory that contains ``ontology_index.pkl.gz`` and
            (optionally) ``ontology_graph.graphml``, as written by
            :func:`~rdfsolve.ontology.index.save_ontology_index`.

    Returns:
        OntologyIndex: Restored index.

    Raises:
        FileNotFoundError: If ``ontology_index.pkl.gz`` does not exist
            under *data_dir*.
    """
    from rdfsolve.ontology.index import load_ontology_index as _impl

    return _impl(data_dir)


def save_ontology_index(index: Any, data_dir: str | Path = "data") -> None:
    """Persist an OntologyIndex to *data_dir* as compressed pickle + GraphML.

    Delegates to :func:`rdfsolve.ontology.index.save_ontology_index`.

    Parameters:
        index: Populated OntologyIndex instance to save.
        data_dir: Target directory.  Created if it does not exist.

    Returns:
        None
    """
    from rdfsolve.ontology.index import save_ontology_index as _impl

    return _impl(index, data_dir)


def save_ontology_index_to_db(index: Any, db: Any) -> None:
    """Persist an OntologyIndex to the rdfsolve SQLite database.

    Delegates to :func:`rdfsolve.ontology.index.save_ontology_index_to_db`.

    Parameters:
        index: Populated OntologyIndex instance.
        db: Open :class:`~rdfsolve.backend.database.Database` instance.

    Returns:
        None
    """
    from rdfsolve.ontology.index import save_ontology_index_to_db as _impl

    return _impl(index, db)


def load_ontology_index_from_db(db: Any) -> Any:
    """Load an OntologyIndex from the rdfsolve SQLite database.

    Delegates to :func:`rdfsolve.ontology.index.load_ontology_index_from_db`.

    Parameters:
        db: Open :class:`~rdfsolve.backend.database.Database` instance.

    Returns:
        OntologyIndex: Reconstructed index.

    Raises:
        RuntimeError: If no ontology index is found in the database.
    """
    from rdfsolve.ontology.index import load_ontology_index_from_db as _impl

    return _impl(db)
