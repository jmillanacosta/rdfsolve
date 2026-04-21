"""Main RDFSolve functionalities for extraction, conversion and solving."""

from __future__ import annotations

import json
import logging
import re
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
    "discover_void_source",
    "enrich_source_with_bioregistry",
    "execute_sparql",
    "export_schema_artifacts",
    "extract_partitions_from_void",
    "generate_qleverfiles",
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
    "load_sources",
    "mine_all_sources",
    "mine_local_source",
    "mine_schema",
    "probe_instance_mapping",
    "resolve_iris",
    "resolve_void_uri_base",
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
    inter_request_delay: float = 0.0,
) -> dict[str, Any]:
    """Probe SPARQL endpoints for a bioregistry resource and return JSON-LD.

    For every dataset in *sources* (or the subset in *dataset_names*),
    queries the endpoint for RDF classes whose instances match the resource's
    known URI prefixes.  Generates pairwise ``skos:narrowMatch`` edges (or
    *predicate* override) between classes across different datasets and
    returns the result as a JSON-LD mapping document.

    Args:
        prefix: Bioregistry prefix, e.g. ``"ensembl"``.
        sources_csv: **Deprecated** - use *sources* instead.
        sources: Path to the sources file (JSON-LD or CSV).
            When ``None``, auto-detects the default file.
        predicate: Mapping predicate URI.  Defaults to
            ``skos:narrowMatch``.
        dataset_names: Restrict probing to these dataset names.
        timeout: SPARQL request timeout in seconds.
        inter_request_delay: Seconds to sleep before each SPARQL request.
            Use a positive value for remote public endpoints; ``0.0``
            (default) for local QLever.

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
        inter_request_delay=inter_request_delay,
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
    ports_json: str | None = None,
    inter_request_delay: float = 0.0,
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
        ports_json=ports_json,
        inter_request_delay=inter_request_delay,
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
    endpoint_url: str = "",
    ports_json_path: str | None = None,
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
        ports_json_path=ports_json_path,
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


# ═══════════════════════════════════════════════════════════════════
# Pipeline helpers - single-source discover / mine / export / qlever
# ═══════════════════════════════════════════════════════════════════

# Default base URI template for VoID partition IRIs.
_VOID_URI_DEFAULT = "https://jmillanacosta.com/rdfsolve/{name}/mined/"


def load_sources(
    path: str | Path | None = None,
    name_filter: str | None = None,
) -> list["SourceEntry"]:
    """Load source entries, optionally filtered by name regex.

    Re-exports :func:`rdfsolve.sources.load_sources` with an
    optional *name_filter* convenience argument.

    Parameters
    ----------
    path:
        Path to the sources file (YAML / JSON-LD / CSV).
        ``None`` falls back to the default ``data/sources.yaml``.
    name_filter:
        Regex pattern (case-insensitive) to select sources by name.
        ``None`` returns all sources.
    """
    from .sources import load_sources as _load

    entries = _load(path)
    if name_filter:
        pat = re.compile(name_filter, re.IGNORECASE)
        entries = [e for e in entries if pat.search(e.get("name", ""))]
    return entries


def resolve_void_uri_base(
    name: str,
    override: str | None = None,
    entry: "SourceEntry | dict[str, Any] | None" = None,
) -> str:
    """Return the VoID base URI for a dataset.

    Resolution order:

    1. Explicit *override* value (e.g. from ``--void-uri-base``).
    2. ``void_uri_base`` field in the source entry.
    3. Default template
       ``https://jmillanacosta.com/rdfsolve/{name}/mined/``.
    """
    if override:
        return override.rstrip("/") + "/"
    if entry and entry.get("void_uri_base"):
        return str(entry["void_uri_base"]).rstrip("/") + "/"
    return _VOID_URI_DEFAULT.format(name=name)


def export_schema_artifacts(
    void_graph: Graph,
    name: str,
    endpoint: str,
    output_dir: str | Path,
    tag: str = "discovered_remote",
    fmt: str = "all",
) -> dict[str, str]:
    """Write VoID / JSON-LD / LinkML / SHACL / RDF-config artefacts.

    This is the shared export routine used by :func:`discover_void_source`
    and :func:`mine_local_source`.

    Parameters
    ----------
    void_graph:
        An ``rdflib.Graph`` containing VoID triples.
    name:
        Dataset name (used in file stems).
    endpoint:
        Endpoint URL written into JSON-LD ``@about`` and RDF-config.
    output_dir:
        Directory to write files into (created if needed).
    tag:
        File-name tag inserted between *name* and suffix,
        e.g. ``"discovered_remote"`` → ``<name>_discovered_remote_void.ttl``.
    fmt:
        ``"jsonld"``, ``"void"``, or ``"all"`` (default).

    Returns
    -------
    dict[str, str]
        Mapping of artefact kind → file path written.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}

    # ── VoID Turtle ──────────────────────────────────────────────
    if fmt in ("void", "all"):
        void_path = out / f"{name}_{tag}_void.ttl"
        void_graph.serialize(destination=str(void_path), format="turtle")
        written["void_ttl"] = str(void_path)

    # ── JSON-LD ──────────────────────────────────────────────────
    if fmt in ("jsonld", "all"):
        jsonld_doc = graph_to_jsonld(
            void_graph, endpoint_url=endpoint, dataset_name=name,
        )
        jsonld_path = out / f"{name}_{tag}_schema.jsonld"
        jsonld_path.write_text(
            json.dumps(jsonld_doc, indent=2) + "\n", encoding="utf-8",
        )
        written["schema_jsonld"] = str(jsonld_path)

    # ── LinkML ───────────────────────────────────────────────────
    if fmt in ("all",):
        try:
            export_parser = VoidParser(void_source=void_graph)
            linkml_yaml = export_parser.to_linkml_yaml(
                filter_void_nodes=True, schema_name=name,
            )
            linkml_path = out / f"{name}_{tag}_linkml.yaml"
            linkml_path.write_text(linkml_yaml, encoding="utf-8")
            written["linkml_yaml"] = str(linkml_path)
        except Exception as exc:
            logger.debug("LinkML export failed for %s: %s", name, exc)

    # ── SHACL ────────────────────────────────────────────────────
    if fmt in ("all",):
        try:
            export_parser = VoidParser(void_source=void_graph)
            shacl_ttl = export_parser.to_shacl(
                filter_void_nodes=True, schema_name=name,
            )
            shacl_path = out / f"{name}_{tag}_shacl.ttl"
            shacl_path.write_text(shacl_ttl, encoding="utf-8")
            written["shacl_ttl"] = str(shacl_path)
        except Exception as exc:
            logger.debug("SHACL export failed for %s: %s", name, exc)

    # ── RDF-config ───────────────────────────────────────────────
    if fmt in ("all",):
        try:
            export_parser = VoidParser(void_source=void_graph)
            rdfconfig = export_parser.to_rdfconfig(
                filter_void_nodes=True,
                endpoint_url=endpoint,
                endpoint_name=name,
            )
            config_dir = out / f"{name}_{tag}_config"
            config_dir.mkdir(parents=True, exist_ok=True)
            for fname, content in rdfconfig.items():
                (config_dir / f"{fname}.yaml").write_text(
                    content, encoding="utf-8",
                )
            written["rdfconfig_dir"] = str(config_dir)
        except Exception as exc:
            logger.debug("RDF-config export failed for %s: %s", name, exc)

    return written


def discover_void_source(
    endpoint: str,
    name: str,
    output_dir: str | Path = ".",
    *,
    tag: str = "discovered_remote",
    void_uri_base: str | None = None,
    entry: "SourceEntry | dict[str, Any] | None" = None,
    fmt: str = "all",
) -> dict[str, Any]:
    """Discover VoID descriptions for one source and export artefacts.

    Calls :func:`discover_void_graphs` then
    :func:`export_schema_artifacts` for the result.

    Parameters
    ----------
    endpoint:
        SPARQL endpoint URL.
    name:
        Dataset name.
    output_dir:
        Directory for output files.
    tag:
        File-name tag (default ``"discovered_remote"``).
    void_uri_base:
        Explicit base URI override (``None`` → resolved via
        :func:`resolve_void_uri_base`).
    entry:
        Source entry dict — used to resolve *void_uri_base* when no
        explicit override is given.
    fmt:
        Export format (``"jsonld"``, ``"void"``, ``"all"``).

    Returns
    -------
    dict with ``partitions_found``, ``graphs_found``, ``files``.
    Returns ``partitions_found == 0`` when the endpoint has no VoID
    data.
    """
    result = discover_void_graphs(endpoint, exclude_graphs=False)
    partitions = result.get("partitions", [])

    if not partitions:
        return {
            "partitions_found": 0,
            "graphs_found": 0,
            "files": {},
        }

    base_uri = void_uri_base or resolve_void_uri_base(
        name, entry=entry,
    )
    parser = VoidParser()
    void_graph = parser.build_void_graph_from_partitions(
        partitions, base_uri=base_uri,
    )

    files = export_schema_artifacts(
        void_graph, name, endpoint, output_dir, tag=tag, fmt=fmt,
    )

    # Discovery report
    out = Path(output_dir)
    report = {
        "dataset": name,
        "endpoint": endpoint,
        "source": "discovered",
        "graphs_found": len(result.get("found_graphs", [])),
        "partitions_found": len(partitions),
    }
    report_path = out / f"{name}_{tag}_report.json"
    report_path.write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8",
    )
    files["report"] = str(report_path)

    return {
        "partitions_found": len(partitions),
        "graphs_found": len(result.get("found_graphs", [])),
        "files": files,
    }


def mine_local_source(
    endpoint: str,
    name: str,
    output_dir: str | Path = ".",
    *,
    graph_uris: list[str] | None = None,
    void_uri_base: str | None = None,
    entry: "SourceEntry | dict[str, Any] | None" = None,
    chunk_size: int = 10_000,
    class_batch_size: int = 15,
    class_chunk_size: int | None = None,
    timeout: float = 120.0,
    counts: bool = True,
    one_shot: bool = False,
    untyped_as_classes: bool = False,
    fmt: str = "all",
    authors: list[dict[str, str]] | None = None,
    discover_first: bool = False,
    qlever_version: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Mine a single dataset from a (local) SPARQL endpoint.

    Calls :func:`rdfsolve.miner.mine_schema` (returns ``MinedSchema``)
    then exports artefacts via :func:`export_schema_artifacts`.

    When *discover_first* is ``True`` a VoID discovery pass is run
    **before** mining and its results are saved with the
    ``discovered_local`` tag.

    Parameters
    ----------
    endpoint:
        SPARQL endpoint URL (typically ``http://localhost:<port>``).
    name:
        Dataset name.
    output_dir:
        Directory for output files.
    graph_uris:
        Named-graph URIs to scope mining queries.  ``None`` means
        "use entry's graph_uris if available, else mine all graphs".
    void_uri_base:
        Explicit VoID base URI override.
    entry:
        Source entry dict — used for graph_uris fallback and
        void_uri_base resolution.
    chunk_size, class_batch_size, class_chunk_size, timeout, counts,
    one_shot, untyped_as_classes, fmt, authors:
        Forwarded to :func:`rdfsolve.miner.mine_schema`.
    discover_first:
        Run VoID discovery before mining.
    qlever_version:
        ``{"git_hash_server": ..., "git_hash_index": ...}`` from
        ``?cmd=stats``.  Written into the mining report.

    Returns
    -------
    dict with ``classes``, ``properties``, ``files``, ``report_path``.
    """
    from .miner import mine_schema as _mine

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # ── Optional discovery pass ──────────────────────────────────
    if discover_first:
        disc = discover_void_source(
            endpoint, name, output_dir,
            tag="discovered_local",
            void_uri_base=void_uri_base,
            entry=entry,
            fmt=fmt,
        )
        logger.info(
            "%s: discovered %d partitions",
            name, disc.get("partitions_found", 0),
        )

    # ── Resolve graph_uris ───────────────────────────────────────
    if graph_uris is None and entry is not None:
        raw = entry.get("graph_uris")
        if raw:
            graph_uris = list(raw) if isinstance(raw, (list, tuple)) else [raw]

    # ── Tag ──────────────────────────────────────────────────────
    _tag = "mined_local_untyped" if untyped_as_classes else "mined_local"
    rpt_path = out / f"{name}_{_tag}_report.json"

    # ── Mine ─────────────────────────────────────────────────────
    schema = _mine(
        endpoint_url=endpoint,
        dataset_name=name,
        graph_uris=graph_uris,
        chunk_size=chunk_size,
        class_chunk_size=class_chunk_size,
        class_batch_size=class_batch_size,
        timeout=timeout,
        counts=counts,
        two_phase=True,
        report_path=rpt_path,
        filter_service_namespaces=True,
        untyped_as_classes=untyped_as_classes,
        authors=authors,
        qlever_version=qlever_version,
        one_shot=one_shot,
    )

    # Override the endpoint in schema metadata so localhost:PORT
    # doesn't leak into the published artefacts.
    resolved = resolve_void_uri_base(
        name, override=void_uri_base, entry=entry,
    )
    schema.about.endpoint = resolved.rstrip("/")

    result_files: dict[str, str] = {"report": str(rpt_path)}

    # ── Export from MinedSchema ──────────────────────────────────
    if fmt in ("jsonld", "all"):
        jsonld_path = out / f"{name}_{_tag}_schema.jsonld"
        jsonld_path.write_text(
            json.dumps(schema.to_jsonld(), indent=2) + "\n",
            encoding="utf-8",
        )
        result_files["schema_jsonld"] = str(jsonld_path)

    if fmt in ("void", "all"):
        void_g = schema.to_void_graph()
        void_path = out / f"{name}_{_tag}_void.ttl"
        void_g.serialize(destination=str(void_path), format="turtle")
        result_files["void_ttl"] = str(void_path)

    return {
        "classes": len(schema.get_classes()),
        "properties": len(schema.get_properties()),
        "files": result_files,
        "report_path": str(rpt_path),
    }


def generate_qleverfiles(
    sources_path: str | Path | None = None,
    data_dir: str | Path = ".",
    *,
    base_port: int = 7019,
    runtime: str = "docker",
    name_filter: str | None = None,
    test: bool = False,
    server_memory: str = "500G",
) -> dict[str, Any]:
    """Generate Qleverfiles for all downloadable sources.

    For each eligible source a per-source ``Qleverfile`` is written to
    ``<data_dir>/qlever_workdirs/<name>/Qleverfile``.

    Additionally, for every ``local_provider`` group a *combined*
    ``Qleverfile`` is written that indexes all member sources together
    into one QLever instance.

    Parameters
    ----------
    sources_path:
        Path to sources file (``None`` → default).
    data_dir:
        Root directory where RDF dumps live.
    base_port:
        First port number for allocation.
    runtime:
        ``"docker"`` or ``"native"``.
    name_filter:
        Regex to select sources by name.
    test:
        If ``True``, generate only for the 3 smallest sources.
    server_memory:
        ``MEMORY_FOR_QUERIES`` written into every Qleverfile (``-m``
        flag passed to ``qlever-server``).  Defaults to ``"500G"``.
        Lower this when many servers run concurrently on a single node.

    Returns
    -------
    dict with ``generated``, ``skipped``, ``failed`` lists.
    """
    from .qlever import (
        QleverConfig,
        build_provider_qleverfile as _build_provider,
        build_qleverfile as _build_single,
        detect_data_format as _detect,
    )

    cfg = QleverConfig(memory_for_queries=server_memory)

    data_dir = Path(data_dir).resolve()
    entries = load_sources(sources_path, name_filter=name_filter)

    # Keep only sources with a recognised download format.
    downloadable = [e for e in entries if _detect(e) is not None]

    if test:
        downloadable = _select_test_sources(downloadable)

    if not downloadable:
        return {"generated": [], "skipped": [], "failed": []}

    generated: list[str] = []
    skipped: list[str] = []
    failed: list[dict[str, str]] = []
    port_map: dict[str, int] = {}

    # ── Per-source Qleverfiles ───────────────────────────────────
    for idx, entry in enumerate(downloadable):
        name = entry.get("name", "unknown")
        port = base_port + idx
        port_map[name] = port

        workdir = data_dir / "qlever_workdirs" / name
        qleverfile_path = workdir / "Qleverfile"

        try:
            content = _build_single(entry, data_dir, port, runtime)
            workdir.mkdir(parents=True, exist_ok=True)
            qleverfile_path.write_text(content, encoding="utf-8")
            generated.append(name)
            logger.info(
                "[%d/%d] %s: port %d -> %s",
                idx + 1, len(downloadable), name, port, qleverfile_path,
            )
        except Exception as exc:
            failed.append({"dataset": name, "error": str(exc)[:200]})

    # ── Combined provider Qleverfiles ────────────────────────────
    from collections import defaultdict

    provider_groups: dict[str, list[Any]] = defaultdict(list)
    for entry in downloadable:
        provider = entry.get("local_provider", "")
        if provider:
            provider_groups[provider].append(entry)

    provider_base_port = base_port + len(downloadable)
    for p_idx, (provider, members) in enumerate(sorted(provider_groups.items())):
        prov_port = provider_base_port + p_idx
        port_map[provider] = prov_port

        workdir = data_dir / "qlever_workdirs" / provider
        qleverfile_path = workdir / "Qleverfile"

        try:
            content = _build_provider(
                provider, members, data_dir, prov_port, runtime,
            )
            workdir.mkdir(parents=True, exist_ok=True)
            qleverfile_path.write_text(content, encoding="utf-8")
            generated.append(f"{provider} (combined)")
            logger.info(
                "[combined] %s: port %d, %d members -> %s",
                provider, prov_port, len(members), qleverfile_path,
            )
        except Exception as exc:
            failed.append({
                "dataset": f"{provider} (combined)",
                "error": str(exc)[:200],
            })

    # ── Port manifest ────────────────────────────────────────────
    manifest_path = data_dir / "qlever_workdirs" / "ports.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(port_map, indent=2) + "\n", encoding="utf-8",
    )

    return {"generated": generated, "skipped": skipped, "failed": failed}


def _select_test_sources(entries: list) -> list:
    """Pick the 3 smallest downloadable sources for test mode.

    Prefers sources whose download URLs point to single files
    (not multi-file lists) and sorts alphabetically as tie-breaker.
    """
    downloadable = []
    for e in entries:
        has_download = any(
            k.startswith("download_") and e.get(k) for k in e
        )
        if has_download:
            download_fields = [
                k for k in e if k.startswith("download_") and e.get(k)
            ]
            is_single = all(isinstance(e[k], str) for k in download_fields)
            downloadable.append((is_single, e.get("name", ""), e))

    downloadable.sort(key=lambda t: (not t[0], t[1]))
    selected = [t[2] for t in downloadable[:3]]

    if not selected:
        logger.warning("No downloadable sources found for test mode")
    else:
        logger.info(
            "Test mode: selected %s",
            [s.get("name", "?") for s in selected],
        )

    return selected


def fetch_qlever_stats(
    endpoint: str,
    timeout: float = 10.0,
) -> dict[str, str] | None:
    """Fetch QLever build info from ``{endpoint}?cmd=stats``.

    Returns a dict with ``git_hash_server`` and ``git_hash_index``
    keys, or ``None`` if the endpoint does not expose stats.
    """
    import urllib.error
    import urllib.request

    url = endpoint.rstrip("/") + "?cmd=stats"
    try:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        result: dict[str, str] = {}
        if "git-hash-server" in data:
            result["git_hash_server"] = str(data["git-hash-server"])
        if "git-hash-index" in data:
            result["git_hash_index"] = str(data["git-hash-index"])
        return result or None
    except Exception as exc:
        logger.debug("Could not fetch QLever stats from %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Graph building (step 4b + 12)
# ---------------------------------------------------------------------------


def run_graph_pipeline(
    schemas_dir: str | Path,
    mappings_dir: str | Path,
    output_dir: str | Path,
    *,
    datasets: list[str] | None = None,
    schema_only: bool = False,
    copy_schemas: bool = True,
) -> dict[str, Any]:
    """Build dataset connectivity graphs and export to Parquet.

    Delegates to :func:`rdfsolve.graphs.run_graph_pipeline`.

    Parameters
    ----------
    schemas_dir:
        Root directory containing ``*_schema.jsonld`` files.
    mappings_dir:
        Root directory with ``sssom/``, ``semra/``, ``instance_matching/``,
        ``inferenced/`` sub-directories.
    output_dir:
        Output root for graphs, schemas, Parquet tables.
    datasets:
        Optional dataset name globs to restrict processing.
    schema_only:
        If True, only select schemas (step 4b); skip graph build.
    copy_schemas:
        Copy selected schemas to ``output_dir/schemas/``.

    Returns
    -------
    dict with ``metadata`` and ``benchmarks_path`` keys.
    """
    from rdfsolve.graphs import run_graph_pipeline as _impl

    return _impl(
        schemas_dir,
        mappings_dir,
        output_dir,
        datasets=datasets,
        schema_only=schema_only,
        copy_schemas=copy_schemas,
    )


# ---------------------------------------------------------------------------
# QLever boot
# ---------------------------------------------------------------------------


def boot_qlever_sources(
    sources_yaml: str | Path = "data/sources.yaml",
    *,
    source_names: list[str] | None = None,
    name_filter: str | None = None,
    step: str = "all",
    data_dir: str | Path = "data",
    base_port: int = 7019,
    runtime: str = "native",
    singularity_image: str = "./data/qlever.sif",
    docker_ref: str = "docker://adfreiburg/qlever:latest",
    memory_for_queries: str = "500G",
    timeout: str = "9999999999s",
    parser_buffer_size: str = "8GB",
    parallel_parsing: bool = False,
    num_triples_per_batch: int = 1_000_000,
    qlever_image: str = "docker.io/adfreiburg/qlever:latest",
    num_threads: int = 8,
    cache_size: str = "8G",
    server_memory: str = "40G",
    wait_timeout: int = 120,
) -> list[dict[str, Any]]:
    """Boot one or more QLever SPARQL endpoints via Singularity.

    Delegates to :func:`rdfsolve.qlever.boot.boot_sources`.

    Returns a list of result dicts (one per source).
    """
    from rdfsolve.qlever.boot import boot_sources as _impl

    return _impl(
        sources_yaml,
        source_names=source_names,
        name_filter=name_filter,
        step=step,
        data_dir=data_dir,
        base_port=base_port,
        runtime=runtime,
        singularity_image=singularity_image,
        docker_ref=docker_ref,
        memory_for_queries=memory_for_queries,
        timeout=timeout,
        parser_buffer_size=parser_buffer_size,
        parallel_parsing=parallel_parsing,
        num_triples_per_batch=num_triples_per_batch,
        qlever_image=qlever_image,
        num_threads=num_threads,
        cache_size=cache_size,
        server_memory=server_memory,
        wait_timeout=wait_timeout,
    )


def list_qlever_sources(
    sources_yaml: str | Path = "data/sources.yaml",
) -> list[dict[str, str]]:
    """List downloadable sources that can be booted as QLever endpoints.

    Delegates to :func:`rdfsolve.qlever.boot.list_downloadable_sources`.
    """
    from rdfsolve.qlever.boot import list_downloadable_sources as _impl

    return _impl(sources_yaml)

