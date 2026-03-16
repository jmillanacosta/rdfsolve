"""Main RDFSolve functionalities for VoID extraction and conversion."""

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
from rdflib import Graph

from .miner import _mine_one_source
from .parser import VoidParser

logger = logging.getLogger(__name__)

__all__ = [
    "compose_query_from_paths",
    "execute_sparql",
    "graph_to_jsonld",
    "graph_to_linkml",
    "graph_to_schema",
    "graph_to_shacl",
    "import_semra_source",
    "import_sssom_source",
    "infer_mappings",
    "load_mapping_jsonld",
    "load_parser_from_file",
    "load_parser_from_graph",
    "load_parser_from_jsonld",
    "mine_all_sources",
    "mine_schema",
    "probe_instance_mapping",
    "resolve_iris",
    "seed_inferenced_mappings",
    "seed_instance_mappings",
    "seed_semra_mappings",
    "seed_sssom_mappings",
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

    Reads a sources file (JSON-LD preferred, CSV still accepted)
    and runs :func:`mine_schema` for each entry whose *endpoint*
    is non-empty.  Results are written to *output_dir* as
    ``{name}_schema.jsonld`` and / or ``{name}_void.ttl``.

    Per-source overrides (``chunk_size``, ``class_batch_size``,
    ``timeout``, etc.) in the JSON-LD file take precedence over
    the function-level defaults.

    Args:
        sources_csv: **Deprecated** - use *sources* instead.
            Path to a CSV file with data sources.  Kept for
            backwards compatibility; ignored when *sources* is
            given.
        sources: Path to the sources file (JSON-LD or CSV).
            When ``None``, the default ``data/sources.jsonld``
            (or ``.csv`` fallback) is used.
        output_dir: Directory where outputs are written.
        fmt: Export format - ``"jsonld"``, ``"void"``, or
            ``"all"``.
        chunk_size: Pagination page size for SPARQL queries.
        class_chunk_size: Page size for Phase-1 class discovery
            in two-phase mode.  ``None`` = no pagination.
            Ignored for rows that are not two-phase.
        class_batch_size: Number of classes per VALUES query in
            Phase-2 of two-phase mining (default 15).
        delay: Delay between paginated pages (seconds).
        timeout: HTTP timeout per request (seconds).
        counts: Whether to fetch triple-count queries.
        reports: Write per-source analytics JSON reports.
        filter_service_namespaces: Strip service/system namespace
            patterns from each mined schema (default ``True``).
        untyped_as_classes: Treat untyped URI objects as
            ``owl:Class`` references instead of the generic
            ``rdfs:Resource`` sentinel (default ``False``).
        on_progress:
            Optional callback invoked after each source is
            processed.  Signature:
            ``(dataset_name, index, total, status_or_error)``.
            *status_or_error* is ``None`` on success, or an
            error message string.

    Returns:
        Summary dict with keys ``"succeeded"``, ``"failed"``, and
        ``"skipped"`` mapping to lists of dataset names.
    """
    from .sources import load_sources

    # Resolve the path: new kwarg > legacy positional > auto-detect
    src_path: str | None = sources or sources_csv or None

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    entries = load_sources(src_path)

    succeeded: list[str] = []
    failed: list[dict[str, str]] = []
    skipped: list[str] = []

    total = len(entries)
    for idx, entry in enumerate(entries, 1):
        name = entry.get("name", "")
        endpoint = entry.get("endpoint", "")

        if not endpoint:
            logger.info(
                "[%d/%d] Skipping %r: no endpoint",
                idx,
                total,
                name,
            )
            skipped.append(name)
            if on_progress:
                on_progress(name, idx, total, "skipped")
            continue

        _mine_one_source(
            entry,
            idx=idx,
            total=total,
            out=out,
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
            succeeded=succeeded,
            failed=failed,
        )

    return {
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
    }


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

    Iterates over *prefixes*, runs :func:`probe_instance_mapping` for each,
    and writes the result to
    ``{output_dir}/{prefix}_instance_mapping.jsonld``.

    When a file already exists on disk the new probe results are **merged**
    into it rather than overwriting it:

    * New ``@graph`` nodes (source classes not yet in the file) are appended.
    * For existing source nodes, new predicate->target entries are added;
      duplicates are silently skipped.
    * ``uri_formats_queried`` in ``@about`` is unioned.
    * ``pattern_count`` and ``generated_at`` are refreshed.

    The default behaviour (``skip_existing=False``) is to always probe and
    merge.  Pass ``skip_existing=True`` only when you explicitly want to skip
    prefixes whose output file already exists without re-probing.

    Args:
        prefixes: List of bioregistry prefixes to process.
        sources_csv: **Deprecated** - use *sources* instead.
        sources: Path to the sources file (JSON-LD or CSV).
            When ``None``, auto-detects the default file.
        output_dir: Directory where JSON-LD files are written
            (created if absent).
        predicate: Mapping predicate URI.
        dataset_names: Restrict probing to these dataset names.
        timeout: SPARQL request timeout per request.
        skip_existing: If ``True``, skip prefixes whose output file
            already exists without re-probing.  Defaults to ``False``
            (always probe and merge).

    Returns:
        Summary dict: ``{"succeeded": [...], "failed": [...]}``.
    """
    import json as _json

    from rdfsolve.instance_matcher import probe_resource
    from rdfsolve.sources import load_sources_dataframe

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    src_path = sources or sources_csv or None
    datasources = load_sources_dataframe(src_path)

    succeeded: list[str] = []
    failed: list[dict[str, str]] = []

    for prefix in prefixes:
        logger.info("Querying prefix: %s", prefix)
        outfile = out / f"{prefix}_instance_mapping.jsonld"

        if skip_existing and outfile.exists():
            logger.info(
                "Skipping %s: already exists at %s (skip_existing=True)",
                prefix,
                outfile,
            )
            succeeded.append(prefix)
            continue

        try:
            mapping = probe_resource(
                prefix=prefix,
                datasources=datasources,
                predicate=predicate,
                dataset_names=dataset_names,
                timeout=timeout,
            )
            new_jsonld = mapping.to_jsonld()

            if outfile.exists():
                try:
                    existing_jsonld = _json.loads(outfile.read_text())
                    merged = _merge_instance_mapping_jsonld(existing_jsonld, new_jsonld)
                    outfile.write_text(_json.dumps(merged, indent=2))
                    logger.info("Merged into existing: %s", outfile)
                except Exception as merge_exc:
                    logger.warning(
                        "Could not merge into %s (%s); overwriting.",
                        outfile,
                        merge_exc,
                    )
                    outfile.write_text(_json.dumps(new_jsonld, indent=2))
                    logger.info("Overwritten: %s", outfile)
            else:
                outfile.write_text(_json.dumps(new_jsonld, indent=2))
                logger.info("Written: %s", outfile)

            succeeded.append(prefix)
        except Exception as exc:
            logger.error("Failed %s: %s", prefix, exc)
            failed.append({"prefix": prefix, "error": str(exc)})

    return {"succeeded": succeeded, "failed": failed}


# ── SeMRA import API ─────────────────────────────────────────────


def import_semra_source(
    source: str,
    keep_prefixes: list[str] | None = None,
    output_dir: str = "docker/mappings/semra",
) -> dict[str, Any]:
    """Import mappings from a SeMRA source and write one JSON-LD per prefix.

    Delegates to :func:`rdfsolve.semra_converter.import_source`.

    Args:
        source: SeMRA source key (e.g. ``"biomappings"``).
        keep_prefixes: Optional prefix filter.
        output_dir: Directory for output files.

    Returns:
        Summary dict ``{"succeeded", "failed", "skipped"}``.
    """
    from rdfsolve.semra_converter import import_source

    return import_source(
        source=source,
        keep_prefixes=keep_prefixes,
        output_dir=output_dir,
    )


def seed_semra_mappings(
    sources: list[str],
    keep_prefixes: list[str] | None = None,
    output_dir: str = "docker/mappings/semra",
) -> dict[str, Any]:
    """Seed semra mapping files for multiple sources.

    Calls :func:`import_semra_source` for each entry in *sources* and
    aggregates the results.

    Args:
        sources: List of SeMRA source keys
            (e.g. ``["biomappings", "gilda"]``).
        keep_prefixes: Optional shared prefix filter applied to all sources.
        output_dir: Directory for output files.

    Returns:
        Aggregated summary with keys ``"succeeded"``, ``"failed"``,
        ``"skipped"``.
    """
    succeeded: list[str] = []
    failed: list[dict[str, str]] = []
    skipped: list[str] = []

    for source in sources:
        result = import_semra_source(
            source=source,
            keep_prefixes=keep_prefixes,
            output_dir=output_dir,
        )
        succeeded.extend(result.get("succeeded", []))
        failed.extend(result.get("failed", []))
        skipped.extend(result.get("skipped", []))

    return {"succeeded": succeeded, "failed": failed, "skipped": skipped}


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

    Returns:
        Summary dict with keys ``"succeeded"``, ``"failed"``,
        ``"skipped"``.
    """
    from rdfsolve.sssom_importer import import_sssom_source as _import

    return _import(entry=entry, output_dir=output_dir)


def seed_sssom_mappings(
    sssom_sources_yaml: str = "data/sssom_sources.yaml",
    output_dir: str = "docker/mappings/sssom",
    names: list[str] | None = None,
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

    Returns:
        Aggregated summary with keys ``"succeeded"``, ``"failed"``,
        ``"skipped"``.
    """
    from rdfsolve.sssom_importer import seed_sssom_mappings as _seed

    return _seed(
        sssom_sources_yaml=sssom_sources_yaml,
        output_dir=output_dir,
        names=names,
    )
