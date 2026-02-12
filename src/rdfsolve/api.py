"""Main RDFSolve functionalities for VoID extraction and conversion."""

import csv
import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from rdflib import Graph

from .parser import VoidParser

logger = logging.getLogger(__name__)

__all__ = [
    "compose_query_from_paths",
    "count_instances",
    "count_instances_per_class",
    "discover_void_graphs",
    "execute_sparql",
    "extract_partitions_from_void",
    "generate_void_from_endpoint",
    "graph_to_jsonld",
    "graph_to_linkml",
    "graph_to_schema",
    "graph_to_shacl",
    "load_parser_from_file",
    "load_parser_from_graph",
    "mine_all_sources",
    "mine_schema",
    "resolve_iris",
    "retrieve_void_from_graphs",
    "to_jsonld_from_file",
    "to_linkml_from_file",
    "to_rdfconfig_from_file",
    "to_shacl_from_file",
]


def load_parser_from_file(
    void_file_path: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
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
    graph_uris: Optional[Union[str, List[str]]] = None,
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


def to_linkml_from_file(
    void_file_path: str,
    filter_void_nodes: bool = True,
    schema_name: Optional[str] = None,
    schema_description: Optional[str] = None,
    schema_base_uri: Optional[str] = None,
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
    schema_name: Optional[str] = None,
    schema_description: Optional[str] = None,
    schema_base_uri: Optional[str] = None,
    closed: bool = True,
    suffix: Optional[str] = None,
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
    endpoint_url: Optional[str] = None,
    endpoint_name: Optional[str] = None,
    graph_uri: Optional[str] = None,
) -> Dict[str, str]:
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


def to_jsonld_from_file(
    void_file_path: str,
    filter_void_admin_nodes: bool = True,
    endpoint_url: Optional[str] = None,
    dataset_name: Optional[str] = None,
    graph_uris: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
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
    graph_uris_list = (
        [graph_uris] if isinstance(graph_uris, str) else graph_uris
    )
    return parser.to_jsonld(
        filter_void_admin_nodes=filter_void_admin_nodes,
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_jsonld(
    graph: Graph,
    graph_uris: Optional[Union[str, List[str]]] = None,
    filter_void_admin_nodes: bool = True,
    endpoint_url: Optional[str] = None,
    dataset_name: Optional[str] = None,
) -> Dict[str, Any]:
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
    graph_uris_list = (
        [graph_uris] if isinstance(graph_uris, str) else graph_uris
    )
    return parser.to_jsonld(
        filter_void_admin_nodes=filter_void_admin_nodes,
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_linkml(
    graph: Graph,
    graph_uris: Optional[Union[str, List[str]]] = None,
    filter_void_nodes: bool = True,
    schema_name: Optional[str] = None,
    schema_description: Optional[str] = None,
    schema_base_uri: Optional[str] = None,
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
    graph_uris: Optional[Union[str, List[str]]] = None,
    filter_void_nodes: bool = True,
    schema_name: Optional[str] = None,
    schema_description: Optional[str] = None,
    schema_base_uri: Optional[str] = None,
    closed: bool = True,
    suffix: Optional[str] = None,
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


def discover_void_graphs(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    exclude_graphs: bool = False,
) -> Dict[str, Any]:
    """Find VoID graphs in a SPARQL endpoint.

    Discovery always includes well-known URIs and VoID graphs by default,
    as these commonly contain metadata descriptions. Only Virtuoso system
    graphs are excluded by default.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URIs to search
        exclude_graphs: Exclude Virtuoso system graphs (default: False for discovery)

    Returns:
        Discovery metadata per graph URI
    """
    parser = VoidParser(graph_uris=graph_uris, exclude_graphs=exclude_graphs)
    return parser.discover_void_graphs(endpoint_url)


def count_instances(
    endpoint_url: str,
    sample_limit: Optional[int] = None,
    sample_offset: Optional[int] = None,
    chunk_size: Optional[int] = None,
    offset_limit_steps: Optional[int] = None,
    delay_between_chunks: float = 20.0,
    streaming: bool = False,
) -> Union[Dict[str, int], Any]:
    """Count instances per class in a SPARQL endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL
        sample_limit: Max results to return
        sample_offset: Starting offset
        chunk_size: Chunk size for pagination
        offset_limit_steps: Combined LIMIT/OFFSET step
        delay_between_chunks: Seconds between chunks
        streaming: Return generator if True

    Returns:
        Dict mapping class URI to count, or generator
    """
    parser = VoidParser()
    return parser.count_instances_per_class(
        endpoint_url,
        sample_limit=sample_limit,
        sample_offset=sample_offset,
        chunk_size=chunk_size,
        offset_limit_steps=offset_limit_steps,
        delay_between_chunks=delay_between_chunks,
        streaming=streaming,
    )


def extract_partitions_from_void(
    endpoint_url: str, void_graph_uris: List[str]
) -> List[Dict[str, str]]:
    """Extract partition data from discovered VoID graphs.

    Args:
        endpoint_url: SPARQL endpoint URL
        void_graph_uris: List of VoID graph URIs with partitions

    Returns:
        List of partition records (class-property-object)
    """
    parser = VoidParser()
    return parser.retrieve_partitions_from_void(endpoint_url, void_graph_uris)


def retrieve_void_from_graphs(
    endpoint_url: str,
    void_graph_uris: List[str],
    graph_uris: Optional[Union[str, List[str]]] = None,
    partitions: Optional[List[Dict[str, str]]] = None,
) -> Graph:
    """Retrieve VoID descriptions from specific graphs at endpoint.

    If partition data is provided (from discover_void_graphs), builds the
    graph directly from that data. Otherwise, runs a new discovery query.

    Args:
        endpoint_url: SPARQL endpoint URL
        void_graph_uris: List of graph URIs containing VoID
        graph_uris: Graph URIs to filter queries
        partitions: Optional partition data from discover_void_graphs result

    Returns:
        RDF Graph with VoID descriptions built from partition data
    """
    parser = VoidParser(graph_uris=graph_uris)

    # If partition data provided, build graph directly (no CONSTRUCT needed)
    if partitions:
        base_uri = void_graph_uris[0] if void_graph_uris else None
        return parser.build_void_graph_from_partitions(partitions, base_uri=base_uri)

    # Otherwise, run discovery to get partitions and build graph
    discovery_result = parser.discover_void_graphs(endpoint_url)
    partitions = discovery_result.get("partitions", [])

    if partitions:
        base_uri = void_graph_uris[0] if void_graph_uris else None
        return parser.build_void_graph_from_partitions(partitions, base_uri=base_uri)

    # Fallback: return empty graph if no partitions found
    from rdflib import Graph

    return Graph()


def generate_void_from_endpoint(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    output_file: Optional[str] = None,
    counts: bool = True,
    offset_limit_steps: Optional[int] = None,
    exclude_graphs: bool = True,
    dataset_uri: Optional[str] = None,
    void_base_uri: Optional[str] = None,
) -> Graph:
    """Generate VoID description from a SPARQL endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URI(s) to analyze
        output_file: Path to save Turtle output
        counts: Include instance counts
        offset_limit_steps: Chunk size for pagination
        exclude_graphs: Exclude system graphs
        dataset_uri: Custom URI for the VoID dataset (default: uses first graph_uri or endpoint URL)
        void_base_uri: Custom base URI for VoID partition IRIs

    Returns:
        RDF graph with VoID description
    """
    # Determine dataset_uri if not provided
    if dataset_uri is None:
        if graph_uris:
            dataset_uri = graph_uris[0] if isinstance(graph_uris, list) else graph_uris
        else:
            # Use endpoint URL as fallback
            dataset_uri = endpoint_url.rstrip("/")

    # Note: VoidParser.generate_void_from_sparql uses graph_uris for building partition IRIs
    # The dataset_uri is embedded in the VoID graph structure
    return VoidParser.generate_void_from_sparql(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        output_file=output_file,
        counts=counts,
        offset_limit_steps=offset_limit_steps,
        exclude_graphs=exclude_graphs,
        void_base_uri=void_base_uri,
    )


def graph_to_schema(
    void_graph: Graph,
    graph_uris: Optional[Union[str, List[str]]] = None,
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


def count_instances_per_class(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    sample_limit: Optional[int] = None,
    exclude_graphs: bool = True,
) -> Dict[str, int]:
    """Count instances per class in a SPARQL endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URI(s) to query
        sample_limit: Max results to sample
        exclude_graphs: Exclude service/system graphs from counting

    Returns:
        Class URI to instance count mapping
    """
    parser = VoidParser(graph_uris=graph_uris, exclude_graphs=exclude_graphs)
    result = parser.count_instances_per_class(endpoint_url, sample_limit=sample_limit)
    if isinstance(result, dict):
        return result
    return dict(result)  # Convert generator to dict if needed


def mine_schema(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    dataset_name: Optional[str] = None,
    chunk_size: int = 10_000,
    class_chunk_size: Optional[int] = None,
    class_batch_size: int = 15,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
    two_phase: bool = False,
    report_path: Optional[str] = None,
) -> Dict[str, Any]:
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
        two_phase: Use two-phase mining for large endpoints
        report_path: If given, write analytics JSON to this path

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
    )
    return schema.to_jsonld()


def mine_all_sources(
    sources_csv: str,
    output_dir: str = ".",
    fmt: str = "all",
    chunk_size: int = 10_000,
    class_chunk_size: Optional[int] = None,
    class_batch_size: int = 15,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
    reports: bool = True,
    on_progress: Optional[
        Callable[[str, int, int, Optional[str]], None]
    ] = None,
) -> Dict[str, Any]:
    """Mine schemas for all sources listed in a CSV file.

    Reads a sources CSV (columns: ``dataset_name``, ``endpoint_url``,
    ``graph_uri``, ``use_graph``) and runs :func:`mine_schema` for each
    row whose ``endpoint_url`` is non-empty.  Results are written to
    *output_dir* as ``{dataset_name}_schema.jsonld`` and / or
    ``{dataset_name}_void.ttl``.

    Args:
        sources_csv: Path to the CSV file with data sources.
        output_dir: Directory where outputs are written.
        fmt: Export format – ``"jsonld"``, ``"void"``, or
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
    from .miner import mine_schema as _mine

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Read the sources CSV
    with open(sources_csv, newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    succeeded: List[str] = []
    failed: List[Dict[str, str]] = []
    skipped: List[str] = []

    total = len(rows)
    for idx, row in enumerate(rows, 1):
        name = row.get("dataset_name", "").strip()
        endpoint = row.get("endpoint_url", "").strip()
        graph_uri = row.get("graph_uri", "").strip()
        use_graph = (
            row.get("use_graph", "").strip().lower()
            in ("true", "1", "yes")
        )
        row_two_phase = (
            row.get("two_phase", "").strip().lower()
            in ("true", "1", "yes")
        )

        if not endpoint:
            logger.info(
                f"[{idx}/{total}] Skipping {name!r}: "
                f"no endpoint_url"
            )
            skipped.append(name)
            if on_progress:
                on_progress(name, idx, total, "skipped")
            continue

        graph_uris_arg: Optional[List[str]] = None
        if use_graph and graph_uri:
            graph_uris_arg = [graph_uri]

        logger.info(
            f"[{idx}/{total}] Mining {name!r} "
            f"({endpoint})"
        )

        # class_chunk_size only applies to two-phase rows
        effective_ccs: Optional[int] = None
        if row_two_phase:
            effective_ccs = class_chunk_size
        elif class_chunk_size is not None:
            logger.info(
                "[%d/%d] --class-chunk-size ignored for "
                "%r (not two-phase)",
                idx, total, name,
            )

        try:
            rpt_path = (
                out / f"{name}_report.json"
                if reports else None
            )
            schema = _mine(
                endpoint_url=endpoint,
                graph_uris=graph_uris_arg,
                dataset_name=name,
                chunk_size=chunk_size,
                class_chunk_size=effective_ccs,
                class_batch_size=class_batch_size,
                delay=delay,
                timeout=timeout,
                counts=counts,
                two_phase=row_two_phase,
                report_path=rpt_path,
            )

            if fmt in ("jsonld", "all"):
                jsonld_path = out / f"{name}_schema.jsonld"
                with open(jsonld_path, "w") as f:
                    json.dump(
                        schema.to_jsonld(), f, indent=2,
                    )
                logger.info(f"  → {jsonld_path}")

            if fmt in ("void", "all"):
                void_path = out / f"{name}_void.ttl"
                void_g = schema.to_void_graph()
                void_g.serialize(
                    destination=str(void_path),
                    format="turtle",
                )
                logger.info(
                    f"  → {void_path} "
                    f"({len(void_g)} triples)"
                )

            succeeded.append(name)
            if on_progress:
                on_progress(name, idx, total, None)

        except Exception as exc:
            msg = str(exc)
            logger.warning(
                f"  ✗ {name}: {msg}"
            )
            failed.append({"dataset": name, "error": msg})
            if on_progress:
                on_progress(name, idx, total, msg)

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
    variable_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Execute a SPARQL query against a remote endpoint.

    This is a pure-Python function — no Flask required.  It delegates to
    :func:`rdfsolve.query.execute_sparql` which uses the robust
    :class:`~rdfsolve.sparql_helper.SparqlHelper` under the hood.

    Args:
        query:        Full SPARQL query string.
        endpoint:     SPARQL endpoint URL.
        method:       HTTP method (``"GET"`` or ``"POST"``).
        timeout:      Timeout in seconds.
        variable_map: Optional mapping of SPARQL ?variable → schema URI.

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
    iris: List[str],
    endpoints: List[Dict[str, Any]],
    timeout: int = 15,
) -> Dict[str, Any]:
    """Resolve IRIs against SPARQL endpoints to discover their rdf:type.

    This is a pure-Python function — no Flask required.  It delegates to
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
    paths: List[Dict[str, Any]],
    prefixes: Optional[Dict[str, str]] = None,
    include_types: bool = False,
    include_labels: bool = True,
    limit: int = 100,
    value_bindings: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    """Generate a SPARQL query from diagram paths.

    This is a pure-Python function — no Flask required.  It delegates to
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
        (var → schema URI), and ``jsonld``
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
