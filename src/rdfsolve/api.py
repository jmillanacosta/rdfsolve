"""Main RDFSolve functionalities for VoID extraction and conversion."""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
from rdflib import Graph

from .parser import VoidParser

__all__ = [
    "count_instances",
    "count_instances_per_class",
    "discover_void_graphs",
    "extract_partitions_from_void",
    "generate_void_alternative_method",
    "generate_void_from_endpoint",
    "graph_to_jsonld",
    "graph_to_linkml",
    "graph_to_schema",
    "load_parser_from_file",
    "load_parser_from_graph",
    "retrieve_void_from_graphs",
    "to_jsonld_from_file",
    "to_linkml_from_file",
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


def to_jsonld_from_file(
    void_file_path: str, filter_void_admin_nodes: bool = True
) -> Dict[str, Any]:
    """Convert a VoID file to JSON-LD format.

    Args:
        void_file_path: Path to VoID file
        filter_void_admin_nodes: Remove VoID administrative nodes

    Returns:
        JSON-LD with @context and @graph
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_jsonld(filter_void_admin_nodes=filter_void_admin_nodes)


def graph_to_jsonld(
    graph: Graph,
    graph_uris: Optional[Union[str, List[str]]] = None,
    filter_void_admin_nodes: bool = True,
) -> Dict[str, Any]:
    """Convert a VoID graph to JSON-LD format.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        filter_void_admin_nodes: Remove VoID administrative nodes

    Returns:
        JSON-LD with @context and @graph
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    return parser.to_jsonld(filter_void_admin_nodes=filter_void_admin_nodes)


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


def discover_void_graphs(
    endpoint_url: str, graph_uris: Optional[Union[str, List[str]]] = None
) -> Dict[str, Any]:
    """Find VoID graphs in a SPARQL endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URIs to search

    Returns:
        Discovery metadata per graph URI
    """
    parser = VoidParser(graph_uris=graph_uris)
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
) -> Graph:
    """Generate VoID description from a SPARQL endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URI(s) to analyze
        output_file: Path to save Turtle output
        counts: Include instance counts
        offset_limit_steps: Chunk size for pagination
        exclude_graphs: Exclude system graphs

    Returns:
        RDF graph with VoID description
    """
    return VoidParser.generate_void_from_sparql(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        output_file=output_file,
        counts=counts,
        offset_limit_steps=offset_limit_steps,
        exclude_graphs=exclude_graphs,
    )


def generate_void_alternative_method(
    endpoint_url: str,
    dataset_prefix: str,
    graph_uri: Optional[str] = None,
    output_file: Optional[str] = None,
) -> Graph:
    """Generate VoID using alternative single-query method.

    Uses a unified non-paginated CONSTRUCT query that extracts all VoID
    partition data in one request. Read-only approach adapted from
    void-generator project.

    Source: https://github.com/sib-swiss/void-generator/issues/30

    Args:
        endpoint_url: SPARQL endpoint URL
        dataset_prefix: Prefix for partition node IRIs
        graph_uri: Optional graph URI to restrict queries
        output_file: Path to save Turtle output

    Returns:
        RDF graph with VoID description
    """
    return VoidParser.generate_void_alternative_method(
        endpoint_url=endpoint_url,
        dataset_prefix=dataset_prefix,
        graph_uri=graph_uri,
        output_file=output_file,
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
        filter_void_admin_nodes: Filter VoID admin nodes

    Returns:
        DataFrame with schema patterns (subject/property/object URIs)
    """
    parser = VoidParser(void_source=void_graph, graph_uris=graph_uris)
    return parser.to_schema(filter_void_admin_nodes=filter_void_admin_nodes)


def count_instances_per_class(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    sample_limit: Optional[int] = None,
) -> Dict[str, int]:
    """Count instances per class in a SPARQL endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Graph URI(s) to query
        sample_limit: Max results to sample

    Returns:
        Class URI to instance count mapping
    """
    parser = VoidParser(graph_uris=graph_uris)
    result = parser.count_instances_per_class(endpoint_url, sample_limit=sample_limit)
    if isinstance(result, dict):
        return result
    return dict(result)  # Convert generator to dict if needed
