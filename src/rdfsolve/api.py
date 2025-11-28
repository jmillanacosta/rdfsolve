"""
User-facing API helpers for rdfsolve.

This module provides small, well-typed convenience wrappers around the
core `VoidParser` class.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
from rdflib import Graph

from .parser import VoidParser

__all__ = [
    "count_instances",
    "count_instances_per_class",
    "discover_void_graphs",
    "generate_void_from_endpoint",
    "graph_to_schema",
    "load_parser_from_file",
    "load_parser_from_graph",
    "to_jsonld_from_file",
    "to_linkml_from_file",
]


def load_parser_from_file(
    void_file_path: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Create a ``VoidParser`` loaded from a Turtle VoID file.

    Args:
        void_file_path: Path to a VoID Turtle file to load.
        graph_uris: Optional graph URIs to restrict queries to.
        exclude_graphs: Whether to exclude system graphs when querying.

    Returns:
        An initialized ``VoidParser`` instance with the file parsed.
    """
    return VoidParser(
        void_source=void_file_path, graph_uris=graph_uris, exclude_graphs=exclude_graphs
    )


def load_parser_from_graph(
    graph: Graph,
    graph_uris: Optional[Union[str, List[str]]] = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Create a ``VoidParser`` from an existing RDFLib ``Graph``.

    Args:
        graph: RDFLib Graph already populated with VoID data.
        graph_uris: Optional graph URIs to restrict queries to.
        exclude_graphs: Whether to exclude system graphs when querying.

    Returns:
        An initialized ``VoidParser`` instance using the provided graph.
    """
    return VoidParser(void_source=graph, graph_uris=graph_uris, exclude_graphs=exclude_graphs)


def to_linkml_from_file(
    void_file_path: str,
    filter_void_nodes: bool = True,
    schema_name: Optional[str] = None,
    schema_description: Optional[str] = None,
    schema_base_uri: Optional[str] = None,
) -> str:
    """Parse a VoID file and return a LinkML YAML schema string.

    This is a convenience wrapper used by docs to show the simplest
    invocation path.
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
    """Parse a VoID file and return a JSON-LD dictionary (context + @graph)."""
    parser = load_parser_from_file(void_file_path)
    return parser.to_jsonld(filter_void_admin_nodes=filter_void_admin_nodes)


def discover_void_graphs(
    endpoint_url: str, graph_uris: Optional[Union[str, List[str]]] = None
) -> Dict[str, Any]:
    """Discover VoID graphs available in a SPARQL endpoint.

    Returns a mapping of graph URI -> discovery metadata.
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

    This is a thin wrapper around ``VoidParser.count_instances_per_class``.
    If ``streaming=True`` a generator may be returned; otherwise a
    dictionary mapping class URI -> count is returned.
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


def generate_void_from_endpoint(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    output_file: Optional[str] = None,
    counts: bool = True,
    offset_limit_steps: Optional[int] = None,
    exclude_graphs: bool = True,
) -> Graph:
    """Generate a VoID description by querying a SPARQL endpoint.

    This executes CONSTRUCT queries to discover schema patterns and
    returns an RDF graph containing the VoID description.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Optional graph URI(s) to restrict the analysis to
        output_file: Optional file path to save the Turtle output
        counts: Whether to include COUNT aggregations (slower but more complete)
        offset_limit_steps: Optional chunk size for paginated queries
        exclude_graphs: Whether to exclude system graphs

    Returns:
        RDF Graph containing the VoID description
    """
    return VoidParser.generate_void_from_sparql(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        output_file=output_file,
        counts=counts,
        offset_limit_steps=offset_limit_steps,
        exclude_graphs=exclude_graphs,
    )


def graph_to_schema(
    void_graph: Graph,
    graph_uris: Optional[Union[str, List[str]]] = None,
    filter_void_admin_nodes: bool = True,
) -> pd.DataFrame:
    """Convert a VoID graph to a schema DataFrame.

    Args:
        void_graph: RDFLib Graph containing VoID data
        graph_uris: Optional graph URIs to restrict the schema extraction
        filter_void_admin_nodes: Whether to filter out VoID administrative nodes

    Returns:
        DataFrame with schema patterns (subject_uri, property_uri, object_uri, etc.)
    """
    parser = VoidParser(void_source=void_graph, graph_uris=graph_uris)
    return parser.to_schema(filter_void_admin_nodes=filter_void_admin_nodes)


def count_instances_per_class(
    endpoint_url: str,
    graph_uris: Optional[Union[str, List[str]]] = None,
    sample_limit: Optional[int] = None,
) -> Dict[str, int]:
    """Count instances per class in a SPARQL endpoint.

    Simplified wrapper that returns a dict mapping class URI -> count.

    Args:
        endpoint_url: SPARQL endpoint URL
        graph_uris: Optional graph URI(s) to restrict the query
        sample_limit: Optional limit for sampling

    Returns:
        Dictionary mapping class URIs to instance counts
    """
    parser = VoidParser(graph_uris=graph_uris)
    result = parser.count_instances_per_class(endpoint_url, sample_limit=sample_limit)
    if isinstance(result, dict):
        return result
    return dict(result)  # Convert generator to dict if needed
