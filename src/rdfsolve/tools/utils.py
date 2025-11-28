"""Utility functions for RDFSolve."""

import requests

__all__ = [
    "IGNORE_graph_uris",
    "get_graph_uris",
]


def get_graph_uris(endpoint_url):
    """
    Discover named graphs in a SPARQL endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL

    Returns:
        List of graph IRIs
    """
    query = """
    SELECT DISTINCT ?graph
    WHERE {
      GRAPH ?graph {
        ?s ?p ?o.
      }
    }
    """
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    data = {"query": query}

    response = requests.post(endpoint_url, data=data, headers=headers)

    if response.status_code != 200:
        raise Exception(f"SPARQL query failed: {response.status_code} {response.text}")

    results = response.json()
    graph_list = [result["graph"]["value"] for result in results["results"]["bindings"]]
    return graph_list


IGNORE_graph_uris = [
    "http://www.openlinksw.com/schemas/virtrdf#",
]
