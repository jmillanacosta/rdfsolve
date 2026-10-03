"""Choose and check the named graphs a source is mined from."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = ["discover_data_graphs", "missing_graphs"]


def missing_graphs(helper: SparqlHelper, graph_uris: Sequence[str]) -> list[str]:
    """Return the requested graphs that hold no triple.

    One ASK per graph: a batched FILTER EXISTS costs a scan per graph on large
    Virtuoso datasets and times out, while ASK stops at the first match.
    """
    from rdflib import URIRef

    absent: list[str] = []
    for uri in graph_uris:
        query = f"ASK {{ GRAPH {URIRef(uri).n3()} {{ ?s ?p ?o }} }}"
        response = helper.select(query, purpose="graph-scope")
        present = response.get("boolean")
        if not isinstance(present, bool):
            raise ValueError(f"Graph scope check returned no answer for {uri}")
        if not present:
            absent.append(uri)
    return absent


def discover_data_graphs(
    helper: SparqlHelper,
    *,
    excluded_prefixes: Sequence[str] = (),
    batch_size: int = 100,
    max_pages: int = 100,
) -> list[str]:
    """List the endpoint's named graphs without the excluded prefixes."""
    from rdfsolve.void_retrieval import discover_graph_names

    discovered = discover_graph_names(helper, batch_size=batch_size, max_pages=max_pages)
    prefixes = tuple(excluded_prefixes)
    selected = [uri for uri in discovered if not (prefixes and uri.startswith(prefixes))]
    logger.info(
        "Discovered %d named graphs; %d remain after excluding %d prefixes",
        len(discovered),
        len(selected),
        len(prefixes),
    )
    return selected
