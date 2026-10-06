"""Choose and check the named graphs a source is mined from."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = ["described_graph_names", "discover_data_graphs", "missing_graphs"]


def described_graph_names(helper: SparqlHelper, *, seconds: float = 60.0) -> list[str]:
    """Return the graph names that the endpoint's service description states (sd:name)."""
    from rdfsolve.sparql_helper import SparqlHelperError

    query = (
        "SELECT DISTINCT ?g WHERE { "
        "?named <http://www.w3.org/ns/sparql-service-description#name> ?g "
        "FILTER(isIRI(?g)) } LIMIT 10000"
    )
    try:
        with helper.budget(seconds):
            rows = helper.select(query, purpose="graph-scope/service-description")
    except SparqlHelperError as error:
        logger.info("No service description of the graphs: %s", str(error)[:200])
        return []
    return sorted(
        {r["g"]["value"] for r in rows.get("results", {}).get("bindings", []) if "g" in r}
    )


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
    """List the endpoint's named graphs without the excluded prefixes.

    The listing reads every quad. When it is refused or cut (STRING: HTTP 502 after 62 s,
    rehearsal 2026-10-06), the graphs that the endpoint's service description names
    (sd:name) are taken instead, read from the predicate's index; when it names none, the
    refusal is raised and the source fails, rather than being mined as empty.
    """
    from rdfsolve.sparql_helper import SparqlHelperError
    from rdfsolve.void_retrieval import discover_graph_names

    try:
        discovered = discover_graph_names(helper, batch_size=batch_size, max_pages=max_pages)
    except SparqlHelperError as error:
        discovered = described_graph_names(helper)
        if not discovered:
            raise
        logger.warning(
            "Named graphs not listed (%s); %d graphs from the service description are used",
            str(error)[:200],
            len(discovered),
        )
    prefixes = tuple(excluded_prefixes)
    selected = [uri for uri in discovered if not (prefixes and uri.startswith(prefixes))]
    logger.info(
        "Discovered %d named graphs; %d remain after excluding %d prefixes",
        len(discovered),
        len(selected),
        len(prefixes),
    )
    return selected
