"""Choose and check the named graphs a source is mined from."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = [
    "described_graph_names",
    "discover_data_graphs",
    "missing_graphs",
    "wait_out_gateway",
]


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

    The listing reads every quad. When a gateway answers for an overloaded host (a 502, while
    the same listing answers when the host is quiet), the host is waited out with the busy-host backoff (30, 60, 120, 240 s, shared by
    every request to the host) and the listing is sent again: for a source whose data is only
    in named graphs, this listing decides everything. When it is still refused or cut, the
    graphs that the endpoint's service description names (sd:name) are taken instead, read
    from the predicate's index; when it names none, the refusal is raised and the source
    fails, rather than being mined as empty.
    """
    from rdfsolve.sparql_helper import SparqlHelperError
    from rdfsolve.void_retrieval import discover_graph_names

    try:
        discovered = wait_out_gateway(
            helper,
            lambda: discover_graph_names(helper, batch_size=batch_size, max_pages=max_pages),
            "Graph listing",
        )
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


def _gateway_overloaded(error: BaseException) -> bool:
    """Return whether *error*, or an error it was raised from, is a gateway overload."""
    seen: BaseException | None = error
    while seen is not None:
        if getattr(seen, "gateway_overload", False):
            return True
        seen = seen.__cause__
    return False


# The query with which a host shows that it answers at all, and how long it gets.
HOST_PROBE = "ASK { ?s ?p ?o }"
HOST_PROBE_SECONDS = 15.0


def host_answers(helper: SparqlHelper) -> bool:
    """Return whether the host answers a trivial query (HOST_PROBE) within HOST_PROBE_SECONDS."""
    from rdfsolve.sparql_helper import SparqlHelperError

    try:
        with helper.budget(HOST_PROBE_SECONDS):
            helper.select(HOST_PROBE, purpose="gateway/host-probe")
    except SparqlHelperError:
        return False
    return True


def wait_out_gateway[T](helper: SparqlHelper, call: Callable[[], T], what: str) -> T:
    """Run *call*, a listing that a step depends on; after a gateway overload, run it again.

    Before each repeat the host is deferred (defer_host, so that every request to it waits)
    by SparqlHelper.overload_backoff (30, 60, 120, 240 s), up to SparqlHelper.OVERLOAD_RETRIES
    times; a wait beyond the helper's rate_limit_wait is not taken. A proxy that could not
    reach the server is not an overload and is raised at once.

    The first repeat is always made. Before each later one the host is asked a trivial query
    (host_answers): a host that answers it at once is not overloaded, and the listing is cut
    by the gateway's own timer because it costs more than the timer allows, which no wait
    mends; the error is then raised.
    """
    from urllib.parse import urlsplit

    from rdfsolve._http_policy import defer_host
    from rdfsolve.sparql_helper import SparqlHelper, SparqlHelperError

    url = str(getattr(helper, "endpoint_url", "") or "")
    host = urlsplit(url).hostname or url
    longest = float(getattr(helper, "rate_limit_wait", 600.0))
    tries = 0
    while True:
        try:
            return call()
        except SparqlHelperError as error:
            tries += 1
            wait = SparqlHelper.overload_backoff(tries)
            if (
                not _gateway_overloaded(error)
                or tries > SparqlHelper.OVERLOAD_RETRIES
                or wait > longest
            ):
                raise
            if tries > 1 and host_answers(helper):
                logger.warning(
                    "%s: a gateway cut it again (%s), but %s answers a trivial query: the "
                    "listing costs more than the gateway's timer, not waited for",
                    what,
                    str(error)[:160],
                    host,
                )
                raise
            logger.warning(
                "%s: a gateway answered for %s (%s); the host is waited out for %.0f s "
                "(try %d of %d)",
                what,
                host,
                str(error)[:160],
                wait,
                tries + 1,
                SparqlHelper.OVERLOAD_RETRIES + 1,
            )
            defer_host(host, wait)
