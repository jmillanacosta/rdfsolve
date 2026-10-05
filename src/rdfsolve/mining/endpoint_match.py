"""Check with cheap counts whether a public endpoint serves the data of a local record.

Where an endpoint serves the same data as the local index of a source, the costly work (mining,
exact statistics, declared identities, paths) is done on the local index only, and the endpoint
is checked against the local record (the owner decision of 2026-09-30). The local record of a
QLever index holds the exact triple count of each property (about.property_partitions). The
endpoint is asked for the triple count of each of its properties in one grouped query; when it
refuses that query, for the count of each local property alone, and then properties that only
the endpoint has cannot be seen. Engine and service data that endpoints serve beside the data
(SUGGESTED_SERVICE_NAMESPACES, for example the virtrdf graph of Virtuoso) are listed apart.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

from rdfsolve.mining.query_builders import _graph_scope
from rdfsolve.schema_models._constants import (
    SUGGESTED_SERVICE_GRAPHS,
    SUGGESTED_SERVICE_NAMESPACES,
)
from rdfsolve.schema_models.core import MinedSchema

logger = logging.getLogger(__name__)


class EndpointMatch(BaseModel):
    """The result of checking an endpoint against a local record."""

    state: Literal["equal", "differs", "partial", "not_checked"]
    endpoint: str | None = None
    graph_uris: list[str] = Field(default_factory=list)
    # True when no scope was given and the check found the graphs that hold the local data
    graphs_found_by_the_check: bool = False
    checked_at: str
    properties_checked: int = 0
    # Properties with another triple count on the endpoint: {property: {local, remote}}
    differing: dict[str, dict[str, int]] = Field(default_factory=dict)
    # Data properties that only the endpoint has, with their triple counts; None when not seen
    remote_only: dict[str, int] | None = Field(default_factory=dict)
    engine_or_service: dict[str, int] = Field(default_factory=dict)
    failed_properties: list[str] = Field(default_factory=list)
    query_count: int = 0
    reason: str | None = None


def _property_list_query(dataset: str) -> str:
    """Return the query of the triple count of each property of the endpoint."""
    return f"SELECT ?p (COUNT(*) AS ?n) {dataset} WHERE {{ ?s ?p ?o }} GROUP BY ?p"


def _count_query(prop: str, dataset: str) -> str:
    """Return the query of the triple count of one property."""
    return f"SELECT (COUNT(*) AS ?n) {dataset} WHERE {{ ?s <{prop}> ?o }}"


def _service(prop: str) -> bool:
    """Tell whether a property belongs to engine or service data."""
    return prop.startswith(SUGGESTED_SERVICE_NAMESPACES)


def _graph_sizes_query() -> str:
    """Return the query of the triple count of each named graph of the endpoint."""
    return "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g"


def _candidate_scopes(helper: Any, total: int) -> list[list[str]]:
    """Return graph scopes of the endpoint that hold as many triples as the local record.

    First each graph with the local triple count, then all graphs but the engine and service
    graphs when they hold that count together.
    """
    try:
        answer = helper.select(_graph_sizes_query(), purpose="endpoint-match/graphs")
    except Exception as error:
        logger.warning("The endpoint refused the list of its graphs: %s", error)
        return []
    sizes = {
        row["g"]["value"]: int(row["n"]["value"])
        for row in answer.get("results", {}).get("bindings", [])
    }
    scopes = [[graph] for graph, n in sorted(sizes.items()) if n == total]
    data = sorted(g for g in sizes if not g.startswith(SUGGESTED_SERVICE_GRAPHS))
    if len(data) > 1 and sum(sizes[g] for g in data) == total:
        scopes.append(data)
    return scopes


def check_endpoint_matches(
    local: MinedSchema,
    helper: Any,
    *,
    graph_uris: list[str] | None = None,
    budget_s: float = 900.0,
    clock: Callable[[], float] = time.monotonic,
) -> EndpointMatch:
    """Compare the exact triple count of each property of *local* with the endpoint of *helper*.

    Without *graph_uris*, an endpoint that differs is checked again in the graphs that hold as
    many triples as the local record (an endpoint serves engine and service graphs beside the
    data, with general properties such as rdf:type); when they are equal, the result names them.
    """
    result = _compare(local, helper, graph_uris=graph_uris, budget_s=budget_s, clock=clock)
    if graph_uris or result.state != "differs":
        return result
    total = sum(
        int(values["triples"])
        for values in (local.about.property_partitions or {}).values()
        if "triples" in values
    )
    for scope in _candidate_scopes(helper, total):
        scoped = _compare(local, helper, graph_uris=scope, budget_s=budget_s, clock=clock)
        scoped.query_count += result.query_count + 1
        if scoped.state == "equal":
            scoped.graphs_found_by_the_check = True
            scoped.reason = (
                "without a graph scope the endpoint differs (other graphs of the endpoint); "
                "equal in the graphs named here"
            )
            return scoped
    return result


def _compare(
    local: MinedSchema,
    helper: Any,
    *,
    graph_uris: list[str] | None = None,
    budget_s: float = 900.0,
    clock: Callable[[], float] = time.monotonic,
) -> EndpointMatch:
    """Compare the exact triple count of each property of *local* with the endpoint of *helper*.

    The state is equal when every count is the same and the endpoint has no other data
    property; differs when a count or a property differs; partial when the property list or
    some counts could not be read (or the budget ended) and nothing differed; not_checked when
    the local record has no exact counts.
    """
    counts = {
        prop: int(values["triples"])
        for prop, values in (local.about.property_partitions or {}).items()
        if "triples" in values
    }
    result = EndpointMatch(
        state="not_checked",
        endpoint=getattr(helper, "endpoint_url", None),
        graph_uris=list(graph_uris or []),
        checked_at=datetime.now(UTC).isoformat(),
    )
    if not counts:
        result.reason = "the local record has no exact triple counts of its properties"
        return result
    dataset, _, _ = _graph_scope(graph_uris)
    deadline = clock() + budget_s
    remote: dict[str, int] = {}

    def number(query: str, purpose: str) -> list[dict[str, Any]]:
        """Return the rows of one query."""
        result.query_count += 1
        answer = helper.select(query, purpose=purpose)
        rows: list[dict[str, Any]] = answer.get("results", {}).get("bindings", [])
        return rows

    reasons: list[str] = []
    try:
        for row in number(_property_list_query(dataset), "endpoint-match/properties"):
            remote[row["p"]["value"]] = int(row["n"]["value"])
    except Exception as error:
        logger.warning("The endpoint refused the list of its properties: %s", error)
        reasons.append("the endpoint refused the list of properties; each property was counted")
        result.remote_only = None
        for prop in sorted(counts):
            if clock() >= deadline:
                result.failed_properties.append(prop)
                continue
            try:
                rows = number(_count_query(prop, dataset), "endpoint-match/property")
                remote[prop] = int(rows[0]["n"]["value"]) if rows else 0
            except Exception as count_error:
                logger.warning("Count of %s failed: %s", prop, count_error)
                result.failed_properties.append(prop)
        if result.failed_properties:
            reasons.append(f"{len(result.failed_properties)} property counts were not read")
    checked = [p for p in counts if p in remote or result.remote_only is not None]
    result.properties_checked = len(checked)
    for prop in checked:
        if remote.get(prop, 0) != counts[prop]:
            result.differing[prop] = {"local": counts[prop], "remote": remote.get(prop, 0)}
    for prop, n in sorted(remote.items()):
        if prop in counts:
            continue
        if _service(prop):
            result.engine_or_service[prop] = n
        elif result.remote_only is not None:
            result.remote_only[prop] = n
    if result.differing or result.remote_only:
        result.state = "differs"
    elif reasons:
        result.state = "partial"
    else:
        result.state = "equal"
    result.reason = "; ".join(reasons) or None
    return result
