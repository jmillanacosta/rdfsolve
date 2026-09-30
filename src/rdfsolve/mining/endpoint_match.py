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
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

from rdfsolve.mining.query_builders import _graph_scope
from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_NAMESPACES
from rdfsolve.schema_models.core import MinedSchema

logger = logging.getLogger(__name__)


class EndpointMatch(BaseModel):
    """The result of checking an endpoint against a local record."""

    state: Literal["equal", "differs", "partial", "not_checked"]
    endpoint: str | None = None
    graph_uris: list[str] = Field(default_factory=list)
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


def check_endpoint_matches(
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
        checked_at=datetime.now(timezone.utc).isoformat(),
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
