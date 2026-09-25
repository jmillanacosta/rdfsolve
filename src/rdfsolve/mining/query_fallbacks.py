"""Bounded query decomposition with explicit incomplete outcomes."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from rdfsolve._outcomes import Bindings, FailureCategory, QueryFailure, QueryOutcome
from rdfsolve.mining.query_builders import (
    _DECOMP_CHUNK,
    _build_batched_typed_object_query,
    _build_properties_for_class_query,
    _build_typed_object_for_class_property_query,
)
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointTimeoutError,
    PaginationTruncatedError,
    SparqlHelperError,
)

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)
CollectBindings = Callable[[str, str, int | None], Bindings]

__all__ = [
    "collect_outcome",
    "enumerate_oc_for_class_property",
    "enumerate_properties_for_class",
    "query_with_bisect",
    "select_outcome",
    "typed_object_by_property",
]


def _failure(
    error: SparqlHelperError,
    purpose: str,
    classes: list[str],
    graph_uris: list[str] | None,
) -> QueryOutcome:
    category: FailureCategory
    if isinstance(error, PaginationTruncatedError):
        category = "truncated"
    elif isinstance(error, EndpointTimeoutError):
        category = "timeout"
    elif isinstance(error, EndpointError):
        category = "endpoint"
    else:
        category = "query"
    rows = error.partial_rows if isinstance(error, PaginationTruncatedError) else []
    return QueryOutcome(
        rows,
        "partial" if rows else "failed",
        [QueryFailure(category, str(error), purpose, list(classes), graph_uris)],
    )


def _deduplicate(rows: Bindings) -> Bindings:
    seen: set[str] = set()
    result: Bindings = []
    for row in rows:
        # Include datatype, language, and node kind in the identity.
        key = json.dumps(row, sort_keys=True)
        if key not in seen:
            seen.add(key)
            result.append(row)
    return result


def select_outcome(
    query: str,
    purpose: str,
    helper: SparqlHelper,
    classes: list[str] | None = None,
    graph_uris: list[str] | None = None,
) -> QueryOutcome:
    """Run one SELECT. Do not turn malformed responses into empty results."""
    scope = classes or []
    try:
        raw = helper.select(query, purpose=purpose)
    except SparqlHelperError as error:
        return _failure(error, purpose, scope, graph_uris)
    results = raw.get("results")
    rows = results.get("bindings") if isinstance(results, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return QueryOutcome(
            state="failed",
            failures=[
                QueryFailure(
                    "invalid_response",
                    "Expected SELECT result bindings",
                    purpose,
                    scope,
                    graph_uris,
                )
            ],
        )
    return QueryOutcome(rows)


def collect_outcome(
    query: str,
    purpose: str,
    collect_bindings: CollectBindings,
    chunk_size: int,
    classes: list[str] | None = None,
    graph_uris: list[str] | None = None,
) -> QueryOutcome:
    """Collect pages and retain rows when a later page fails."""
    try:
        return QueryOutcome(_deduplicate(collect_bindings(query, purpose, chunk_size)))
    except SparqlHelperError as error:
        return _failure(error, purpose, classes or [], graph_uris)


def query_with_bisect(
    classes: list[str],
    graph_uris: list[str] | None,
    build_fn: Callable[..., str],
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: CollectBindings,
    chunk_size: int,
    unsafe_paging: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> QueryOutcome:
    """Retry timed-out class queries with smaller groups, then pages."""
    if not classes:
        return QueryOutcome()
    from rdfsolve.mining.property_queries import decomposes, query_by_property

    if decomposes(helper, classes, build_fn):
        return query_by_property(
            classes[0],
            graph_uris,
            build_fn,
            purpose,
            helper,
            collect_bindings,
            chunk_size,
            type_context_graph_uris,
        )
    scope = {"type_context_graph_uris": type_context_graph_uris} if type_context_graph_uris else {}
    outcome = select_outcome(
        build_fn(classes, graph_uris, **scope), purpose, helper, classes, graph_uris
    )
    if not outcome.failures or outcome.failures[0].category != "timeout":
        return outcome

    logger.warning("%s timed out for %d classes; retrying smaller queries", purpose, len(classes))
    if len(classes) > 1:
        mid = len(classes) // 2
        left = query_with_bisect(
            classes[:mid],
            graph_uris,
            build_fn,
            purpose,
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
            type_context_graph_uris,
        )
        right = query_with_bisect(
            classes[mid:],
            graph_uris,
            build_fn,
            purpose,
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
            type_context_graph_uris,
        )
        return left.merge(right)

    decomposed = None
    if build_fn is _build_batched_typed_object_query:
        decomposed = typed_object_by_property(
            classes[0],
            graph_uris,
            purpose,
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
            type_context_graph_uris,
        )
        if decomposed.state == "complete":
            return decomposed

    query = build_fn(classes, graph_uris, paginated=True, drop_distinct=unsafe_paging, **scope)
    paged = collect_outcome(query, purpose, collect_bindings, chunk_size, classes, graph_uris)
    if paged.state == "complete":
        return paged
    combined = paged if decomposed is None else paged.merge(decomposed)
    combined.rows = _deduplicate(combined.rows)
    if getattr(build_fn, "__name__", "") in WINDOWED and not combined.rows:
        return query_windows(classes[0], graph_uris, build_fn, purpose, helper, combined, scope)
    return combined


# Discovery builders that accept a member window.
WINDOWED = frozenset(
    f"_build_batched_{kind}_query"
    for kind in ("typed_object", "literal", "untyped_uri", "blank_node")
)
WINDOW_SIZE = 1000
WINDOW_OFFSETS = (0, 1_000, 10_000, 100_000, 1_000_000)


def query_windows(
    class_uri: str,
    graph_uris: list[str] | None,
    build_fn: Callable[..., str],
    purpose: str,
    helper: SparqlHelper,
    failed: QueryOutcome,
    scope: dict[str, Any],
) -> QueryOutcome:
    """Observe rows in bounded member windows after every whole-class strategy failed.

    Windows are cheap slices near the start of the member list. They give positive
    evidence only: the outcome is partial and says how the rows were obtained.
    """
    from rdfsolve.mining.query_builders import Window

    rows: Bindings = []
    used: list[int] = []
    for offset in WINDOW_OFFSETS:
        window = Window(WINDOW_SIZE, offset, run_first=helper.sparql_engine == "blazegraph")
        query = build_fn([class_uri], graph_uris, window=window, **scope)
        found = select_outcome(query, f"{purpose}/window", helper, [class_uri], graph_uris)
        if found.state != "complete":
            break
        rows.extend(found.rows)
        used.append(offset)
    if not used:
        return failed
    note = QueryFailure(
        "sampled",
        f"The whole-class query exceeded its budget; rows come from {len(used)} windows of "
        f"{WINDOW_SIZE} members at offsets {used}. Other members may add rows.",
        purpose,
        [class_uri],
        graph_uris,
    )
    return QueryOutcome(_deduplicate(rows), "partial", [*failed.failures, note])


def _select_or_page(
    query: str,
    paged_query: str,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: CollectBindings,
    chunk_size: int,
    class_uri: str,
    graph_uris: list[str] | None,
) -> QueryOutcome:
    result = select_outcome(query, purpose, helper, [class_uri], graph_uris)
    if result.failures and result.failures[0].category == "timeout":
        return collect_outcome(
            paged_query,
            purpose,
            collect_bindings,
            chunk_size,
            [class_uri],
            graph_uris,
        )
    return result


def enumerate_properties_for_class(
    class_uri: str,
    graph_uris: list[str] | None,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: CollectBindings,
    unsafe_paging: bool = False,
    *,
    chunk_size: int = _DECOMP_CHUNK,
    type_context_graph_uris: list[str] | None = None,
) -> QueryOutcome:
    """Return property bindings and the completion state of their enumeration."""
    return _select_or_page(
        _build_properties_for_class_query(
            class_uri, graph_uris, type_context_graph_uris=type_context_graph_uris
        ),
        _build_properties_for_class_query(
            class_uri,
            graph_uris,
            paginated=True,
            drop_distinct=unsafe_paging,
            type_context_graph_uris=type_context_graph_uris,
        ),
        f"{purpose}/properties",
        helper,
        collect_bindings,
        chunk_size,
        class_uri,
        graph_uris,
    )


def enumerate_oc_for_class_property(
    class_uri: str,
    prop_uri: str,
    graph_uris: list[str] | None,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: CollectBindings,
    unsafe_paging: bool = False,
    type_context_graph_uris: list[str] | None = None,
    *,
    chunk_size: int = _DECOMP_CHUNK,
) -> QueryOutcome:
    """Return object-class bindings without hiding a failed property query."""
    return _select_or_page(
        _build_typed_object_for_class_property_query(
            class_uri, prop_uri, graph_uris, type_context_graph_uris=type_context_graph_uris
        ),
        _build_typed_object_for_class_property_query(
            class_uri,
            prop_uri,
            graph_uris,
            paginated=True,
            drop_distinct=unsafe_paging,
            type_context_graph_uris=type_context_graph_uris,
        ),
        f"{purpose}/property/{prop_uri}",
        helper,
        collect_bindings,
        chunk_size,
        class_uri,
        graph_uris,
    )


def typed_object_by_property(
    class_uri: str,
    graph_uris: list[str] | None,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: CollectBindings,
    chunk_size: int,
    unsafe_paging: bool = False,
    type_context_graph_uris: list[str] | None = None,
) -> QueryOutcome:
    """Combine independent property queries and retain unresolved failures."""
    props = enumerate_properties_for_class(
        class_uri,
        graph_uris,
        purpose,
        helper,
        collect_bindings,
        unsafe_paging,
        chunk_size=chunk_size,
        type_context_graph_uris=type_context_graph_uris,
    )
    combined = QueryOutcome(state=props.state, failures=props.failures) if props.failures else None
    for prop_uri in dict.fromkeys(
        row["p"]["value"] for row in props.rows if row.get("p", {}).get("value")
    ):
        result = enumerate_oc_for_class_property(
            class_uri,
            prop_uri,
            graph_uris,
            purpose,
            helper,
            collect_bindings,
            unsafe_paging,
            type_context_graph_uris,
            chunk_size=chunk_size,
        )
        result.rows = [
            {
                "class": {"type": "uri", "value": class_uri},
                "p": {"type": "uri", "value": prop_uri},
                "oc": row["oc"],
            }
            for row in result.rows
            if row.get("oc", {}).get("value")
        ]
        combined = result if combined is None else combined.merge(result)
    return combined if combined is not None else QueryOutcome()
