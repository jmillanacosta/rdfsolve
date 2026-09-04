"""Query fallback strategies for handling large/expensive queries."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from rdfsolve.mining.query_builders import (
    _DECOMP_CHUNK,
    _build_batched_typed_object_query,
    _build_properties_for_class_query,
    _build_typed_object_for_class_property_query,
)
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError, PaginationTruncatedError

if TYPE_CHECKING:
    from collections.abc import Callable

    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = [
    "enumerate_oc_for_class_property",
    "enumerate_properties_for_class",
    "query_with_bisect",
    "typed_object_by_property",
]


def query_with_bisect(
    classes: list[str],
    graph_uris: list[str] | None,
    build_fn: Any,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    chunk_size: int,
    unsafe_paging: bool = False,
) -> list[dict[str, Any]]:
    """Run a batched VALUES query with automatic bisection fallback.

    Args:
        classes: Class URIs to include in VALUES block
        graph_uris: Named graphs to restrict queries to
        build_fn: One of the _build_batched_* functions
        purpose: Label for logging and tracking
        helper: SPARQL helper for query execution
        collect_bindings: Function to collect paginated results
        chunk_size: Page size for pagination
        unsafe_paging: Drop DISTINCT for faster paging

    Returns:
        All SPARQL result bindings collected across all sub-queries
    """
    # Attempt 1: single-shot SELECT
    label = f"{classes[0]}…" if len(classes) > 1 else classes[0]
    q = build_fn(classes, graph_uris)
    try:
        result = helper.select(q, purpose=purpose)
        bindings: list[dict[str, Any]] = result.get("results", {}).get("bindings", [])
        return bindings
    except EndpointTimeoutError as e:
        # 502 means server overload - wait longer before retrying
        if getattr(e, "status_code", None) == 502:
            wait_time = 120.0  # 2 minutes for severe rate limiting
            logger.warning(
                "  %s got 502 for [%s] (%d classes) - server overloaded, waiting %.0fs before retry",
                purpose,
                label,
                len(classes),
                wait_time,
            )
            time.sleep(wait_time)
        else:
            logger.warning(
                "  %s single-shot timed out for [%s] (%d classes) - %s",
                purpose,
                label,
                len(classes),
                "trying paginated" if len(classes) == 1 else "bisecting",
            )
    except EndpointError as e:
        # Hard failure - no point retrying
        logger.warning(
            "  %s endpoint error for [%s] - skipping all fallbacks: %s",
            purpose,
            label,
            e,
        )
        return []
    except Exception:
        logger.warning(
            "  %s single-shot failed for [%s] (%d classes) - %s\n    query: %s",
            purpose,
            label,
            len(classes),
            "trying paginated" if len(classes) == 1 else "bisecting",
            q,
        )

    # Attempt 2: bisect (multi-class) or paginate (single-class)
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
        )
        return left + right

    # Attempt 3: paginated SELECT (single-class only)
    qt = build_fn(
        classes,
        graph_uris,
        paginated=True,
        drop_distinct=unsafe_paging,
    )
    try:
        raw = collect_bindings(qt, purpose, chunk_size)
        # Deduplicate in Python
        seen: set[tuple[tuple[str, str], ...]] = set()
        deduped: list[dict[str, Any]] = []
        for b in raw:
            key = tuple(sorted((k, v.get("value", "")) for k, v in b.items()))
            if key not in seen:
                seen.add(key)
                deduped.append(b)
        return deduped
    except PaginationTruncatedError as e2:
        logger.warning(
            "  %s pagination truncated at offset %d"
            " for <%s> - trying property decomposition\n"
            "    query template: %s",
            purpose,
            e2.offset,
            classes[0],
            qt,
        )
    except Exception as e2:
        logger.warning(
            "  %s paginated fallback failed for <%s>: %s"
            " - trying property decomposition\n    query template: %s",
            purpose,
            classes[0],
            e2,
            qt,
        )

    # Attempt 4: property-first decomposition (typed-object only)
    if build_fn is _build_batched_typed_object_query:
        return typed_object_by_property(
            classes[0],
            graph_uris,
            purpose,
            helper,
            collect_bindings,
            chunk_size,
            unsafe_paging,
        )
    logger.warning(
        "  %s: all strategies exhausted for <%s> - skipping",
        purpose,
        classes[0],
    )
    return []


def enumerate_properties_for_class(
    class_uri: str,
    graph_uris: list[str] | None,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    unsafe_paging: bool = False,
) -> list[str] | None:
    """Return distinct property URIs for class_uri, or None on failure."""
    prop_q = _build_properties_for_class_query(class_uri, graph_uris)
    try:
        prop_result = helper.select(prop_q, purpose=purpose)
        return [
            b["p"]["value"]
            for b in prop_result.get("results", {}).get("bindings", [])
            if b.get("p", {}).get("value")
        ]
    except Exception as e:
        logger.warning(
            "  %s: property single-shot for <%s> failed: %s - retrying paginated",
            purpose,
            class_uri,
            e,
        )

    prop_qt = _build_properties_for_class_query(
        class_uri,
        graph_uris,
        paginated=True,
        drop_distinct=unsafe_paging,
    )
    try:
        raw = collect_bindings(prop_qt, purpose, _DECOMP_CHUNK)
        seen: set[str] = set()
        props: list[str] = []
        for b in raw:
            p_val = b.get("p", {}).get("value", "")
            if p_val and p_val not in seen:
                seen.add(p_val)
                props.append(p_val)
        return props
    except Exception as e2:
        logger.warning(
            "  %s: property enumeration for <%s> failed even paginated: %s - skipping",
            purpose,
            class_uri,
            e2,
        )
        return None


def enumerate_oc_for_class_property(
    class_uri: str,
    prop_uri: str,
    graph_uris: list[str] | None,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    unsafe_paging: bool = False,
) -> list[str]:
    """Return distinct object-class URIs for (class_uri, prop_uri)."""
    oc_q = _build_typed_object_for_class_property_query(
        class_uri,
        prop_uri,
        graph_uris,
    )
    try:
        oc_result = helper.select(oc_q, purpose=purpose)
        return [
            b.get("oc", {}).get("value", "")
            for b in oc_result.get("results", {}).get("bindings", [])
            if b.get("oc", {}).get("value")
        ]
    except Exception as e:
        logger.warning(
            "  %s: oc single-shot for <%s>/<%s> failed: %s - retrying paginated",
            purpose,
            class_uri,
            prop_uri,
            e,
        )

    oc_qt = _build_typed_object_for_class_property_query(
        class_uri,
        prop_uri,
        graph_uris,
        paginated=True,
        drop_distinct=unsafe_paging,
    )
    try:
        raw_oc = collect_bindings(oc_qt, purpose, _DECOMP_CHUNK)
        seen: set[str] = set()
        oc_vals: list[str] = []
        for b in raw_oc:
            v = b.get("oc", {}).get("value", "")
            if v and v not in seen:
                seen.add(v)
                oc_vals.append(v)
        return oc_vals
    except Exception as e2:
        logger.warning(
            "  %s: oc query for <%s>/<%s> failed even paginated: %s - skipping this property",
            purpose,
            class_uri,
            prop_uri,
            e2,
        )
        return []


def typed_object_by_property(
    class_uri: str,
    graph_uris: list[str] | None,
    purpose: str,
    helper: SparqlHelper,
    collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
    chunk_size: int,
    unsafe_paging: bool = False,
) -> list[dict[str, Any]]:
    """Typed-object patterns for one class via property-first decomposition."""
    logger.info(
        "  %s: <%s> too expensive for 3-way join - trying property-first decomposition",
        purpose,
        class_uri,
    )

    # Step 1: enumerate properties
    props = enumerate_properties_for_class(
        class_uri,
        graph_uris,
        purpose,
        helper,
        collect_bindings,
        unsafe_paging,
    )
    if props is None:
        return []

    logger.info(
        "  %s: <%s> has %d distinct properties - querying each",
        purpose,
        class_uri,
        len(props),
    )

    # Step 2: for each property, collect typed-object classes
    bindings: list[dict[str, Any]] = []
    for prop_uri in props:
        for oc in enumerate_oc_for_class_property(
            class_uri,
            prop_uri,
            graph_uris,
            purpose,
            helper,
            collect_bindings,
            unsafe_paging,
        ):
            bindings.append(
                {
                    "class": {"type": "uri", "value": class_uri},
                    "p": {"type": "uri", "value": prop_uri},
                    "oc": {"type": "uri", "value": oc},
                }
            )

    logger.info(
        "  %s: property-first decomposition for <%s> yielded %d bindings",
        purpose,
        class_uri,
        len(bindings),
    )
    return bindings
