"""
Schema Miner - extract RDF schema patterns via SELECT queries.

This module runs three lightweight SELECT DISTINCT queries
and assembles the schema in Python:

1. **Typed-object patterns**::

       SELECT DISTINCT ?sc ?p ?oc WHERE {
         ?s ?p ?o . ?s a ?sc . ?o a ?oc .
       }

2. **Literal patterns** (datatype properties)::

       SELECT DISTINCT ?sc ?p (DATATYPE(?o) AS ?dt) WHERE {
         ?s ?p ?o . ?s a ?sc . FILTER(isLiteral(?o))
       }

3. **Untyped-URI patterns** (URI objects without ``rdf:type``)::

       SELECT DISTINCT ?sc ?p WHERE {
         ?s ?p ?o . ?s a ?sc .
         FILTER(isURI(?o))
         FILTER NOT EXISTS { ?o a ?any }
       }

All queries use OFFSET / LIMIT pagination via
:meth:`SparqlHelper.select_chunked`.

The primary export is :class:`MinedSchema` (-> JSON-LD).  It can also
be converted to downstream LinkML / SHACL / RDF-config
exports.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from rdfsolve.models import (
    AboutMetadata,
    MinedSchema,
    MiningReport,
    OneShotQueryResult,
    PhaseReport,
    QueryStats,
    SchemaPattern,
)
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointTimeoutError,
    PaginationTruncatedError,
    SparqlHelper,
)
from rdfsolve.utils import get_local_name, pick_label
from rdfsolve.version import VERSION

if TYPE_CHECKING:
    from rdfsolve.sources import SourceEntry

logger = logging.getLogger(__name__)

__all__ = [
    "SchemaMiner",
    "_mine_one_source",
    "_resolve_source_overrides",
    "_write_schema_outputs",
    "count_instances",
    "count_instances_per_class",
    "extract_partitions_from_void",
    "generate_void_from_endpoint",
    "mine_all_sources",
    "mine_schema",
    "retrieve_void_from_graphs",
]


# -------------------------------------------------------------------
# SPARQL query templates (braces pre-escaped for str.format)
# -------------------------------------------------------------------


def _graph_clause(
    graph_uris: list[str] | None,
) -> tuple[str, str]:
    """Return (open, close) strings for an optional GRAPH clause.

    If *graph_uris* is ``None`` -> empty strings (default graph).
    If a single URI -> ``GRAPH <uri> {`` / ``}``.
    If multiple -> VALUES-based pattern.
    """
    if not graph_uris:
        return "", ""
    if len(graph_uris) == 1:
        return f"GRAPH <{graph_uris[0]}> {{", "}"
    # Multiple graphs - use VALUES
    values = " ".join(f"(<{u}>)" for u in graph_uris)
    open_ = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"
    return open_, "}"


def _build_typed_object_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 1: typed-object patterns (``?o a ?oc``)."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p ?oc
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    ?o a ?oc .
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_literal_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 2: literal patterns with datatype."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p ?dt
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_untyped_uri_query(
    graph_uris: list[str] | None,
) -> str:
    """Query 3: URI objects that lack an explicit ``rdf:type``."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?sc ?p
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_typed_object_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 1 - typed-object patterns, no LIMIT/OFFSET placeholders.

    Intended for one-shot execution against engines (e.g. QLever)
    that can return an unbounded result set in a single response.
    """
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?oc
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    ?o a ?oc .
  {g_close}
}}"""


def _build_literal_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 2 - literal patterns, no LIMIT/OFFSET placeholders."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p ?dt
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}"""


def _build_untyped_uri_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Query 3 - untyped-URI patterns, no LIMIT/OFFSET placeholders."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?sc ?p
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""


def _build_label_query(
    uris: list[str],
    graph_uris: list[str] | None,
) -> str:
    """Fetch labels for a set of URIs.

    Returns bindings with ``?uri``, ``?rdfsLabel``, ``?dcTitle``,
    ``?iaoLabel``, ``?skosPrefLabel``, ``?skosAltLabel``.
    Priority is resolved in Python via :func:`pick_label`.
    """
    values = " ".join(f"(<{u}>)" for u in uris)
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?uri ?rdfsLabel ?dcTitle ?iaoLabel ?skosPrefLabel ?skosAltLabel
WHERE {{
  VALUES (?uri) {{ {values} }}
  {g_open}
    OPTIONAL {{ ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?rdfsLabel . }}
    OPTIONAL {{ ?uri <http://purl.org/dc/elements/1.1/title> ?dcTitle . }}
    OPTIONAL {{ ?uri <http://purl.org/dc/terms/title> ?dcTitle . }}
    OPTIONAL {{ ?uri <http://purl.obolibrary.org/obo/IAO_0000118> ?iaoLabel . }}
    OPTIONAL {{ ?uri <http://www.w3.org/2004/02/skos/core#prefLabel> ?skosPrefLabel . }}
    OPTIONAL {{ ?uri <http://www.w3.org/2004/02/skos/core#altLabel> ?skosAltLabel . }}
  {g_close}
}}"""
    return q


# -------------------------------------------------------------------
# Two-phase query builders (class-scoped, for large endpoints)
# -------------------------------------------------------------------


def _build_class_discovery_query(
    graph_uris: list[str] | None,
) -> str:
    """Discover all distinct rdf:type classes (paginated template)."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT DISTINCT ?class
WHERE {{
  {g_open}
    ?s a ?class .
  {g_close}
}}"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_class_discovery_query_plain(
    graph_uris: list[str] | None,
) -> str:
    """Discover all distinct rdf:type classes (single shot)."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?class
WHERE {{
  {g_open}
    ?s a ?class .
  {g_close}
}}"""


def _build_class_typed_object_query(
    class_uri: str,
    graph_uris: list[str] | None,
) -> str:
    """Per-class typed-object patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?p ?oc
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s ?p ?o .
    ?o a ?oc .
  {g_close}
}}"""


def _build_class_literal_query(
    class_uri: str,
    graph_uris: list[str] | None,
) -> str:
    """Per-class literal patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?p (DATATYPE(?o) AS ?dt)
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s ?p ?o .
    FILTER(isLiteral(?o))
  {g_close}
}}"""


def _build_class_untyped_uri_query(
    class_uri: str,
    graph_uris: list[str] | None,
) -> str:
    """Per-class untyped-URI patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    return f"""\
SELECT DISTINCT ?p
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s ?p ?o .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""


# ---- batched two-phase query builders (VALUES) --------------------


def _build_properties_for_class_query(
    class_uri: str,
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Enumerate all distinct properties used by instances of *class_uri*.

    This is a cheap single-hop query (one join) used as the first step of
    the property-first decomposition fallback for expensive classes.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` / ``{limit}``
        placeholders.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from paginated queries; the
        caller deduplicates in Python.  Only active when
        ``paginated=True``.  See
        :func:`_build_batched_typed_object_query` for caveats.
    """
    g_open, g_close = _graph_clause(graph_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?p
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s ?p ?o .
  {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_typed_object_for_class_property_query(
    class_uri: str,
    prop_uri: str,
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Enumerate typed-object classes for a single *(class, property)* pair.

    Two-hop query but scoped to one property, so Virtuoso can use
    the property index and stays well under the cost limit.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` / ``{limit}``
        placeholders.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from paginated queries; the
        caller deduplicates in Python.  Only active when
        ``paginated=True``.  See
        :func:`_build_batched_typed_object_query` for caveats.
    """
    g_open, g_close = _graph_clause(graph_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?oc
WHERE {{
  {g_open}
    ?s a <{class_uri}> .
    ?s <{prop_uri}> ?o .
    ?o a ?oc .
  {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


# Page size for property-first decomposition fallback.  Large enough
# to collect most property lists in one or two pages, small enough to
# stay under per-page cost limits on Virtuoso-style endpoints.
_DECOMP_CHUNK = 1_000  # lmin


def _values_block(class_uris: list[str]) -> str:
    """Build a ``VALUES ?class { <u1> <u2> … }`` clause."""
    entries = " ".join(f"<{u}>" for u in class_uris)
    return f"VALUES ?class {{ {entries} }}"


def _build_batched_typed_object_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Typed-object patterns for a batch of classes.

    The ``VALUES`` clause is placed **inside** the ``GRAPH``
    block so that the binding is visible to the triple patterns
    on endpoints that enforce strict scoping (IDSM, QLever, …).

    Parameters
    ----------
    paginated:
        When ``True`` the returned string is a template with
        ``{offset}`` / ``{limit}`` placeholders suitable for
        :meth:`~rdfsolve.sparql_helper.SparqlHelper.select_chunked`.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from the query.  On some
        endpoints (e.g. Virtuoso) ``DISTINCT`` combined with
        ``OFFSET`` forces a full table scan on every page; dropping
        it lets the engine page cheaply at the cost of duplicate rows
        that the caller must deduplicate in Python.  This is
        **unsafe** in general (join cardinality may increase on other
        engines) and is only activated via ``--unsafe-paging``.
        Has no effect when ``paginated=False``; the non-paginated
        single-shot query always retains ``DISTINCT``.
    """
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p ?oc
WHERE {{
  {g_open}
    {values}
    ?s a ?class .
    ?s ?p ?o .
    ?o a ?oc .
  {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_literal_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Literal patterns for a batch of classes.

    ``VALUES`` is inside the ``GRAPH`` block - see
    :func:`_build_batched_typed_object_query` for rationale.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` /
        ``{limit}`` placeholders for
        :meth:`~rdfsolve.sparql_helper.SparqlHelper.select_chunked`.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from paginated queries.
        See :func:`_build_batched_typed_object_query` for caveats.
        Only active when ``paginated=True``.
    """
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p (DATATYPE(?o) AS ?dt)
WHERE {{
  {g_open}
    {values}
    ?s a ?class .
    ?s ?p ?o .
    FILTER(isLiteral(?o))
  {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_untyped_uri_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,
) -> str:
    """Untyped-URI patterns for a batch of classes.

    ``VALUES`` is inside the ``GRAPH`` block - see
    :func:`_build_batched_typed_object_query` for rationale.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` /
        ``{limit}`` placeholders for
        :meth:`~rdfsolve.sparql_helper.SparqlHelper.select_chunked`.
    drop_distinct:
        When ``True`` omits ``DISTINCT`` from paginated queries.
        See :func:`_build_batched_typed_object_query` for caveats.
        Only active when ``paginated=True``.
    """
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    distinct = "" if (paginated and drop_distinct) else "DISTINCT "
    q = f"""\
SELECT {distinct}?class ?p
WHERE {{
  {g_open}
    {values}
    ?s a ?class .
    ?s ?p ?o .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


# ---- batched count query builders (VALUES) -----------------------


def _build_batched_typed_count_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,  # accepted for API compat
) -> str:
    """Typed-object COUNT grouped by ``(class, p, oc)`` for a class batch.

    ``GROUP BY`` replaces ``DISTINCT`` so *drop_distinct* is ignored.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` /
        ``{limit}`` placeholders for
        :meth:`~rdfsolve.sparql_helper.SparqlHelper.select_chunked`.
    """
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    q = f"""\
SELECT ?class ?p ?oc (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    {values}
    ?s a ?class .
    ?s ?p ?o .
    ?o a ?oc .
  {g_close}
}}
GROUP BY ?class ?p ?oc"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_literal_count_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,  # accepted for API compat
) -> str:
    """Literal COUNT grouped by ``(class, p, dt)`` for a class batch.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` /
        ``{limit}`` placeholders.
    """
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    q = f"""\
SELECT ?class ?p ?dt (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    {values}
    ?s a ?class .
    ?s ?p ?o .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}
GROUP BY ?class ?p ?dt"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


def _build_batched_untyped_count_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
    paginated: bool = False,
    drop_distinct: bool = False,  # accepted for API compat
) -> str:
    """Untyped-URI COUNT grouped by ``(class, p)`` for a class batch.

    Parameters
    ----------
    paginated:
        When ``True`` returns a template with ``{offset}`` /
        ``{limit}`` placeholders.
    """
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    q = f"""\
SELECT ?class ?p (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    {values}
    ?s a ?class .
    ?s ?p ?o .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}
GROUP BY ?class ?p"""
    if paginated:
        return SparqlHelper.prepare_paginated_query(q)
    return q


# -------------------------------------------------------------------
# Report collector
# -------------------------------------------------------------------


class _ReportCollector:
    """Accumulates analytics during a mining run.

    Writes the JSON report to *report_path* after each phase so
    partial data is persisted even if the process crashes.

    Also captures resource-usage snapshots (CPU, memory, disk I/O)
    at init and finalise time so that every report is self-contained.
    """

    def __init__(
        self,
        report: MiningReport,
        report_path: Path | None = None,
    ) -> None:
        """Set up the collector with *report* and optional *report_path*."""
        self._report = report
        self._path = report_path

        # Resource-usage snapshots (populated in _snapshot_start)
        self._t0: float = 0.0
        self._cpu0_user: float = 0.0
        self._cpu0_sys: float = 0.0
        self._io0: dict[str, int] = {}
        self._snapshot_start()

    # ── Resource snapshots ─────────────────────────────────────────

    @staticmethod
    def _read_proc_io() -> dict[str, int]:
        result: dict[str, int] = {}
        try:
            with open("/proc/self/io", encoding="utf-8") as f:
                for line in f:
                    key, _, val = line.partition(":")
                    result[key.strip()] = int(val.strip())
        except OSError:
            pass
        return result

    @staticmethod
    def _get_rusage() -> tuple[float, float, float]:
        import platform as _platform
        import resource as _resource

        r = _resource.getrusage(_resource.RUSAGE_SELF)
        div = 1024 if _platform.system() == "Linux" else 1048576
        return r.ru_utime, r.ru_stime, r.ru_maxrss / div

    def _snapshot_start(self) -> None:
        import time as _time

        self._t0 = _time.monotonic()
        self._cpu0_user, self._cpu0_sys, _ = self._get_rusage()
        self._io0 = self._read_proc_io()

    def _collect_machine_info(self) -> dict[str, Any]:
        """Gather static machine info (lightweight)."""
        import platform as _platform

        info: dict[str, Any] = {
            "hostname": _platform.node(),
            "os_name": _platform.system(),
            "os_release": _platform.release(),
            "architecture": _platform.machine(),
            "cpu_model": _platform.processor() or "",
            "cpu_count_logical": os.cpu_count() or 0,
            "python_version": _platform.python_version(),
        }
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("model name"):
                        info["cpu_model"] = line.split(":", 1)[1].strip()
                        break
        except OSError:
            pass
        try:
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        kb = int(line.split()[1])
                        info["ram_total_gb"] = round(
                            kb / 1048576,
                            2,
                        )
                        break
        except OSError:
            pass
        return info

    def _collect_resource_usage(self) -> dict[str, Any]:
        """Capture delta resource usage since init."""
        import time as _time

        t1 = _time.monotonic()
        cpu1_user, cpu1_sys, peak_rss = self._get_rusage()
        io1 = self._read_proc_io()
        return {
            "wall_time_s": round(t1 - self._t0, 3),
            "cpu_user_s": round(cpu1_user - self._cpu0_user, 3),
            "cpu_system_s": round(cpu1_sys - self._cpu0_sys, 3),
            "peak_rss_mb": round(peak_rss, 2),
            "read_bytes": (io1.get("read_bytes", 0) - self._io0.get("read_bytes", 0)),
            "write_bytes": (io1.get("write_bytes", 0) - self._io0.get("write_bytes", 0)),
        }

    # ── Query tracking ─────────────────────────────────────────────

    def record_query(
        self,
        purpose: str,
        duration_s: float,
        success: bool = True,
    ) -> None:
        """Record one query execution."""
        stats = self._report.query_stats.setdefault(
            purpose,
            QueryStats(),
        )
        stats.sent += 1
        stats.total_time_s += duration_s
        self._report.total_queries_sent += 1
        if not success:
            stats.failed += 1
            self._report.total_queries_failed += 1

    # ── Dropped URI tracking ───────────────────────────────────────

    _MAX_DROPPED_SAMPLES: int = 20

    def record_dropped_uri(self, sample: str) -> None:
        """Record a pattern dropped due to an invalid URI value.

        Increments the counter and keeps the first few examples so
        the report gives actionable debugging info.
        """
        self._report.dropped_invalid_uris += 1
        if len(self._report.dropped_invalid_uri_samples) < self._MAX_DROPPED_SAMPLES:
            self._report.dropped_invalid_uri_samples.append(sample)

    # ── Phase tracking ─────────────────────────────────────────────

    def start_phase(self, name: str) -> PhaseReport:
        """Start a new phase and return its report object."""
        phase = PhaseReport(
            name=name,
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        self._report.phases.append(phase)
        return phase

    def finish_phase(
        self,
        phase: PhaseReport,
        items: int = 0,
        error: str | None = None,
    ) -> None:
        """Mark a phase as finished and flush the report."""
        now = datetime.now(timezone.utc)
        phase.finished_at = now.isoformat()
        if phase.started_at:
            started = datetime.fromisoformat(phase.started_at)
            phase.duration_s = round(
                (now - started).total_seconds(),
                3,
            )
        phase.items_discovered = items
        phase.error = error
        self.flush()

    def set_abort_reason(self, reason: str) -> None:
        """Record why mining was cut short and flush."""
        self._report.abort_reason = reason
        self.flush()

    # ── Finalisation ───────────────────────────────────────────────

    def finalise(
        self,
        pattern_count: int,
        class_count: int,
        property_count: int,
        uris_labelled: int,
    ) -> MiningReport:
        """Set final summary fields and flush."""
        r = self._report
        r.finished_at = datetime.now(
            timezone.utc,
        ).isoformat()
        if r.started_at:
            started = datetime.fromisoformat(r.started_at)
            finished = datetime.fromisoformat(r.finished_at)
            r.total_duration_s = round(
                (finished - started).total_seconds(),
                3,
            )
        r.pattern_count = pattern_count
        r.class_count = class_count
        r.property_count = property_count
        r.unique_uris_labelled = uris_labelled

        # Embed machine info and resource usage
        r.machine = self._collect_machine_info()
        r.benchmark = self._collect_resource_usage()

        self.flush()
        return r

    # ── I/O ────────────────────────────────────────────────────────

    def flush(self) -> None:
        """Write current state to disk (if a path was given)."""
        if self._path is None:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(
                    self._report.model_dump(),
                    indent=2,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("Could not write report: %s", exc)

    @property
    def report(self) -> MiningReport:
        """Return the accumulated :class:`MiningReport`."""
        return self._report


# -------------------------------------------------------------------
# SchemaMiner
# -------------------------------------------------------------------


class SchemaMiner:
    """Mine RDF schema patterns from a SPARQL endpoint.

    Parameters
    ----------
    endpoint_url:
        SPARQL endpoint URL.
    graph_uris:
        Optional named-graph URI(s) to restrict queries to.
    chunk_size:
        Number of rows per paginated request.
    class_chunk_size:
        Page size for Phase-1 class discovery in two-phase mode.
        ``None`` disables pagination (single query).
    class_batch_size:
        Number of classes grouped into one ``VALUES`` query in
        Phase-2 of two-phase mining.  Default ``15``.  Higher
        values send fewer queries but each query is heavier.
    delay:
        Seconds to sleep between pagination requests.
    timeout:
        HTTP timeout per request (seconds).
    counts:
        Whether to also run COUNT queries for triple counts.
    two_phase:
        Use two-phase mining (default).  Phase 1 discovers all
        ``rdf:type`` classes; phase 2 queries properties per
        class.  Much gentler on heavyweight endpoints like
        QLever/PubChem/UniProt.  Pass ``False`` for the legacy
        single-pass strategy.
    filter_service_namespaces:
        When ``True`` (the default), remove patterns whose
        subject, property, or object URI belongs to a
        service/system namespace (Virtuoso, OpenLink, etc.)
        from the final result.
    untyped_as_classes:
        When ``True``, treat untyped URI objects (those without
        an explicit ``rdf:type``) as ``owl:Class`` references
        instead of the generic ``rdfs:Resource`` sentinel.
        Default ``False``.
    """

    def __init__(
        self,
        endpoint_url: str,
        graph_uris: str | list[str] | None = None,
        chunk_size: int = 10_000,
        class_chunk_size: int | None = None,
        class_batch_size: int = 15,
        delay: float = 0.5,
        timeout: float = 120.0,
        counts: bool = True,
        two_phase: bool = True,
        unsafe_paging: bool = False,
        report_path: str | Path | None = None,
        filter_service_namespaces: bool = True,
        untyped_as_classes: bool = False,
        authors: list[dict[str, str]] | None = None,
        qlever_version: dict[str, str] | None = None,
        one_shot: bool = False,
    ) -> None:
        """Initialize a SchemaMiner."""
        self.endpoint_url = endpoint_url
        self.graph_uris: list[str] | None = (
            [graph_uris] if isinstance(graph_uris, str) else graph_uris
        )
        self.chunk_size = chunk_size
        self.class_chunk_size = class_chunk_size
        self.class_batch_size = max(1, class_batch_size)
        self.delay = delay
        self.timeout = timeout
        self.counts = counts
        self.two_phase = two_phase
        self.unsafe_paging = unsafe_paging
        self.filter_service_namespaces = filter_service_namespaces
        self.untyped_as_classes = untyped_as_classes
        self.authors = authors
        self.qlever_version = qlever_version
        self.one_shot = one_shot
        self._helper = SparqlHelper(
            endpoint_url,
            timeout=timeout,
        )
        self._report_path = Path(report_path) if report_path else None
        self._rc: _ReportCollector | None = None
        self.last_report: MiningReport | None = None

    @property
    def _report(self) -> _ReportCollector:
        """Return the active report collector (raises if not set)."""
        if self._rc is None:
            raise RuntimeError("mine() must be called first")
        return self._rc

    # ---- public API -----------------------------------------------

    def _build_strategy_string(self) -> str:
        """Return the strategy tag that describes the active mining flags."""
        base = "miner/two-phase" if self.two_phase else "miner"
        suffix = ""
        if self.counts:
            suffix += "+counts"
        if self.untyped_as_classes:
            suffix += "+untyped-as-classes"
        return base + suffix

    def _init_report(
        self,
        dataset_name: str | None,
        strategy: str,
        started_at: str,
    ) -> None:
        """Create a :class:`MiningReport` and attach a collector to ``self``."""
        report = MiningReport(
            dataset_name=dataset_name,
            endpoint_url=self.endpoint_url,
            graph_uris=self.graph_uris,
            strategy=strategy,
            rdfsolve_version=VERSION,
            python_version=sys.version,
            started_at=started_at,
            finished_at=None,
            total_duration_s=0.0,
            total_queries_sent=0,
            total_queries_failed=0,
            abort_reason=None,
            pattern_count=0,
            class_count=0,
            property_count=0,
            unique_uris_labelled=0,
            authors=self.authors,
            qlever_version=self.qlever_version,
            config={
                "chunk_size": self.chunk_size,
                "class_chunk_size": self.class_chunk_size,
                "delay": self.delay,
                "timeout": self.timeout,
                "counts": self.counts,
                "two_phase": self.two_phase,
                "one_shot": self.one_shot,
                "untyped_as_classes": self.untyped_as_classes,
            },
        )
        self._rc = _ReportCollector(report, self._report_path)

    def _run_patterns_phase(
        self,
    ) -> tuple[list[SchemaPattern], list[OneShotQueryResult] | None]:
        """Dispatch to the appropriate pattern-mining strategy.

        Returns
        -------
        (patterns, one_shot_results)
            *one_shot_results* is ``None`` unless ``self.one_shot`` is
            ``True``.
        """
        if self.one_shot:
            patterns, one_shot_results = self._mine_one_shot()
            return patterns, one_shot_results
        if self.two_phase:
            return self._mine_two_phase(), None
        return self._mine_single_pass(), None

    def _run_counts_phase(
        self,
        patterns: list[SchemaPattern],
    ) -> list[SchemaPattern]:
        """Run the counts phase and return enriched patterns."""
        phase = self._report.start_phase("counts")
        logger.info("Fetching triple counts …")
        try:
            patterns = self._enrich_counts(patterns)
            self._report.finish_phase(phase, items=len(patterns))
        except Exception as exc:
            self._report.finish_phase(phase, error=str(exc))
            raise
        return patterns

    def _run_labels_phase(
        self,
        patterns: list[SchemaPattern],
    ) -> tuple[list[SchemaPattern], set[str]]:
        """Run the labels phase and return (enriched patterns, uri set)."""
        phase = self._report.start_phase("labels")
        logger.info("Fetching labels …")
        uris_before = self._unique_uris(patterns)
        patterns = self._enrich_labels(patterns)
        self._report.finish_phase(phase, items=len(uris_before))
        return patterns, uris_before

    @staticmethod
    def _collect_class_property_sets(
        patterns: list[SchemaPattern],
    ) -> tuple[set[str], set[str]]:
        """Return (classes, properties) sets from *patterns*."""
        classes: set[str] = set()
        properties: set[str] = set()
        for p in patterns:
            classes.add(p.subject_class)
            if p.object_class not in ("Literal", "Resource"):
                classes.add(p.object_class)
            properties.add(p.property_uri)
        return classes, properties

    def _build_about_metadata(
        self,
        dataset_name: str | None,
        strategy: str,
        started_at: str,
        patterns: list[SchemaPattern],
    ) -> AboutMetadata:
        """Construct :class:`AboutMetadata` from the completed mining run."""
        finished_at = self._report.report.finished_at
        return AboutMetadata.build(
            endpoint=self.endpoint_url,
            dataset_name=dataset_name,
            graph_uris=self.graph_uris,
            pattern_count=len(patterns),
            strategy=strategy,
            started_at=started_at,
            finished_at=finished_at,
            total_duration_s=self._report.report.total_duration_s,
            authors=self.authors,
            qlever_version=self.qlever_version,
        )

    @staticmethod
    def _apply_namespace_filter(schema: MinedSchema) -> MinedSchema:
        """Strip service-namespace patterns and log if any were dropped."""
        before = len(schema.patterns)
        schema = schema.filter_service_namespaces()
        dropped = before - len(schema.patterns)
        if dropped:
            logger.info(
                "Filtered %d service-namespace patterns (%d -> %d)",
                dropped,
                before,
                len(schema.patterns),
            )
        return schema

    def mine(
        self,
        dataset_name: str | None = None,
    ) -> MinedSchema:
        """Run all queries and return a :class:`MinedSchema`.

        Parameters
        ----------
        dataset_name:
            Optional human-readable name attached to the metadata.

        Notes
        -----
        The method also populates a :class:`MiningReport` with
        per-phase timing, query counts, and failure stats.  If a
        *report_path* was given at construction time, the JSON is
        flushed to disk after each phase completes.
        """
        strategy = self._build_strategy_string()
        started_at = datetime.now(timezone.utc).isoformat()
        self._init_report(dataset_name, strategy, started_at)

        t0 = time.monotonic()
        patterns, one_shot_results = self._run_patterns_phase()

        if self.counts:
            patterns = self._run_counts_phase(patterns)

        patterns, uris_before = self._run_labels_phase(patterns)

        dt = time.monotonic() - t0
        logger.info(
            "Mining complete: %d patterns in %.1fs",
            len(patterns),
            dt,
        )

        classes, properties = self._collect_class_property_sets(
            patterns,
        )
        self._report.finalise(
            pattern_count=len(patterns),
            class_count=len(classes),
            property_count=len(properties),
            uris_labelled=len(uris_before),
        )
        if one_shot_results is not None:
            self._report.report.one_shot_results = one_shot_results
            self._report.flush()
        self.last_report = self._report.report

        about = self._build_about_metadata(
            dataset_name,
            strategy,
            started_at,
            patterns,
        )
        schema = MinedSchema(patterns=patterns, about=about)

        if self.filter_service_namespaces:
            schema = self._apply_namespace_filter(schema)

        return schema

    # ---- helpers ---------------------------------------------------

    @staticmethod
    def _unique_uris(
        patterns: list[SchemaPattern],
    ) -> set[str]:
        """Collect all unique URIs from patterns."""
        uris: set[str] = set()
        for p in patterns:
            uris.add(p.subject_class)
            uris.add(p.property_uri)
            if p.object_class not in ("Literal", "Resource"):
                uris.add(p.object_class)
        return uris

    # ---- single-pass mining (original) ----------------------------

    def _mine_single_pass(self) -> list[SchemaPattern]:
        """Original three-query mining approach."""
        patterns: list[SchemaPattern] = []

        phase = self._report.start_phase("typed-object")
        logger.info("Mining typed-object patterns …")
        typed = self._run_typed_object()
        patterns.extend(typed)
        logger.info(f"  -> {len(typed)} typed-object patterns")
        self._report.finish_phase(phase, items=len(typed))

        phase = self._report.start_phase("literal")
        logger.info("Mining literal patterns …")
        literals = self._run_literal()
        patterns.extend(literals)
        logger.info(f"  -> {len(literals)} literal patterns")
        self._report.finish_phase(phase, items=len(literals))

        phase = self._report.start_phase("untyped-uri")
        logger.info("Mining untyped-URI patterns …")
        untyped = self._run_untyped_uri()
        patterns.extend(untyped)
        logger.info(f"  -> {len(untyped)} untyped-URI patterns")
        self._report.finish_phase(phase, items=len(untyped))

        return patterns

    # ---- one-shot mining (baseline for QLever comparison) ---------

    def _parse_one_shot_bindings(
        self,
        qtype: str,
        bindings: list[dict[str, Any]],
        oc_default: str,
    ) -> list[SchemaPattern]:
        """Convert raw SPARQL bindings to :class:`SchemaPattern` objects.

        Parameters
        ----------
        qtype:
            One of ``"typed-object"``, ``"literal"``, ``"untyped-uri"``.
        bindings:
            The ``results.bindings`` list from a SPARQL JSON response.
        oc_default:
            Object-class sentinel used for untyped-URI patterns
            (``"Resource"`` or the OWL Class URI).
        """
        patterns: list[SchemaPattern] = []
        if qtype == "typed-object":
            for b in bindings:
                sc = b.get("sc", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                oc = b.get("oc", {}).get("value", "")
                if sc and p and oc:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=sc,
                                property_uri=p,
                                object_class=oc,
                            )
                        )
                    except (ValueError, ValidationError) as exc:
                        self._report.record_dropped_uri(f"{sc} {p} {oc}")
                        logger.debug("Skipping invalid pattern (%s %s %s): %s", sc, p, oc, exc)
        elif qtype == "literal":
            for b in bindings:
                sc = b.get("sc", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                dt = b.get("dt", {}).get("value")
                if sc and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=sc,
                                property_uri=p,
                                object_class="Literal",
                                datatype=dt if dt else None,
                            )
                        )
                    except (ValueError, ValidationError) as exc:
                        self._report.record_dropped_uri(f"{sc} {p} Literal")
                        logger.debug("Skipping invalid pattern (%s %s Literal): %s", sc, p, exc)
        else:  # untyped-uri
            for b in bindings:
                sc = b.get("sc", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                if sc and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=sc,
                                property_uri=p,
                                object_class=oc_default,
                            )
                        )
                    except (ValueError, ValidationError) as exc:
                        self._report.record_dropped_uri(f"{sc} {p} {oc_default}")
                        logger.debug("Skipping invalid pattern (%s %s %s): %s", sc, p, oc_default, exc)
        return patterns

    def _run_one_shot_query(
        self,
        qtype: str,
        query: str,
    ) -> tuple[list[dict[str, Any]], OneShotQueryResult]:
        """Execute a single one-shot SELECT and record analytics.

        Parameters
        ----------
        qtype:
            Query type label (e.g. ``"typed-object"``).  Used as the
            phase name and report key.
        query:
            The SPARQL SELECT string to execute.

        Returns
        -------
        (bindings, result)
            *bindings* is the raw ``results.bindings`` list (empty on
            failure).  *result* is a :class:`OneShotQueryResult`
            capturing success/failure, timing, and row count.
        """
        phase = self._report.start_phase(f"one-shot/{qtype}")
        t0 = time.monotonic()
        try:
            raw = self._helper.select(
                query,
                purpose=f"one-shot/{qtype}",
            )
            duration = time.monotonic() - t0
            bindings = raw.get("results", {}).get("bindings", [])
            row_count = len(bindings)
            logger.info(
                "One-shot %s: %d rows in %.1fs",
                qtype,
                row_count,
                duration,
            )
            self._report.record_query(f"one-shot/{qtype}", duration)
            self._report.finish_phase(phase, items=row_count)
            return bindings, OneShotQueryResult(
                query_type=qtype,
                success=True,
                duration_s=round(duration, 3),
                row_count=row_count,
            )
        except Exception as exc:
            duration = time.monotonic() - t0
            logger.warning(
                "One-shot %s failed after %.1fs: %s",
                qtype,
                duration,
                exc,
            )
            self._report.record_query(
                f"one-shot/{qtype}",
                duration,
                success=False,
            )
            self._report.finish_phase(phase, items=0, error=str(exc))
            return [], OneShotQueryResult(
                query_type=qtype,
                success=False,
                duration_s=round(duration, 3),
                row_count=None,
                error=str(exc),
            )

    def _mine_one_shot(
        self,
    ) -> tuple[list[SchemaPattern], list[OneShotQueryResult]]:
        """Run each pattern query as a single unbounded SELECT.

        No LIMIT/OFFSET, no bisection, no pagination fallback.
        Intended for local QLever endpoints that can return an
        unlimited result set in one HTTP response.

        Returns
        -------
        patterns:
            All patterns extracted from successful queries.
            Patterns from failed queries are absent (not recovered).
        results:
            One :class:`OneShotQueryResult` per query type,
            recording wall-clock time, row count, and any error.
            These are stored in :attr:`MiningReport.one_shot_results`
            so they can be compared against the fallback-chain run.
        """
        oc_default = (
            "http://www.w3.org/2002/07/owl#Class" if self.untyped_as_classes else "Resource"
        )

        _specs: list[tuple[str, str]] = [
            ("typed-object", _build_typed_object_query_plain(self.graph_uris)),
            ("literal", _build_literal_query_plain(self.graph_uris)),
            ("untyped-uri", _build_untyped_uri_query_plain(self.graph_uris)),
        ]

        patterns: list[SchemaPattern] = []
        results: list[OneShotQueryResult] = []

        for qtype, query in _specs:
            bindings, result = self._run_one_shot_query(qtype, query)
            results.append(result)
            if result.success:
                patterns.extend(
                    self._parse_one_shot_bindings(
                        qtype,
                        bindings,
                        oc_default,
                    )
                )

        return patterns, results

    # ---- two-phase mining (for large endpoints) -------------------

    def _mine_two_phase(self) -> list[SchemaPattern]:
        """Two-phase mining: discover classes, then query per class.

        Phase 1: ``SELECT DISTINCT ?class WHERE { ?s a ?class }``
        Phase 2: For each class, run three lightweight queries
                 scoped to ``?s a <class>``.

        This avoids the massive unscoped triple-join that chokes
        large endpoints (QLever, PubChem, UniProt, etc.).

        When ``class_chunk_size`` is ``None`` the class-discovery
        query runs without OFFSET/LIMIT pagination (single shot).
        A positive value enables paginated retrieval with that page
        size - useful when the endpoint returns *very* many classes.
        """
        # Phase 1 - discover classes
        p1 = self._report.start_phase("class-discovery")
        ccs = self.class_chunk_size
        if ccs is None:
            logger.info(
                "Phase 1: discovering classes (no pagination) …",
            )
            q = _build_class_discovery_query_plain(
                self.graph_uris,
            )
            t0 = time.monotonic()
            try:
                result = self._helper.select(
                    q,
                    purpose="two-phase/classes",
                )
                class_bindings = result.get("results", {}).get("bindings", [])
                self._report.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                )
            except Exception:
                self._report.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                    success=False,
                )
                raise
        else:
            logger.info(
                "Phase 1: discovering classes (chunk_size=%d) …",
                ccs,
            )
            q = _build_class_discovery_query(self.graph_uris)
            class_bindings = self._collect_bindings(
                q,
                purpose="two-phase/classes",
                chunk_size=ccs,
            )
        classes = [b.get("class", {}).get("value", "") for b in class_bindings]
        classes = [c for c in classes if c]
        logger.info(f"  -> {len(classes)} classes found")
        self._report.finish_phase(p1, items=len(classes))

        # Phase 2 - batched per-class pattern discovery
        p2 = self._report.start_phase("per-class-patterns")
        patterns, abort_reason = self._run_phase2_batches(
            classes,
            self.graph_uris,
        )

        # ── Ontology-graph fallback ───────────────────────────
        # If the GRAPH-scoped pass found 0 patterns but Phase 1
        # discovered classes, instances live in the
        # default graph or other named graphs.  Retry without the
        # GRAPH restriction so the instance triple patterns are
        # visible.
        if not patterns and classes and self.graph_uris and abort_reason is None:
            logger.warning(
                "Phase 2 returned 0 patterns with GRAPH <%s> "
                "- retrying without GRAPH restriction "
                "(ontology-graph fallback)",
                ", ".join(self.graph_uris),
            )
            patterns, abort_reason = self._run_phase2_batches(
                classes,
                None,
            )

        logger.info(f"  -> {len(patterns)} total patterns from {len(classes)} classes")
        self._report.finish_phase(p2, items=len(patterns))
        if abort_reason:
            self._report.set_abort_reason(abort_reason)
        return patterns

    # ---- Bisecting query helper -----------------------------------

    def _query_with_bisect(
        self,
        classes: list[str],
        graph_uris: list[str] | None,
        build_fn: Any,
        purpose: str,
    ) -> list[dict[str, Any]]:
        """Run a batched VALUES query with automatic bisection fallback.

        Strategy (in order):
        1. Single SELECT for the full *classes* list.
        2. Recursively bisect the list and retry each half independently,
           down to individual single-class queries.
        3. For a single-class batch that still fails: paginated SELECT
           (LIMIT/OFFSET) - the result set itself is just large, so
           pagination (not batch splitting) is the right tool.

        Bisecting is tried before pagination for multi-class batches
        because pagination does not reduce join cardinality: every page
        still joins over the full VALUES block.  Splitting the batch
        into smaller groups immediately lowers query cost.

        This guarantees that a persistently expensive batch is eventually
        broken down into single-class queries which always complete,
        rather than silently dropping data.

        Parameters
        ----------
        classes:
            The class URIs to include in the VALUES block.
        graph_uris:
            Named graphs to restrict queries to.
        build_fn:
            One of the ``_build_batched_*`` module-level functions.
            Called as ``build_fn(classes, graph_uris)`` or
            ``build_fn(classes, graph_uris, paginated=True)``.
        purpose:
            Label used for logging and report tracking.

        Returns
        -------
        list[dict]
            All SPARQL result bindings collected across all sub-queries.
        """
        # ── attempt 1: single-shot SELECT ──────────────────────────
        label = f"{classes[0]}…" if len(classes) > 1 else classes[0]
        q = build_fn(classes, graph_uris)
        try:
            result = self._helper.select(q, purpose=purpose)
            bindings: list[dict[str, Any]] = result.get("results", {}).get("bindings", [])
            return bindings
        except EndpointTimeoutError:
            # Cost/timeout — worth bisecting/paginating, fall through.
            logger.warning(
                "  %s single-shot timed out for [%s] (%d classes) - %s",
                purpose,
                label,
                len(classes),
                "trying paginated" if len(classes) == 1 else "bisecting",
            )
        except EndpointError as e:
            # Hard failure (502, unreachable, etc.) — no point retrying
            # with smaller batches or pagination against the same dead host.
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

        # ── attempt 2: bisect (multi-class) or paginate (single-class) ──
        #
        # For multi-class batches, bisect immediately: pagination does not
        # reduce join cardinality (every page still scans the full VALUES
        # block), so splitting the batch is strictly cheaper.
        #
        # For single-class batches the batch can't shrink further, so the
        # only lever left is pagination of the (potentially large) result set.
        if len(classes) > 1:
            mid = len(classes) // 2
            left = self._query_with_bisect(
                classes[:mid],
                graph_uris,
                build_fn,
                purpose,
            )
            right = self._query_with_bisect(
                classes[mid:],
                graph_uris,
                build_fn,
                purpose,
            )
            return left + right

        # ── attempt 3: paginated SELECT (single-class only) ──────────
        qt = build_fn(
            classes,
            graph_uris,
            paginated=True,
            drop_distinct=self.unsafe_paging,
        )
        try:
            raw = self._collect_bindings(
                qt,
                purpose=purpose,
                chunk_size=self.chunk_size,
            )
            # Deduplicate in Python (results may contain duplicates when
            # unsafe_paging drops DISTINCT, or from page-boundary overlaps)
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

        # ── attempt 4: property-first decomposition (typed-object only) ──
        # Can't bisect or paginate further. For typed-object queries, enumerate
        # ?p (cheap, 1-hop), then look up ?oc per property (cheap, 2-hop).
        # This sidesteps the 3-way join that exceeds Virtuoso's cost limit.
        if build_fn is _build_batched_typed_object_query:
            return self._typed_object_by_property(
                classes[0],
                graph_uris,
                purpose,
            )
        logger.warning(
            "  %s: all strategies exhausted for <%s> - skipping",
            purpose,
            classes[0],
        )
        return []

    # ---- Property-first typed-object decomposition ---------------

    def _enumerate_properties_for_class(
        self,
        class_uri: str,
        graph_uris: list[str] | None,
        purpose: str,
    ) -> list[str] | None:
        """Return distinct property URIs for *class_uri*, or ``None`` on failure.

        Tries a single-shot SELECT first; falls back to paginated retrieval.
        Returns ``None`` (not an empty list) when even the paginated attempt
        fails, so the caller can distinguish "no properties" from "query
        failed".
        """
        prop_q = _build_properties_for_class_query(class_uri, graph_uris)
        try:
            prop_result = self._helper.select(prop_q, purpose=purpose)
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
            drop_distinct=self.unsafe_paging,
        )
        try:
            raw = self._collect_bindings(
                prop_qt,
                purpose=purpose,
                chunk_size=_DECOMP_CHUNK,
            )
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

    def _enumerate_oc_for_class_property(
        self,
        class_uri: str,
        prop_uri: str,
        graph_uris: list[str] | None,
        purpose: str,
    ) -> list[str]:
        """Return distinct object-class URIs for *(class_uri, prop_uri)*.

        Tries a single-shot SELECT first; falls back to paginated retrieval.
        Returns an empty list when both attempts fail (the property is skipped
        silently after a warning).
        """
        oc_q = _build_typed_object_for_class_property_query(
            class_uri,
            prop_uri,
            graph_uris,
        )
        try:
            oc_result = self._helper.select(oc_q, purpose=purpose)
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
            drop_distinct=self.unsafe_paging,
        )
        try:
            raw_oc = self._collect_bindings(
                oc_qt,
                purpose=purpose,
                chunk_size=_DECOMP_CHUNK,
            )
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

    def _typed_object_by_property(
        self,
        class_uri: str,
        graph_uris: list[str] | None,
        purpose: str,
    ) -> list[dict[str, Any]]:
        """Typed-object patterns for one class via property-first decomposition.

        Used when the standard 3-way join ``?s a ?class . ?s ?p ?o . ?o a ?oc``
        exceeds Virtuoso's cost limit even for a single class.

        Strategy
        --------
        1. Enumerate distinct ``?p`` for the class (1-hop, cheap).
        2. For each ``?p``, enumerate distinct ``?oc`` (2-hop, property-indexed,
           cheap because the property scope drastically reduces the scan).

        Returns synthetic bindings in the same shape as the normal
        typed-object query so the caller needs no special handling.

        Pagination for decomposed queries uses :data:`_DECOMP_CHUNK`
        (1 000 rows) - large enough to collect
        most property lists in one or two pages, small enough to stay
        under Virtuoso's per-page cost limit.  ``select_chunked`` will
        adaptively shrink further if individual pages still time out.
        """
        logger.info(
            "  %s: <%s> too expensive for 3-way join - trying property-first decomposition",
            purpose,
            class_uri,
        )

        # Step 1: enumerate properties
        props = self._enumerate_properties_for_class(
            class_uri,
            graph_uris,
            purpose,
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
            for oc in self._enumerate_oc_for_class_property(
                class_uri,
                prop_uri,
                graph_uris,
                purpose,
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

    # ---- Phase 2 batch runner -------------------------------------

    def _run_phase2_batches(
        self,
        classes: list[str],
        graph_uris: list[str] | None,
    ) -> tuple[list[SchemaPattern], str | None]:
        """Execute Phase 2 batched queries for *classes*.

        Parameters
        ----------
        classes:
            Class URIs discovered in Phase 1.
        graph_uris:
            Graph URIs to wrap queries in a ``GRAPH`` clause.
            Pass ``None`` to query without graph restriction
            (needed when the discovery graph is an ontology
            that contains no instance data).

        Returns
        -------
        (patterns, abort_reason)
            *abort_reason* is ``None`` when all batches succeed.
        """
        bs = self.class_batch_size
        total = len(classes)
        n_batches = (total + bs - 1) // bs

        scope = f"GRAPH <{', '.join(graph_uris)}>" if graph_uris else "default graph"
        logger.info(
            "Phase 2: mining patterns in %d batches of ≤%d classes (%d classes total, scope: %s) …",
            n_batches,
            bs,
            total,
            scope,
        )

        patterns: list[SchemaPattern] = []
        abort_reason: str | None = None

        for batch_idx in range(n_batches):
            batch_start = batch_idx * bs
            batch = classes[batch_start : batch_start + bs]
            batch_label = (
                f"batch {batch_idx + 1}/{n_batches} "
                f"(classes {batch_start + 1}"
                f"-{batch_start + len(batch)}/{total})"
            )
            logger.info("  %s", batch_label)

            # 2a. Typed-object patterns for this batch
            t0 = time.monotonic()
            typed_bindings = self._query_with_bisect(
                batch,
                graph_uris,
                _build_batched_typed_object_query,
                "two-phase/typed-object",
            )
            self._report.record_query(
                "two-phase/typed-object",
                time.monotonic() - t0,
            )
            for b in typed_bindings:
                cls = b.get("class", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                oc = b.get("oc", {}).get("value", "")
                if cls and p and oc:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=cls,
                                property_uri=p,
                                object_class=oc,
                            )
                        )
                    except (ValueError, ValidationError):
                        self._report.record_dropped_uri(f"{cls} {p} {oc}")

            # 2b. Literal patterns for this batch
            t0 = time.monotonic()
            literal_bindings = self._query_with_bisect(
                batch,
                graph_uris,
                _build_batched_literal_query,
                "two-phase/literal",
            )
            self._report.record_query(
                "two-phase/literal",
                time.monotonic() - t0,
            )
            for b in literal_bindings:
                cls = b.get("class", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                dt = b.get("dt", {}).get("value")
                if cls and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=cls,
                                property_uri=p,
                                object_class="Literal",
                                datatype=dt if dt else None,
                            )
                        )
                    except (ValueError, ValidationError):
                        self._report.record_dropped_uri(f"{cls} {p} Literal")

            # 2c. Untyped-URI patterns for this batch
            t0 = time.monotonic()
            untyped_bindings = self._query_with_bisect(
                batch,
                graph_uris,
                _build_batched_untyped_uri_query,
                "two-phase/untyped-uri",
            )
            self._report.record_query(
                "two-phase/untyped-uri",
                time.monotonic() - t0,
            )
            untyped_oc = (
                "http://www.w3.org/2002/07/owl#Class" if self.untyped_as_classes else "Resource"
            )
            for b in untyped_bindings:
                cls = b.get("class", {}).get("value", "")
                p = b.get("p", {}).get("value", "")
                if cls and p:
                    try:
                        patterns.append(
                            SchemaPattern(
                                subject_class=cls,
                                property_uri=p,
                                object_class=untyped_oc,
                            )
                        )
                    except (ValueError, ValidationError):
                        self._report.record_dropped_uri(f"{cls} {p} {untyped_oc}")

            # Polite delay between batches
            if self.delay > 0:
                time.sleep(self.delay)

        return patterns, abort_reason

    # ---- private query runners ------------------------------------

    def _collect_bindings(
        self,
        query_template: str,
        purpose: str = "",
        chunk_size: int | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through a SELECT query and collect all bindings.

        Parameters
        ----------
        query_template:
            SPARQL query with ``{offset}`` / ``{limit}`` placeholders.
        purpose:
            Tag for logging and report tracking.
        chunk_size:
            Override the default ``self.chunk_size`` for this call.
            Useful for phase-specific page sizes.
        """
        effective = chunk_size if chunk_size is not None else self.chunk_size
        all_bindings: list[dict[str, Any]] = []
        has_rc = hasattr(self, "_rc")
        page = 0
        for chunk in self._helper.select_chunked(
            query_template,
            chunk_size=effective,
            delay_between_chunks=self.delay,
            purpose=purpose,
        ):
            page += 1
            all_bindings.extend(chunk)
            if has_rc:
                self._report.record_query(purpose, 0.0)
            logger.info(
                "  %s page %d: +%d rows (%d total)",
                purpose,
                page,
                len(chunk),
                len(all_bindings),
            )
        return all_bindings

    def _run_typed_object(self) -> list[SchemaPattern]:
        """Run the typed-object SELECT query."""
        q = _build_typed_object_query(self.graph_uris)
        bindings = self._collect_bindings(
            q,
            purpose="mining/typed-object",
        )
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            oc = b.get("oc", {}).get("value", "")
            if sc and p and oc:
                try:
                    results.append(
                        SchemaPattern(
                            subject_class=sc,
                            property_uri=p,
                            object_class=oc,
                        )
                    )
                except (ValueError, ValidationError):
                    self._report.record_dropped_uri(f"{sc} {p} {oc}")
        return results

    def _run_literal(self) -> list[SchemaPattern]:
        """Run the literal-property SELECT query."""
        q = _build_literal_query(self.graph_uris)
        bindings = self._collect_bindings(
            q,
            purpose="mining/literal",
        )
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            dt = b.get("dt", {}).get("value")
            if sc and p:
                try:
                    results.append(
                        SchemaPattern(
                            subject_class=sc,
                            property_uri=p,
                            object_class="Literal",
                            datatype=dt if dt else None,
                        )
                    )
                except (ValueError, ValidationError):
                    self._report.record_dropped_uri(f"{sc} {p} Literal")
        return results

    def _run_untyped_uri(self) -> list[SchemaPattern]:
        """Run the untyped-URI SELECT query."""
        q = _build_untyped_uri_query(self.graph_uris)
        bindings = self._collect_bindings(
            q,
            purpose="mining/untyped-uri",
        )
        oc = "http://www.w3.org/2002/07/owl#Class" if self.untyped_as_classes else "Resource"
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            if sc and p:
                try:
                    results.append(
                        SchemaPattern(
                            subject_class=sc,
                            property_uri=p,
                            object_class=oc,
                            count=None,
                            datatype=None,
                            subject_label=None,
                            object_label=None,
                            property_label=None,
                        )
                    )
                except (ValueError, ValidationError):
                    self._report.record_dropped_uri(f"{sc} {p} {oc}")
        return results

    def _fetch_typed_count_batch(
        self,
        batch: list[str],
        label: str,
        counts: dict[tuple[str, str, str], int],
    ) -> None:
        """Query typed-object counts for one class batch and update *counts*."""
        try:
            t0 = time.monotonic()
            bindings = self._query_with_bisect(
                batch,
                self.graph_uris,
                _build_batched_typed_count_query,
                "counts/typed-object",
            )
            if hasattr(self, "_rc"):
                self._report.record_query(
                    "counts/typed-object",
                    time.monotonic() - t0,
                )
            for b in bindings:
                key = (
                    b.get("class", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    b.get("oc", {}).get("value", ""),
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(float(cnt))
        except Exception as e:
            logger.warning(
                "Typed-object count query failed (%s): %s",
                label,
                e,
            )

    def _fetch_literal_count_batch(
        self,
        batch: list[str],
        label: str,
        counts: dict[tuple[str, str, str], int],
    ) -> None:
        """Query literal counts for one class batch and update *counts*."""
        try:
            t0 = time.monotonic()
            bindings = self._query_with_bisect(
                batch,
                self.graph_uris,
                _build_batched_literal_count_query,
                "counts/literal",
            )
            if hasattr(self, "_rc"):
                self._report.record_query(
                    "counts/literal",
                    time.monotonic() - t0,
                )
            for b in bindings:
                dt = b.get("dt", {}).get("value", "")
                key = (
                    b.get("class", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    f"Literal:{dt}" if dt else "Literal",
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(float(cnt))
        except Exception as e:
            logger.warning(
                "Literal count query failed (%s): %s",
                label,
                e,
            )

    def _fetch_untyped_count_batch(
        self,
        batch: list[str],
        label: str,
        counts: dict[tuple[str, str, str], int],
    ) -> None:
        """Query untyped-URI counts for one class batch and update *counts*."""
        try:
            t0 = time.monotonic()
            bindings = self._query_with_bisect(
                batch,
                self.graph_uris,
                _build_batched_untyped_count_query,
                "counts/untyped-uri",
            )
            if hasattr(self, "_rc"):
                self._report.record_query(
                    "counts/untyped-uri",
                    time.monotonic() - t0,
                )
            for b in bindings:
                key = (
                    b.get("class", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    "Resource",
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(float(cnt))
        except Exception as e:
            logger.warning(
                "Untyped-URI count query failed (%s): %s",
                label,
                e,
            )

    def _enrich_counts(
        self,
        patterns: list[SchemaPattern],
    ) -> list[SchemaPattern]:
        """Run COUNT queries and merge counts into patterns.

        Count queries use the same batched VALUES / bisect
        infrastructure as the pattern queries so that they
        remain feasible on large endpoints.  Each query type
        (typed-object, literal, untyped-URI) is run per class
        batch; failures are handled per-batch (logged and
        skipped) rather than aborting all counts.
        """
        # Collect unique subject classes from already-mined
        # patterns - these are the classes we need counts for.
        subject_classes = sorted({p.subject_class for p in patterns})
        if not subject_classes:
            return patterns

        bs = self.class_batch_size
        total = len(subject_classes)
        n_batches = (total + bs - 1) // bs
        logger.info(
            "Counting phase: %d classes in %d batches of ≤%d …",
            total,
            n_batches,
            bs,
        )

        # Build lookup: (sc, p, oc) -> count
        counts: dict[tuple[str, str, str], int] = {}

        for batch_idx in range(n_batches):
            start = batch_idx * bs
            batch = subject_classes[start : start + bs]
            label = f"batch {batch_idx + 1}/{n_batches}"

            self._fetch_typed_count_batch(batch, label, counts)
            self._fetch_literal_count_batch(batch, label, counts)
            self._fetch_untyped_count_batch(batch, label, counts)

            # Polite delay between batches
            if self.delay > 0:
                time.sleep(self.delay)

        logger.info(
            "Counting phase: collected %d count entries",
            len(counts),
        )

        # Merge counts into patterns
        enriched: list[SchemaPattern] = []
        for pat in patterns:
            if pat.object_class == "Literal":
                dt_key = f"Literal:{pat.datatype}" if pat.datatype else "Literal"
                key = (
                    pat.subject_class,
                    pat.property_uri,
                    dt_key,
                )
            else:
                key = (
                    pat.subject_class,
                    pat.property_uri,
                    pat.object_class,
                )
            cnt = counts.get(key)
            enriched.append(
                pat.model_copy(update={"count": cnt}),
            )

        return enriched

    def _fetch_label_batch(
        self,
        batch: list[str],
        label_map: dict[str, str],
    ) -> None:
        """Query labels for one batch of URIs and update *label_map* in place.

        Parameters
        ----------
        batch:
            URIs to look up labels for.
        label_map:
            Mapping that will be updated with ``{uri: label}`` entries.
            URIs already present in the map are skipped.
        """
        t0 = time.monotonic()
        try:
            q = _build_label_query(batch, self.graph_uris)
            result = self._helper.select(q, purpose="labels")
            if hasattr(self, "_rc"):
                self._report.record_query(
                    "labels",
                    time.monotonic() - t0,
                )
            bindings = result.get("results", {}).get("bindings", [])
            for b in bindings:
                uri = b.get("uri", {}).get("value", "")
                if not uri or uri in label_map:
                    continue
                rdfs_lbl = b.get("rdfsLabel", {}).get("value")
                dc_lbl = b.get("dcTitle", {}).get("value")
                iao_lbl = b.get("iaoLabel", {}).get("value")
                skos_pref = b.get("skosPrefLabel", {}).get("value")
                skos_alt = b.get("skosAltLabel", {}).get("value")
                label_map[uri] = pick_label(
                    rdfs_lbl,
                    dc_lbl,
                    uri,
                    iao_label=iao_lbl,
                    skos_pref_label=skos_pref,
                    skos_alt_label=skos_alt,
                )
        except Exception as e:
            if hasattr(self, "_rc"):
                self._report.record_query(
                    "labels",
                    time.monotonic() - t0,
                    success=False,
                )
            logger.warning("Label batch failed (%d URIs) : %s", len(batch), e)

    def _enrich_labels(
        self,
        patterns: list[SchemaPattern],
    ) -> list[SchemaPattern]:
        """Fetch rdfs:label / dc:title for all URIs in patterns.

        URIs are queried in batches (max 50 per request) to avoid
        HTTP 414 URI-too-long errors on endpoints that reject large
        GET query strings.
        """
        # Collect all unique URIs that need labels
        all_uris: set[str] = set()
        for pat in patterns:
            all_uris.add(pat.subject_class)
            all_uris.add(pat.property_uri)
            if pat.object_class not in ("Literal", "Resource"):
                all_uris.add(pat.object_class)

        if not all_uris:
            return patterns

        # Fetch labels in batches to keep query size small
        label_map: dict[str, str] = {}
        batch_size = 50
        uri_list = sorted(all_uris)

        for start in range(0, len(uri_list), batch_size):
            batch = uri_list[start : start + batch_size]
            self._fetch_label_batch(batch, label_map)

        # Fill in labels using local name as fallback
        enriched = _enrich_with_local(patterns, label_map)

        return enriched


def _enrich_with_local(
    patterns: list[SchemaPattern], label_map: dict[str, str]
) -> list[SchemaPattern]:
    enriched: list[SchemaPattern] = []
    for pat in patterns:
        updates: dict[str, Any] = {}
        updates["subject_label"] = label_map.get(
            pat.subject_class,
            get_local_name(pat.subject_class),
        )
        updates["property_label"] = label_map.get(
            pat.property_uri,
            get_local_name(pat.property_uri),
        )
        if pat.object_class in ("Literal", "Resource"):
            updates["object_label"] = pat.object_class
        else:
            updates["object_label"] = label_map.get(
                pat.object_class,
                get_local_name(pat.object_class),
            )
        enriched.append(pat.model_copy(update=updates))
    return enriched


# -------------------------------------------------------------------
# Convenience function
# -------------------------------------------------------------------


def mine_schema(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    dataset_name: str | None = None,
    chunk_size: int = 10_000,
    class_chunk_size: int | None = None,
    class_batch_size: int = 15,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
    two_phase: bool = True,
    report_path: str | Path | None = None,
    filter_service_namespaces: bool = True,
    untyped_as_classes: bool = False,
    authors: list[dict[str, str]] | None = None,
    qlever_version: dict[str, str] | None = None,
    one_shot: bool = False,
) -> MinedSchema:
    """One-shot helper: mine a schema and return :class:`MinedSchema`.

    Parameters
    ----------
    endpoint_url:
        SPARQL endpoint URL.
    graph_uris:
        Named-graph URI(s) to restrict queries to.
    dataset_name:
        Human-readable name for the dataset.
    chunk_size:
        Pagination page size for pattern queries (single-pass and
        count queries).
    class_chunk_size:
        Page size for the Phase-1 class-discovery query in two-phase
        mode.  ``None`` (default) disables pagination - the class
        list is fetched in a single query.  Set to a positive integer
        when the endpoint has too many classes for one response.
    class_batch_size:
        Number of classes to group into a single VALUES query in
        Phase-2 of two-phase mining.  Default ``15``.  Higher values
        send fewer queries but each query is heavier.
    delay:
        Delay between pages (seconds).
    timeout:
        HTTP timeout per request.
    counts:
        Fetch triple counts per pattern.
    two_phase:
        Use two-phase mining (default ``True``).  Pass ``False``
        for the legacy single-pass strategy.
    one_shot:
        Run each pattern query as a single unbounded SELECT with no
        LIMIT/OFFSET and no fallback chain.  Intended for local
        QLever endpoints.  When ``True``, ``two_phase`` is ignored.
    report_path:
        If given, write an analytics JSON report to this path.
        The file is updated incrementally after each mining phase.
    filter_service_namespaces:
        Strip patterns whose URIs belong to service / system
        namespaces (Virtuoso, OpenLink, etc.) from the
        result.  Default ``True``.
    untyped_as_classes:
        Treat untyped URI objects as ``owl:Class`` references
        instead of the generic ``rdfs:Resource`` sentinel.
        Default ``False``.

    Returns
    -------
    MinedSchema
        Contains patterns and provenance metadata.
    """
    miner = SchemaMiner(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        chunk_size=chunk_size,
        class_chunk_size=class_chunk_size,
        class_batch_size=class_batch_size,
        delay=delay,
        timeout=timeout,
        counts=counts,
        two_phase=two_phase,
        report_path=report_path,
        filter_service_namespaces=filter_service_namespaces,
        untyped_as_classes=untyped_as_classes,
        authors=authors,
        qlever_version=qlever_version,
        one_shot=one_shot,
    )
    return miner.mine(dataset_name=dataset_name)


# -------------------------------------------------------------------
# Batch-mining helpers (used by api.mine_all_sources)
# -------------------------------------------------------------------


def _resolve_source_overrides(
    entry: SourceEntry,
    *,
    chunk_size: int,
    class_chunk_size: int | None,
    class_batch_size: int,
    delay: float,
    timeout: float,
    counts: bool,
    two_phase: bool,
    idx: int,
    total: int,
    name: str,
) -> dict[str, Any]:
    """Return effective per-source mining parameters.

    Values from *entry* take precedence over the function-level
    defaults.  ``class_chunk_size`` is only forwarded for two-phase
    rows; a warning is emitted otherwise.
    """
    effective_ccs: int | None = None
    if two_phase:
        effective_ccs = entry.get("class_chunk_size", class_chunk_size)
    elif class_chunk_size is not None:
        logger.info(
            "[%d/%d] --class-chunk-size ignored for %r (not two-phase)",
            idx,
            total,
            name,
        )
    return {
        "chunk_size": entry.get("chunk_size", chunk_size),
        "class_chunk_size": effective_ccs,
        "class_batch_size": entry.get(
            "class_batch_size",
            class_batch_size,
        ),
        "delay": entry.get("delay", delay),
        "timeout": entry.get("timeout", timeout),
        "counts": entry.get("counts", counts),
    }


def _write_schema_outputs(
    schema: MinedSchema,
    *,
    out: Path,
    name: str,
    tag: str,
    fmt: str,
) -> None:
    """Serialise *schema* to the requested format(s) under *out*."""
    if fmt in ("jsonld", "all"):
        jsonld_path = out / f"{name}_{tag}_schema.jsonld"
        with open(jsonld_path, "w") as fh:
            json.dump(schema.to_jsonld(), fh, indent=2)
        logger.info("  -> %s", jsonld_path)

    if fmt in ("void", "all"):
        void_path = out / f"{name}_{tag}_void.ttl"
        void_g = schema.to_void_graph()
        void_g.serialize(destination=str(void_path), format="turtle")
        logger.info("  -> %s (%d triples)", void_path, len(void_g))


def _mine_one_source(
    entry: SourceEntry,
    *,
    idx: int,
    total: int,
    out: Path,
    fmt: str,
    chunk_size: int,
    class_chunk_size: int | None,
    class_batch_size: int,
    delay: float,
    timeout: float,
    counts: bool,
    reports: bool,
    filter_service_namespaces: bool,
    untyped_as_classes: bool,
    authors: list[dict[str, str]] | None,
    on_progress: (Callable[[str, int, int, str | None], None] | None),
    succeeded: list[str],
    failed: list[dict[str, str]],
) -> None:
    """Mine one source entry, write outputs, update *succeeded*/*failed*.

    All complex logic (parameter resolution, path building, error
    handling) lives here so that :func:`~rdfsolve.api.mine_all_sources`
    stays a thin loop.
    """
    name: str = entry.get("name", "")
    endpoint: str = entry.get("endpoint", "")
    use_graph: bool = entry.get("use_graph", False)
    row_two_phase: bool = entry.get("two_phase", True)

    graph_uris_arg: list[str] | None = None
    entry_graphs = entry.get("graph_uris", [])
    if use_graph and entry_graphs:
        graph_uris_arg = list(entry_graphs)

    logger.info("[%d/%d] Mining %r (%s)", idx, total, name, endpoint)

    params = _resolve_source_overrides(
        entry,
        chunk_size=chunk_size,
        class_chunk_size=class_chunk_size,
        class_batch_size=class_batch_size,
        delay=delay,
        timeout=timeout,
        counts=counts,
        two_phase=row_two_phase,
        idx=idx,
        total=total,
        name=name,
    )
    tag = "mined_remote_untyped" if untyped_as_classes else "mined_remote"
    rpt_path: Path | None = out / f"{name}_{tag}_report.json" if reports else None

    try:
        schema = mine_schema(
            endpoint_url=endpoint,
            graph_uris=graph_uris_arg,
            dataset_name=name,
            two_phase=row_two_phase,
            report_path=rpt_path,
            filter_service_namespaces=filter_service_namespaces,
            untyped_as_classes=untyped_as_classes,
            authors=authors,
            **params,
        )
        _write_schema_outputs(
            schema,
            out=out,
            name=name,
            tag=tag,
            fmt=fmt,
        )
        succeeded.append(name)
        if on_progress:
            on_progress(name, idx, total, None)
    except Exception as exc:
        msg = str(exc)
        logger.warning("  FAIL %s: %s", name, msg)
        failed.append({"dataset": name, "error": msg})
        if on_progress:
            on_progress(name, idx, total, msg)


# -------------------------------------------------------------------
# VoID / instance-counting helpers
# -------------------------------------------------------------------


def count_instances(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    sample_limit: int | None = None,
    sample_offset: int | None = None,
    chunk_size: int | None = None,
    offset_limit_steps: int | None = None,
    delay_between_chunks: float = 20.0,
    streaming: bool = False,
    timeout: float = 120.0,
) -> dict[str, int] | Any:
    """Count instances per class at *endpoint_url*.

    Args:
        endpoint_url: SPARQL endpoint URL.
        graph_uris: Optional named-graph URI(s) to restrict queries.
        sample_limit: Maximum number of classes to return.
        sample_offset: Starting offset for pagination.
        chunk_size: Page size when paginating.
        offset_limit_steps: Use this value as both LIMIT and OFFSET
            step (overrides *chunk_size*).
        delay_between_chunks: Seconds to sleep between pages.
        streaming: If ``True`` return a generator of
            ``(class_uri, count)`` tuples instead of a dict.
        timeout: HTTP timeout per request.

    Returns:
        ``{class_uri: count}`` dict, or a generator when
        *streaming* is ``True``.
    """
    helper = SparqlHelper(endpoint_url, timeout=timeout)
    step = offset_limit_steps or chunk_size
    offset = sample_offset or 0

    if step is not None:

        def _chunked() -> Any:
            off = offset
            seen = 0
            while True:
                q = _count_instances_query(
                    graph_uris,
                    limit=step,
                    offset=off,
                )
                results = helper.select(q, purpose="coverage/class")
                bindings = results["results"]["bindings"]
                if not bindings:
                    break
                for row in bindings:
                    if sample_limit and seen >= sample_limit:
                        return
                    yield (
                        row["class"]["value"],
                        int(row["count"]["value"]),
                    )
                    seen += 1
                if len(bindings) < step:
                    break
                off += step
                time.sleep(delay_between_chunks)

        gen = _chunked()
        return gen if streaming else dict(gen)

    q = _count_instances_query(
        graph_uris,
        limit=sample_limit,
        offset=sample_offset,
    )
    try:
        results = helper.select(q, purpose="coverage/class")
        pairs = (
            (r["class"]["value"], int(r["count"]["value"])) for r in results["results"]["bindings"]
        )
        if streaming:
            return pairs
        return dict(pairs)
    except Exception:
        return iter([]) if streaming else {}


def _count_instances_query(
    graph_uris: str | list[str] | None,
    limit: int | None,
    offset: int | None,
) -> str:
    gc = _graph_clause([graph_uris] if isinstance(graph_uris, str) else graph_uris)
    tail = ""
    if offset:
        tail += f"\nOFFSET {offset}"
    if limit:
        tail += f"\nLIMIT {limit}"
    return (
        f"SELECT ?class (COUNT(DISTINCT ?instance) AS ?count) WHERE {{"
        f"\n{gc}"
        f"\n  ?instance a ?class ."
        f"\n{'}}' if not gc else ''}"
        f"\n}}\nGROUP BY ?class\nORDER BY DESC(?count){tail}"
    )


def count_instances_per_class(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    sample_limit: int | None = None,
    exclude_graphs: bool = True,
    timeout: float = 120.0,
) -> dict[str, int]:
    """Return ``{class_uri: instance_count}`` for *endpoint_url*.

    A simplified single-query variant of :func:`count_instances`.

    Args:
        endpoint_url: SPARQL endpoint URL.
        graph_uris: Optional named-graph URI(s).
        sample_limit: Cap on the number of classes returned.
        exclude_graphs: Unused; kept for backwards-compatibility.
        timeout: HTTP timeout per request.

    Returns:
        ``{class_uri: count}`` dict.
    """
    result = count_instances(
        endpoint_url,
        graph_uris=graph_uris,
        sample_limit=sample_limit,
        timeout=timeout,
    )
    return result if isinstance(result, dict) else dict(result)


def extract_partitions_from_void(
    endpoint_url: str,
    void_graph_uris: list[str],
    timeout: float = 120.0,
) -> list[dict[str, str]]:
    """Query partition records from named VoID graphs.

    Runs a SELECT query against each graph URI in *void_graph_uris*
    and returns the raw partition records suitable for passing to
    :meth:`~rdfsolve.parser.VoidParser.build_void_graph_from_partitions`.

    Args:
        endpoint_url: SPARQL endpoint URL.
        void_graph_uris: Graph URIs that are known to contain VoID.
        timeout: HTTP timeout per request.

    Returns:
        List of partition dicts with keys ``subject_class``,
        ``property``, and optionally ``object_class`` /
        ``object_datatype``.
    """
    helper = SparqlHelper(endpoint_url, timeout=timeout)
    all_partitions: list[dict[str, str]] = []

    for graph_uri in void_graph_uris:
        esc = graph_uri.replace("\\", "\\\\").replace('"', '\\"')
        query = f"""
        PREFIX void: <http://rdfs.org/ns/void#>
        PREFIX void-ext: <http://ldf.fi/void-ext#>
        SELECT DISTINCT ?subjectClass ?prop ?objectClass ?objectDatatype
        WHERE {{
          GRAPH <{esc}> {{
            {{
              ?cp void:class ?subjectClass ;
                  void:propertyPartition ?pp .
              ?pp void:property ?prop .
              OPTIONAL {{
                {{
                  ?pp void:classPartition [ void:class ?objectClass ] .
                }} UNION {{
                  ?pp void-ext:datatypePartition
                      [ void-ext:datatype ?objectDatatype ] .
                }}
              }}
            }} UNION {{
              ?ls void:subjectsTarget [ void:class ?subjectClass ] ;
                  void:linkPredicate ?prop ;
                  void:objectsTarget [ void:class ?objectClass ] .
            }}
          }}
        }}
        """
        try:
            results = helper.select(query, purpose="void/partition-detail")
            for row in results.get("results", {}).get("bindings", []):
                rec: dict[str, str] = {
                    "subject_class": row.get("subjectClass", {}).get("value", ""),
                    "property": row.get("prop", {}).get("value", ""),
                }
                if row.get("objectClass", {}).get("value"):
                    rec["object_class"] = row["objectClass"]["value"]
                elif row.get("objectDatatype", {}).get("value"):
                    rec["object_datatype"] = row["objectDatatype"]["value"]
                all_partitions.append(rec)
        except Exception as exc:
            logger.warning(
                "Failed to retrieve partitions from %s: %s",
                graph_uri,
                exc,
            )

    return all_partitions


def retrieve_void_from_graphs(
    endpoint_url: str,
    void_graph_uris: list[str],
    graph_uris: str | list[str] | None = None,
    partitions: list[dict[str, str]] | None = None,
    timeout: float = 120.0,
) -> Any:
    """Build an RDF VoID graph from partition records.

    If *partitions* are provided they are used directly; otherwise a
    fresh discovery query is run via
    :meth:`~rdfsolve.parser.VoidParser.discover_void_graphs`.

    Args:
        endpoint_url: SPARQL endpoint URL.
        void_graph_uris: Graph URIs containing VoID (used as base URI).
        graph_uris: Unused; kept for backwards-compatibility.
        partitions: Pre-fetched partition records.
        timeout: HTTP timeout per request.

    Returns:
        :class:`~rdflib.Graph` with VoID triples.
    """
    from rdflib import Graph as _Graph

    from rdfsolve.parser import VoidParser

    if not partitions:
        result = VoidParser().discover_void_graphs(endpoint_url)
        partitions = result.get("partitions", [])

    if partitions:
        base_uri = void_graph_uris[0] if void_graph_uris else None
        return VoidParser().build_void_graph_from_partitions(partitions, base_uri=base_uri)
    return _Graph()


def generate_void_from_endpoint(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    output_file: str | None = None,
    counts: bool = True,
    offset_limit_steps: int | None = None,
    exclude_graphs: bool = True,
    dataset_uri: str | None = None,
    void_base_uri: str | None = None,
    timeout: float = 120.0,
) -> Any:
    """Mine a VoID description from a SPARQL endpoint.

    .. deprecated::
        Use :func:`mine_schema` instead.

    Args:
        endpoint_url: SPARQL endpoint URL.
        graph_uris: Named-graph URI(s) to restrict queries.
        output_file: If given, serialise result as Turtle here.
        counts: Include triple counts (passed to :func:`mine_schema`).
        offset_limit_steps: Pagination chunk size.
        exclude_graphs: Unused; kept for backwards-compatibility.
        dataset_uri: Unused; kept for backwards-compatibility.
        void_base_uri: Unused; kept for backwards-compatibility.
        timeout: HTTP timeout per request.

    Returns:
        :class:`~rdflib.Graph` with VoID triples.
    """
    import warnings

    warnings.warn(
        "generate_void_from_endpoint is deprecated; use mine_schema().",
        DeprecationWarning,
        stacklevel=2,
    )
    schema = mine_schema(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        counts=counts,
        timeout=timeout,
    )
    void_g = schema.to_void_graph()
    if output_file:
        void_g.serialize(destination=output_file, format="turtle")
    return void_g


def mine_all_sources(
    sources_csv: str | None = None,
    *,
    sources: str | None = None,
    output_dir: str = ".",
    fmt: str = "all",
    chunk_size: int = 10_000,
    class_chunk_size: int | None = None,
    class_batch_size: int = 15,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
    reports: bool = True,
    filter_service_namespaces: bool = True,
    untyped_as_classes: bool = False,
    authors: list[dict[str, str]] | None = None,
    on_progress: Callable[[str, int, int, str | None], None] | None = None,
) -> dict[str, Any]:
    """Mine schemas for all sources in a JSON-LD or CSV file.

    Reads a sources file (JSON-LD preferred, CSV still accepted) and runs
    :func:`mine_schema` for each entry whose *endpoint* is non-empty.
    Results are written to *output_dir* as ``{name}_schema.jsonld`` and/or
    ``{name}_void.ttl``.

    Per-source overrides (``chunk_size``, ``class_batch_size``, ``timeout``,
    etc.) in the JSON-LD file take precedence over the function-level
    defaults.

    Args:
        sources_csv: **Deprecated** - use *sources* instead.
        sources: Path to the sources file (JSON-LD or CSV).
        output_dir: Directory where outputs are written.
        fmt: Export format - ``"jsonld"``, ``"void"``, or ``"all"``.
        chunk_size: Pagination page size for SPARQL queries.
        class_chunk_size: Page size for Phase-1 class discovery.
        class_batch_size: Number of classes per VALUES query in Phase-2.
        delay: Delay between paginated pages (seconds).
        timeout: HTTP timeout per request (seconds).
        counts: Whether to fetch triple-count queries.
        reports: Write per-source analytics JSON reports.
        filter_service_namespaces: Strip service/system namespace patterns.
        untyped_as_classes: Treat untyped URI objects as ``owl:Class``.
        on_progress: Optional callback ``(dataset_name, index, total,
            status_or_error)``.

    Returns:
        Summary dict with keys ``"succeeded"``, ``"failed"``, ``"skipped"``.
    """
    from rdfsolve.sources import load_sources

    src_path: str | None = sources or sources_csv or None

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    entries = load_sources(src_path)

    succeeded: list[str] = []
    failed: list[dict[str, str]] = []
    skipped: list[str] = []

    total = len(entries)
    for idx, entry in enumerate(entries, 1):
        name = entry.get("name", "")
        endpoint = entry.get("endpoint", "")

        if not endpoint:
            logger.info("[%d/%d] Skipping %r: no endpoint", idx, total, name)
            skipped.append(name)
            if on_progress:
                on_progress(name, idx, total, "skipped")
            continue

        _mine_one_source(
            entry,
            idx=idx,
            total=total,
            out=out,
            fmt=fmt,
            chunk_size=chunk_size,
            class_chunk_size=class_chunk_size,
            class_batch_size=class_batch_size,
            delay=delay,
            timeout=timeout,
            counts=counts,
            reports=reports,
            filter_service_namespaces=filter_service_namespaces,
            untyped_as_classes=untyped_as_classes,
            authors=authors,
            on_progress=on_progress,
            succeeded=succeeded,
            failed=failed,
        )

    return {
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
    }
