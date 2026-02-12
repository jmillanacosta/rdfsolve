"""
Schema Miner – extract RDF schema patterns via simple SELECT queries.

Instead of building VoID on the endpoint with heavy CONSTRUCT + BIND
queries, this module runs three lightweight SELECT DISTINCT queries
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

The primary export is :class:`MinedSchema` (→ JSON-LD).  It can also
be converted to a VoID graph for downstream LinkML / SHACL / RDF-config
export via :class:`~rdfsolve.parser.VoidParser`.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rdfsolve.models import (
    AboutMetadata,
    MinedSchema,
    MiningReport,
    PhaseReport,
    QueryStats,
    SchemaPattern,
)
from rdfsolve.sparql_helper import SparqlHelper
from rdfsolve.utils import get_local_name, pick_label
from rdfsolve.version import VERSION

logger = logging.getLogger(__name__)

__all__ = [
    "SchemaMiner",
    "mine_schema",
]


# -------------------------------------------------------------------
# SPARQL query templates (braces pre-escaped for str.format)
# -------------------------------------------------------------------

def _graph_clause(
    graph_uris: list[str] | None,
) -> tuple[str, str]:
    """Return (open, close) strings for an optional GRAPH clause.

    If *graph_uris* is ``None`` → empty strings (default graph).
    If a single URI → ``GRAPH <uri> {`` / ``}``.
    If multiple → VALUES-based pattern.
    """
    if not graph_uris:
        return "", ""
    if len(graph_uris) == 1:
        return f"GRAPH <{graph_uris[0]}> {{", "}"
    # Multiple graphs – use VALUES
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


def _build_typed_count_query(
    graph_uris: list[str] | None,
) -> str:
    """Count query for typed-object patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?sc ?p ?oc (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    ?o a ?oc .
  {g_close}
}}
GROUP BY ?sc ?p ?oc"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_literal_count_query(
    graph_uris: list[str] | None,
) -> str:
    """Count query for literal patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?sc ?p ?dt (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isLiteral(?o))
    BIND(DATATYPE(?o) AS ?dt)
  {g_close}
}}
GROUP BY ?sc ?p ?dt"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_untyped_count_query(
    graph_uris: list[str] | None,
) -> str:
    """Count query for untyped-URI patterns."""
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?sc ?p (COUNT(*) AS ?cnt)
WHERE {{
  {g_open}
    ?s ?p ?o .
    ?s a ?sc .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}
GROUP BY ?sc ?p"""
    return SparqlHelper.prepare_paginated_query(q)


def _build_label_query(
    uris: list[str],
    graph_uris: list[str] | None,
) -> str:
    """Fetch rdfs:label and dc:title for a set of URIs.

    Returns bindings with ``?uri``, ``?rdfsLabel``, ``?dcTitle``.
    Priority is resolved in Python via :func:`pick_label`.
    """
    values = " ".join(f"(<{u}>)" for u in uris)
    g_open, g_close = _graph_clause(graph_uris)
    q = f"""\
SELECT ?uri ?rdfsLabel ?dcTitle
WHERE {{
  VALUES (?uri) {{ {values} }}
  {g_open}
    OPTIONAL {{ ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?rdfsLabel . }}
    OPTIONAL {{ ?uri <http://purl.org/dc/elements/1.1/title> ?dcTitle . }}
    OPTIONAL {{ ?uri <http://purl.org/dc/terms/title> ?dcTitle . }}
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

def _values_block(class_uris: list[str]) -> str:
    """Build a ``VALUES ?class { <u1> <u2> … }`` clause."""
    entries = " ".join(f"<{u}>" for u in class_uris)
    return f"VALUES ?class {{ {entries} }}"


def _build_batched_typed_object_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
) -> str:
    """Typed-object patterns for a batch of classes."""
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    return f"""\
SELECT DISTINCT ?class ?p ?oc
WHERE {{
  {values}
  {g_open}
    ?s a ?class .
    ?s ?p ?o .
    ?o a ?oc .
  {g_close}
}}"""


def _build_batched_literal_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
) -> str:
    """Literal patterns for a batch of classes."""
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    return f"""\
SELECT DISTINCT ?class ?p (DATATYPE(?o) AS ?dt)
WHERE {{
  {values}
  {g_open}
    ?s a ?class .
    ?s ?p ?o .
    FILTER(isLiteral(?o))
  {g_close}
}}"""


def _build_batched_untyped_uri_query(
    class_uris: list[str],
    graph_uris: list[str] | None,
) -> str:
    """Untyped-URI patterns for a batch of classes."""
    g_open, g_close = _graph_clause(graph_uris)
    values = _values_block(class_uris)
    return f"""\
SELECT DISTINCT ?class ?p
WHERE {{
  {values}
  {g_open}
    ?s a ?class .
    ?s ?p ?o .
    FILTER(isURI(?o))
    FILTER NOT EXISTS {{ ?o a ?any }}
  {g_close}
}}"""


# -------------------------------------------------------------------
# Report collector
# -------------------------------------------------------------------


class _ReportCollector:
    """Accumulates analytics during a mining run.

    Writes the JSON report to *report_path* after each phase so
    partial data is persisted even if the process crashes.
    """

    def __init__(
        self,
        report: MiningReport,
        report_path: Path | None = None,
    ) -> None:
        self._report = report
        self._path = report_path

    # ── Query tracking ─────────────────────────────────────────────

    def record_query(
        self,
        purpose: str,
        duration_s: float,
        success: bool = True,
    ) -> None:
        """Record one query execution."""
        stats = self._report.query_stats.setdefault(
            purpose, QueryStats(),
        )
        stats.sent += 1
        stats.total_time_s += duration_s
        self._report.total_queries_sent += 1
        if not success:
            stats.failed += 1
            self._report.total_queries_failed += 1

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
                (now - started).total_seconds(), 3,
            )
        phase.items_discovered = items
        phase.error = error
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
                (finished - started).total_seconds(), 3,
            )
        r.pattern_count = pattern_count
        r.class_count = class_count
        r.property_count = property_count
        r.unique_uris_labelled = uris_labelled
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
        Use two-phase mining for large endpoints.  Phase 1
        discovers all ``rdf:type`` classes; phase 2 queries
        properties per class.  Much gentler on heavyweight
        endpoints like QLever/PubChem/UniProt.
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
        two_phase: bool = False,
        report_path: str | Path | None = None,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.graph_uris: list[str] | None = (
            [graph_uris] if isinstance(graph_uris, str)
            else graph_uris
        )
        self.chunk_size = chunk_size
        self.class_chunk_size = class_chunk_size
        self.class_batch_size = max(1, class_batch_size)
        self.delay = delay
        self.timeout = timeout
        self.counts = counts
        self.two_phase = two_phase
        self._helper = SparqlHelper(
            endpoint_url, timeout=timeout,
        )
        self._report_path = (
            Path(report_path) if report_path else None
        )
        self._rc: _ReportCollector | None = None  # type: ignore[assignment]
        self.last_report: MiningReport | None = None

    # ---- public API -----------------------------------------------

    def mine(
        self,
        dataset_name: str | None = None,
    ) -> MinedSchema:
        """Run all queries and return a :class:`MinedSchema`.

        Parameters
        ----------
        dataset_name:
            Optional human-readable name attached to the metadata.

        The method also populates a :class:`MiningReport` with
        per-phase timing, query counts, and failure stats.  If a
        *report_path* was given at construction time, the JSON is
        flushed to disk after each phase completes.
        """
        strategy = (
            "miner/two-phase" if self.two_phase
            else "miner"
        )
        # Initialise analytics collector
        report = MiningReport(
            dataset_name=dataset_name,
            endpoint_url=self.endpoint_url,
            graph_uris=self.graph_uris,
            strategy=strategy,
            rdfsolve_version=VERSION,
            python_version=sys.version,
            started_at=datetime.now(
                timezone.utc,
            ).isoformat(),
            config={
                "chunk_size": self.chunk_size,
                "class_chunk_size": self.class_chunk_size,
                "delay": self.delay,
                "timeout": self.timeout,
                "counts": self.counts,
                "two_phase": self.two_phase,
            },
        )
        self._rc = _ReportCollector(
            report, self._report_path,
        )

        t0 = time.monotonic()

        if self.two_phase:
            patterns = self._mine_two_phase()
        else:
            patterns = self._mine_single_pass()

        # Optional: counts (skip for two-phase — too expensive)
        if self.counts and not self.two_phase:
            phase = self._rc.start_phase("counts")
            logger.info("Fetching triple counts …")
            try:
                patterns = self._enrich_counts(patterns)
                self._rc.finish_phase(
                    phase, items=len(patterns),
                )
            except Exception as exc:
                self._rc.finish_phase(
                    phase, error=str(exc),
                )
                raise

        # Fetch rdfs:label / dc:title for all URIs
        phase = self._rc.start_phase("labels")
        logger.info("Fetching labels …")
        uris_before = self._unique_uris(patterns)
        patterns = self._enrich_labels(patterns)
        self._rc.finish_phase(
            phase, items=len(uris_before),
        )

        dt = time.monotonic() - t0
        logger.info(
            f"Mining complete: {len(patterns)} patterns "
            f"in {dt:.1f}s"
        )

        # Compute final class/property counts
        classes: set[str] = set()
        properties: set[str] = set()
        for p in patterns:
            classes.add(p.subject_class)
            if p.object_class not in ("Literal", "Resource"):
                classes.add(p.object_class)
            properties.add(p.property_uri)

        self._rc.finalise(
            pattern_count=len(patterns),
            class_count=len(classes),
            property_count=len(properties),
            uris_labelled=len(uris_before),
        )
        self.last_report = self._rc.report

        about = AboutMetadata.build(
            endpoint=self.endpoint_url,
            dataset_name=dataset_name,
            graph_uris=self.graph_uris,
            pattern_count=len(patterns),
            strategy=strategy,
        )

        return MinedSchema(patterns=patterns, about=about)

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

        phase = self._rc.start_phase("typed-object")
        logger.info("Mining typed-object patterns …")
        typed = self._run_typed_object()
        patterns.extend(typed)
        logger.info(f"  → {len(typed)} typed-object patterns")
        self._rc.finish_phase(phase, items=len(typed))

        phase = self._rc.start_phase("literal")
        logger.info("Mining literal patterns …")
        literals = self._run_literal()
        patterns.extend(literals)
        logger.info(f"  → {len(literals)} literal patterns")
        self._rc.finish_phase(phase, items=len(literals))

        phase = self._rc.start_phase("untyped-uri")
        logger.info("Mining untyped-URI patterns …")
        untyped = self._run_untyped_uri()
        patterns.extend(untyped)
        logger.info(f"  → {len(untyped)} untyped-URI patterns")
        self._rc.finish_phase(phase, items=len(untyped))

        return patterns

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
        size — useful when the endpoint returns *very* many classes.
        """
        # Phase 1 — discover classes
        p1 = self._rc.start_phase("class-discovery")
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
                    q, purpose="two-phase/classes",
                )
                class_bindings = (
                    result.get("results", {})
                    .get("bindings", [])
                )
                self._rc.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                )
            except Exception:
                self._rc.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                    success=False,
                )
                raise
        else:
            logger.info(
                "Phase 1: discovering classes "
                "(chunk_size=%d) …",
                ccs,
            )
            q = _build_class_discovery_query(self.graph_uris)
            class_bindings = self._collect_bindings(
                q,
                purpose="two-phase/classes",
                chunk_size=ccs,
            )
        classes = [
            b.get("class", {}).get("value", "")
            for b in class_bindings
        ]
        classes = [c for c in classes if c]
        logger.info(f"  → {len(classes)} classes found")
        self._rc.finish_phase(p1, items=len(classes))

        # Phase 2 — batched per-class pattern discovery
        p2 = self._rc.start_phase("per-class-patterns")
        bs = self.class_batch_size
        n_batches = (len(classes) + bs - 1) // bs
        logger.info(
            "Phase 2: mining patterns in %d batches of "
            "≤%d classes (%d classes total) …",
            n_batches, bs, len(classes),
        )
        patterns: list[SchemaPattern] = []
        total = len(classes)

        for batch_idx in range(n_batches):
            batch_start = batch_idx * bs
            batch = classes[batch_start : batch_start + bs]
            batch_label = (
                f"batch {batch_idx + 1}/{n_batches} "
                f"(classes {batch_start + 1}"
                f"–{batch_start + len(batch)}/{total})"
            )
            logger.info("  %s", batch_label)

            # 2a. Typed-object patterns for this batch
            t0 = time.monotonic()
            try:
                q = _build_batched_typed_object_query(
                    batch, self.graph_uris,
                )
                result = self._helper.select(
                    q, purpose="two-phase/typed-object",
                )
                self._rc.record_query(
                    "two-phase/typed-object",
                    time.monotonic() - t0,
                )
                for b in result.get(
                    "results", {},
                ).get("bindings", []):
                    cls = b.get(
                        "class", {},
                    ).get("value", "")
                    p = b.get("p", {}).get("value", "")
                    oc = b.get("oc", {}).get("value", "")
                    if cls and p and oc:
                        patterns.append(SchemaPattern(
                            subject_class=cls,
                            property_uri=p,
                            object_class=oc,
                        ))
            except Exception as e:
                self._rc.record_query(
                    "two-phase/typed-object",
                    time.monotonic() - t0,
                    success=False,
                )
                logger.warning(
                    "  typed-object failed for %s: %s",
                    batch_label, e,
                )

            # 2b. Literal patterns for this batch
            t0 = time.monotonic()
            try:
                q = _build_batched_literal_query(
                    batch, self.graph_uris,
                )
                result = self._helper.select(
                    q, purpose="two-phase/literal",
                )
                self._rc.record_query(
                    "two-phase/literal",
                    time.monotonic() - t0,
                )
                for b in result.get(
                    "results", {},
                ).get("bindings", []):
                    cls = b.get(
                        "class", {},
                    ).get("value", "")
                    p = b.get("p", {}).get("value", "")
                    dt = b.get("dt", {}).get("value")
                    if cls and p:
                        patterns.append(SchemaPattern(
                            subject_class=cls,
                            property_uri=p,
                            object_class="Literal",
                            datatype=dt if dt else None,
                        ))
            except Exception as e:
                self._rc.record_query(
                    "two-phase/literal",
                    time.monotonic() - t0,
                    success=False,
                )
                logger.warning(
                    "  literal failed for %s: %s",
                    batch_label, e,
                )

            # 2c. Untyped-URI patterns for this batch
            t0 = time.monotonic()
            try:
                q = _build_batched_untyped_uri_query(
                    batch, self.graph_uris,
                )
                result = self._helper.select(
                    q, purpose="two-phase/untyped-uri",
                )
                self._rc.record_query(
                    "two-phase/untyped-uri",
                    time.monotonic() - t0,
                )
                for b in result.get(
                    "results", {},
                ).get("bindings", []):
                    cls = b.get(
                        "class", {},
                    ).get("value", "")
                    p = b.get("p", {}).get("value", "")
                    if cls and p:
                        patterns.append(SchemaPattern(
                            subject_class=cls,
                            property_uri=p,
                            object_class="Resource",
                        ))
            except Exception as e:
                self._rc.record_query(
                    "two-phase/untyped-uri",
                    time.monotonic() - t0,
                    success=False,
                )
                logger.warning(
                    "  untyped-uri failed for %s: %s",
                    batch_label, e,
                )

            # Polite delay between batches
            if self.delay > 0:
                time.sleep(self.delay)

        logger.info(
            f"  → {len(patterns)} total patterns "
            f"from {total} classes"
        )
        self._rc.finish_phase(p2, items=len(patterns))
        return patterns

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
        for chunk in self._helper.select_chunked(
            query_template,
            chunk_size=effective,
            delay_between_chunks=self.delay,
            purpose=purpose,
        ):
            all_bindings.extend(chunk)
            # Each chunk is one HTTP round-trip; record it.
            if has_rc:
                self._rc.record_query(purpose, 0.0)
        return all_bindings

    def _run_typed_object(self) -> list[SchemaPattern]:
        """Run the typed-object SELECT query."""
        q = _build_typed_object_query(self.graph_uris)
        bindings = self._collect_bindings(
            q, purpose="mining/typed-object",
        )
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            oc = b.get("oc", {}).get("value", "")
            if sc and p and oc:
                results.append(SchemaPattern(
                    subject_class=sc,
                    property_uri=p,
                    object_class=oc,
                ))
        return results

    def _run_literal(self) -> list[SchemaPattern]:
        """Run the literal-property SELECT query."""
        q = _build_literal_query(self.graph_uris)
        bindings = self._collect_bindings(
            q, purpose="mining/literal",
        )
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            dt = b.get("dt", {}).get("value")
            if sc and p:
                results.append(SchemaPattern(
                    subject_class=sc,
                    property_uri=p,
                    object_class="Literal",
                    datatype=dt if dt else None,
                ))
        return results

    def _run_untyped_uri(self) -> list[SchemaPattern]:
        """Run the untyped-URI SELECT query."""
        q = _build_untyped_uri_query(self.graph_uris)
        bindings = self._collect_bindings(
            q, purpose="mining/untyped-uri",
        )
        results: list[SchemaPattern] = []
        for b in bindings:
            sc = b.get("sc", {}).get("value", "")
            p = b.get("p", {}).get("value", "")
            if sc and p:
                results.append(SchemaPattern(
                    subject_class=sc,
                    property_uri=p,
                    object_class="Resource",
                ))
        return results

    def _enrich_counts(
        self, patterns: list[SchemaPattern],
    ) -> list[SchemaPattern]:
        """Run COUNT queries and merge counts into patterns."""
        # Build lookup: (sc, p, oc) → count
        counts: dict[tuple[str, str, str], int] = {}

        # Typed-object counts
        try:
            q = _build_typed_count_query(self.graph_uris)
            for b in self._collect_bindings(
                q, purpose="counts/typed-object",
            ):
                key = (
                    b.get("sc", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    b.get("oc", {}).get("value", ""),
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(cnt)
        except Exception as e:
            logger.warning(f"Typed-object count query failed: {e}")

        # Literal counts
        try:
            q = _build_literal_count_query(self.graph_uris)
            for b in self._collect_bindings(
                q, purpose="counts/literal",
            ):
                dt = b.get("dt", {}).get("value", "")
                key = (
                    b.get("sc", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    f"Literal:{dt}" if dt else "Literal",
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(cnt)
        except Exception as e:
            logger.warning(f"Literal count query failed: {e}")

        # Untyped-URI counts
        try:
            q = _build_untyped_count_query(self.graph_uris)
            for b in self._collect_bindings(
                q, purpose="counts/untyped-uri",
            ):
                key = (
                    b.get("sc", {}).get("value", ""),
                    b.get("p", {}).get("value", ""),
                    "Resource",
                )
                cnt = b.get("cnt", {}).get("value")
                if cnt:
                    counts[key] = int(cnt)
        except Exception as e:
            logger.warning(f"Untyped-URI count query failed: {e}")

        # Merge counts into patterns
        enriched: list[SchemaPattern] = []
        for pat in patterns:
            # Try exact key
            if pat.object_class == "Literal":
                dt_key = (
                    f"Literal:{pat.datatype}"
                    if pat.datatype
                    else "Literal"
                )
                key = (
                    pat.subject_class, pat.property_uri, dt_key,
                )
            else:
                key = (
                    pat.subject_class,
                    pat.property_uri,
                    pat.object_class,
                )
            cnt = counts.get(key)
            enriched.append(pat.model_copy(update={"count": cnt}))

        return enriched

    def _enrich_labels(
        self, patterns: list[SchemaPattern],
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
            batch = uri_list[start:start + batch_size]
            t0 = time.monotonic()
            try:
                q = _build_label_query(batch, self.graph_uris)
                result = self._helper.select(q, purpose="labels")
                if hasattr(self, "_rc"):
                    self._rc.record_query(
                        "labels", time.monotonic() - t0,
                    )
                bindings = (
                    result.get("results", {})
                    .get("bindings", [])
                )
                for b in bindings:
                    uri = b.get("uri", {}).get("value", "")
                    if not uri or uri in label_map:
                        continue
                    rdfs_lbl = b.get(
                        "rdfsLabel", {},
                    ).get("value")
                    dc_lbl = b.get(
                        "dcTitle", {},
                    ).get("value")
                    label_map[uri] = pick_label(
                        rdfs_lbl, dc_lbl, uri,
                    )
            except Exception as e:
                if hasattr(self, "_rc"):
                    self._rc.record_query(
                        "labels", time.monotonic() - t0,
                        success=False,
                    )
                logger.warning(
                    f"Label batch failed ({len(batch)} URIs): {e}"
                )

        # Fill in labels using local name as fallback
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
    two_phase: bool = False,
    report_path: str | Path | None = None,
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
        mode.  ``None`` (default) disables pagination — the class
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
        Use two-phase mining (discover classes first, then per-class
        queries). Required for large endpoints like QLever.
    report_path:
        If given, write an analytics JSON report to this path.
        The file is updated incrementally after each mining phase.

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
    )
    return miner.mine(dataset_name=dataset_name)
