"""Extract RDF schema patterns from SPARQL endpoints using SELECT queries for typed objects, literals, URIs, and blank nodes."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from rdfsolve._uri import get_local_name, pick_label
from rdfsolve.mining.one_shot_strategy import OneShotStrategy
from rdfsolve.mining.pattern_enrichment import (
    enrich_patterns_with_counts,
    enrich_patterns_with_labels,
)
from rdfsolve.mining.query_builders import (
    _DECOMP_CHUNK,
    _build_batched_blank_node_query,
    _build_batched_literal_count_query,
    _build_batched_literal_query,
    _build_batched_typed_count_query,
    _build_batched_typed_object_query,
    _build_batched_untyped_count_query,
    _build_batched_untyped_uri_query,
    _build_blank_node_query,
    _build_blank_node_query_plain,
    _build_cardinality_query,
    _build_class_discovery_query,
    _build_class_discovery_query_plain,
    _build_declared_classes_query,
    _build_example_query,
    _build_label_query,
    _build_literal_for_class_property_query,
    _build_literal_query,
    _build_literal_query_plain,
    _build_properties_for_class_query,
    _build_typed_object_for_class_property_query,
    _build_typed_object_query,
    _build_typed_object_query_plain,
    _build_untyped_uri_query,
    _build_untyped_uri_query_plain,
    _graph_clause,
    _values_block,
    pick_description,
)
from rdfsolve.mining.query_fallbacks import query_with_bisect
from rdfsolve.mining.report_tracking import ReportCollector
from rdfsolve.mining.single_pass_strategy import SinglePassStrategy
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
from rdfsolve.mining.types import ONTOLOGY_METACLASSES
from rdfsolve.models import (
    AboutMetadata,
    MinedSchema,
    MiningReport,
    OneShotQueryResult,
    PatternType,
    PhaseReport,
    QueryStats,
    SchemaPattern,
)
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointTimeoutError,
    PaginationTruncatedError,
    SparqlHelper,
)
from rdfsolve.version import VERSION

if TYPE_CHECKING:
    from rdfsolve.sources import SourceEntry

logger = logging.getLogger(__name__)

__all__ = [
    "SchemaMiner",
    "mine_schema",
]


# ReportCollector moved to rdfsolve.mining.report_tracking

# SchemaMiner


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
    strategy:
        Choose two-phase, single-pass, or one-shot mining.
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
        strategy: str | MiningStrategy | None = None,
        unsafe_paging: bool = False,
        report_path: str | Path | None = None,
        filter_service_namespaces: bool = True,
        untyped_as_classes: bool = False,
        authors: list[dict[str, str]] | None = None,
        qlever_version: dict[str, str] | None = None,
        sparql_engine: str = "",
        sparql_strategy: str = "",
        source_name: str = "",
        enrich: bool = False,
        examples_per_pattern: int = 2,
        max_response_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        """Initialize a SchemaMiner.

        Args:
            strategy: Mining strategy to use. Can be:
                - A string: "two-phase" (default), "single-pass", or "one-shot"
                - A MiningStrategy instance for custom strategies
                - None (uses "two-phase" as default)
        """
        self.endpoint_url = endpoint_url
        if not 0 <= examples_per_pattern <= 20:
            raise ValueError("examples_per_pattern must be between 0 and 20")
        self.enrich = enrich
        self.examples_per_pattern = examples_per_pattern
        self.graph_uris: list[str] | None = (
            [graph_uris] if isinstance(graph_uris, str) else graph_uris
        )
        self.chunk_size = chunk_size
        self.class_chunk_size = class_chunk_size
        self.class_batch_size = max(1, class_batch_size)
        self.delay = delay
        self.timeout = timeout
        self.counts = counts
        self.unsafe_paging = unsafe_paging
        self.filter_service_namespaces = filter_service_namespaces
        self.untyped_as_classes = untyped_as_classes
        self.authors = authors
        self.qlever_version = qlever_version

        # Handle strategy parameter - resolve string names to strategy instances
        self._strategy = self._resolve_strategy(strategy)

        self._helper = SparqlHelper(
            endpoint_url,
            timeout=timeout,
            sparql_engine=sparql_engine,
            sparql_strategy=sparql_strategy,
            source_name=source_name,
            inter_request_delay=delay,
            max_response_bytes=max_response_bytes,
        )
        self._report_path = Path(report_path) if report_path else None
        self._rc: ReportCollector | None = None
        self._ontology_classes: list[str] | None = None
        self._declared_classes: set[str] = set()
        self.last_report: MiningReport | None = None

    def _resolve_strategy(
        self,
        strategy: str | MiningStrategy | None,
    ) -> MiningStrategy:
        """Resolve a strategy name or use the supplied strategy."""
        # Handle strategy parameter
        if strategy is None:
            # Default to two-phase
            return TwoPhaseStrategy()

        if isinstance(strategy, MiningStrategy):
            # Already a strategy instance
            return strategy

        # String name - map to strategy class
        strategy_map: dict[str, type[MiningStrategy]] = {
            "two-phase": TwoPhaseStrategy,
            "single-pass": SinglePassStrategy,
            "one-shot": OneShotStrategy,
        }

        if strategy not in strategy_map:
            raise ValueError(
                f"Unknown strategy: {strategy}. Valid options: {', '.join(strategy_map.keys())}"
            )

        return strategy_map[strategy]()

    @property
    def _report(self) -> ReportCollector:
        """Return the active report collector (raises if not set)."""
        if self._rc is None:
            raise RuntimeError("mine() must be called first")
        return self._rc

    # public API

    def _build_strategy_string(self) -> str:
        """Return the strategy tag that describes the active mining flags."""
        base = f"miner/{self._strategy.name}"
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
                "source_name": self._helper.source_name,
                "graph_uris": self.graph_uris,
                "graph_scope": "within_named_graphs" if self.graph_uris else "endpoint_default",
                "sparql_engine": self._helper.sparql_engine,
                "sparql_strategy": self._helper.sparql_strategy,
                "chunk_size": self.chunk_size,
                "class_chunk_size": self.class_chunk_size,
                "class_batch_size": self.class_batch_size,
                "delay": self.delay,
                "timeout": self.timeout,
                "max_response_bytes": self._helper.max_response_bytes,
                "max_retries": self._helper.max_retries,
                "host_request_interval": self._helper.inter_request_delay,
                "counts": self.counts,
                "strategy": self._strategy.name,
                "untyped_as_classes": self.untyped_as_classes,
                "unsafe_paging": self.unsafe_paging,
                "filter_service_namespaces": self.filter_service_namespaces,
                "enrich": self.enrich,
                "examples_per_pattern": self.examples_per_pattern,
            },
        )
        self._rc = ReportCollector(report, self._report_path)

    def _run_patterns_phase(
        self,
    ) -> tuple[list[SchemaPattern], list[OneShotQueryResult] | None]:
        """Execute pattern mining using the configured strategy.

        Returns
        -------
        (patterns, one_shot_results)
            *one_shot_results* is ``None`` unless strategy is OneShotStrategy.
        """
        # Prepare mining context
        ontology_classes = getattr(self, "_ontology_classes", None)
        context = MiningContext(
            helper=self._helper,
            graph_uris=self.graph_uris,
            report=self._report,
            collect_bindings=self._collect_bindings,
            untyped_as_classes=self.untyped_as_classes,
            class_chunk_size=self.class_chunk_size,
            class_batch_size=self.class_batch_size,
            ontology_classes=ontology_classes,
            chunk_size=self.chunk_size,
            unsafe_paging=self.unsafe_paging,
        )

        # Run strategy
        patterns = self._strategy.mine(context)

        # Extract one-shot results if available
        one_shot_results = None
        if isinstance(self._strategy, OneShotStrategy):
            one_shot_results = self._report.report.one_shot_results or []

        return patterns, one_shot_results

    def _run_counts_phase(
        self,
        patterns: list[SchemaPattern],
    ) -> list[SchemaPattern]:
        """Run the counts phase and return enriched patterns."""
        phase = self._report.start_phase("counts")
        logger.info("Fetching triple counts …")
        try:
            patterns = enrich_patterns_with_counts(
                patterns,
                self._helper,
                self.graph_uris,
                self._report,
                self._collect_bindings,
                self.class_batch_size,
                self.class_chunk_size,
                self.unsafe_paging,
                self.delay,
            )
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
        patterns = enrich_patterns_with_labels(
            patterns,
            self._helper,
            self.graph_uris,
            self._report,
        )
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

    def _query_declared_classes(self) -> set[str]:
        """Query for classes formally declared as owl:Class or rdfs:Class.

        Returns a set of class URIs that have explicit class definitions
        in the dataset. This helps distinguish between:
        - Declared classes: formally defined in this dataset
        - Used types: URIs used as rdf:type but defined elsewhere (or not at all)
        """
        query = _build_declared_classes_query(self.graph_uris)
        declared: set[str] = set()
        try:
            raw = self._helper.select(query, purpose="declared_classes")
            bindings = raw.get("results", {}).get("bindings", [])
            for row in bindings:
                class_val = row.get("class", {}).get("value")
                if class_val:
                    declared.add(str(class_val))
            logger.info("Found %d declared classes (owl:Class/rdfs:Class)", len(declared))
        except Exception as e:
            raise RuntimeError(f"Class declarations could not be queried: {e}") from e
        return declared

    def _build_about_metadata(
        self,
        dataset_name: str | None,
        strategy: str,
        started_at: str,
        patterns: list[SchemaPattern],
        declared_class_count: int = 0,
        used_type_count: int = 0,
        discovered_metadata: dict[str, Any] | None = None,
    ) -> AboutMetadata:
        """Construct :class:`AboutMetadata` from the completed mining run.

        Merges user-provided metadata with auto-discovered metadata from the endpoint.
        """
        finished_at = self._report.report.finished_at
        total_classes = declared_class_count + used_type_count

        # Calculate unique property count from patterns
        unique_properties = {p.property_uri for p in patterns}
        property_count = len(unique_properties)

        # Merge discovered metadata (prefer discovered over None)
        discovered = discovered_metadata or {}

        return AboutMetadata.build(
            endpoint=self.endpoint_url,
            dataset_name=dataset_name,
            graph_uris=self.graph_uris,
            pattern_count=len(patterns),
            class_count=total_classes,
            declared_class_count=declared_class_count,
            used_type_count=used_type_count,
            property_count=property_count,
            strategy=strategy,
            started_at=started_at,
            finished_at=finished_at,
            total_duration_s=self._report.report.total_duration_s,
            authors=self.authors,
            qlever_version=self.qlever_version,
            # Discovered metadata
            title=discovered.get("title"),
            description=discovered.get("description"),
            source_license=discovered.get("source_license"),
            source_version=discovered.get("source_version"),
            source_version_iri=discovered.get("source_version_iri"),
            source_issued=discovered.get("source_issued"),
            source_modified=discovered.get("source_modified"),
            source_publisher=discovered.get("source_publisher"),
            source_creator=discovered.get("source_creator"),
            homepage=discovered.get("homepage"),
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

    def query_dataset_metadata(self) -> dict[str, Any]:
        """Query endpoint for dataset metadata using multiple patterns."""
        from rdfsolve.metadata import query_endpoint_metadata

        return query_endpoint_metadata(self._helper)

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
        with self._session(dataset_name):
            return self._finish_schema(self._mine_schema(dataset_name))

    @contextmanager
    def _session(self, dataset_name: str | None) -> Iterator[None]:
        """Start one report before any phase and retain failures."""
        self._ontology_classes = None
        self._declared_classes = set()
        self._init_report(
            dataset_name, self._build_strategy_string(), datetime.now(timezone.utc).isoformat()
        )
        self.last_report = self._report.report
        try:
            yield
        except BaseException as exc:
            reason = f"{type(exc).__name__}: {exc}"
            self._report.set_abort_reason(reason)
            report = self._report.report
            for phase in report.phases:
                if phase.finished_at is None:
                    self._report.finish_phase(phase, error=reason)
            self._report.finalise(
                pattern_count=report.pattern_count,
                class_count=report.class_count,
                property_count=report.property_count,
                uris_labelled=report.unique_uris_labelled,
            )
            raise

    def _finish_schema(
        self, schema: MinedSchema, annotation_iris: list[str] | None = None
    ) -> MinedSchema:
        """Filter the result and set final counts and times once."""
        if self.filter_service_namespaces:
            schema = self._apply_namespace_filter(schema)
        if self.enrich:
            schema.enrichment = self.query_enrichment(schema, annotation_iris=annotation_iris)
        classes, properties = self._collect_class_property_sets(schema.patterns)
        entity_counts = {}
        if self.counts:
            from rdfsolve.mining.pattern_enrichment import query_class_entity_counts

            entity_counts = query_class_entity_counts(
                sorted(classes),
                self._helper,
                self.graph_uris,
                self._report,
                self.class_batch_size,
                self.delay,
            )
        report = self._report.report
        report.strategy = schema.about.strategy or report.strategy
        self._report.finalise(
            pattern_count=len(schema.patterns),
            class_count=len(classes),
            property_count=len(properties),
            uris_labelled=report.unique_uris_labelled,
        )
        schema.about = self._build_about_metadata(
            report.dataset_name,
            report.strategy,
            report.started_at,
            schema.patterns,
            declared_class_count=len(classes & self._declared_classes),
            used_type_count=len(classes - self._declared_classes),
            discovered_metadata=report.discovered_metadata,
        )
        schema.about.class_entity_counts = entity_counts
        return schema

    def query_enrichment(
        self, schema: MinedSchema, *, annotation_iris: list[str] | None = None
    ) -> SchemaEnrichment:
        """Query definitions and observed examples with this miner's settings.

        Assign the return value to ``schema.enrichment`` when called after mining.
        No ontology or external vocabulary download is attempted.
        """
        from rdfsolve.mining.enrichment import query_enrichment

        return query_enrichment(
            schema,
            self._helper,
            self.graph_uris,
            examples_per_pattern=self.examples_per_pattern,
            delay=self.delay,
            report=self._rc,
            annotation_iris=annotation_iris,
        )

    def _mine_schema(self, dataset_name: str | None) -> MinedSchema:
        """Mine data patterns within the active report session."""
        strategy = self._report.report.strategy
        started_at = self._report.report.started_at

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

        classes, _ = self._collect_class_property_sets(
            patterns,
        )

        # Query for formally declared classes (owl:Class / rdfs:Class)
        declared_classes = self._query_declared_classes()
        self._declared_classes = declared_classes
        declared_in_patterns = declared_classes & classes
        used_types = classes - declared_classes

        logger.info(
            "Class breakdown: %d declared (in patterns: %d), %d used-only types",
            len(declared_classes),
            len(declared_in_patterns),
            len(used_types),
        )

        self._report.report.unique_uris_labelled = len(uris_before)
        if one_shot_results is not None:
            self._report.report.one_shot_results = one_shot_results
            self._report.flush()
        # Query dataset metadata for MiningReport (not VoID)
        phase = self._report.start_phase("dataset-metadata")
        discovered_metadata: dict[str, Any] = {}
        try:
            logger.info("Querying dataset metadata...")
            discovered_metadata = self.query_dataset_metadata()
            if discovered_metadata:
                logger.info("Discovered metadata: %s", list(discovered_metadata.keys()))
                self._report.report.discovered_metadata = discovered_metadata
                self._report.flush()
        except Exception as e:
            logger.warning("Could not query dataset metadata: %s", e)
            self._report.finish_phase(phase, error=str(e))
        else:
            self._report.finish_phase(phase, items=len(discovered_metadata))

        about = self._build_about_metadata(
            dataset_name,
            strategy,
            started_at,
            patterns,
            declared_class_count=len(declared_in_patterns),
            used_type_count=len(used_types),
            discovered_metadata=discovered_metadata if discovered_metadata else {},
        )
        schema = MinedSchema(patterns=patterns, about=about)

        return schema

    # helpers

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

    # private query runners

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
        try:
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
        except PaginationTruncatedError as error:
            error.partial_rows = all_bindings + error.partial_rows
            raise
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


# Convenience function


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
    strategy: str | MiningStrategy | None = None,
    report_path: str | Path | None = None,
    filter_service_namespaces: bool = True,
    untyped_as_classes: bool = False,
    authors: list[dict[str, str]] | None = None,
    qlever_version: dict[str, str] | None = None,
    sparql_engine: str = "",
    sparql_strategy: str = "",
    source_name: str = "",
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
    strategy:
        Mining strategy to use. Can be "two-phase" (default),
        "single-pass", "one-shot", or a MiningStrategy instance.
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
        strategy=strategy,
        report_path=report_path,
        filter_service_namespaces=filter_service_namespaces,
        untyped_as_classes=untyped_as_classes,
        authors=authors,
        qlever_version=qlever_version,
        sparql_engine=sparql_engine,
        sparql_strategy=sparql_strategy,
        source_name=source_name,
    )
    return miner.mine(dataset_name=dataset_name)
