"""Extract RDF schema patterns from SPARQL endpoints using SELECT queries for typed objects, literals, URIs, and blank nodes."""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

import pyoxigraph as ox
from rdflib import Graph, URIRef

from rdfsolve._outcomes import QueryFailure, QueryOutcome, QueryState
from rdfsolve.local_rdf import LocalBackend
from rdfsolve.mining.one_shot_strategy import OneShotStrategy
from rdfsolve.mining.pattern_enrichment import (
    enrich_patterns_with_counts,
    enrich_patterns_with_labels,
)
from rdfsolve.mining.query_builders import (
    MEMBERSHIP,
    _build_declared_classes_query,
)
from rdfsolve.mining.report_tracking import ReportCollector
from rdfsolve.mining.sampling import DEFAULT_SAMPLE_SIZE, SAMPLE_SIZE
from rdfsolve.mining.single_pass_strategy import SinglePassStrategy
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.structural_strategy import StructuralStrategy
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
from rdfsolve.models import (
    AboutMetadata,
    MinedSchema,
    MiningReport,
    OneShotQueryResult,
    SchemaPattern,
)
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.schema_models.pattern import PatternType
from rdfsolve.schema_models.structural import StructuralPattern
from rdfsolve.sparql_helper import (
    PaginationTruncatedError,
    ResponseLimitError,
    SparqlHelper,
)
from rdfsolve.version import VERSION

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

logger = logging.getLogger(__name__)

__all__ = [
    "SchemaMiner",
    "mine_schema",
]


def _failed_classes(checkpoint: str | Path) -> set[str]:
    """Return the classes with a two-phase failure in the report beside a checkpoint."""
    import json

    path = Path(str(checkpoint).removesuffix(".checkpoint.jsonl") + ".json")
    try:
        failures = json.loads(path.read_text(encoding="utf-8")).get("query_failures") or []
    except (OSError, ValueError):
        return set()
    return {
        str(c)
        for f in failures
        if str(f.get("purpose") or "").startswith("two-phase")
        for c in f.get("classes") or []
    }


class SchemaMiner:
    """Mine RDF schema patterns from a SPARQL endpoint."""

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
        enrich: bool = False,
        classes_as_data: bool = False,
        membership_properties: list[str] | None = None,
        examples_per_pattern: int = 2,
        max_response_bytes: int = 64 * 1024 * 1024,
        get_graphs_from_store: bool = False,
        graph_store_url: str | None = None,
        graph_store_dir: str | Path = "graph-store",
        graph_store_max_bytes: int = 64 * 1024 * 1024,
        pagination: Literal["offset", "cursor"] = "offset",
        excluded_graph_prefixes: tuple[str, ...] = (),
        type_context_graph_uris: list[str] | None = None,
        local_backend: LocalBackend = "oxigraph",
        resume_checkpoint: str | Path | None = None,
        untyped_subjects: bool = True,
        sample_size: int = DEFAULT_SAMPLE_SIZE,
    ) -> None:
        """Initialize a SchemaMiner.

        *classes_as_data* is for a source that keeps its records as classes (each entity an
        rdfs:Class under its kind): with ontology-as-data,
        the rows of these terms join the typed patterns and are grouped under their ancestors.
        *membership_properties* are the properties that place a record in its class when the
        source does not use rdf:type alone (rdf:type and a category, for example).
        With *untyped_subjects*, the IRI subjects that have no type get property-level
        patterns (subject_binding "untyped"; rdfsolve.mining.untyped_subjects), so that data
        without classes has a schema; a scan run counts them from its rows.
        *sample_size* is the first sample (edges or members) of a query that the endpoint
        refuses after every other fallback: its rows are kept, flagged sampled, with counts
        that are lower bounds (rdfsolve.mining.sampling); 0 turns sampling off.
        """
        self.endpoint_url = endpoint_url
        self.untyped_subjects = untyped_subjects
        self.sample_size = sample_size
        self.classes_as_data = classes_as_data
        self.membership_properties = list(membership_properties or [])
        self.local_backend = local_backend
        if pagination not in {"offset", "cursor"}:
            raise ValueError("pagination must be offset or cursor")
        if pagination == "cursor" and unsafe_paging:
            raise ValueError("Cursor paging requires distinct rows; disable unsafe_paging")
        self.pagination = pagination
        self.get_graphs_from_store = get_graphs_from_store
        self.graph_store_url = graph_store_url
        self.graph_store_dir = Path(graph_store_dir)
        self.graph_store_max_bytes = graph_store_max_bytes
        if get_graphs_from_store and (not graph_store_url or not graph_uris):
            raise ValueError("Graph Store mining requires graph_store_url and graph_uris")
        if not 0 <= examples_per_pattern <= 20:
            raise ValueError("examples_per_pattern must be between 0 and 20")
        self.enrich = enrich
        self.examples_per_pattern = examples_per_pattern
        self.graph_uris: list[str] | None = (
            [graph_uris] if isinstance(graph_uris, str) else graph_uris
        )
        self.type_context_graph_uris = list(dict.fromkeys(type_context_graph_uris or []))
        self.chunk_size = chunk_size
        self.class_chunk_size = class_chunk_size
        self.class_batch_size = max(1, class_batch_size)
        self.delay = delay
        self.timeout = timeout
        self.counts = counts
        self.unsafe_paging = unsafe_paging
        self.filter_service_namespaces = filter_service_namespaces
        self.excluded_graph_prefixes = tuple(excluded_graph_prefixes)
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
            inter_request_delay=delay,
            max_response_bytes=max_response_bytes,
        )
        self._report_path = Path(report_path) if report_path else None
        # Read now: a report session clears its own checkpoint path when it starts.
        self._resume = (
            (str(resume_checkpoint), Path(resume_checkpoint).read_text(encoding="utf-8"))
            if resume_checkpoint
            else None
        )
        # A checkpoint written before batch states were recorded is read with the report
        # beside it: a batch with a class that has a two-phase failure there is mined again.
        self._resume_failed = _failed_classes(resume_checkpoint) if resume_checkpoint else set()
        self._rc: ReportCollector | None = None
        self._ontology_classes: list[str] | None = None
        self._ontology_term_budget: int | None = None
        self._group_before_mining: int | None = None
        # Seconds for each query of the ontology-term probe of a VoID-first source, which then
        # gives up at the first query that does not answer in time (no page recovery): UniProt
        # spent 1337 s in four paged attempts of ontology-terms/object, each at the 330 s
        # deadline, and the probe found nothing (job 115330). None: the probe is not limited.
        self.light_probe_seconds: float | None = None
        # Most type values that class discovery lists (None: all); the remote stage sets it.
        self.class_listing_limit: int | None = None
        self._hierarchy_files: list[str] = []
        self._ontology_graph_uris: list[str] | None = None
        self._class_batches: list[list[str]] | None = None
        self._shared_extensions: dict[str, str] = {}
        self._subsumed_classes: set[str] = set()
        self._grouped_members: dict[str, list[str]] = {}
        self._scan_types: Any = None
        self._scan_grouping: Any = None
        self._declared_classes: set[str] = set()
        self.last_report: MiningReport | None = None
        self._structural_patterns: list[StructuralPattern] | None = None

    @property
    def _membership(self) -> str | None:
        """Property that classifies subjects when the strategy does not use rdf:type."""
        return self._strategy.membership_property

    @property
    def _own_view(self) -> bool:
        """Whether the strategy classifies and describes its rows without whole-data phases."""
        return bool(self._membership) or self._strategy.scoped

    @property
    def helper(self) -> SparqlHelper:
        """Expose the miner's helper for query recording and later exploration.

        The miner owns this helper and closes it on exit.
        """
        return self._helper

    @classmethod
    def from_graph(
        cls,
        graph: Graph | ox.Dataset | ox.Store,
        *,
        endpoint_url: str = "urn:rdfsolve:local",
        **kwargs: Any,
    ) -> Self:
        """Mine a bounded RDF snapshot using the existing SPARQL mining strategy.

        The snapshot is an RDFLib graph or dataset, or Oxigraph data (for example
        ``pyoxigraph.Dataset(pyoxigraph.parse(path="data.ttl"))``).
        """
        from rdflib import Dataset

        from rdfsolve.mining.local_graph import LocalGraphHelper

        if isinstance(graph, (ox.Dataset, ox.Store)):
            miner = cls(endpoint_url, **kwargs)
            miner._helper.close()
            miner._helper = LocalGraphHelper(endpoint_url, graph, backend=miner.local_backend)
            return miner
        dataset = graph if isinstance(graph, Dataset) else Dataset()
        if dataset is not graph:
            for prefix, namespace in graph.namespaces():
                dataset.bind(prefix, namespace, replace=True)
            for triple in graph:
                dataset.default_graph.add(triple)
        miner = cls(endpoint_url, **kwargs)
        miner._helper.close()
        miner._helper = LocalGraphHelper(endpoint_url, dataset, backend=miner.local_backend)
        return miner

    def close(self) -> None:
        """Release the HTTP session."""
        self._helper.close()

    def __enter__(self) -> Self:
        """Use the miner as a context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Release the HTTP session after mining."""
        self.close()

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
            "structural": StructuralStrategy,
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

    @property
    def declared_classes(self) -> frozenset[str]:
        """Return the classes the last run found declared as owl:Class or rdfs:Class."""
        return frozenset(self._declared_classes)

    @property
    def subsumed_classes(self) -> frozenset[str]:
        """Return the classes that stand for subsumed ontology terms in the last run."""
        return frozenset(self._subsumed_classes)

    def count_class_entities(
        self, classes: list[str], graph_uris: list[str] | None
    ) -> tuple[dict[str, int], dict[str, QueryState]]:
        """Count typed entities per class in *graph_uris* after a run.

        Query outcomes are recorded in the last run's report.
        """
        from rdfsolve.mining.pattern_enrichment import query_class_entity_counts

        states: dict[str, QueryState] = {}
        counts = query_class_entity_counts(
            sorted(set(classes)),
            self._helper,
            graph_uris,
            self._report,
            self.class_batch_size,
            self.delay,
            states_out=states,
            type_context_graph_uris=self.type_context_graph_uris,
        )
        self._report.flush()
        return counts, states

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
                "graph_uris": self.graph_uris,
                "type_context_graph_uris": self.type_context_graph_uris,
                "graph_scope": "within_named_graphs" if self.graph_uris else "endpoint_default",
                "sparql_engine": self._helper.sparql_engine,
                "sparql_strategy": self._helper.sparql_strategy,
                "chunk_size": self.chunk_size,
                "pagination": self.pagination,
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
                "untyped_subject_patterns": self.untyped_subjects,
                "sample_size": self.sample_size,
                "unsafe_paging": self.unsafe_paging,
                "filter_service_namespaces": self.filter_service_namespaces,
                "enrich": self.enrich,
                "examples_per_pattern": self.examples_per_pattern,
            },
        )
        from rdfsolve.mining.local_graph import LocalGraphHelper

        if isinstance(self._helper, LocalGraphHelper):
            report.config["local_backend"] = self._helper.local.metadata()
        self._rc = ReportCollector(report, self._report_path)
        self._rc.flush()

    def _run_patterns_phase(
        self,
    ) -> tuple[list[SchemaPattern], list[OneShotQueryResult] | None]:
        """Execute pattern mining using the configured strategy."""
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
            excluded_graph_prefixes=self.excluded_graph_prefixes,
            type_context_graph_uris=self.type_context_graph_uris,
            ontology_graph_uris=self._ontology_graph_uris,
            ontology_term_budget=self._ontology_term_budget,
            group_before_mining=self._group_before_mining,
            ontology_hierarchy_files=self._hierarchy_files,
            pagination=self.pagination,
        )

        context.class_listing_limit = self.class_listing_limit
        if self._resume is not None:
            context.resumed = self._resumed_batches(*self._resume)
        self._resumed: dict[tuple[str, ...], list[dict[str, Any]]] = context.resumed
        # Run strategy
        patterns = self._strategy.mine(context)
        if self._store is not None:
            self._scan_structure(context, patterns)
        elif not isinstance(self._strategy, StructuralStrategy) and not self._own_view:
            StructuralStrategy(patterns).mine(context)
        if self._store is None and not self._own_view and self.untyped_subjects:
            from rdfsolve.mining.untyped_subjects import mine_untyped_subjects

            # After the structural census, whose counts of untyped triples choose the
            # properties to count; a scan run counts untyped subjects with its patterns.
            patterns = [*patterns, *mine_untyped_subjects(context)]
        elif not self.untyped_subjects:
            patterns = [p for p in patterns if not p.untyped_subject]
        self._class_batches = context.class_batches
        if context.grouped_members:
            # A row of a representative is a grouping of its members' rows, not an observation
            # of the representative itself, and its direct instances do not describe it.
            self._subsumed_classes |= set(context.grouped_members)
            self._grouped_members = dict(context.grouped_members)
            for pattern in patterns:
                if pattern.subject_class in context.grouped_members:
                    pattern.evidence_source = "inferred"
        self._shared_extensions = dict(context.shared_extensions)
        if context.structural_patterns or any(
            p.name == "structural-patterns" for p in self._report.report.phases
        ):
            self._structural_patterns = context.structural_patterns
        if context.graph_uris != self.graph_uris:
            logger.info("Mining continues in %d discovered graphs", len(context.graph_uris or []))
            self.graph_uris = context.graph_uris
            self._report.report.graph_uris = self.graph_uris
            self._report.report.config.update(
                graph_uris=self.graph_uris, graph_scope="within_named_graphs"
            )

        # Extract one-shot results if available
        one_shot_results = None
        if isinstance(self._strategy, OneShotStrategy):
            one_shot_results = self._report.report.one_shot_results or []

        return patterns, one_shot_results

    @property
    def _store(self) -> Any:
        """The row store of a scan run (rdfsolve.mining.scan), whose phases count rows."""
        return getattr(self._strategy, "store", None)

    def _scan_structure(self, context: MiningContext, patterns: list[SchemaPattern]) -> None:
        """Count the structural census and patterns from the rows (rdfsolve.mining.scan_structure)."""
        from rdfsolve.mining.scan_structure import structural_coverage

        phase = self._report.start_phase("structural-patterns")
        config = self._report.report.config
        config["structural_execution"] = "scan"
        # One census for each data graph of a graph scope, as StructuralStrategy.mine.
        entries, structural = structural_coverage(self._store, patterns)
        config["structural_coverage"] = entries
        context.structural_patterns = structural
        config["structural_pattern_count"] = len(structural)
        self._report.finish_phase(phase, items=len(structural))

    def _resumed_batches(self, path: str, text: str) -> dict[tuple[str, ...], list[dict[str, Any]]]:
        """Reuse completed class batches and census counts from an earlier checkpoint.

        A census count is keyed by its queries ("census|" and their hash), so it is reused only
        for the same queries.
        """
        import hashlib
        import json

        batches = {}
        skipped = []
        for line in map(json.loads, text.splitlines()):
            if line.get("phase") not in ("patterns", "census", "counts"):
                continue
            state = line.get("state")
            if state is None and self._resume_failed & set(line["classes"]):
                state = "partial"
            if state in (None, "complete"):
                batches[tuple(line["classes"])] = line["rows"]
            else:
                skipped.append(line["classes"])
        self._report.report.config["resumed_from"] = {
            "path": path,
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
        }
        self._report.report.config["resumed_batches"] = []
        self._report.report.config["resumed_partial_batches_mined_again"] = skipped
        return batches

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
                class_batches=self._class_batches,
                type_context_graph_uris=self.type_context_graph_uris,
                shared_extensions=self._shared_extensions,
                resumed=self._resumed,
            )
            self._report.finish_phase(phase, items=len(patterns))
        except Exception as exc:
            self._report.finish_phase(phase, error=str(exc))
            raise
        return patterns

    def _scan_terms(
        self, patterns: list[SchemaPattern]
    ) -> tuple[list[SchemaPattern], list[SchemaPattern] | None, list[SchemaPattern] | None]:
        """Fold class expressions and group ontology terms on the rows (rdfsolve.mining.scan_terms).

        Anonymous classes typed ``p some F`` become edges (C, p, F) of their members' named
        classes, the fillers grouped per (C, p) slot. With ontology terms as data, the terms are
        grouped as _run_term_subsumption_phase chooses them (before and after mining), by one
        rewrite of the type table on both ends and a recount: the counts are exact, not sums.
        The term patterns are read from the rows, as probe_term_patterns defines them. Returns
        patterns, raw patterns, term patterns.
        """
        from rdfsolve.mining.scan import count_patterns
        from rdfsolve.mining.scan_terms import (
            RetypedStore,
            fold_class_expressions,
            foldable_expressions,
            group_terms,
            type_table,
            without_classes,
        )

        store = self._store
        config = self._report.report.config
        expressions = foldable_expressions(store)
        types = without_classes(type_table(store), expressions)
        raw_patterns = term_patterns = None
        budget = self._ontology_term_budget
        phase = self._report.start_phase("ontology-terms")
        if budget is not None:
            from rdfsolve.mining.scan_enrichment import term_patterns as read_term_patterns

            # The exact term observations, read from the rows (the probe's definitions).
            term_patterns = read_term_patterns(
                store, patterns, ontology_graph_uris=self._ontology_graph_uris
            )
            grouping = group_terms(
                store,
                # Minimal types (ABSTAT): a record's types never include their own ancestors,
                # so that a client can merge the released per-term rows exactly.
                minimal=True,
                types=types,
                # Without class expressions the type table is the store's: reuse its counts.
                counted=None if expressions else patterns,
                budget=budget,
                group_before_mining=self._group_before_mining,
                ontology_graph_uris=self._ontology_graph_uris,
                hierarchy_files=self._hierarchy_files,
            )
            patterns, raw_patterns, types = grouping.patterns, grouping.raw_patterns, grouping.types
            self._scan_grouping = grouping
            config["ontology_term_subsumption"] = grouping.summary
            if grouping.before_mining:
                config["ontology_term_grouping"] = grouping.before_mining
                self._grouped_members = dict(grouping.members)
            self._subsumed_classes |= set(grouping.subsumed_classes)
        elif expressions:
            patterns = count_patterns(RetypedStore(store, types))
        if expressions:
            folding = fold_class_expressions(
                store,
                expressions,
                types=types,
                ontology_graph_uris=self._ontology_graph_uris,
                hierarchy_files=self._hierarchy_files,
            )
            patterns = patterns + folding.patterns
            config["class_expression_folding"] = folding.record
        # Class counts and extensions read the rewritten type table (representatives counted).
        self._scan_types = RetypedStore(store, types)
        self._report.finish_phase(phase, items=len(patterns))
        return patterns, raw_patterns, term_patterns

    def _run_term_subsumption_phase(
        self, patterns: list[SchemaPattern], budget: int
    ) -> tuple[list[SchemaPattern], list[SchemaPattern] | None]:
        """Retain exact term probes and group typed patterns by ancestry.

        With classes_as_data, the term rows also join the typed patterns (in place of the rows
        that say only that a property points at a class) before the grouping.
        """
        from rdfsolve._outcomes import QueryFailure, QueryOutcome
        from rdfsolve.mining.ontology_as_data import (
            choose_representatives,
            pattern_classes,
            probe_term_patterns,
            subsume_patterns,
        )
        from rdfsolve.ontology.hierarchy import fetch_superclasses
        from rdfsolve.ontology.vocabulary import OWL_CLASS, RDFS_CLASS
        from rdfsolve.sparql_helper import EndpointError

        phase = self._report.start_phase("ontology-terms")
        try:
            # Term counts are enrichment: a refused query is read in batches of terms or over
            # a sample, and what stays refused is a measurement gap, never a failure (DGIdb on
            # med2rdf: the subject counts are cut at 120 s by its gateway).
            probed: list[SchemaPattern] | None
            probe = QueryOutcome()
            budget_of = getattr(self._helper, "budget", None)
            light = (
                budget_of(self.light_probe_seconds)
                if self.light_probe_seconds is not None and budget_of is not None
                else nullcontext()
            )
            try:
                with light:
                    probed = probe_term_patterns(
                        self._helper,
                        self.graph_uris,
                        self._collect_bindings,
                        self.chunk_size,
                        ontology_graph_uris=self._ontology_graph_uris,
                        type_context_graph_uris=self.type_context_graph_uris,
                        typed=patterns,
                        classes={str(c): c for batch in self._class_batches or [] for c in batch},
                        outcome=probe,
                    )
            except EndpointError as error:
                failure = QueryFailure("timeout", str(error)[:500], "ontology-terms")
                self._report.record_outcome(QueryOutcome(gaps=[failure]))
                self._report.report.config["ontology_term_probe"] = {
                    "state": "not_probed",
                    "reason": str(error)[:500],
                    "seconds_per_query": self.light_probe_seconds,
                }
                probed = None
            else:
                self._report.record_outcome(probe)
                self._report.report.config["ontology_term_probe"] = {
                    "state": "gaps" if probe.gaps else "sampled" if probe.samples else "complete",
                    "sampled_queries": len(probe.samples),
                    "gaps": len(probe.gaps),
                }
            if self.classes_as_data and probed:
                covered = {(p.subject_class, p.property_uri) for p in probed}
                patterns = [
                    p
                    for p in patterns
                    if not (
                        p.object_class in (OWL_CLASS, RDFS_CLASS)
                        and (p.subject_class, p.property_uri) in covered
                    )
                ]
                # the copies take each term as the class of its records; the exact rows stay
                patterns = patterns + [
                    p.model_copy(
                        deep=True,
                        update={
                            "subject_binding": "type",
                            "object_binding": "type"
                            if p.object_binding == "term"
                            else p.object_binding,
                        },
                    )
                    for p in probed
                ]
            # The objects typed by terms grouped before mining are counted per term; their rows
            # join the group, as the subjects did (lifesciencedict: 33,030 MeSH terms as objects
            # in 158,056 rows, one shape group each as subjects).
            grouped = {m: rep for rep, terms in self._grouped_members.items() for m in terms}
            objects_before = len({p.object_class for p in patterns})
            if grouped:
                patterns = subsume_patterns(patterns, grouped)
            classes = pattern_classes(patterns)
            summary: dict[str, Any] = {
                "budget": budget,
                "probed_patterns": len(probed) if probed is not None else None,
                "classes_before": len(classes),
                "classes_after": len(classes),
                "subsumed": False,
                "representative_members": {},
                "classes_as_data": self.classes_as_data,
                "object_classes_grouped_before_mining": {
                    "before": objects_before,
                    "after": len({p.object_class for p in patterns}),
                },
            }
            if len(classes) > budget:
                t0 = time.monotonic()
                parents = fetch_superclasses(
                    self._helper, classes, graph_uris=self._ontology_graph_uris
                )
                self._report.record_query("ontology-terms/superclasses", time.monotonic() - t0)
                chosen = choose_representatives(classes, parents, budget)
                patterns = subsume_patterns(patterns, chosen.representative)
                members = chosen.members()
                self._subsumed_classes |= set(members)
                summary.update(
                    {
                        "classes_after": chosen.classes_after,
                        "subsumed": bool(members),
                        "levels_lifted": chosen.levels_lifted,
                        "over_budget": chosen.over_budget,
                        "hierarchy_source": "selected named graphs rdfs:subClassOf"
                        if self._ontology_graph_uris
                        else "endpoint default dataset rdfs:subClassOf",
                        "hierarchy_graph_uris": self._ontology_graph_uris,
                        "representatives": {rep: len(terms) for rep, terms in members.items()},
                        "representative_members": members,
                    }
                )
                logger.info(
                    "Subsumed %d classes into %d (budget %d, %d levels lifted)",
                    chosen.classes_before,
                    chosen.classes_after,
                    budget,
                    chosen.levels_lifted,
                )
                if chosen.over_budget:
                    logger.warning(
                        "%d classes remain above the budget of %d: the endpoint has no "
                        "further rdfs:subClassOf parents for them",
                        chosen.classes_after,
                        budget,
                    )
            self._report.report.config["ontology_term_subsumption"] = summary
            self._report.finish_phase(phase, items=len(patterns))
        except Exception as exc:
            self._report.finish_phase(phase, error=str(exc))
            raise
        return patterns, probed

    def _run_labels_phase(
        self,
        patterns: list[SchemaPattern],
    ) -> tuple[list[SchemaPattern], set[str]]:
        """Run the labels phase and return (enriched patterns, uri set).

        A strategy with its own view gives the labels itself; they are copied, not queried.
        """
        if self._own_view:
            from rdfsolve.mining.pattern_enrichment import _enrich_with_local

            found = {a.term_iri: a.text.value for a in getattr(self._strategy, "labels", [])}
            return _enrich_with_local(patterns, found), self._unique_uris(patterns)
        phase = self._report.start_phase("labels")
        logger.info("Fetching labels …")
        uris_before = self._unique_uris(patterns)
        if self._store is not None:
            from rdfsolve.mining.scan_enrichment import pattern_labels

            patterns = pattern_labels(self._store, patterns)
            self._report.finish_phase(phase, items=len(uris_before))
            return patterns, uris_before
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
            if not p.untyped_subject:
                classes.add(p.subject_class)
            if p.object_class not in _SENTINEL_OBJECTS:
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
            try:
                raw = self._helper.select(query, purpose="declared_classes")
                bindings = raw.get("results", {}).get("bindings", [])
                if SparqlHelper.row_cap_suspected(len(bindings)):
                    logger.warning(
                        "Declared classes: exactly %d rows, a common server cap; paging them",
                        len(bindings),
                    )
                    bindings = self._collect_bindings(
                        SparqlHelper.prepare_paginated_query(query),
                        "declared_classes",
                        len(bindings),
                    )
            except ResponseLimitError:
                logger.warning("Declared classes exceeded the response limit; paging the query")
                bindings = self._collect_bindings(
                    SparqlHelper.prepare_paginated_query(query),
                    "declared_classes",
                    self.chunk_size,
                )
            for row in bindings:
                class_val = row.get("class", {}).get("value")
                if class_val:
                    declared.add(str(class_val))
            logger.info("Found %d declared classes (owl:Class/rdfs:Class)", len(declared))
        except Exception as e:
            self._report.record_outcome(
                QueryOutcome(
                    state="failed",
                    failures=[
                        QueryFailure(
                            "endpoint",
                            str(e),
                            "declared_classes",
                            graph_uris=self.graph_uris,
                        )
                    ],
                )
            )
            logger.warning("Class declarations unavailable: %s", e)
        return declared

    def _measure_class_extensions(self, classes: list[str], sizes: dict[str, int]) -> Any:
        """Measure the relations between the member sets of the classes (QLever)."""
        from rdfsolve.mining.class_extensions import measure_class_extensions

        phase = self._report.start_phase("class-extensions")
        found = measure_class_extensions(
            self._helper,
            classes,
            sizes,
            self.graph_uris,
            self.type_context_graph_uris,
            self._report.record_query,
        )
        self._report.finish_phase(phase, items=len(found.members) if found else 0)
        return found

    def _count_dataset(self) -> None:
        """Record the exact dataset statistics of a QLever index (rdfsolve.mining.dataset_statistics)."""
        from rdfsolve.mining.dataset_statistics import count_dataset

        phase = self._report.start_phase("dataset-statistics")
        if self._store is not None:
            from rdfsolve.mining.scan_structure import dataset_statistics

            statistics = dataset_statistics(self._store)
        else:
            statistics = count_dataset(self._helper, self.graph_uris, self._report.record_query)
        self._report.report.config["dataset_statistics"] = statistics
        self._report.flush()
        self._report.finish_phase(phase, items=len(statistics.get("property_partitions", ())))

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
        counted = self._report.report.config.get("dataset_statistics") or {}
        if counted.get("state") != "counted":
            counted = {}

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
            triple_count_estimate=counted.get("triples"),
            distinct_subject_count=counted.get("distinct_subjects"),
            distinct_object_count=counted.get("distinct_objects"),
            distinct_predicate_count=counted.get("distinct_properties"),
            property_partitions=counted.get("property_partitions"),
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

        return query_endpoint_metadata(self._helper, graph_uris=self.graph_uris)

    def mine(
        self,
        dataset_name: str | None = None,
    ) -> MinedSchema:
        """Run all queries and return a :class:`MinedSchema`.

        Write the report after each phase when a report path is set.
        """
        with self._session(dataset_name):
            self._verify_graph_scope()
            return self._finish_schema(self._mine_schema(dataset_name))

    def _verify_graph_scope(self) -> None:
        """Reject a configured graph the endpoint does not hold."""
        if not self.graph_uris:
            return
        from rdfsolve.mining.graph_selection import missing_graphs

        missing = missing_graphs(self._helper, self.graph_uris)
        if missing:
            raise ValueError(
                f"Endpoint holds no triples in the selected graphs: {missing}. "
                "Correct graph_uris or leave it empty to discover graphs."
            )

    def _verify_context_graphs(self, name: str, graphs: list[str] | None) -> None:
        """Record whether each required context graph holds triples."""
        from rdfsolve.mining.graph_selection import missing_graphs

        context: dict[str, object] = {"graph_uris": graphs, "state": "not_checked"}
        self._report.report.config[name] = context
        if not graphs:
            return
        context["state"] = "checking"
        self._report.flush()
        try:
            missing = missing_graphs(self.helper, graphs)
        except Exception:
            context["state"] = "failed"
            raise
        if missing:
            context.update({"state": "missing", "missing_graph_uris": missing})
            raise ValueError(
                f"No triples in requested {name.removesuffix('_context')} graphs: {missing}"
            )
        context["state"] = "nonempty"
        self._report.flush()

    # Seconds for listing the endpoint's graphs before the system graphs are left out. The
    # listing reads every quad (DISTINCT ?g); where it does not end in 30 s it did not end in
    # 120 s either (colil.dbcls.jp, sparql.orthodb.org: 120 s each, rehearsal 2026-10-06), and
    # the source is then mined without the exclusion, which is recorded.
    GRAPH_LISTING_BUDGET_S = 30.0

    def _exclude_engine_graphs(self) -> None:
        """Leave the engine's own graphs out of every query of a source mined without graphs.

        Without FROM, Virtuoso reads every graph, its system graphs included (virtrdf#, the
        WebDAV graph, ...): their predicates and edges entered the census and the structural
        patterns (WikiPathways: virtrdf# properties counted as properties 208/348 of the census).
        The graphs whose IRIs start with *excluded_graph_prefixes* (the prefixes that graph
        discovery already leaves out) are excluded from the default graph of each query without
        a dataset clause with Virtuoso's input:default-graph-exclude (graph_exclusion_prologue,
        which says why input:named-graph-exclude is never sent), which keeps the default graph
        of the endpoint otherwise unchanged. The pragma is first checked on the endpoint
        (_check_exclusion): a pragma that empties a non-empty answer, or that makes a one-row
        query slow, is recorded as an endpoint quirk and nothing is excluded. An engine that
        refuses the pragma is recorded and nothing is excluded. When the listing is refused or
        does not end in GRAPH_LISTING_BUDGET_S, the engine graphs known by name
        (KNOWN_ENGINE_GRAPHS) that match the prefixes are asked for by name, which needs no scan
        of the quads.
        """
        from rdfsolve.mining.local_graph import LocalGraphHelper
        from rdfsolve.sparql_helper import SparqlHelperError
        from rdfsolve.void_retrieval import discover_graph_names

        helper = self._helper
        if self.graph_uris or not self.excluded_graph_prefixes:
            return
        if isinstance(helper, LocalGraphHelper):
            return
        record: dict[str, Any] = {"prefixes": list(self.excluded_graph_prefixes)}
        self._report.report.config["excluded_graphs"] = record
        try:
            with helper.budget(self.GRAPH_LISTING_BUDGET_S):
                graphs = discover_graph_names(helper, batch_size=1000, max_pages=100)
        except (SparqlHelperError, ValueError) as error:
            logger.warning("Graph exclusion: graphs not listed: %s", str(error)[:200])
            record.update(state="graph_listing_refused", error=str(error)[:500])
            graphs = self._known_engine_graphs(record)
            if not graphs:
                return
        excluded = sorted(g for g in graphs if g.startswith(self.excluded_graph_prefixes))
        record.setdefault("listed_graphs", len(graphs))
        if not excluded:
            record["state"] = "none_found"
            return
        record["graph_uris"] = excluded
        try:
            with helper.budget(self.GRAPH_LISTING_BUDGET_S):
                usable = self._check_exclusion(excluded, record)
        except SparqlHelperError as error:
            logger.warning("Graph exclusion: the endpoint refuses it: %s", str(error)[:200])
            record.update(state="not_supported", error=str(error)[:500])
            return
        if not usable:
            return
        self._find_engine_only_classes(excluded, record)
        helper.excluded_graphs = excluded
        record.update(state="excluded", method="virtuoso_define_default_graph_exclude")
        logger.info("Graph exclusion: %d engine graphs left out: %s", len(excluded), excluded)

    # The self-check of the exclusion: one row of the default graph, without and with the
    # prologue. With the prologue it must answer a row, and not take more than
    # EXCLUSION_SLOW_FACTOR times as long nor more than EXCLUSION_SLOW_S beyond the answer
    # without it (sparql.string-db.org answered a graph listing in 0.2 s without a pragma and
    # in 37.7 s with input:named-graph-exclude, 2026-10-06).
    EXCLUSION_CHECK = "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1"
    EXCLUSION_SLOW_FACTOR = 10.0
    EXCLUSION_SLOW_S = 5.0

    def _check_exclusion(self, excluded: list[str], record: dict[str, Any]) -> bool:
        """Return whether the exclusion pragma leaves a one-row answer non-empty and fast.

        Some Virtuoso versions answer an empty result, with HTTP 200 and no error, to a query
        with an exclusion pragma (dbpedia.org, 2026-10-06, input:named-graph-exclude: 0 classes
        where 1099 without it), and an ASK can still answer true there, so the check uses
        SELECT. A pragma that empties a non-empty answer, or that makes the query slow, is an
        endpoint quirk, recorded in *record* under ``quirks``. SparqlHelperError is raised for
        a refused pragma.
        """
        import time

        from rdfsolve.sparql_helper import graph_exclusion_prologue

        helper = self._helper

        def rows(query: str) -> tuple[int, float]:
            """Return the number of rows of *query* and the seconds it took."""
            started = time.monotonic()
            bindings = helper.select(query).get("results", {}).get("bindings", [])
            return len(bindings), time.monotonic() - started

        quirks: list[dict[str, Any]] = []
        record["quirks"] = quirks
        plain, plain_s = rows(self.EXCLUSION_CHECK)
        if not plain:
            record["state"] = "check_inconclusive"
            logger.warning("Graph exclusion: not used; the endpoint answers no triple to check it")
            return False
        found, found_s = rows(graph_exclusion_prologue(excluded) + self.EXCLUSION_CHECK)
        record["check"] = {"query": self.EXCLUSION_CHECK, "seconds": [plain_s, found_s]}
        slow = found_s > max(plain_s * self.EXCLUSION_SLOW_FACTOR, plain_s + self.EXCLUSION_SLOW_S)
        if found and not slow:
            return True
        effect = "empties a non-empty answer" if not found else "makes a one-row query slow"
        quirks.append({"pragma": "input:default-graph-exclude", "effect": effect})
        record["state"] = "emptied_by_pragma" if not found else "slowed_by_pragma"
        logger.warning("Graph exclusion: not used; the pragma %s (%.1f s)", effect, found_s)
        return False

    # At most this many classes typed in the engine graphs are checked one by one (forum: 12).
    ENGINE_CLASS_CHECK_LIMIT = 200

    def _find_engine_only_classes(self, excluded: list[str], record: dict[str, Any]) -> None:
        """Find the classes typed only in the engine graphs, for a cheaper class listing.

        A class listing with the exclusion prologue can be much slower than without it (forum,
        2026-10-06: 155,649 classes in 175 s without it, a 502 after 315 s with it). The classes
        typed in the engine graphs are listed graph by graph with FROM, which reads those small
        graphs only (before the prologue is set: Virtuoso drops a FROM that names an excluded
        graph), and each is asked for once in the rest of the endpoint (LIMIT 1, with the prologue). The
        classes found nowhere else are dropped from a class listing read without the prologue
        (SparqlHelper.graph_exclusion_suspended), which gives the same classes. When the classes
        cannot be checked, the listing keeps the prologue.
        """
        from rdfsolve.sparql_helper import SparqlHelperError, graph_exclusion_prologue

        helper = self._helper
        limit = self.ENGINE_CLASS_CHECK_LIMIT
        prologue = graph_exclusion_prologue(excluded)
        try:
            with helper.budget(self.GRAPH_LISTING_BUDGET_S):
                found: set[str] = set()
                # One graph per query: several FROM in one query did not answer in 60 s on
                # forum, where each graph alone answered in under 1 s (2026-10-06).
                for graph in excluded:
                    listing = helper.select(
                        f"SELECT DISTINCT ?c FROM {URIRef(graph).n3()} "  # noqa: S608 (SPARQL)
                        f"WHERE {{ ?s a ?c }} LIMIT {limit + 1}"
                    )
                    found.update(
                        b["c"]["value"]
                        for b in listing.get("results", {}).get("bindings", [])
                        if b.get("c", {}).get("type") == "uri"
                    )
                    if len(found) > limit:
                        break
                typed = sorted(found)
                if len(typed) > limit:
                    record["engine_classes"] = {"state": f"more than {limit}; not checked"}
                    return
                only = []
                for cls in typed:
                    query = f"SELECT ?s WHERE {{ ?s a {URIRef(cls).n3()} }} LIMIT 1"
                    rows = helper.select(prologue + query).get("results", {}).get("bindings", [])
                    if not rows:
                        only.append(cls)
        except (SparqlHelperError, ValueError) as error:
            record["engine_classes"] = {"state": "failed", "error": str(error)[:300]}
            logger.warning("Graph exclusion: engine classes not checked: %s", str(error)[:200])
            return
        helper.engine_only_classes = frozenset(only)
        record["engine_classes"] = {"state": "checked", "typed": len(typed), "engine_only": only}
        logger.info(
            "Graph exclusion: %d classes typed only in engine graphs; class listings are read "
            "without the prologue and cleaned of them",
            len(only),
        )

    def _known_engine_graphs(self, record: dict[str, Any]) -> list[str]:
        """Return the engine graphs known by name that the endpoint holds; record the probe."""
        from rdfsolve.schema_models._constants import KNOWN_ENGINE_GRAPHS
        from rdfsolve.sparql_helper import SparqlHelperError

        names = [g for g in KNOWN_ENGINE_GRAPHS if g.startswith(self.excluded_graph_prefixes)]
        if not names:
            return []
        from rdfsolve.mining.graph_selection import missing_graphs

        try:
            # One ASK per graph: it stops at the first triple (missing_graphs).
            with self._helper.budget(self.GRAPH_LISTING_BUDGET_S):
                absent = set(missing_graphs(self._helper, names))
        except (SparqlHelperError, ValueError) as error:
            record["known_graphs_probe"] = {"state": "failed", "error": str(error)[:300]}
            return []
        found = [g for g in names if g not in absent]
        record["known_graphs_probe"] = {"state": "complete", "asked": names, "found": found}
        logger.info(
            "Graph exclusion: %d of %d known engine graphs found by name", len(found), len(names)
        )
        return found

    @contextmanager
    def _session(
        self, dataset_name: str | None, ontology_graph_uris: list[str] | None = None
    ) -> Iterator[None]:
        """Start one report before any phase and retain failures."""
        self._ontology_classes = None
        self._ontology_term_budget = None
        self._group_before_mining = None
        self._hierarchy_files = []
        self._subsumed_classes = set()
        self._ontology_graph_uris = ontology_graph_uris
        self._class_batches = None
        self._resumed = {}
        self._structural_patterns = None
        self._subsumed_classes = set()
        self._grouped_members = {}
        self._declared_classes = set()
        self._init_report(
            dataset_name, self._build_strategy_string(), datetime.now(UTC).isoformat()
        )
        self.last_report = self._report.report
        remote_helper = self._helper
        member = MEMBERSHIP.set(tuple(self.membership_properties or [RDF_TYPE]))
        sampling = SAMPLE_SIZE.set(self.sample_size)
        try:
            if self.get_graphs_from_store:
                from dataclasses import asdict

                from rdfsolve.graph_store import download_graphs, load_downloads
                from rdfsolve.mining.local_graph import LocalGraphHelper

                downloads = download_graphs(
                    self.graph_store_url or "",
                    list(
                        dict.fromkeys(
                            (self.graph_uris or [])
                            + self.type_context_graph_uris
                            + (ontology_graph_uris or [])
                        )
                    ),
                    self.graph_store_dir,
                    max_bytes=self.graph_store_max_bytes,
                    timeout=self.timeout,
                )
                self._helper = LocalGraphHelper(
                    self.endpoint_url,
                    load_downloads(downloads, endpoint_url=self.endpoint_url, timeout=self.timeout),
                    backend=self.local_backend,
                )
                self._report.report.config["graph_store"] = {
                    "url": self.graph_store_url,
                    "engine": self._helper.sparql_engine,
                    "graphs": [asdict(item) for item in downloads],
                }
                self._report.report.config["local_backend"] = self._helper.local.metadata()
                self._report.report.config["sparql_engine"] = self._helper.sparql_engine
            self._verify_context_graphs("type_context", self.type_context_graph_uris)
            self._exclude_engine_graphs()
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
        finally:
            MEMBERSHIP.reset(member)
            SAMPLE_SIZE.reset(sampling)
            remote_helper.excluded_graphs = []
            remote_helper.engine_only_classes = None
            if self._helper is not remote_helper:
                self._helper.close()
                self._helper = remote_helper

    def _finish_schema(
        self, schema: MinedSchema, annotation_iris: list[str] | None = None
    ) -> MinedSchema:
        """Filter the result and set final counts and times once."""
        if self.filter_service_namespaces:
            schema = self._apply_namespace_filter(schema)
        for pattern in [
            *schema.patterns,
            *(schema.raw_patterns or []),
            *(schema.term_patterns or []),
        ]:
            if pattern.pattern_type == PatternType.UNKNOWN:
                pattern.pattern_type = {
                    "Literal": PatternType.DATATYPE_PROPERTY,
                    "BlankNode": PatternType.BLANK_NODE_PROPERTY,
                }.get(pattern.object_class, PatternType.OBJECT_PROPERTY)
        if self._own_view:
            schema.enrichment = SchemaEnrichment(
                state="partial",
                labels=getattr(self._strategy, "labels", []),
                examples=getattr(self._strategy, "examples", []),
                class_examples=getattr(self._strategy, "class_examples", {}),
            )
        elif self.enrich:
            schema.enrichment = self.query_enrichment(schema, annotation_iris=annotation_iris)
        classes, properties = self._collect_class_property_sets(schema.patterns)
        # A strategy with its own view may state the members of its classes (VoID).
        entity_counts = dict(getattr(self._strategy, "entity_counts", {})) if self._own_view else {}
        # A count that the strategy holds for a lower bound (VoID: contradicted by its own
        # property partitions) keeps the state "partial".
        entity_count_states: dict[str, QueryState] = (
            dict(getattr(self._strategy, "entity_count_states", {})) if self._own_view else {}
        )
        if self.counts and self._store is not None:
            from rdfsolve.mining import scan_structure

            phase = self._report.start_phase("class-entity-counts")
            counted = sorted(classes - self._subsumed_classes)
            members = getattr(self, "_scan_types", None) or self._store
            entity_counts, entity_count_states = scan_structure.class_entity_counts(
                members, counted
            )
            self._report.finish_phase(phase, items=len(entity_counts))
            phase = self._report.start_phase("class-extensions")
            schema.class_extensions = scan_structure.class_extensions(members, counted)
            self._report.finish_phase(phase, items=len(schema.class_extensions.members))
        elif self.counts and not self._own_view:
            from rdfsolve.mining.pattern_enrichment import query_class_entity_counts

            # Subsumed representatives stand for many terms; their direct instance
            # count would not describe them, so they are not counted here.
            entity_counts = query_class_entity_counts(
                sorted(classes - self._subsumed_classes),
                self._helper,
                self.graph_uris,
                self._report,
                self.class_batch_size,
                self.delay,
                states_out=entity_count_states,
                type_context_graph_uris=self.type_context_graph_uris,
            )
            schema.class_extensions = self._measure_class_extensions(
                sorted(classes - self._subsumed_classes), entity_counts
            )
        # A (C, rdf:type, Resource) row says only that the class IRI has no type of its own: the
        # membership is already the subject class of every row and the member counts above. Such
        # rows are left out after the classes are counted, so that a class whose members have
        # only rdf:type keeps its count. A row whose type value has a class (for example
        # (C, rdf:type, owl:Class)) describes the source's model and is kept. The structural
        # census, which ran before, counts rdf:type edges as covered by the typed profiles.
        kept = [
            p
            for p in schema.patterns
            if not (p.property_uri in MEMBERSHIP.get() and p.object_class == "Resource")
        ]
        self._report.report.config["membership_rows_left_out"] = len(schema.patterns) - len(kept)
        schema.patterns = kept
        dataset = getattr(self._helper, "dataset", None)
        if self._store is not None:
            from rdfsolve.mining.scan_enrichment import collections

            phase = self._report.start_phase("collections")
            schema.collections = collections(self._store)
            self._report.report.config["collection_profile_count"] = len(schema.collections)
            self._report.report.config["invalid_collection_count"] = sum(
                p.invalid_count for p in schema.collections
            )
            self._report.finish_phase(phase, items=len(schema.collections))
        elif isinstance(dataset, Graph):
            phase = self._report.start_phase("collections")
            try:
                profiles = schema.discover_collections(
                    dataset,
                    graph_uris=self.graph_uris or [],
                    type_context_graph_uris=self.type_context_graph_uris or [],
                )
                self._report.report.config["collection_profile_count"] = len(profiles)
                self._report.report.config["invalid_collection_count"] = sum(
                    p.invalid_count for p in profiles
                )
            except Exception as error:
                self._report.finish_phase(phase, error=str(error))
                raise
            self._report.finish_phase(phase, items=len(profiles))
            if self.filter_service_namespaces:
                schema = schema.filter_service_namespaces()
        report = self._report.report
        report.config["structural_pattern_count"] = len(schema.structural_patterns or [])
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
        schema.about.type_context_graph_uris = self.type_context_graph_uris or None
        schema.about.membership_property = self._membership or (
            self.membership_properties if len(self.membership_properties) > 1 else None
        )
        schema.about.ontology_graph_uris = self._ontology_graph_uris
        schema.about.class_entity_counts = entity_counts
        schema.about.class_entity_count_states = entity_count_states
        dataset = getattr(self._helper, "dataset", None)
        if isinstance(dataset, Graph):
            schema.prefixes.update(
                {prefix: str(namespace) for prefix, namespace in dataset.namespaces()}
            )
        schema.prefixes = schema.get_prefixes()
        from rdfsolve.schema_models.iri_quality import findings

        rows = [*schema.patterns, *(schema.structural_patterns or [])]
        report.config["count_coverage"] = {
            "patterns": len(rows),
            "with_triples": sum(getattr(p, "count", None) is not None for p in rows),
            "with_distinct_subjects": sum(
                getattr(p, "distinct_subjects", None) is not None for p in rows
            ),
            "with_distinct_objects": sum(
                getattr(p, "distinct_objects", None) is not None for p in rows
            ),
            "measurement_gaps": len(report.measurement_gaps),
            "sampled_queries": len(report.sampled_queries),
            "sampled_patterns": sum(getattr(p, "sampled", None) is not None for p in rows),
            "lower_bound_counts": sum(
                getattr(p, "count_bound", None) == "lower_bound" for p in rows
            ),
        }
        if report.sampled_queries:
            logger.warning(
                "%d refused queries answered over a sample: their rows are flagged sampled, with "
                "lower-bound counts (report: sampled_queries)",
                len(report.sampled_queries),
            )
        if report.measurement_gaps:
            logger.warning(
                "%d measures refused for rows that stand (report: measurement_gaps)",
                len(report.measurement_gaps),
            )
        found = findings(schema, report.config.get("dataset_statistics"))
        graphs = (report.config.get("iri_findings") or {}).get("graphs")
        if graphs:  # recorded while mining (the published VoID)
            found["graphs"] = graphs
        report.config["iri_findings"] = found
        self._report.flush()
        if found["terms"]:
            logger.warning(
                "%d terms are not RDF IRIs: queried with IRI(), kept in the JSON schema and left "
                "out of the RDF outputs (report: iri_findings)",
                len(found["terms"]),
            )
        return schema

    def query_enrichment(
        self, schema: MinedSchema, *, annotation_iris: list[str] | None = None
    ) -> SchemaEnrichment:
        """Query definitions and observed examples with this miner's settings.

        A scan run reads them from its rows (rdfsolve.mining.scan_enrichment).
        """
        if self._store is not None:
            from rdfsolve.mining.scan_enrichment import enrichment

            return enrichment(
                self._store,
                schema,
                examples_per_pattern=self.examples_per_pattern,
                annotation_iris=annotation_iris,
            )
        from rdfsolve.mining.enrichment import query_enrichment

        return query_enrichment(
            schema,
            self._helper,
            self.graph_uris,
            examples_per_pattern=self.examples_per_pattern,
            delay=self.delay,
            report=self._rc,
            annotation_iris=annotation_iris,
            type_context_graph_uris=self.type_context_graph_uris,
        )

    def _mine_schema(self, dataset_name: str | None) -> MinedSchema:
        """Mine data patterns within the active report session."""
        strategy = self._report.report.strategy
        started_at = self._report.report.started_at

        t0 = time.monotonic()
        patterns, one_shot_results = self._run_patterns_phase()
        if self._structural_patterns:
            strategy += "+structural"
        self._report.report.pattern_count = len(patterns)

        # A strategy that counts its patterns itself (scan) needs no counts phase.
        if self.counts and not self._own_view and not getattr(self._strategy, "counted", False):
            patterns = self._run_counts_phase(patterns)

        raw_patterns = None
        term_patterns = None
        if self._store is not None:
            patterns, raw_patterns, term_patterns = self._scan_terms(patterns)
        elif self._ontology_term_budget is not None:
            raw_patterns = [pattern.model_copy(deep=True) for pattern in patterns]
            patterns, term_patterns = self._run_term_subsumption_phase(
                patterns, self._ontology_term_budget
            )

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
        self._count_dataset()

        about = self._build_about_metadata(
            dataset_name,
            strategy,
            started_at,
            patterns,
            declared_class_count=len(declared_in_patterns),
            used_type_count=len(used_types),
            discovered_metadata=discovered_metadata if discovered_metadata else {},
        )
        schema = MinedSchema(
            patterns=patterns,
            raw_patterns=raw_patterns,
            term_patterns=term_patterns,
            structural_patterns=self._structural_patterns,
            about=about,
        )

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
        *,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through a SELECT query and collect all bindings.

        With *max_rows*, raise ClassListingLimitError (with the rows so far) once more rows
        than that are read.
        """
        effective = chunk_size if chunk_size is not None else self.chunk_size
        all_bindings: list[dict[str, Any]] = []
        has_rc = self._rc is not None
        page = 0
        cursor_keys = None
        if self.pagination == "cursor":
            from rdflib.plugins.sparql import prepareQuery

            base = query_template.removesuffix("\nOFFSET {offset}\nLIMIT {limit}").format()
            cursor_keys = [str(variable) for variable in prepareQuery(base).algebra["PV"]]
        try:
            for chunk in self._helper.select_chunked(
                query_template,
                chunk_size=effective,
                delay_between_chunks=self.delay,
                purpose=purpose,
                pagination=self.pagination,
                cursor_keys=cursor_keys,
            ):
                page += 1
                all_bindings.extend(chunk)
                if max_rows is not None and len(all_bindings) > max_rows:
                    from rdfsolve.mining.strategy import ClassListingLimitError

                    raise ClassListingLimitError(all_bindings, max_rows)
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
    get_graphs_from_store: bool = False,
    graph_store_url: str | None = None,
    graph_store_dir: str | Path = "graph-store",
    graph_store_max_bytes: int = 64 * 1024 * 1024,
    pagination: Literal["offset", "cursor"] = "offset",
    navigation_hops: int = 0,
    navigation_limit: int = 100,
    navigation_probes: int = 0,
    type_context_graph_uris: list[str] | None = None,
    local_backend: LocalBackend = "oxigraph",
) -> MinedSchema:
    """One-shot helper: mine a schema and return :class:`MinedSchema`."""
    from urllib.parse import urlsplit

    if urlsplit(endpoint_url).hostname in {"localhost", "127.0.0.1", "::1"}:
        delay = 0.0
    miner = SchemaMiner(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        chunk_size=chunk_size,
        pagination=pagination,
        type_context_graph_uris=type_context_graph_uris,
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
        get_graphs_from_store=get_graphs_from_store,
        graph_store_url=graph_store_url,
        graph_store_dir=graph_store_dir,
        graph_store_max_bytes=graph_store_max_bytes,
        local_backend=local_backend,
    )
    try:
        schema = miner.mine(dataset_name=dataset_name)
        if navigation_probes and not navigation_hops:
            raise ValueError("navigation_probes requires navigation_hops")
        if navigation_hops:
            schema.discover_paths(
                max_hops=navigation_hops,
                max_paths_per_length=navigation_limit,
                helper=miner.helper,
                probe_limit=navigation_probes,
            )
        return schema
    finally:
        miner.close()
