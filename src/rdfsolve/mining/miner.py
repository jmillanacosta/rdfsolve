"""Extract RDF schema patterns from SPARQL endpoints using SELECT queries for typed objects, literals, URIs, and blank nodes."""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from rdflib import Graph
from typing_extensions import Self

from rdfsolve._outcomes import QueryFailure, QueryOutcome, QueryState
from rdfsolve.mining.one_shot_strategy import OneShotStrategy
from rdfsolve.mining.pattern_enrichment import (
    enrich_patterns_with_counts,
    enrich_patterns_with_labels,
)
from rdfsolve.mining.query_builders import (
    _build_declared_classes_query,
)
from rdfsolve.mining.report_tracking import ReportCollector
from rdfsolve.mining.single_pass_strategy import SinglePassStrategy
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
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
from rdfsolve.sparql_helper import (
    PaginationTruncatedError,
    ResponseLimitError,
    SparqlHelper,
)
from rdfsolve.version import VERSION

logger = logging.getLogger(__name__)

__all__ = [
    "SchemaMiner",
    "mine_schema",
]


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
        examples_per_pattern: int = 2,
        max_response_bytes: int = 64 * 1024 * 1024,
        get_graphs_from_store: bool = False,
        graph_store_url: str | None = None,
        graph_store_dir: str | Path = "graph-store",
        graph_store_max_bytes: int = 64 * 1024 * 1024,
        pagination: Literal["offset", "cursor"] = "offset",
        excluded_graph_prefixes: tuple[str, ...] = (),
        type_context_graph_uris: list[str] | None = None,
    ) -> None:
        """Initialize a SchemaMiner."""
        self.endpoint_url = endpoint_url
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
        self._rc: ReportCollector | None = None
        self._ontology_classes: list[str] | None = None
        self._ontology_term_budget: int | None = None
        self._ontology_graph_uris: list[str] | None = None
        self._class_batches: list[list[str]] | None = None
        self._subsumed_classes: set[str] = set()
        self._declared_classes: set[str] = set()
        self.last_report: MiningReport | None = None

    @property
    def helper(self) -> SparqlHelper:
        """Expose the miner's helper for query recording and later exploration.

        The miner owns this helper and closes it on exit.
        """
        return self._helper

    @classmethod
    def from_graph(
        cls, graph: Graph, *, endpoint_url: str = "urn:rdfsolve:local", **kwargs: Any
    ) -> Self:
        """Mine a bounded RDF snapshot using the existing SPARQL mining strategy."""
        from rdflib import Dataset

        from rdfsolve.mining.local_graph import LocalGraphHelper

        dataset = graph if isinstance(graph, Dataset) else Dataset()
        if dataset is not graph:
            for prefix, namespace in graph.namespaces():
                dataset.bind(prefix, namespace, replace=True)
            for triple in graph:
                dataset.default_graph.add(triple)
        miner = cls(endpoint_url, **kwargs)
        miner._helper.close()
        miner._helper = LocalGraphHelper(endpoint_url, dataset)
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
                "unsafe_paging": self.unsafe_paging,
                "filter_service_namespaces": self.filter_service_namespaces,
                "enrich": self.enrich,
                "examples_per_pattern": self.examples_per_pattern,
            },
        )
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
        )

        # Run strategy
        patterns = self._strategy.mine(context)
        self._class_batches = context.class_batches
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
            )
            self._report.finish_phase(phase, items=len(patterns))
        except Exception as exc:
            self._report.finish_phase(phase, error=str(exc))
            raise
        return patterns

    def _run_term_subsumption_phase(
        self, patterns: list[SchemaPattern], budget: int
    ) -> list[SchemaPattern]:
        """Add term-level ontology patterns, then subsume terms until *budget* classes remain.

        Every pattern is probed at term level first; subsumption only replaces
        classes by ancestors and merges the patterns that then coincide.
        """
        from rdfsolve.mining.ontology_as_data import (
            OWL_CLASS,
            RDFS_CLASS,
            choose_representatives,
            fetch_superclasses,
            pattern_classes,
            probe_term_patterns,
            subsume_patterns,
        )

        phase = self._report.start_phase("ontology-terms")
        try:
            probed = probe_term_patterns(
                self._helper,
                self.graph_uris,
                self._collect_bindings,
                self.chunk_size,
                ontology_graph_uris=self._ontology_graph_uris,
                type_context_graph_uris=self.type_context_graph_uris,
            )
            # Term-level object patterns replace the generic "points at a class" ones.
            covered = {(p.subject_class, p.property_uri) for p in probed}
            patterns = [
                p
                for p in patterns
                if not (
                    p.object_class in (OWL_CLASS, RDFS_CLASS)
                    and (p.subject_class, p.property_uri) in covered
                )
            ]
            patterns = patterns + probed
            classes = pattern_classes(patterns)
            summary: dict[str, Any] = {
                "budget": budget,
                "probed_patterns": len(probed),
                "classes_before": len(classes),
                "classes_after": len(classes),
                "subsumed": False,
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
                self._subsumed_classes = set(members)
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

    @contextmanager
    def _session(
        self, dataset_name: str | None, ontology_graph_uris: list[str] | None = None
    ) -> Iterator[None]:
        """Start one report before any phase and retain failures."""
        self._ontology_classes = None
        self._ontology_term_budget = None
        self._ontology_graph_uris = ontology_graph_uris
        self._class_batches = None
        self._subsumed_classes = set()
        self._declared_classes = set()
        self._init_report(
            dataset_name, self._build_strategy_string(), datetime.now(timezone.utc).isoformat()
        )
        self.last_report = self._report.report
        remote_helper = self._helper
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
                )
                self._report.report.config["graph_store"] = {
                    "url": self.graph_store_url,
                    "engine": "rdflib",
                    "graphs": [asdict(item) for item in downloads],
                }
            self._verify_context_graphs("type_context", self.type_context_graph_uris)
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
            if self._helper is not remote_helper:
                self._helper.close()
                self._helper = remote_helper

    def _finish_schema(
        self, schema: MinedSchema, annotation_iris: list[str] | None = None
    ) -> MinedSchema:
        """Filter the result and set final counts and times once."""
        from rdfsolve.mining.types import EXCLUDED_RECORD_TYPES

        excluded = [p for p in schema.patterns if p.subject_class in EXCLUDED_RECORD_TYPES]
        if excluded:
            schema.patterns = [
                p for p in schema.patterns if p.subject_class not in EXCLUDED_RECORD_TYPES
            ]
            self._report.report.config["excluded_record_patterns"] = {
                "count": len(excluded),
                "subject_classes": sorted({p.subject_class for p in excluded}),
            }
        if self.filter_service_namespaces:
            schema = self._apply_namespace_filter(schema)
        for pattern in schema.patterns:
            if pattern.pattern_type == PatternType.UNKNOWN:
                pattern.pattern_type = {
                    "Literal": PatternType.DATATYPE_PROPERTY,
                    "BlankNode": PatternType.BLANK_NODE_PROPERTY,
                }.get(pattern.object_class, PatternType.OBJECT_PROPERTY)
        if self.enrich:
            schema.enrichment = self.query_enrichment(schema, annotation_iris=annotation_iris)
        classes, properties = self._collect_class_property_sets(schema.patterns)
        entity_counts = {}
        entity_count_states: dict[str, QueryState] = {}
        if self.counts:
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
        schema.about.type_context_graph_uris = self.type_context_graph_uris or None
        schema.about.ontology_graph_uris = self._ontology_graph_uris
        schema.about.class_entity_counts = entity_counts
        schema.about.class_entity_count_states = entity_count_states
        dataset = getattr(self._helper, "dataset", None)
        if isinstance(dataset, Graph):
            schema.prefixes.update(
                {prefix: str(namespace) for prefix, namespace in dataset.namespaces()}
            )
        schema.prefixes = schema.get_prefixes()
        return schema

    def query_enrichment(
        self, schema: MinedSchema, *, annotation_iris: list[str] | None = None
    ) -> SchemaEnrichment:
        """Query definitions and observed examples with this miner's settings."""
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
        self._report.report.pattern_count = len(patterns)

        if self.counts:
            patterns = self._run_counts_phase(patterns)

        if self._ontology_term_budget is not None:
            patterns = self._run_term_subsumption_phase(patterns, self._ontology_term_budget)

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
        """Paginate through a SELECT query and collect all bindings."""
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
