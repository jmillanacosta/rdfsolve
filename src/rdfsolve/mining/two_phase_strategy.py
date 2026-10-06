"""Two-phase mining strategy for large endpoints."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from contextlib import nullcontext
from typing import Any

from pydantic import ValidationError

from rdfsolve._outcomes import FailureCategory, QueryFailure, QueryOutcome
from rdfsolve.mining.blank_nodes import blank_node_patterns
from rdfsolve.mining.query_builders import (
    _build_batched_blank_node_query,
    _build_batched_literal_query,
    _build_batched_typed_object_query,
    _build_batched_untyped_uri_query,
    _build_class_discovery_query,
    _build_class_discovery_query_plain,
    _build_class_weight_query,
    _build_same_members_query,
    membership_path,
)
from rdfsolve.mining.query_fallbacks import query_with_bisect, select_outcome
from rdfsolve.mining.sampling import mark_sampled
from rdfsolve.mining.strategy import ClassListingLimitError, MiningContext, MiningStrategy
from rdfsolve.models import SchemaPattern
from rdfsolve.sparql_helper import (
    EndpointRateLimitError,
    EndpointTimeoutError,
    SparqlHelper,
    SparqlHelperError,
)

logger = logging.getLogger(__name__)

# Listed classes mined when class discovery stops at its limit (see TwoPhaseStrategy._per_record).
CLASS_SAMPLE = 1000

__all__ = ["TwoPhaseStrategy", "plan_class_batches"]

# Above this many classes, batch by instance count instead of a fixed size.
WEIGHTED_BATCHING_ABOVE = 300
MAX_CLASSES_PER_BATCH = 500
MAX_INSTANCES_PER_BATCH = 1_000_000


def plan_class_batches(
    classes: list[str],
    weights: dict[str, int],
    *,
    max_classes: int = MAX_CLASSES_PER_BATCH,
    max_instances: int = MAX_INSTANCES_PER_BATCH,
) -> list[list[str]]:
    """Pack classes into batches bounded by class count and typed-instance count.

    Heavy classes get batches of their own; many light classes (for example
    ontology terms that type a few instances each) share one query.
    """
    ordered = sorted(classes, key=lambda c: (-weights.get(c, 1), c))
    batches: list[list[str]] = []
    batch: list[str] = []
    load = 0
    for cls in ordered:
        weight = max(weights.get(cls, 1), 1)
        if batch and (len(batch) >= max_classes or load + weight > max_instances):
            batches.append(batch)
            batch, load = [], 0
        batch.append(cls)
        load += weight
    if batch:
        batches.append(batch)
    return batches


def _engine_only_classes(context: MiningContext) -> frozenset[str] | None:
    """Return the classes typed only in the excluded engine graphs, when a listing can use them.

    None when the helper excludes no graph or does not know these classes; a class listing is
    then read as it is sent (with the exclusion prologue, if any).
    """
    helper = context.helper
    engine_only = getattr(helper, "engine_only_classes", None)
    if isinstance(engine_only, frozenset) and getattr(helper, "excluded_graphs", None):
        return engine_only
    return None


class TwoPhaseStrategy(MiningStrategy):
    """Two-phase mining strategy for large endpoints.

    Phase 1: Discover classes via SELECT DISTINCT ?class
    Phase 2: For each class, run scoped queries for typed-object, literal,
             untyped-URI, and blank-node patterns

    This avoids massive unscoped triple-joins that choke large endpoints.
    """

    @property
    def name(self) -> str:
        """Return strategy name."""
        return "two-phase"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Execute two-phase mining.

        Args:
            context: Mining context with dependencies

        Returns:
            List of mined schema patterns
        """
        # Phase 1 - discover classes
        p1 = context.report.start_phase("class-discovery")
        classes = self._discover_classes(context)
        if (
            not classes
            and not context.graph_uris
            and not getattr(context, "class_listing_stopped", False)
        ):
            classes = self._discover_classes_in_named_graphs(context)
        if not classes:
            context.report.finish_phase(p1, items=0)
            refused = context.report.report.config.get("class_listing", {}).get("state")
            context.report.report.config["class_schema_state"] = (
                "class_listing_refused" if refused == "refused" else "no_observed_data_classes"
            )
            return []

        # Merge with ontology classes if available
        if context.ontology_classes:
            # Merge, keeping unique classes
            classes_set = set(classes)
            new_from_ontology = [c for c in context.ontology_classes if c not in classes_set]
            if new_from_ontology:
                logger.info(f"  -> Adding {len(new_from_ontology)} classes from ontology structure")
                classes.extend(new_from_ontology)
            logger.info(f"  -> {len(classes)} total classes (data + ontology)")

        context.report.finish_phase(p1, items=len(classes))
        classes = self._group_terms(classes, context)
        context.class_batches = self._plan_resumed(classes, context)

        # Phase 2 - batched per-class pattern discovery
        p2 = context.report.start_phase("per-class-patterns")
        patterns, abort_reason = self._run_phase2_batches(
            classes,
            context.graph_uris,
            context,
            batches=context.class_batches,
        )

        logger.info(f"  -> {len(patterns)} total patterns from {len(classes)} classes")
        context.report.finish_phase(p2, items=len(patterns), error=abort_reason)
        if abort_reason:
            context.report.set_abort_reason(abort_reason)
        return patterns

    def _group_terms(self, classes: list[str], context: MiningContext) -> list[str]:
        """Group ontology terms under their ancestors before per-class mining.

        Only when ontology-as-data is on and there are more classes than the configured limit
        (for example about 181,000 ChEBI types in PubChem, which cannot be mined one by one).
        The representatives are chosen as after mining (choose_representatives) and recorded
        with their members for review; the rows of each representative are then mined over all
        its member terms at once.
        """
        budget, limit = context.ontology_term_budget, context.group_before_mining
        if budget is None or limit is None or len(classes) <= limit:
            return classes
        phase = context.report.start_phase("ontology-terms/group-before-mining")
        try:
            return self._group_terms_now(classes, context, phase)
        except SparqlHelperError as error:
            # Grouping is an optimisation, but these classes are too many to mine one by one
            # (GO-CAM: 1.7 M; at 15 classes a batch and a few queries a batch, weeks). The
            # source is not failed: it ends partial, with the reason and the class count.
            logger.warning("Grouping before mining failed (%s); classes not mined", error)
            context.report.finish_phase(phase, error=str(error)[:300])
            context.report.report.config["ontology_term_grouping"] = {
                "before_mining": False,
                "state": "failed",
                "error": str(error)[:500],
                "limit": limit,
                "classes": len(classes),
            }
            context.report.record_outcome(
                QueryOutcome(
                    state="failed",
                    failures=[
                        QueryFailure(
                            "timeout" if isinstance(error, EndpointTimeoutError) else "endpoint",
                            f"{len(classes)} classes not mined: grouping before mining failed: "
                            f"{str(error)[:300]}",
                            "ontology-terms/group-before-mining",
                        )
                    ],
                )
            )
            return []

    def _group_terms_now(self, classes: list[str], context: MiningContext, phase: Any) -> list[str]:
        """Group the classes (see _group_terms); raise SparqlHelperError when a query fails."""
        from collections import Counter

        from rdfsolve.mining import ontology_as_data
        from rdfsolve.mining.ontology_as_data import (
            choose_representatives,
            fetch_shapes,
            group_by_namespace,
            group_by_shape,
            parentless_candidates,
            spread,
        )
        from rdfsolve.mining.query_builders import Representative
        from rdfsolve.ontology.hierarchy import fetch_superclasses, fill_parents, read_hierarchy
        from rdfsolve.ontology.terms import namespace

        budget, limit = context.ontology_term_budget, context.group_before_mining
        if budget is None or limit is None:
            return classes
        unreadable_parents: set[str] = set()
        parents = fetch_superclasses(
            context.helper,
            classes,
            graph_uris=context.ontology_graph_uris,
            unreadable=unreadable_parents,
        )
        files = {path: read_hierarchy([path]) for path in context.ontology_hierarchy_files}
        with_loaded = fill_parents(parents, list(files.values()), classes)
        chosen = choose_representatives(classes, parents, budget)
        # Terms that no ancestor can take are grouped by the shape of their instances (the owner
        # decision of 2026-09-30, gate 4): a group is a shared unknown type, named later. When
        # they are too many to read their shapes through the endpoint, they are grouped by
        # namespace (owner decision of 2026-10-06, GO-CAM).
        candidates = parentless_candidates(chosen, parents)
        namespace_groups: dict[str, dict[str, Any]] = {}
        shapes: dict[str, Any] = {}
        unreadable: list[str] = []
        if len(candidates) > ontology_as_data.SHAPE_READ_MAX_TERMS:
            logger.info(
                "%d terms without a parent: grouped by namespace, not by shape", len(candidates)
            )
            namespace_groups = group_by_namespace(chosen, parents)
            shape_groups: dict[str, frozenset[str]] = {}
        else:
            shapes, unreadable = fetch_shapes(
                context.helper, candidates, graph_uris=context.graph_uris
            )
            shape_groups = group_by_shape(
                chosen, parents, {t: s.properties for t, s in shapes.items()}
            )
        members = chosen.members()
        without_parent = Counter(namespace(c) for c in classes if not parents.get(c))
        present = set(classes)
        grouped: list[str] = []
        sampled: dict[str, int] = {}
        cap = ontology_as_data.NAMESPACE_GROUP_MINED_MEMBERS
        for rep in sorted(set(chosen.representative.values())):
            terms = members.get(rep, [])
            if rep in namespace_groups and len(terms) > cap:
                # Every member stays on record (representative_members); the queries name a
                # spread of them, and the rows of the group are marked sampled.
                sampled[rep] = len(terms)
                namespace_groups[rep]["mined_members"] = cap
                grouped.append(Representative(rep, spread(terms, cap)))
            elif terms:
                grouped.append(Representative(rep, [*terms, *([rep] if rep in present else [])]))
            else:
                grouped.append(rep)
        if sampled:
            context.report.record_outcome(
                QueryOutcome(
                    state="partial",
                    failures=[
                        QueryFailure(
                            "sampled",
                            f"namespace group mined over {cap} of its {n} member terms",
                            "ontology-terms/group-before-mining",
                            classes=[rep],
                        )
                        for rep, n in sorted(sampled.items())
                    ],
                )
            )
        context.grouped_members = members
        context.report.report.config["ontology_term_grouping"] = {
            "before_mining": True,
            "limit": limit,
            "budget": budget,
            "classes_before": chosen.classes_before,
            "classes_after": chosen.classes_after,
            "levels_lifted": chosen.levels_lifted,
            "over_budget": chosen.over_budget,
            "hierarchy_graph_uris": context.ontology_graph_uris,
            "terms_without_parent_by_namespace": dict(without_parent.most_common()),
            "hierarchy_files": [
                {"path": path, "pairs": sum(len(ps) for ps in table.values())}
                for path, table in files.items()
            ],
            "terms_with_loaded_parent_by_namespace": dict(
                Counter(namespace(c) for c in with_loaded).most_common()
            ),
            "grouping_of_terms_without_parent": "namespace" if namespace_groups else "shape",
            "terms_without_parent_candidates": len(candidates),
            "shape_read_max_terms": ontology_as_data.SHAPE_READ_MAX_TERMS,
            # Only when the terms without a parent were grouped by namespace (the shape groups,
            # the owner decision of 2026-09-30, replaced the earlier namespace groups).
            **({"namespace_groups": namespace_groups} if namespace_groups else {}),
            "namespace_min_terms": ontology_as_data.NAMESPACE_GROUP_MIN_TERMS,
            "shape_groups": {
                group: {
                    "properties": sorted(shape),
                    "terms": len(members.get(group, [])),
                    "namespaces": dict(
                        Counter(namespace(t) for t in members.get(group, [])).most_common()
                    ),
                    "instances": sum(shapes[t].instances for t in members.get(group, [])),
                    "example_instance": next(
                        (shapes[t].example for t in members.get(group, []) if shapes[t].example),
                        None,
                    ),
                }
                for group, shape in shape_groups.items()
            },
            "terms_in_shapes_of_one_term": sum(
                1 for t in shapes if chosen.representative.get(t) == t
            ),
            "unreadable_shape_terms": sorted(unreadable),
            "terms_whose_parents_were_not_read": len(unreadable_parents),
            "representative_members": members,
            "review_state": "unreviewed",
        }
        context.report.finish_phase(phase, items=len(grouped))
        logger.info("Grouped %d classes into %d before mining", len(classes), len(grouped))
        return grouped

    def _plan_resumed(self, classes: list[str], context: MiningContext) -> list[list[str]]:
        """Keep the completed batches of an earlier run and plan only the other classes.

        A pattern row belongs to one subject class, so a completed batch is reused when all its
        classes are classes of this run, also under another plan (PubChem run 6 was planned in
        fixed batches after its weight count was refused).
        """
        by_name = {str(c): c for c in classes}
        kept = [
            [by_name[c] for c in batch] for batch in context.resumed if set(batch) <= set(by_name)
        ]
        done = {str(c) for batch in kept for c in batch}
        rest = [c for c in classes if str(c) not in done]
        # Every class is weighed: _same_members also checks the reused batches.
        return kept + self._plan_batches(rest, context, weigh=classes)

    def _plan_batches(
        self, classes: list[str], context: MiningContext, *, weigh: list[str] | None = None
    ) -> list[list[str]]:
        """Use fixed batches, or instance-count batches when there are many classes.

        *weigh* are the classes whose weights are kept (the planned classes by default).
        """
        size = context.class_batch_size
        fixed = [classes[i : i + size] for i in range(0, len(classes), size)]
        if len(classes) <= WEIGHTED_BATCHING_ABOVE and context.helper.sparql_engine != "qlever":
            return fixed
        counts = {
            "exact": _build_class_weight_query(context.graph_uris, context.type_context_graph_uris)
        }
        if context.helper.sparql_engine == "qlever" and (
            context.graph_uris or context.type_context_graph_uris
        ):
            # The rdf:type triples of each class in the whole index bound its subjects in the
            # scope. The weights only plan batches, and _same_members confirms a match with a
            # count of the scope (PubChem: COUNT(DISTINCT) with 26 FROM clauses asked 68.8 GB;
            # the bound took 1.6 s).
            counts["upper_bound"] = _build_class_weight_query(None)
        rows = None
        for state, query in counts.items():
            t0 = time.monotonic()
            try:
                # Weights only pack batches: read without the exclusion when the listing is.
                suspended = _engine_only_classes(context) is not None
                with context.helper.graph_exclusion_suspended() if suspended else nullcontext():
                    rows = context.collect_bindings(
                        query, "two-phase/class-weights", context.chunk_size
                    )
            except Exception as error:
                context.report.record_query(
                    "two-phase/class-weights", time.monotonic() - t0, success=False
                )
                logger.warning("Instance counts per class (%s) failed: %s", state, error)
                continue
            context.report.record_query("two-phase/class-weights", time.monotonic() - t0)
            context.report.report.config["class_weights"] = state
            break
        if rows is None:
            logger.warning("Instance counts per class failed; using fixed batches")
            context.report.report.config["class_weights"] = "failed"
            return fixed
        weights: dict[str, int] = {}
        for row in rows:
            try:
                weights[row["class"]["value"]] = int(row["n"]["value"])
            except (KeyError, TypeError, ValueError):
                continue
        for cls in weigh if weigh is not None else classes:
            members = getattr(cls, "members", None)
            if members:
                weights[cls] = sum(weights.get(member, 0) for member in members)
        max_classes = size if len(classes) <= WEIGHTED_BATCHING_ABOVE else MAX_CLASSES_PER_BATCH
        context.class_weights = weights
        batches = plan_class_batches(classes, weights, max_classes=max_classes)
        logger.info(
            "  -> %d classes packed into %d batches by instance count (was %d fixed batches)",
            len(classes),
            len(batches),
            len(fixed),
        )
        return batches

    def _discover_classes(self, context: MiningContext) -> list[str]:
        """Discover named classes; read the listing without the graph exclusion when it can be.

        When the classes typed only in the excluded engine graphs are known
        (SparqlHelper.engine_only_classes), the listing is read without the exclusion prologue,
        which can make it much slower (forum, 2026-10-06: 175 s without it, a 502 after 315 s
        with it), and those classes are dropped from it: the same classes.
        """
        try:
            return self._discover_classes_within_limit(context)
        except ClassListingLimitError as stopped:
            return self._per_record(context, stopped)

    def _discover_classes_within_limit(self, context: MiningContext) -> list[str]:
        """Discover the classes (see _discover_classes); raise ClassListingLimitError when the
        listing passes context.class_listing_limit.
        """
        engine_only = _engine_only_classes(context)
        if engine_only is None:
            return self._list_discovered_classes(context)
        with context.helper.graph_exclusion_suspended():
            classes = self._list_discovered_classes(context)
        kept = [c for c in classes if c not in engine_only]
        if len(kept) < len(classes):
            logger.info("  -> %d classes of the engine graphs left out", len(classes) - len(kept))
        if context.discovered_classes is not None:  # None: a sampled listing
            context.discovered_classes = list(kept)
        return kept

    def _per_record(self, context: MiningContext, stopped: ClassListingLimitError) -> list[str]:
        """Record why class discovery stopped, and return a sample of the listed classes.

        Two small queries describe a spread of 200 listed classes: their instances, and their
        named parents. Classes with about one instance each, under a few parents, are records
        or per-record types (BioGateway: SO_0000727 cis-regulatory modules, one evidence
        individual each). The run goes on with a spread of CLASS_SAMPLE listed classes, grouped
        before mining as any other list of classes when ontology terms are grouped; the rows are
        marked sampled (a "sampled" failure), and the listing is the sorted start of all
        classes, so classes after it are not in the sample.
        """
        from statistics import median

        from rdfsolve.mining.ontology_as_data import spread
        from rdfsolve.ontology.hierarchy import RDFS_SUBCLASS_OF

        context.class_listing_stopped = True
        listed = [
            b["class"]["value"] for b in stopped.rows if b.get("class", {}).get("type") == "uri"
        ]
        sample = spread(listed, 200)
        values = " ".join(f"<{c}>" for c in sample)
        evidence: dict[str, Any] = {
            "state": "stopped",
            "limit": stopped.limit,
            "listed": len(stopped.rows),
            "sample": len(sample),
        }
        budget = getattr(context.helper, "budget", None)
        try:
            with budget(60) if budget is not None else nullcontext():
                counts = context.helper.select(
                    f"SELECT ?c (COUNT(?s) AS ?n) WHERE {{ VALUES ?c {{ {values} }} "
                    f"?s {membership_path()} ?c }} GROUP BY ?c",
                    purpose="two-phase/class-sample",
                )["results"]["bindings"]
                parents = context.helper.select(
                    f"SELECT ?k (COUNT(DISTINCT ?c) AS ?n) WHERE {{ VALUES ?c {{ {values} }} "
                    f"?c <{RDFS_SUBCLASS_OF}> ?k FILTER(isIRI(?k)) }} GROUP BY ?k",
                    purpose="two-phase/class-sample",
                )["results"]["bindings"]
            instances = [int(row["n"]["value"]) for row in counts]
            evidence.update(
                median_instances=median(instances) if instances else None,
                classes_with_one_instance=sum(1 for n in instances if n == 1),
                parents=dict(
                    sorted(
                        ((row["k"]["value"], int(row["n"]["value"])) for row in parents),
                        key=lambda item: -item[1],
                    )[:10]
                ),
            )
        except (SparqlHelperError, KeyError, ValueError) as error:
            evidence["sample_error"] = str(error)[:300]
        mined = spread(listed, CLASS_SAMPLE)
        evidence["mined_classes"] = len(mined)
        context.report.report.config["class_discovery"] = evidence
        message = (
            f"class discovery stopped after {len(stopped.rows)} type values (limit "
            f"{stopped.limit}); {len(mined)} of the listed classes are mined"
        )
        logger.warning("%s: %s", message, {k: v for k, v in evidence.items() if k != "parents"})
        context.report.record_outcome(
            QueryOutcome(
                state="partial",
                failures=[QueryFailure("sampled", message, "two-phase/classes")],
            )
        )
        # Not every class was discovered: the census tests each edge (_typed_edges_covered).
        context.discovered_classes = None
        return mined

    def _list_discovered_classes(self, context: MiningContext) -> list[str]:
        """Discover named classes in the current scope.

        Every later result depends on this listing, so a gateway that answers for an
        overloaded host (pdbj.bmrb: Apache "Proxy Error" 502, job 115329) is waited out before
        the listing is given up (graph_selection.wait_out_gateway).
        """
        listed = True
        try:
            class_bindings = self._read_class_listing(context)
        except (EndpointTimeoutError, EndpointRateLimitError) as error:
            class_bindings = self._sample_classes(context, error)
            listed = False

        classes = []
        non_iri_count = 0
        for b in class_bindings:
            binding = b.get("class", {})
            if binding.get("type") == "uri":
                value = binding.get("value", "")
                if value:
                    classes.append(value)
            else:
                non_iri_count += 1
        if non_iri_count:
            logger.info(f"  -> Skipped {non_iri_count} non-IRI type values")
        logger.info(f"  -> {len(classes)} data classes found")
        # A sampled listing leaves discovered_classes None: not every class was listed.
        context.discovered_classes = list(classes) if listed else None
        context.skipped_type_values = non_iri_count
        return classes

    def _read_class_listing(self, context: MiningContext) -> list[dict[str, Any]]:
        """Read the whole class listing, in one query or in pages (_list_discovered_classes)."""
        from rdfsolve.mining.graph_selection import wait_out_gateway

        ccs = context.class_chunk_size
        if ccs is None:
            t0 = time.monotonic()
            try:
                class_bindings = wait_out_gateway(
                    context.helper, lambda: self._list_classes(context), "Class listing"
                )
                context.report.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                )
            except Exception:
                context.report.record_query(
                    "two-phase/classes",
                    time.monotonic() - t0,
                    success=False,
                )
                raise
            return class_bindings
        logger.info("Phase 1: discovering classes (chunk_size=%d) …", ccs)
        q = _build_class_discovery_query(context.graph_uris, context.type_context_graph_uris)
        return wait_out_gateway(
            context.helper,
            lambda: self._page_classes(context, q, ccs),
            "Class listing",
        )

    def _sample_classes(
        self, context: MiningContext, error: SparqlHelperError
    ) -> list[dict[str, Any]]:
        """List the classes of a sample of the type statements when the whole listing failed.

        pdbj.bmrb (job 115591): every form of ``SELECT DISTINCT ?class WHERE { ?s a ?class }``
        (one query, then pages from offset 0) was cut by the proxy with HTTP 502 at about
        122 s, while a trivial query answered at once, and the source ended FAILED after
        1295 s. The listing is asked again over the first N type statements
        (rdfsolve.mining.sampling: N, N/10, N/100). The classes of the sample are mined as
        any others; classes outside it are not, so the listing is recorded as sampled (a lower
        bound, config class_listing) and the source ends partial. When the samples are refused
        too, or the host stayed busy, the listing is recorded as a gap and no class is mined:
        the source goes on with the steps that do not need classes (untyped subjects). A host
        that does not answer a trivial query either (host_answers) answers nothing at all, and
        the error is raised: the source fails.
        """
        from rdfsolve.mining.query_fallbacks import _failure
        from rdfsolve.mining.sampling import sample_query, sampled_select

        plain = _build_class_discovery_query_plain(
            context.graph_uris, context.type_context_graph_uris
        )
        refused = _failure(error, "two-phase/classes", [], context.graph_uris)
        found = sampled_select(
            lambda size: sample_query(plain, size),
            "two-phase/classes",
            context.helper,
            refused,
            unit="type statements",
            graph_uris=context.graph_uris,
        )
        context.class_listing_stopped = True  # the named-graph retry would be refused alike
        if found.state == "complete" and found.samples:
            (sample, *_) = found.samples
            listing = {
                "state": "sampled",
                "count_bound": "lower_bound",
                "sample": sample.provenance(),
                "error": str(error)[:500],
            }
            message = (
                f"class listing refused ({str(error)[:300]}); the classes of a sample of "
                f"{sample.size} type statements are mined, a lower bound"
            )
            category: FailureCategory = "sampled"
            logger.warning("Class listing: %s", message)
        else:
            from rdfsolve.mining.graph_selection import host_answers

            if not host_answers(context.helper):
                raise error  # an endpoint that answers nothing at all fails the source
            listing = {"state": "refused", "error": str(error)[:500]}
            message = f"class listing refused, also over samples: {str(error)[:300]}"
            category = refused.failures[0].category
            logger.warning("Class listing: %s; no class is mined", message)
        context.report.report.config["class_listing"] = listing
        context.report.record_outcome(
            QueryOutcome(
                state="partial",
                failures=[QueryFailure(category, message, "two-phase/classes")],
                samples=found.samples,
            )
        )
        return found.rows if found.state == "complete" else []

    def _list_classes(self, context: MiningContext) -> list[dict[str, Any]]:
        """List the classes in one query; page the listing when it is cut or capped."""
        logger.info("Phase 1: discovering classes (no pagination) …")
        q = _build_class_discovery_query_plain(context.graph_uris, context.type_context_graph_uris)
        try:
            result = context.helper.select(q, purpose="two-phase/classes")
            class_bindings: list[dict[str, Any]] = result.get("results", {}).get("bindings", [])
            limit = getattr(context, "class_listing_limit", None)
            if limit is not None and len(class_bindings) > limit:
                raise ClassListingLimitError(class_bindings, limit)
            if SparqlHelper.row_cap_suspected(len(class_bindings)):
                # A server cap cuts a listing silently: read it in pages of the cap.
                logger.warning(
                    "Class listing: exactly %d rows, a common server cap; paging it",
                    len(class_bindings),
                )
                class_bindings = self._page_classes(
                    context,
                    _build_class_discovery_query(
                        context.graph_uris, context.type_context_graph_uris
                    ),
                    len(class_bindings),
                )
        except EndpointTimeoutError as error:
            # A response limit, a time limit, or an answer cut off at the time limit of
            # the engine (PubChem on a shared node: QLever stopped after 600 s at 22 MB).
            logger.warning("Class listing refused (%s); paging it", error)
            class_bindings = self._page_classes(
                context,
                _build_class_discovery_query(context.graph_uris, context.type_context_graph_uris),
                context.chunk_size,
            )
        return class_bindings

    def _page_classes(
        self, context: MiningContext, query: str, size: int | None
    ) -> list[dict[str, Any]]:
        """Read the class listing in pages; on a deep OFFSET failure, read it by key.

        forum (job 115328) read 89,375 classes in OFFSET pages of 0.2-0.7 s, then each page
        from offset 80,000 took about 5 min and ended in a 502: an engine that skips OFFSET
        rows pays for every row skipped. When an OFFSET page fails after the first, the
        listing is read again by key (keyset paging: ORDER BY the class's key and FILTER past
        the last key read, SparqlHelper.select_chunked with pagination="cursor"), which costs
        the same at any depth. The OFFSET pages stay the first choice: a key page sorts the
        whole listing (forum: more than its proxy's 300 s), while a shallow OFFSET page does
        not. When the key pages fail too, the classes read by both are kept and mined: the
        listing is recorded as truncated (an unresolved failure, so the source ends PARTIAL,
        and config.class_listing, whose class count is a lower bound). A failure before any
        row is read is raised as before.
        """
        from rdfsolve.sparql_helper import PaginationTruncatedError

        limit = getattr(context, "class_listing_limit", None)
        try:
            if limit is None:
                return context.collect_bindings(query, "two-phase/classes", size)
            return context.collect_bindings(  # type: ignore[call-arg]
                query, "two-phase/classes", size, max_rows=limit
            )
        except PaginationTruncatedError as error:
            failure = error
            rows = list(error.partial_rows)
        keyset: dict[str, Any] | None = None
        if failure.offset > 0 and getattr(context, "pagination", "offset") != "cursor":
            logger.warning(
                "Class listing: an OFFSET page failed at offset %d (%s); reading it by key",
                failure.offset,
                str(failure)[:200],
            )
            keyed: list[dict[str, Any]] = []
            try:
                for page in context.helper.select_chunked(
                    query,
                    chunk_size=size or context.chunk_size,
                    purpose="two-phase/classes",
                    pagination="cursor",
                    cursor_keys=["class"],
                    max_page_retries=1,
                    # The host gate already spaces the requests to the endpoint.
                    delay_between_chunks=0.0,
                ):
                    keyed.extend(page)
                    if limit is not None and len(keyed) > limit:
                        raise ClassListingLimitError(keyed, limit)
            except PaginationTruncatedError as error:
                # The OFFSET failure stays the one recorded: it says how far the listing got.
                keyset = {"state": "truncated", "rows_read": len(keyed), "error": str(error)[:500]}
            else:
                context.report.report.config["class_listing"] = {
                    "state": "complete",
                    "read_by": "keyset",
                    "offset_failure": {"offset": failure.offset, "error": str(failure)[:500]},
                }
                return keyed
            seen = {json.dumps(row, sort_keys=True) for row in rows}
            rows += [row for row in keyed if json.dumps(row, sort_keys=True) not in seen]
        if not rows:
            raise failure
        logger.warning(
            "Class listing truncated at offset %d after %d rows (%s); mining what was read",
            failure.offset,
            len(rows),
            str(failure)[:200],
        )
        message = (
            f"class listing truncated at offset {failure.offset}: {len(rows)} rows read, "
            f"the classes are a lower bound: {str(failure)[:300]}"
        )
        context.report.record_outcome(
            QueryOutcome(
                state="partial",
                failures=[QueryFailure("truncated", message, "two-phase/classes")],
            )
        )
        context.report.report.config["class_listing"] = {
            "state": "truncated",
            "offset": failure.offset,
            "rows_read": len(rows),
            "count_bound": "lower_bound",
            "error": str(failure)[:500],
            **({"keyset": keyset} if keyset is not None else {}),
        }
        return rows

    def _discover_classes_in_named_graphs(self, context: MiningContext) -> list[str]:
        """Retry Phase 1 in the named graphs when the default graph has no types.

        Graph listing is optional: when the endpoint does not list its graphs (STRING's proxy
        cuts the listing with HTTP 502 at 62 s), the source is not failed for it. The refusal
        is recorded as an unresolved failure of graph discovery, so that the source ends partial
        with the reason, not complete and apparently empty.
        """
        from rdfsolve.mining.graph_selection import discover_data_graphs

        try:
            graphs = discover_data_graphs(
                context.helper, excluded_prefixes=context.excluded_graph_prefixes
            )
        except SparqlHelperError as error:
            logger.warning(
                "The default graph holds no typed data, and the named graphs are not listed: %s",
                str(error)[:200],
            )
            failure = QueryFailure(
                "timeout" if isinstance(error, EndpointTimeoutError) else "endpoint",
                f"named graphs not listed: {str(error)[:300]}",
                "void/graph-discovery",
            )
            context.report.record_outcome(QueryOutcome(state="failed", failures=[failure]))
            context.report.report.config["named_graph_discovery"] = {
                "state": "graphs_not_listed",
                "error": str(error)[:500],
            }
            return []
        companion = set(context.type_context_graph_uris or []) | set(
            context.ontology_graph_uris or []
        )
        graphs = [graph for graph in graphs if graph not in companion]
        if not graphs:
            return []
        logger.info(
            "Default graph holds no typed data; retrying Phase 1 in %d named graphs", len(graphs)
        )
        context.graph_uris = graphs
        return self._discover_classes(context)

    def _run_phase2_batches(
        self,
        classes: list[str],
        graph_uris: list[str] | None,
        context: MiningContext,
        batches: list[list[str]] | None = None,
    ) -> tuple[list[SchemaPattern], str | None]:
        """Execute Phase 2 batched queries for classes, in fixed or given batches."""
        if batches is None:
            bs = context.class_batch_size
            batches = [classes[i : i + bs] for i in range(0, len(classes), bs)]
        total = len(classes)
        n_batches = len(batches)

        scope = f"{len(graph_uris)} named graphs" if graph_uris else "default graph"
        logger.info(
            "Phase 2: mining patterns in %d batches (%d classes total, scope: %s) …",
            n_batches,
            total,
            scope,
        )

        patterns: list[SchemaPattern] = []
        abort_reason: str | None = None
        anonymous_classes = 0

        def query_bisect(
            batch: list[str],
            build_fn: Callable[..., str],
            purpose: str,
        ) -> QueryOutcome:
            """Run a query group and record unresolved failures."""
            nonlocal abort_reason
            outcome = query_with_bisect(
                batch,
                graph_uris,
                build_fn,
                purpose,
                context.helper,
                lambda q, p, cs=None: context.collect_bindings(  # type: ignore[misc]
                    q, p, cs or context.chunk_size
                ),
                context.chunk_size,
                unsafe_paging=context.unsafe_paging,
                type_context_graph_uris=context.type_context_graph_uris,
            )
            context.report.record_outcome(outcome)
            if outcome.state != "complete":
                abort_reason = context.report.report.abort_reason
            return outcome

        done = 0
        mined: dict[str, tuple[int, int]] = {}  # single mined class -> its pattern slice
        partial: set[str] = set()  # classes of batches with an unresolved failure
        for batch_idx, batch in enumerate(batches):
            batch_label = (
                f"batch {batch_idx + 1}/{n_batches} "
                f"(classes {done + 1}-{done + len(batch)}/{total})"
            )
            done += len(batch)
            logger.info("  %s", batch_label)
            first = len(patterns)
            failures = len(context.report.report.query_failures)
            if tuple(batch) in context.resumed:
                rows = context.resumed[tuple(batch)]
                patterns.extend(SchemaPattern.model_validate(row) for row in rows)
                context.report.report.config["resumed_batches"].append(list(batch))
                context.report.checkpoint("patterns", batch, rows)
                source = _same_members(batch, mined, context)
                if source:
                    context.shared_extensions[batch[0]] = source
                    config = context.report.report.config
                    config.setdefault("shared_extensions", {})[batch[0]] = source
                mined[batch[0]] = (first, len(patterns))
                continue
            source = _same_members(batch, mined, context)
            if source:
                start, end = mined[source]
                copies = [
                    p.model_copy(update={"subject_class": batch[0]}) for p in patterns[start:end]
                ]
                patterns.extend(copies)
                context.shared_extensions[batch[0]] = source
                context.report.report.config.setdefault("shared_extensions", {})[batch[0]] = source
                context.report.checkpoint(
                    "patterns",
                    batch,
                    [p.model_dump(mode="json") for p in copies],
                    "partial" if source in partial else "complete",
                )
                continue

            # 2a. Typed-object patterns
            kind_start = len(patterns)
            t0 = time.monotonic()
            typed_bindings = query_bisect(
                batch, _build_batched_typed_object_query, "two-phase/typed-object"
            )
            context.report.record_query(
                "two-phase/typed-object",
                time.monotonic() - t0,
                success=typed_bindings.state == "complete",
            )
            anonymous_typed: list[dict[str, Any]] = []
            for b in typed_bindings.rows:
                if b.get("class", {}).get("type") == "bnode":
                    # An anonymous subject class cannot name a pattern.
                    anonymous_classes += 1
                    continue
                if b.get("oc", {}).get("type") == "bnode":
                    # Keep the edge, aggregated by the blank object class it points at.
                    anonymous_typed.append(b)
                    continue
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
                        context.report.record_dropped_uri(f"{cls} {p} {oc}", b)
            patterns.extend(blank_node_patterns(anonymous_typed, context, "class"))
            mark_sampled(patterns[kind_start:], typed_bindings.rows)

            # 2b. Literal patterns
            kind_start = len(patterns)
            t0 = time.monotonic()
            literal_bindings = query_bisect(
                batch, _build_batched_literal_query, "two-phase/literal"
            )
            context.report.record_query(
                "two-phase/literal",
                time.monotonic() - t0,
                success=literal_bindings.state == "complete",
            )
            for b in literal_bindings.rows:
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
                        context.report.record_dropped_uri(f"{cls} {p} Literal", b)

            mark_sampled(patterns[kind_start:], literal_bindings.rows)

            # 2c. Untyped-URI patterns
            kind_start = len(patterns)
            t0 = time.monotonic()
            untyped_bindings = query_bisect(
                batch, _build_batched_untyped_uri_query, "two-phase/untyped-uri"
            )
            context.report.record_query(
                "two-phase/untyped-uri",
                time.monotonic() - t0,
                success=untyped_bindings.state == "complete",
            )
            untyped_oc = (
                "http://www.w3.org/2002/07/owl#Class" if context.untyped_as_classes else "Resource"
            )
            for b in untyped_bindings.rows:
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
                        context.report.record_dropped_uri(f"{cls} {p} {untyped_oc}", b)

            mark_sampled(patterns[kind_start:], untyped_bindings.rows)

            # 2d. Blank node patterns
            kind_start = len(patterns)
            t0 = time.monotonic()
            blank_bindings = query_bisect(
                batch, _build_batched_blank_node_query, "two-phase/blank-node"
            )
            context.report.record_query(
                "two-phase/blank-node",
                time.monotonic() - t0,
                success=blank_bindings.state == "complete",
            )
            patterns.extend(blank_node_patterns(blank_bindings.rows, context, "class"))
            mark_sampled(patterns[kind_start:], blank_bindings.rows)
            # A batch with an unresolved failure is partial: a resumed run mines it again.
            state = (
                "complete" if len(context.report.report.query_failures) == failures else "partial"
            )
            if state == "partial":
                partial.update(batch)
            context.report.checkpoint(
                "patterns", batch, [p.model_dump(mode="json") for p in patterns[first:]], state
            )
            if len(batch) == 1:
                mined[batch[0]] = (first, len(patterns))

        if anonymous_classes:
            logger.info(
                "  -> Skipped %d rows whose subject or object class is an anonymous node",
                anonymous_classes,
            )
        return patterns, abort_reason


def _same_members(
    batch: list[str], mined: dict[str, tuple[int, int]], context: MiningContext
) -> str | None:
    """Return a mined class with exactly the same typed subjects as a one-class batch.

    Equal populations are confirmed by counting subjects typed with both classes;
    identical subject sets give identical class-property patterns and counts.
    """
    if len(batch) != 1 or batch[0] not in context.class_weights:
        return None
    size = context.class_weights[batch[0]]
    for other in mined:
        if context.class_weights.get(other) != size or other in context.shared_extensions:
            continue
        query = _build_same_members_query(
            other, batch[0], context.graph_uris, context.type_context_graph_uris
        )
        outcome = select_outcome(query, "two-phase/same-members", context.helper, batch)
        rows = outcome.rows if outcome.state == "complete" else []
        if rows and rows[0].get("n", {}).get("value") == str(size):
            return other
    return None
