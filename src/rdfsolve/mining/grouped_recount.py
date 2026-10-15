"""Exact counts for the rows that grouping ontology terms after mining merges.

subsume_patterns (rdfsolve.mining.ontology_as_data) replaces the subject and object classes of
the mined rows by their groups and sums the rows that meet: a record typed with two member
terms of a group counts twice in the sum, and the distinct counts are dropped (count_semantics
upper_bound). Each merged row is counted again here, through the endpoint, over the member terms
of its subject group and of its object group, each edge once; the row then carries the counts
of the count phase (count_semantics as apply_counts gives them). A row whose recount is refused,
answered only over a sample, or not found, keeps its upper bound, and the refusal is a
measurement gap.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from rdfsolve._outcomes import QueryFailure, QueryOutcome
from rdfsolve.mining import query_builders as builders
from rdfsolve.mining.pattern_enrichment import _count_from_binding, _literal_key, apply_counts
from rdfsolve.mining.sampling import sample_of
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS

if TYPE_CHECKING:
    from rdfsolve.mining.pattern_enrichment import _PatternCount
    from rdfsolve.mining.query_fallbacks import CollectBindings
    from rdfsolve.mining.report_tracking import ReportCollector
    from rdfsolve.schema_models.pattern import SchemaPattern
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

PURPOSE = "ontology-terms/recount"
# The count builder of each object kind of a row whose subject is a group.
_KIND_BUILDERS: dict[str, Callable[..., str]] = {
    "Literal": builders._build_batched_literal_count_query,
    "Resource": builders._build_batched_untyped_count_query,
    "BlankNode": builders._build_batched_blank_node_count_query,
}

__all__ = ["group_members", "merged_rows", "recount_merged"]


def group_members(
    before: Mapping[str, Iterable[str]],
    after: Mapping[str, str] | None = None,
) -> dict[str, frozenset[str]]:
    """Return the member terms of every group, from the groups before and after mining.

    *before* maps each group formed before mining to its member terms; *after* maps each class
    of the mined rows (a term, or a group formed before mining) to its group after mining. A
    group holds its own term and, through the groups it takes in, the terms of the data.
    """

    def terms(group: str) -> set[str]:
        """Return a group's own term with the terms grouped under it before mining."""
        return {group, *before.get(group, ())}

    members = {group: frozenset(terms(group)) for group in before}
    if after:
        lifted: dict[str, set[str]] = defaultdict(set)
        for cls, group in after.items():
            lifted[group] |= terms(cls) | terms(group)
        members.update({group: frozenset(found) for group, found in lifted.items()})
    return {group: found for group, found in members.items() if found != {group}}


def merged_rows(
    patterns: Iterable[SchemaPattern], members: Mapping[str, Iterable[str]]
) -> list[SchemaPattern]:
    """Return the rows that grouping merged as sums (upper bounds) and that can be recounted.

    Rows counted over a sample keep their flag: a recount would be a sample again. Rows of
    untyped subjects have no subject group.
    """
    return [
        p
        for p in patterns
        if p.count_semantics == "upper_bound"
        and p.evidence_source == "inferred"
        and p.sampled is None
        and p.count_bound is None
        and not p.untyped_subject
        and p.subject_binding == "type"
        and (p.subject_class in members or p.object_class in members)
    ]


def _key(pattern: SchemaPattern) -> tuple[str, str, str]:
    """Return the count key of a row, as the count phase keys it."""
    if pattern.object_class == "Literal":
        datatype = f"Literal:{pattern.datatype}" if pattern.datatype else "Literal"
        return pattern.subject_class, pattern.property_uri, datatype
    return pattern.subject_class, pattern.property_uri, pattern.object_class


def _store(
    rows: Iterable[dict[str, Any]],
    key: Callable[[dict[str, Any]], tuple[str, str, str]],
    counts: dict[tuple[str, str, str], dict[str, _PatternCount]],
) -> None:
    """Add the count rows of one query to *counts* by key and edge graph."""
    for row in rows:
        metric = _count_from_binding(row)
        if metric is not None:
            counts.setdefault(key(row), {})[row.get("_g", {}).get("value", "")] = metric


def _value(row: dict[str, Any], name: str) -> str:
    return str(row.get(name, {}).get("value", ""))


class _Recount:
    """Run the recount queries of one source and collect their counts by key and graph."""

    def __init__(
        self,
        helper: SparqlHelper,
        graph_uris: list[str] | None,
        type_context_graph_uris: list[str] | None,
        collect: CollectBindings,
        chunk_size: int,
    ) -> None:
        """Keep the scope of the source and start with no counts and no gaps."""
        self.helper = helper
        self.graph_uris = graph_uris
        self.context = type_context_graph_uris
        self.collect = collect
        self.chunk_size = chunk_size
        self.counts: dict[tuple[str, str, str], dict[str, _PatternCount]] = {}
        self.outcome = QueryOutcome()
        self.queries = 0

    def by_property(
        self, subject: str, builder: Callable[..., str], properties: list[str]
    ) -> list[dict[str, Any]]:
        """Run a count builder for the given properties of a subject group."""
        from rdfsolve.mining.property_queries import query_by_property

        found = query_by_property(
            subject,
            self.graph_uris,
            builder,
            PURPOSE,
            self.helper,
            self.collect,
            self.chunk_size,
            self.context,
            properties=properties,
        )
        self.queries += len(properties)
        self._keep(found)
        return found.rows

    def objects(
        self, subject: str, prop: str, groups: dict[str, frozenset[str]]
    ) -> list[dict[str, Any]]:
        """Count the edges of one property to the object groups; a refused query is split."""
        from rdfsolve.mining.query_fallbacks import select_outcome

        query = builders._build_grouped_object_count_query(
            [subject],
            self.graph_uris,
            prop,
            {group: sorted(terms) for group, terms in groups.items()},
            self.context,
        )
        self.queries += 1
        found = select_outcome(query, f"{PURPOSE}/objects", self.helper, [subject], self.graph_uris)
        if found.state == "complete":
            return found.rows
        if len(groups) == 1:
            self._keep(found)
            return []
        names = sorted(groups)
        half = len(names) // 2
        return [
            *self.objects(subject, prop, {g: groups[g] for g in names[:half]}),
            *self.objects(subject, prop, {g: groups[g] for g in names[half:]}),
        ]

    def _keep(self, found: QueryOutcome) -> None:
        """Keep refusals and samples as gaps of the recount, never as failures of the source."""
        self.outcome = self.outcome.merge(
            QueryOutcome(gaps=[*found.failures, *found.gaps], samples=found.samples)
        )


def recount_merged(
    patterns: list[SchemaPattern],
    members: Mapping[str, Iterable[str]],
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    report: ReportCollector,
    collect: CollectBindings,
    chunk_size: int,
    type_context_graph_uris: list[str] | None = None,
) -> tuple[list[SchemaPattern], dict[str, Any]]:
    """Count the merged rows of *patterns* again, exactly; return the rows and a summary.

    *members* maps each group to its member terms (group_members). The rows of a subject group
    are counted per property with the count builders over a Representative of its members
    (objects that are typed by a term, literals, untyped IRIs, blank nodes); the rows whose
    object is a group, with _build_grouped_object_count_query.
    """
    targets = merged_rows(patterns, members)
    summary: dict[str, Any] = {"merged_rows": len(targets), "recounted": 0, "queries": 0}
    if not targets:
        return patterns, summary
    run = _Recount(helper, graph_uris, type_context_graph_uris, collect, chunk_size)
    started = time.monotonic()
    by_subject: dict[str, list[SchemaPattern]] = defaultdict(list)
    for pattern in targets:
        by_subject[pattern.subject_class].append(pattern)
    for subject_iri, rows in sorted(by_subject.items()):
        subject = (
            builders.Representative(subject_iri, sorted(members[subject_iri]))
            if subject_iri in members
            else subject_iri
        )
        grouped_objects: dict[str, dict[str, frozenset[str]]] = defaultdict(dict)
        plain: dict[str, set[str]] = defaultdict(set)
        for pattern in rows:
            obj = pattern.object_class
            if obj in members:
                grouped_objects[pattern.property_uri][obj] = frozenset(members[obj])
            elif obj in _SENTINEL_OBJECTS:
                plain[obj].add(pattern.property_uri)
            else:
                plain["typed"].add(pattern.property_uri)
        for prop, groups in sorted(grouped_objects.items()):
            _store(
                run.objects(subject, prop, groups),
                lambda r: (_value(r, "class"), _value(r, "p"), _value(r, "oc")),
                run.counts,
            )
        if plain.get("typed"):
            _store(
                run.by_property(
                    subject, builders._build_batched_typed_count_query, sorted(plain["typed"])
                ),
                lambda r: (_value(r, "class"), _value(r, "p"), _value(r, "oc")),
                run.counts,
            )
        for kind, builder in _KIND_BUILDERS.items():
            if not plain.get(kind):
                continue
            found = run.by_property(subject, builder, sorted(plain[kind]))
            key = (
                _literal_key
                if kind == "Literal"
                else lambda r, kind=kind: (_value(r, "class"), _value(r, "p"), kind)
            )
            _store(found, key, run.counts)
            if kind == "Literal":
                _literal_objects(run, subject, sorted(plain[kind]))
    recounted: list[SchemaPattern] = []
    targeted = {id(p) for p in targets}
    unconfirmed = sampled = 0
    for pattern in patterns:
        if id(pattern) not in targeted:
            recounted.append(pattern)
            continue
        per_graph = run.counts.get(_key(pattern))
        if not per_graph:
            unconfirmed += 1
            recounted.append(pattern)
            continue
        if any(metric.sample is not None for metric in per_graph.values()):
            # A sample gives lower bounds; the sum stays, as the upper bound.
            sampled += 1
            recounted.append(pattern)
            continue
        # Counted over the members of its groups: exact, no longer a sum. The row stays an
        # interpretation of its members' rows (evidence_source inferred), as in scan mining.
        recounted.append(apply_counts(pattern, per_graph, graph_uris))
        summary["recounted"] += 1
    gaps = run.outcome.gaps
    if unconfirmed:
        gaps = [
            *gaps,
            QueryFailure(
                "invalid_response",
                f"{unconfirmed} merged rows not found by their recount; they keep their sums "
                "(upper bounds)",
                PURPOSE,
            ),
        ]
    report.record_outcome(QueryOutcome(gaps=gaps, samples=run.outcome.samples))
    report.record_query(PURPOSE, time.monotonic() - started, success=not gaps)
    summary.update(
        {
            "queries": run.queries,
            "kept_upper_bound": len(targets) - summary["recounted"],
            "not_found": unconfirmed,
            "sampled": sampled,
            "refused_queries": len(run.outcome.gaps),
        }
    )
    logger.info(
        "Recounted %d of %d merged rows (%d queries)",
        summary["recounted"],
        len(targets),
        run.queries,
    )
    return recounted, summary


def _literal_objects(run: _Recount, subject: str, properties: list[str]) -> None:
    """Add the distinct literal objects of a subject group's properties to the counts."""
    found = run.by_property(subject, builders._build_batched_literal_objects_query, properties)
    for row in found:
        key, graph = _literal_key(row), row.get("_g", {}).get("value", "")
        metric = run.counts.get(key, {}).get(graph)
        value = row.get("objects", {}).get("value")
        if metric is not None and value is not None and (metric.sample or not sample_of(row)):
            run.counts[key][graph] = replace(metric, distinct_objects=int(value))
