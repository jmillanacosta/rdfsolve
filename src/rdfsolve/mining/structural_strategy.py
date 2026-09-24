"""Mine graph-local shapes for edges absent from typed profiles."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from rdflib import Literal

from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy
from rdfsolve.mining.typed_coverage import typed_match, uncovered_filter
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern


def _dataset(graph: str | None, type_graphs: list[str] | None = None) -> str:
    if graph is None:
        return ""
    return " ".join(
        ([f"FROM <{graph}>"] if graph else []) + [f"FROM NAMED <{g}>" for g in type_graphs or []]
    )


def _types(graphs: list[str] | None) -> str:
    if not graphs:
        return "?s a ?_type ."
    values = " ".join(f"<{g}>" for g in graphs)
    return f"VALUES ?_typeGraph {{ {values} }} GRAPH ?_typeGraph {{ ?s a ?_type }}"


def _untyped(graphs: list[str] | None) -> str:
    return f"FILTER NOT EXISTS {{ {_types(graphs)} }}"


def _discovery_query(
    graph: str | None,
    named_graphs: list[str],
    residual: str,
    *,
    include_edges: bool = False,
) -> str:
    edges = "?s ?o " if include_edges else ""
    edge_pattern = f"?s ?p ?o . {residual}"
    if include_edges:
        edge_pattern = f"{{ SELECT ?s ?p ?o WHERE {{ {edge_pattern} }} }}"
    return f"""SELECT DISTINCT {edges}?ss ?os ?p ?sk ?ok ?dt ?lang
{_dataset(graph, named_graphs)} WHERE {{
  {edge_pattern}
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=" ") AS ?ss)
     WHERE {{ ?s ?sp ?sv }} GROUP BY ?s }}
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=" ") AS ?os)
       WHERE {{ ?o ?op ?ov }} GROUP BY ?o }}
  }}
  BIND(IF(isIRI(?s), "IRI", "BlankNode") AS ?sk)
  BIND(IF(isIRI(?o), "IRI", IF(isBlank(?o), "BlankNode", "Literal")) AS ?ok)
  BIND(DATATYPE(?o) AS ?dt)
  BIND(LANG(?o) AS ?lang)
}}"""


def _shape(node: str, properties: list[str], kind: str, *, exact: bool = True) -> str:
    test = {"IRI": "isIRI", "BlankNode": "isBlank", "Literal": "isLiteral"}[kind]
    clauses = [f"FILTER({test}({node}))"]
    if kind == "Literal" or not exact:
        return " ".join(clauses)
    for prop in properties:
        clauses.append(f"FILTER EXISTS {{ {node} <{prop}> ?_value }}")
    exclusion = (
        "FILTER(?_property NOT IN (" + ", ".join(f"<{p}>" for p in properties) + "))"
        if properties
        else ""
    )
    clauses.append(f"FILTER NOT EXISTS {{ {node} ?_property ?_value . {exclusion} }}")
    return " ".join(clauses)


def structural_queries(pattern: StructuralPattern) -> tuple[str, str]:
    """Build an exact recount and witness for the recorded profile and scope."""
    exact = pattern.shape_semantics == "exact_property_sets"
    clauses = [
        f"VALUES ?p {{ <{pattern.property_uri}> }} ?s ?p ?o .",
        _shape("?s", pattern.subject_properties, pattern.subject_kind, exact=exact),
        _shape("?o", pattern.object_properties, pattern.object_kind, exact=exact),
    ]
    if pattern.subject_selection == "untyped":
        clauses.append(_untyped(pattern.type_graph_uris))
    if pattern.subject_selection == "uncovered":
        keys = [(s, pattern.property_uri, o, dt) for s, o, dt in pattern.covered_types]
        clauses.append(
            uncovered_filter(keys, pattern.type_graph_uris, pattern.object_type_graph_uris)
        )
    if pattern.datatype:
        clauses.append(f"FILTER(DATATYPE(?o) = <{pattern.datatype}>)")
    if pattern.language is not None:
        clauses.append(f"FILTER(LANG(?o) = {Literal(pattern.language).n3()})")
    named = list(dict.fromkeys(pattern.type_graph_uris + pattern.object_type_graph_uris))
    body = f"{_dataset(pattern.graph_uri, named)} WHERE {{ {' '.join(clauses)} }}"
    return (
        f"SELECT ?s ?o {body} LIMIT 1",
        (
            "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?s) AS ?subjects) "
            f"(COUNT(DISTINCT ?o) AS ?objects) {body}"
        ),
    )


def _select(context: MiningContext, query: str, purpose: str) -> list[dict[str, Any]]:
    started = time.monotonic()
    try:
        response = context.helper.select_with_fallback(query, purpose=purpose)
        rows: list[dict[str, Any]] = response["results"]["bindings"]
    except Exception:
        context.report.record_query(purpose, time.monotonic() - started, success=False)
        raise
    context.report.record_query(purpose, time.monotonic() - started)
    return rows


class StructuralStrategy(MiningStrategy):
    """Add exact structural evidence only for edges absent from typed profiles."""

    def __init__(self, patterns: list[SchemaPattern] | None = None) -> None:
        """Use supplied observations or discover typed profiles first."""
        self.patterns = patterns

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "structural"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Measure typed coverage and mine only uncovered edges."""
        patterns = self.patterns if self.patterns is not None else TwoPhaseStrategy().mine(context)
        if context.report.report.abort_reason or context.report.report.query_failures:
            context.report.report.config["structural_coverage"] = [
                {"state": "not_checked", "reason": "typed_mining_incomplete"}
            ]
            return patterns
        keys = sorted(
            {
                (p.subject_class, p.property_uri, p.object_class, p.datatype)
                for p in patterns
                if p.subject_binding == p.object_binding == "type" and p.evidence_source == "mined"
            },
            key=str,
        )
        local = isinstance(context.helper, LocalGraphHelper)
        context.report.report.config["structural_execution"] = (
            "local_bulk" if local else "per_profile_queries"
        )
        observations: dict[str | None, list[dict[str, Any]]] = {}
        phase = context.report.start_phase("typed-coverage")
        coverage: list[dict[str, Any]] = []
        context.report.report.config["structural_coverage"] = coverage
        graphs: Sequence[str | None] = (
            sorted(set(context.graph_uris)) if context.graph_uris else [None]
        )
        named = list(
            dict.fromkeys((context.graph_uris or []) + (context.type_context_graph_uris or []))
        )
        for graph in graphs:
            entry: dict[str, Any] = {"graph_uri": graph, "state": "checking"}
            coverage.append(entry)
            context.report.flush()
            try:
                match = typed_match(keys, context.graph_uris, context.type_context_graph_uris)
                selection = "" if local else "?covered"
                binding = "" if local else f"BIND({match} AS ?covered)"
                rows = _select(
                    context,
                    f"""SELECT ?typed {selection} (COUNT(*) AS ?n)
{_dataset(graph, named)} WHERE {{ ?s ?p ?o .
BIND(EXISTS {{ {_types(context.graph_uris)} }} AS ?typed)
{binding}
}} GROUP BY ?typed {selection}""",
                    "structural/coverage",
                )
                total = sum(int(r["n"]["value"]) for r in rows)
                untyped = sum(
                    int(r["n"]["value"]) for r in rows if r["typed"]["value"] in {"false", "0"}
                )
                if local:
                    needed = untyped > 0 or bool(
                        _select(
                            context,
                            f"SELECT ?s {_dataset(graph, named)} WHERE {{ "
                            f"?s ?p ?o . FILTER(!{match}) }} LIMIT 1",
                            "structural/check",
                        )
                    )
                    observations[graph] = (
                        _select(
                            context,
                            _discovery_query(graph, named, f"FILTER(!{match})", include_edges=True),
                            "structural/discovery",
                        )
                        if needed
                        else []
                    )
                    missing = len(observations[graph])
                    covered = total - missing
                    if covered < 0:
                        raise ValueError("Structural bindings exceed the graph triple count")
                else:
                    covered = sum(
                        int(r["n"]["value"]) for r in rows if r["covered"]["value"] in {"true", "1"}
                    )
                    missing = total - covered
                entry.update(
                    triple_count=total,
                    untyped_subject_triples=untyped,
                    excluded_subject_triples=0,
                    covered_triples=covered,
                    uncovered_triples=missing,
                    type_graph_uris=context.graph_uris,
                    subject_selection="uncovered",
                    state="needed" if missing else "not_needed",
                )
            except Exception:
                entry["state"] = "failed"
                raise
        if keys and not any(entry["covered_triples"] for entry in coverage):
            for entry in coverage:
                entry.update(state="failed", reason="typed_coverage_mismatch")
            raise ValueError("Typed observations have zero edge coverage")
        context.report.finish_phase(phase, items=len(coverage))
        if not any(entry["uncovered_triples"] for entry in coverage):
            return patterns
        phase = context.report.start_phase("structural-patterns")
        for entry in coverage:
            if not entry["uncovered_triples"]:
                continue
            try:
                self._mine_graph(context, entry, keys, named, observations.get(entry["graph_uri"]))
            except Exception:
                entry["state"] = "failed"
                raise
        context.report.report.config["structural_pattern_count"] = len(context.structural_patterns)
        context.report.finish_phase(phase, items=len(context.structural_patterns))
        return patterns

    def _mine_graph(
        self,
        context: MiningContext,
        entry: dict[str, Any],
        keys: list[tuple[str, str, str, str | None]],
        named: list[str],
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        graph = entry["graph_uri"]
        bulk = rows is not None
        if rows is None:
            residual = uncovered_filter(keys, context.graph_uris, context.type_context_graph_uris)
            rows = _select(
                context, _discovery_query(graph, named, residual), "structural/discovery"
            )
        candidates: dict[str, StructuralPattern] = {}
        bindings: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            predicate = row["p"]["value"]
            candidate = StructuralPattern(
                subject_properties=row["ss"]["value"].split(),
                object_properties=row.get("os", {}).get("value", "").split(),
                subject_kind=row["sk"]["value"],
                object_kind=row["ok"]["value"],
                property_uri=predicate,
                datatype=row.get("dt", {}).get("value"),
                language=row.get("lang", {}).get("value"),
                graph_uri=graph,
                type_graph_uris=context.graph_uris or [],
                object_type_graph_uris=context.type_context_graph_uris or [],
                covered_types=[(s, o, dt) for s, p, o, dt in keys if p == predicate],
                subject_selection="uncovered",
                count=0,
                distinct_subjects=0,
                distinct_objects=0,
                witness_query="",
                recount_query="",
            )
            key = json.dumps(candidate.model_dump(), sort_keys=True)
            candidates[key] = candidate
            if bulk:
                bindings[key].append(row)
        structural = []
        for key in sorted(candidates):
            pattern = candidates[key]
            pattern.witness_query, pattern.recount_query = structural_queries(pattern)
            if bulk:
                pairs = {
                    (json.dumps(row["s"], sort_keys=True), json.dumps(row["o"], sort_keys=True))
                    for row in bindings[key]
                }
                pattern.count = len(pairs)
                pattern.distinct_subjects = len({subject for subject, _ in pairs})
                pattern.distinct_objects = len({obj for _, obj in pairs})
                pattern.examples = [{name: bindings[key][0][name] for name in ("s", "o")}]
            else:
                counts = _select(context, pattern.recount_query, "structural/count")
                if len(counts) != 1:
                    raise ValueError("Expected one structural count row")
                pattern.count = int(counts[0]["n"]["value"])
                pattern.distinct_subjects = int(counts[0]["subjects"]["value"])
                pattern.distinct_objects = int(counts[0]["objects"]["value"])
                pattern.examples = _select(context, pattern.witness_query, "structural/witness")
            if pattern.count < 1 or len(pattern.examples) != 1:
                raise ValueError("Structural discovery and witnesses disagree")
            structural.append(pattern)
        if sum(p.count for p in structural) != entry["uncovered_triples"]:
            raise ValueError("Structural patterns do not account for the uncovered edges")
        context.structural_patterns.extend(structural)
        entry.update(
            state="complete", pattern_count=len(structural), representation="exact_property_sets"
        )
