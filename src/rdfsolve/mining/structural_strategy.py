"""Mine bounded graph-local profiles for records without class assertions."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from typing import Any

from rdflib import Literal

from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.mining.types import EXCLUDED_RECORD_TYPES
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern

MAX_STRUCTURAL_PATTERNS = 100


def _dataset(graph: str | None, type_graphs: list[str] | None = None) -> str:
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
    type_graphs: list[str] | None = None,
    *,
    residual_only: bool = False,
    profile: bool = False,
    limit: int = MAX_STRUCTURAL_PATTERNS + 1,
) -> str:
    residual = _untyped(type_graphs) if residual_only else ""
    shapes = (
        ""
        if profile
        else f"""
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=" ") AS ?ss)
     WHERE {{ ?s ?sp ?sv . {residual} }} GROUP BY ?s }}
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=" ") AS ?os)
       WHERE {{ ?o ?op ?ov }} GROUP BY ?o }}
  }}"""
    )
    variables = "" if profile else "?ss ?os"
    return f"""SELECT DISTINCT {variables} ?p ?sk ?ok ?dt ?lang
{_dataset(graph, type_graphs)} WHERE {{
  ?s ?p ?o . {residual}
  {shapes}
  BIND(IF(isIRI(?s), "IRI", "BlankNode") AS ?sk)
  BIND(IF(isIRI(?o), "IRI", IF(isBlank(?o), "BlankNode", "Literal")) AS ?ok)
  BIND(DATATYPE(?o) AS ?dt)
  BIND(LANG(?o) AS ?lang)
}} LIMIT {limit}"""


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
        f"?s <{pattern.property_uri}> ?o .",
        _shape("?s", pattern.subject_properties, pattern.subject_kind, exact=exact),
        _shape("?o", pattern.object_properties, pattern.object_kind, exact=exact),
    ]
    if pattern.subject_selection == "untyped":
        clauses.append(_untyped(pattern.type_graph_uris))
    if pattern.datatype:
        clauses.append(f"FILTER(DATATYPE(?o) = <{pattern.datatype}>)")
    if pattern.language is not None:
        clauses.append(f"FILTER(LANG(?o) = {Literal(pattern.language).n3()})")
    body = f"{_dataset(pattern.graph_uri, pattern.type_graph_uris)} WHERE {{ {' '.join(clauses)} }}"
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
    """Retain exact shapes or compact property profiles within a fixed row budget."""

    def __init__(self, *, residual_only: bool = False) -> None:
        """Select all records or subjects untyped across the data graphs."""
        self.residual_only = residual_only

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "structural"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Measure coverage and retain bounded structural evidence."""
        phase = context.report.start_phase("structural-patterns")
        coverage: list[dict[str, Any]] = []
        context.report.report.config["structural_coverage"] = coverage
        context.report.report.config["structural_pattern_budget"] = MAX_STRUCTURAL_PATTERNS
        graphs: Sequence[str | None] = (
            sorted(set(context.graph_uris)) if context.graph_uris else [None]
        )
        for graph in graphs:
            entry: dict[str, Any] = {"graph_uri": graph, "state": "checking"}
            coverage.append(entry)
            context.report.flush()
            try:
                self._mine_graph(context, graph, entry)
            except Exception:
                entry["state"] = "failed"
                raise
        context.report.report.config["structural_pattern_count"] = len(context.structural_patterns)
        deferred = any(e["state"] == "deferred" for e in coverage)
        context.report.finish_phase(
            phase,
            items=len(context.structural_patterns),
            error="Structural detail exceeds the pattern budget" if deferred else None,
        )
        return []

    def _mine_graph(self, context: MiningContext, graph: str | None, entry: dict[str, Any]) -> None:
        graphs = context.graph_uris
        excluded = ", ".join(f"<{t}>" for t in sorted(EXCLUDED_RECORD_TYPES))
        rows = _select(
            context,
            f"""SELECT ?typed ?eligible (COUNT(*) AS ?n)
{_dataset(graph, graphs)} WHERE {{ ?s ?p ?o .
BIND(EXISTS {{ {_types(graphs)} }} AS ?typed)
BIND(EXISTS {{ {_types(graphs)} FILTER(isIRI(?_type) && ?_type NOT IN ({excluded})) }} AS ?eligible)
}} GROUP BY ?typed ?eligible""",
            "structural/coverage",
        )
        total = sum(int(r["n"]["value"]) for r in rows)
        untyped = sum(int(r["n"]["value"]) for r in rows if r["typed"]["value"] in {"false", "0"})
        excluded_count = sum(
            int(r["n"]["value"])
            for r in rows
            if r["typed"]["value"] in {"true", "1"} and r["eligible"]["value"] in {"false", "0"}
        )
        entry.update(
            triple_count=total,
            untyped_subject_triples=untyped,
            excluded_subject_triples=excluded_count,
            type_graph_uris=graphs,
            subject_selection="untyped" if self.residual_only else "all",
        )
        expected = untyped if self.residual_only else total
        if not expected:
            entry["state"] = "not_needed"
            return
        remaining = MAX_STRUCTURAL_PATTERNS - len(context.structural_patterns)
        if not remaining:
            entry["state"] = "deferred"
            return
        profile = False
        rows = _select(
            context,
            _discovery_query(graph, graphs, residual_only=self.residual_only, limit=remaining + 1),
            "structural/discovery",
        )
        if len(rows) > remaining:
            profile = True
            rows = _select(
                context,
                _discovery_query(
                    graph,
                    graphs,
                    residual_only=self.residual_only,
                    profile=True,
                    limit=remaining + 1,
                ),
                "structural/profile",
            )
        if len(rows) > remaining:
            entry["state"] = "deferred"
            return
        candidates: dict[str, StructuralPattern] = {}
        for row in rows:
            candidate = StructuralPattern(
                subject_properties=row.get("ss", {}).get("value", "").split(),
                object_properties=row.get("os", {}).get("value", "").split(),
                subject_kind=row["sk"]["value"],
                object_kind=row["ok"]["value"],
                property_uri=row["p"]["value"],
                datatype=row.get("dt", {}).get("value"),
                language=row.get("lang", {}).get("value"),
                graph_uri=graph,
                type_graph_uris=graphs or [],
                subject_selection="untyped" if self.residual_only else "all",
                shape_semantics="property_profile" if profile else "exact_property_sets",
                count=0,
                distinct_subjects=0,
                distinct_objects=0,
                witness_query="",
                recount_query="",
            )
            candidates[json.dumps(candidate.model_dump(), sort_keys=True)] = candidate
        patterns = []
        for key in sorted(candidates):
            pattern = candidates[key]
            pattern.witness_query, pattern.recount_query = structural_queries(pattern)
            counts = _select(context, pattern.recount_query, "structural/count")
            if len(counts) != 1:
                raise ValueError("Expected one structural count row")
            pattern.count = int(counts[0]["n"]["value"])
            pattern.distinct_subjects = int(counts[0]["subjects"]["value"])
            pattern.distinct_objects = int(counts[0]["objects"]["value"])
            pattern.examples = _select(context, pattern.witness_query, "structural/witness")
            if pattern.count < 1 or len(pattern.examples) != 1:
                raise ValueError("Structural discovery and witnesses disagree")
            patterns.append(pattern)
        if sum(p.count for p in patterns) != expected:
            raise ValueError("Structural patterns do not account for the selected records")
        context.structural_patterns.extend(patterns)
        entry.update(
            state="complete",
            pattern_count=len(patterns),
            representation="property_profile" if profile else "exact_property_sets",
        )
