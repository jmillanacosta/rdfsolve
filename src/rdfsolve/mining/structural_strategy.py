"""Mine graph-local record shapes without requiring class assertions."""

from __future__ import annotations

import json
import time
from typing import Any

from rdflib import Literal

from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.structural import StructuralPattern


def _dataset(graph: str | None) -> str:
    return f"FROM <{graph}>" if graph else ""


def _discovery_query(graph: str | None) -> str:
    return f"""SELECT DISTINCT ?ss ?os ?p ?sk ?ok ?dt ?lang
{_dataset(graph)} WHERE {{
  {{ SELECT ?s (GROUP_CONCAT(DISTINCT STR(?sp); SEPARATOR=" ") AS ?ss)
     WHERE {{ ?s ?sp ?sv }} GROUP BY ?s }}
  ?s ?p ?o .
  OPTIONAL {{
    {{ SELECT ?o (GROUP_CONCAT(DISTINCT STR(?op); SEPARATOR=" ") AS ?os)
       WHERE {{ ?o ?op ?ov }} GROUP BY ?o }}
  }}
  BIND(IF(isIRI(?s), "IRI", "BlankNode") AS ?sk)
  BIND(IF(isIRI(?o), "IRI", IF(isBlank(?o), "BlankNode", "Literal")) AS ?ok)
  BIND(DATATYPE(?o) AS ?dt)
  BIND(LANG(?o) AS ?lang)
}}"""


def _shape(node: str, properties: list[str], kind: str) -> str:
    test = {"IRI": "isIRI", "BlankNode": "isBlank", "Literal": "isLiteral"}[kind]
    clauses = [f"FILTER({test}({node}))"]
    if kind == "Literal":
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
    """Build a witness and exact recount from the recorded graph-local shapes."""
    clauses = [
        f"?s <{pattern.property_uri}> ?o .",
        _shape("?s", pattern.subject_properties, pattern.subject_kind),
        _shape("?o", pattern.object_properties, pattern.object_kind),
    ]
    if pattern.datatype:
        clauses.append(f"FILTER(DATATYPE(?o) = <{pattern.datatype}>)")
    if pattern.language is not None:
        clauses.append(f"FILTER(LANG(?o) = {Literal(pattern.language).n3()})")
    body = f"{_dataset(pattern.graph_uri)} WHERE {{ {' '.join(clauses)} }}"
    return (
        f"SELECT ?s ?o {body} LIMIT 1",
        (
            "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?s) AS ?subjects) "
            f"(COUNT(DISTINCT ?o) AS ?objects) {body}"
        ),
    )


class StructuralStrategy(MiningStrategy):
    """Group observed edges by the outgoing-property sets at each end."""

    @property
    def name(self) -> str:
        """Return the strategy name."""
        return "structural"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Retain structural evidence separately from typed class patterns."""
        phase = context.report.start_phase("structural-patterns")

        def select(query: str, purpose: str) -> list[dict[str, Any]]:
            """Return bindings and record query success or failure."""
            started = time.monotonic()
            try:
                response = context.helper.select_with_fallback(query, purpose=purpose)
                rows: list[dict[str, Any]] = response["results"]["bindings"]
            except Exception:
                context.report.record_query(purpose, time.monotonic() - started, success=False)
                raise
            context.report.record_query(purpose, time.monotonic() - started)
            return rows

        graphs: list[str | None] = list(context.graph_uris) if context.graph_uris else [None]
        for graph in graphs:
            rows = select(_discovery_query(graph), "structural/discovery")
            candidates: dict[str, StructuralPattern] = {}
            for row in rows:
                candidate = StructuralPattern(
                    subject_properties=row["ss"]["value"].split(),
                    object_properties=row.get("os", {}).get("value", "").split(),
                    subject_kind=row["sk"]["value"],
                    object_kind=row["ok"]["value"],
                    property_uri=row["p"]["value"],
                    datatype=row.get("dt", {}).get("value"),
                    language=row.get("lang", {}).get("value"),
                    graph_uri=graph,
                    count=0,
                    distinct_subjects=0,
                    distinct_objects=0,
                    witness_query="",
                    recount_query="",
                )
                candidates[json.dumps(candidate.model_dump(), sort_keys=True)] = candidate
            for key in sorted(candidates):
                pattern = candidates[key]
                pattern.witness_query, pattern.recount_query = structural_queries(pattern)
                counts = select(pattern.recount_query, "structural/count")
                if len(counts) != 1:
                    raise ValueError("Expected one structural count row")
                pattern.count = int(counts[0]["n"]["value"])
                pattern.distinct_subjects = int(counts[0]["subjects"]["value"])
                pattern.distinct_objects = int(counts[0]["objects"]["value"])
                pattern.examples = select(pattern.witness_query, "structural/witness")
                if pattern.count < 1 or len(pattern.examples) != 1:
                    raise ValueError("Structural discovery and witnesses disagree")
                context.structural_patterns.append(pattern)
            total = select(
                f"SELECT (COUNT(*) AS ?n) {_dataset(graph)} WHERE {{ ?s ?p ?o }}",
                "structural/coverage",
            )
            observed = sum(p.count for p in context.structural_patterns if p.graph_uri == graph)
            if len(total) != 1 or int(total[0]["n"]["value"]) != observed:
                raise ValueError("Structural patterns do not account for all scoped triples")
        context.report.report.config["structural_pattern_count"] = len(context.structural_patterns)
        context.report.finish_phase(phase, items=len(context.structural_patterns))
        return []
