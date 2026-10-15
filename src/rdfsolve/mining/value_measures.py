"""Measures of the values of each (class, property) that SHACL constraints are written from.

- value_counts(): per (class, property), the members that have the property and the most
  distinct values of one member, from one query per pair. sh:minCount 1 and sh:maxCount follow
  from them where the pattern rows alone do not prove them (several rows of one property, or
  rows counted per graph).
- value_parts(): per (class, property), the parts of its IRI values without a type, as the
  parts of a class (rdfsolve.schema_models.class_parts): registered identifiers, other
  namespaces, IRIs without a scheme. Every distinct value is read and classified, so the parts
  hold for every value; a pair whose values are not all read (more than *limit*, a refused or
  cut query) gets none.

Each query has a time budget; a pair whose query does not answer gets no measure and is named
in the returned failures. A measure is never estimated.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from rdfsolve.mining.class_parts import _OTHER, _SCHEME
from rdfsolve.mining.query_builders import _graph_scope, _type_pattern
from rdfsolve.schema_models.class_parts import ClassPart
from rdfsolve.schema_models.value_measures import ValueCount, ValueParts
from rdfsolve.sparql_helper import SparqlHelperError

if TYPE_CHECKING:
    from rdfsolve.schema_models.pattern import SchemaPattern
    from rdfsolve.sparql_helper import SparqlHelper

__all__ = ["VALUE_PART_LIMIT", "classify_iris", "measured_pairs", "value_counts", "value_parts"]

# Most distinct values of one (class, property) read to classify them.
VALUE_PART_LIMIT = 50_000
# Seconds each measuring query may take.
QUERY_BUDGET_S = 60.0
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def measured_pairs(patterns: Iterable[SchemaPattern]) -> dict[tuple[str, str], list[SchemaPattern]]:
    """Return the rows of each (class, property) of typed subjects, but rdf:type."""
    found: dict[tuple[str, str], list[SchemaPattern]] = defaultdict(list)
    for pattern in patterns:
        if pattern.untyped_subject or pattern.subject_binding == "term":
            continue
        if pattern.property_uri == RDF_TYPE:
            continue
        found[(pattern.subject_class, pattern.property_uri)].append(pattern)
    return found


def _ask(helper: SparqlHelper, query: str) -> list[dict[str, Any]]:
    """Run *query* within the budget and return its bindings."""
    with helper.budget(QUERY_BUDGET_S):
        result = helper.select(query)
    rows: list[dict[str, Any]] = result.get("results", {}).get("bindings", [])
    return rows


def value_counts(
    helper: SparqlHelper,
    pairs: Iterable[tuple[str, str]],
    graph_uris: list[str] | None = None,
    type_context_graph_uris: list[str] | None = None,
) -> tuple[list[ValueCount], dict[str, str]]:
    """Return the members with each property and the most values of one member, per pair.

    The values of a member are counted distinct, as a SHACL engine reads them in the merge of
    the graphs. The second item names the pairs that were not measured, with the error.
    """
    dataset, _, _ = _graph_scope(graph_uris, type_context_graph_uris)
    found: list[ValueCount] = []
    failed: dict[str, str] = {}
    for cls, prop in sorted(pairs):
        member = _type_pattern("?s", f"<{cls}>", type_context_graph_uris)
        query = (
            f"SELECT (COUNT(?s) AS ?subjects) (MAX(?n) AS ?most) {dataset} WHERE {{ "
            f"{{ SELECT ?s (COUNT(DISTINCT ?o) AS ?n) WHERE {{ {member} ?s <{prop}> ?o . }} "
            "GROUP BY ?s } }"
        )
        try:
            rows = _ask(helper, query)
            row = rows[0] if rows else {}
            subjects = int(row["subjects"]["value"]) if "subjects" in row else 0
            most = int(row["most"]["value"]) if "most" in row else 0
        except (SparqlHelperError, ValueError, KeyError) as error:
            failed[f"{cls} {prop}"] = f"{type(error).__name__}: {error}"[:300]
            continue
        found.append(ValueCount(subject_class=cls, property_uri=prop, subjects=subjects, most=most))
    return found, failed


def classify_iris(iris: Iterable[str]) -> list[ClassPart]:
    """Return the parts of the IRIs *iris*, as the parts of a class: one part per registered
    prefix (valid or not as its identifier), per other namespace, IRIs without a scheme, and
    IRIs whose scheme is not valid.
    """
    from rdfsolve.identifiers import parse

    parts: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
    for iri in iris:
        kind, prefix, namespace = "namespace", None, None
        if ":" not in iri:
            kind = "relative_iri"
        elif not re.match(_SCHEME, iri):
            kind, namespace = "invalid_scheme", iri.split(":", 1)[0] + ":"
        else:
            found = parse(iri)
            if found is not None and iri.endswith(found.local):
                kind, prefix = "identifier", found.prefix
                namespace = iri[: len(iri) - len(found.local)]
            else:
                match = re.match(_OTHER, iri)
                namespace = match.group(1) if match else iri
        key = (kind, prefix, namespace if kind in ("namespace", "invalid_scheme") else None)
        part = parts.setdefault(key, {"n": 0, "ns": set()})
        part["n"] += 1
        if namespace:
            part["ns"].add(namespace)
    return sorted(
        (
            ClassPart(kind=kind, prefix=prefix, namespaces=sorted(found["ns"]), entities=found["n"])
            for (kind, prefix, _), found in parts.items()
        ),
        key=lambda p: (-p.entities, p.key()),
    )


def value_parts(
    helper: SparqlHelper,
    patterns: Iterable[SchemaPattern],
    graph_uris: list[str] | None = None,
    type_context_graph_uris: list[str] | None = None,
    *,
    limit: int = VALUE_PART_LIMIT,
) -> tuple[list[ValueParts], dict[str, str]]:
    """Return the parts of the IRI values without a type of each (class, property).

    Only pairs whose distinct untyped IRI values are counted (the pattern row's
    distinct_objects) and at most *limit* are read; the parts are kept only when every
    counted value was read. The second item names the pairs that were not measured.
    """
    dataset, _, _ = _graph_scope(graph_uris, type_context_graph_uris)
    found: list[ValueParts] = []
    failed: dict[str, str] = {}
    for (cls, prop), rows in sorted(measured_pairs(patterns).items()):
        untyped = [p for p in rows if p.object_class == "Resource" and p.object_binding == "type"]
        if not untyped:
            continue
        expected = untyped[0].distinct_objects
        if expected is None or untyped[0].sampled is not None or expected > limit:
            failed[f"{cls} {prop}"] = f"values not counted or more than {limit}"
            continue
        member = _type_pattern("?s", f"<{cls}>", type_context_graph_uris)
        typed = _type_pattern("?o", "?_t", type_context_graph_uris)
        query = (
            f"SELECT DISTINCT ?o {dataset} WHERE {{ {member} ?s <{prop}> ?o . "
            f"FILTER(isIRI(?o)) FILTER NOT EXISTS {{ {typed} }} }}"
        )
        try:
            values = [row["o"]["value"] for row in _ask(helper, query) if "o" in row]
        except (SparqlHelperError, ValueError, KeyError) as error:
            failed[f"{cls} {prop}"] = f"{type(error).__name__}: {error}"[:300]
            continue
        if len(values) != expected:
            failed[f"{cls} {prop}"] = f"read {len(values)} of {expected} values"
            continue
        found.append(
            ValueParts(
                subject_class=cls, property_uri=prop, parts=classify_iris(values), values=expected
            )
        )
    return found, failed
