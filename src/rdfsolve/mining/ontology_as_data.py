"""Ontology terms used as data, mined per term and then subsumed.

Some KGs use ontology terms as instance types (a substance typed with a CHEBI
compound class) or as values and described resources (a reaction participant
pointing at a CHEBI class, a reaction modelled as a subclass). Mining such a
graph class by class returns one class per term. This module keeps every
pattern observed for every term and then replaces terms by ancestors from the
``rdfs:subClassOf`` hierarchy until the schema has at most a stated number of
classes.

Only terms that the data uses are counted or lifted, and only their ancestors
are fetched, so an endpoint that also loads whole ontologies is handled the
same way. Ontology axioms (``rdfs:subClassOf``, restrictions, annotation
properties) never become patterns.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from rdfsolve.mining.query_builders import _graph_scope
from rdfsolve.mining.types import ONTOLOGY_METACLASSES
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

Collect = Callable[[str, str, int | None], list[dict[str, Any]]]

OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
RDFS_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class"
RDFS_SUBCLASS_OF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"

# Predicates that describe ontology structure or annotate terms, not data:
# RDF/RDFS/OWL, the common ontology annotation vocabularies, and any property
# the endpoint declares as owl:AnnotationProperty.
_NON_DATA_PREDICATE_NAMESPACES = (
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/2002/07/owl#",
    "http://www.geneontology.org/formats/oboInOwl#",
    "http://purl.obolibrary.org/obo/IAO_",
    "http://www.w3.org/2004/02/skos/core#",
    "http://purl.org/dc/elements/1.1/",
    "http://purl.org/dc/terms/",
)

_DATA_PREDICATE = "\n    ".join(
    [f'FILTER(!STRSTARTS(STR(?p), "{ns}"))' for ns in _NON_DATA_PREDICATE_NAMESPACES]
    + ["FILTER NOT EXISTS { ?p a <http://www.w3.org/2002/07/owl#AnnotationProperty> }"]
)

_TERM = f"{{ ?t a <{OWL_CLASS}> }} UNION {{ ?t a <{RDFS_CLASS}> }}"


def _metaclass_filter(variable: str) -> str:
    values = ", ".join(f"<{iri}>" for iri in sorted(ONTOLOGY_METACLASSES))
    return f"FILTER(?{variable} NOT IN ({values}))"


def build_term_object_query(graph_uris: list[str] | None) -> str:
    """Instances whose property values are ontology terms, grouped per term."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    return f"""\
SELECT ?sc ?p ?t (COUNT(*) AS ?n)
{dataset}
WHERE {{
  {g_open} ?s ?p ?t . {g_close}
  ?s a ?sc .
  {_TERM}
  FILTER(isIRI(?s) && isIRI(?sc) && isIRI(?t))
  FILTER NOT EXISTS {{ ?s a <{OWL_CLASS}> }}
  FILTER NOT EXISTS {{ ?s a <{RDFS_CLASS}> }}
  {_metaclass_filter("sc")}
  {_DATA_PREDICATE}
}}
GROUP BY ?sc ?p ?t
ORDER BY ?sc ?p ?t"""


def build_term_subject_query(graph_uris: list[str] | None) -> str:
    """Ontology terms described with data properties, grouped per term and value kind."""
    dataset, g_open, g_close = _graph_scope(graph_uris)
    return f"""\
SELECT ?t ?p ?kind ?oc ?dt (COUNT(*) AS ?n)
{dataset}
WHERE {{
  {g_open} ?t ?p ?o . {g_close}
  {_TERM}
  FILTER(isIRI(?t))
  {_DATA_PREDICATE}
  OPTIONAL {{
    FILTER(isIRI(?o))
    {{ ?o a <{OWL_CLASS}> BIND(?o AS ?term) }} UNION {{ ?o a <{RDFS_CLASS}> BIND(?o AS ?term) }}
  }}
  OPTIONAL {{
    FILTER(isIRI(?o) && !BOUND(?term))
    ?o a ?type .
    FILTER(isIRI(?type))
  }}
  BIND(IF(isLiteral(?o), "literal", IF(isBlank(?o), "bnode", "iri")) AS ?kind)
  BIND(COALESCE(?term, ?type) AS ?oc)
  BIND(IF(isLiteral(?o), DATATYPE(?o), ?unbound) AS ?dt)
}}
GROUP BY ?t ?p ?kind ?oc ?dt
ORDER BY ?t ?p ?kind ?oc ?dt"""


def _row_count(row: Mapping[str, Any]) -> int | None:
    try:
        return int(row["n"]["value"])
    except (KeyError, TypeError, ValueError):
        return None


def probe_term_patterns(
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect: Collect,
    chunk_size: int,
) -> list[SchemaPattern]:
    """Return every observed pattern that uses an ontology term as value or subject.

    Results are paged to completion. Each pattern carries its triple count.
    """
    patterns: list[SchemaPattern] = []
    rows = collect(
        SparqlHelper.prepare_paginated_query(build_term_object_query(graph_uris)),
        "ontology-terms/object",
        chunk_size,
    )
    for row in rows:
        sc = row.get("sc", {}).get("value")
        p = row.get("p", {}).get("value")
        t = row.get("t", {}).get("value")
        if sc and p and t:
            patterns.append(
                SchemaPattern(
                    subject_class=sc, property_uri=p, object_class=t, count=_row_count(row)
                )
            )
    rows = collect(
        SparqlHelper.prepare_paginated_query(build_term_subject_query(graph_uris)),
        "ontology-terms/subject",
        chunk_size,
    )
    for row in rows:
        t = row.get("t", {}).get("value")
        p = row.get("p", {}).get("value")
        kind = row.get("kind", {}).get("value")
        if not t or not p or kind == "bnode":
            continue
        if kind == "literal":
            dt = row.get("dt", {}).get("value")
            patterns.append(
                SchemaPattern(
                    subject_class=t,
                    property_uri=p,
                    object_class="Literal",
                    datatype=dt or None,
                    count=_row_count(row),
                )
            )
        else:
            oc = row.get("oc", {}).get("value") or "Resource"
            patterns.append(
                SchemaPattern(
                    subject_class=t, property_uri=p, object_class=oc, count=_row_count(row)
                )
            )
    logger.info("Probed %d term-level ontology patterns", len(patterns))
    return patterns


def fetch_superclasses(
    helper: SparqlHelper,
    terms: Iterable[str],
    *,
    batch_size: int = 500,
    purpose: str = "ontology-terms/superclasses",
) -> dict[str, set[str]]:
    """Return named ``rdfs:subClassOf`` parents for *terms* and all their ancestors.

    The hierarchy is read from the whole endpoint rather than the data graphs,
    because the ontology that declares the terms usually sits in its own graph.
    """
    parents: dict[str, set[str]] = {}
    frontier = sorted(set(terms))
    while frontier:
        for start in range(0, len(frontier), batch_size):
            batch = frontier[start : start + batch_size]
            values = " ".join(f"<{iri}>" for iri in batch)
            query = f"""\
SELECT ?c ?parent
WHERE {{
  VALUES ?c {{ {values} }}
  ?c <{RDFS_SUBCLASS_OF}> ?parent .
  FILTER(isIRI(?parent) && ?parent != ?c)
}}"""
            result = helper.select(query, purpose=purpose)
            for iri in batch:
                parents.setdefault(iri, set())
            for row in result.get("results", {}).get("bindings", []):
                child = row.get("c", {}).get("value")
                parent = row.get("parent", {}).get("value")
                if child and parent and parent not in ONTOLOGY_METACLASSES:
                    parents.setdefault(child, set()).add(parent)
        frontier = sorted({p for ps in parents.values() for p in ps} - parents.keys())
    return parents


@dataclass
class Subsumption:
    """Chosen representative for each subsumed term, and how it was reached."""

    budget: int
    representative: dict[str, str] = field(default_factory=dict)
    levels_lifted: int = 0
    classes_before: int = 0
    classes_after: int = 0
    over_budget: bool = False

    def members(self) -> dict[str, list[str]]:
        """Return the terms merged into each representative that replaced them."""
        out: dict[str, list[str]] = defaultdict(list)
        for term, rep in self.representative.items():
            if rep != term:
                out[rep].append(term)
        return {rep: sorted(terms) for rep, terms in sorted(out.items())}


def _depths(parents: Mapping[str, set[str]]) -> dict[str, int]:
    """Longest named path from each node to a root; cycles count once."""
    depth: dict[str, int] = {}

    def visit(node: str, trail: frozenset[str]) -> int:
        """Return the depth of *node*, skipping parents already on the path."""
        if node in depth:
            return depth[node]
        ups = [p for p in parents.get(node, ()) if p not in trail]
        value = 0 if not ups else 1 + max(visit(p, trail | {node}) for p in ups)
        depth[node] = value
        return value

    for node in sorted(parents):
        visit(node, frozenset())
    return depth


def choose_representatives(
    terms: Iterable[str],
    parents: Mapping[str, set[str]],
    budget: int,
    fixed: Iterable[str] = (),
) -> Subsumption:
    """Lift terms up the hierarchy, deepest first, until at most *budget* classes remain.

    *fixed* classes count toward the budget but are never lifted. A lifted node
    moves to the parent that most nodes of its level move to (ties by IRI), so
    each term has one representative and counts stay additive.
    """
    fixed_set = set(fixed)
    current = {t: t for t in sorted(set(terms)) if t not in fixed_set}
    depth = _depths(parents)
    result = Subsumption(budget=budget)

    def size() -> int:
        """Return the number of distinct classes the schema would keep."""
        return len(set(current.values()) | fixed_set)

    result.classes_before = size()
    while size() > budget:
        nodes = {rep for rep in current.values() if parents.get(rep)}
        if not nodes:
            result.over_budget = True
            break
        deepest = max(depth.get(node, 0) for node in nodes)
        level = sorted(node for node in nodes if depth.get(node, 0) == deepest)
        votes: dict[str, int] = defaultdict(int)
        for node in level:
            for parent in parents[node]:
                votes[parent] += 1
        lift = {
            node: min(parents[node], key=lambda parent: (-votes[parent], parent)) for node in level
        }
        current = {term: lift.get(rep, rep) for term, rep in current.items()}
        result.levels_lifted += 1
    result.representative = current
    result.classes_after = size()
    return result


def _merge(group: list[SchemaPattern], subject: str, obj: str) -> SchemaPattern:
    """Merge patterns that map to the same subsumed pattern."""
    first = group[0]
    counts = [p.count for p in group]
    graphs: dict[str, int] = defaultdict(int)
    for pattern in group:
        for graph, count in (pattern.graphs or {}).items():
            graphs[graph] += count
    return first.model_copy(
        update={
            "subject_class": subject,
            "object_class": obj,
            # Sum over member terms; an instance typed with two members of one
            # representative contributes twice, so this is an upper bound.
            "count": None if None in counts else sum(c for c in counts if c is not None),
            "graphs": dict(graphs) or None,
            "distinct_subjects": None,
            "distinct_objects": None,
            "evidence_source": "inferred",
        }
    )


def subsume_patterns(
    patterns: Iterable[SchemaPattern], representative: Mapping[str, str]
) -> list[SchemaPattern]:
    """Replace subject and object classes by their representatives and merge duplicates."""
    groups: dict[tuple[str, str, str, str | None], list[SchemaPattern]] = defaultdict(list)
    changed: set[tuple[str, str, str, str | None]] = set()
    for pattern in patterns:
        subject = representative.get(pattern.subject_class, pattern.subject_class)
        obj = pattern.object_class
        if obj not in _SENTINEL_OBJECTS:
            obj = representative.get(obj, obj)
        key = (subject, pattern.property_uri, obj, pattern.datatype)
        groups[key].append(pattern)
        if subject != pattern.subject_class or obj != pattern.object_class:
            changed.add(key)
    out = []
    for key, group in groups.items():
        if key in changed or len(group) > 1:
            out.append(_merge(group, key[0], key[2]))
        else:
            out.append(group[0])
    return out


def pattern_classes(patterns: Iterable[SchemaPattern]) -> set[str]:
    """Return the subject and non-sentinel object classes of *patterns*."""
    classes: set[str] = set()
    for pattern in patterns:
        classes.add(pattern.subject_class)
        if pattern.object_class not in _SENTINEL_OBJECTS:
            classes.add(pattern.object_class)
    return classes


__all__ = [
    "Subsumption",
    "build_term_object_query",
    "build_term_subject_query",
    "choose_representatives",
    "fetch_superclasses",
    "pattern_classes",
    "probe_term_patterns",
    "subsume_patterns",
]
