"""Mining strategy for the ontology terms of a data graph: exact term bindings, and grouping of
the terms that data use as types (by ancestry from rdfsolve.ontology.hierarchy, then by shape).

The ontology model (vocabulary, namespaces, hierarchy) is in rdfsolve.ontology.
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rdfsolve._outcomes import QueryOutcome
from rdfsolve.mining.query_builders import (
    MEMBERSHIP,
    _bound,
    _context_pattern,
    _graph_scope,
    _type_pattern,
    membership_path,
)
from rdfsolve.mining.types import ONTOLOGY_METACLASSES
from rdfsolve.ontology.terms import namespace
from rdfsolve.ontology.vocabulary import NON_DATA_NAMESPACES, OWL_CLASS, RDF_TYPE, RDFS_CLASS
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

Collect = Callable[[str, str, int | None], list[dict[str, Any]]]


_DATA_PREDICATE = "\n    ".join(
    [f'FILTER(!STRSTARTS(STR(?p), "{ns}"))' for ns in NON_DATA_NAMESPACES]
)


def _term_filter(node: str, ontology_graph_uris: list[str] | None) -> str:
    """Select named class terms without multiplying declaration matches."""
    pattern = f"{{ {node} a <{OWL_CLASS}> }} UNION {{ {node} a <{RDFS_CLASS}> }}"
    return f"FILTER EXISTS {{ {_context_pattern(pattern, ontology_graph_uris)} }}"


def _data_predicate(ontology_graph_uris: list[str] | None) -> str:
    """Exclude ontology structure and declared annotation predicates."""
    annotation = "?p a <http://www.w3.org/2002/07/owl#AnnotationProperty>"
    return (
        _DATA_PREDICATE
        + f" FILTER NOT EXISTS {{ {_context_pattern(annotation, ontology_graph_uris)} }}"
    )


def _metaclass_filter(variable: str) -> str:
    values = ", ".join(f"<{iri}>" for iri in sorted(ONTOLOGY_METACLASSES))
    return f"FILTER(?{variable} NOT IN ({values}))"


def build_term_object_query(
    graph_uris: list[str] | None,
    ontology_graph_uris: list[str] | None = None,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Instances whose property values are ontology terms, grouped per term."""
    dataset, g_open, g_close = _graph_scope(
        graph_uris, (ontology_graph_uris or []) + (type_context_graph_uris or [])
    )
    graph_var = " ?_g" if g_open else ""
    return f"""\
SELECT ?sc ?p ?t{graph_var} (COUNT(*) AS ?n)
{dataset}
WHERE {{
  {g_open} ?s ?p ?t . {g_close}
  ?s {membership_path()} ?sc .
  {_term_filter("?t", ontology_graph_uris)}
  FILTER(isIRI(?s) && isIRI(?sc) && isIRI(?t))
  FILTER NOT EXISTS {{ ?s a <{OWL_CLASS}> }}
  FILTER NOT EXISTS {{ ?s a <{RDFS_CLASS}> }}
  {_metaclass_filter("sc")}
  {_data_predicate(ontology_graph_uris)}
}}
GROUP BY ?sc ?p ?t{graph_var}
ORDER BY ?sc ?p ?t{graph_var}"""


def build_term_object_pair_query(
    class_uri: str,
    property_uri: str,
    graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None = None,
) -> str:
    """Count the ontology terms that are values of one property of one class.

    The pairs come from the typed patterns whose object class is owl:Class or rdfs:Class, so
    that only the edges of such pairs are read. A group of ontology terms is read through its
    members.
    """
    dataset, g_open, g_close = _graph_scope(graph_uris, type_context_graph_uris)
    values, cls, _, _ = _bound([class_uri])
    graph_var = " ?_g" if g_open else ""
    return f"""\
SELECT ?t{graph_var} (COUNT(*) AS ?n)
{dataset}
WHERE {{
  {values} VALUES ?p {{ <{property_uri}> }}
  {_type_pattern("?s", cls, type_context_graph_uris)}
  {g_open} ?s ?p ?t . {g_close}
  {_term_filter("?t", None)}
  FILTER(isIRI(?s) && isIRI(?t))
  FILTER NOT EXISTS {{ ?s a <{OWL_CLASS}> }}
  FILTER NOT EXISTS {{ ?s a <{RDFS_CLASS}> }}
  {_data_predicate(None)}
}}
GROUP BY ?t{graph_var}"""


def _is_data_predicate(iri: str) -> bool:
    """Return False for a predicate of ontology structure or annotation."""
    return not iri.startswith(NON_DATA_NAMESPACES)


def build_term_subject_query(
    graph_uris: list[str] | None,
    ontology_graph_uris: list[str] | None = None,
    type_context_graph_uris: list[str] | None = None,
    *,
    terms_first: bool = False,
    terms: list[str] | None = None,
) -> str:
    """Ontology terms described with data properties, grouped per term and value kind.

    With *terms_first* (QLever) the set of terms is read before their edges; the filter form
    reads every triple of the graph. With *terms*, only these terms (a batch of the declared
    terms, when the query of all of them is refused).
    """
    dataset, g_open, g_close = _graph_scope(
        graph_uris, (ontology_graph_uris or []) + (type_context_graph_uris or [])
    )
    graph_var = " ?_g" if g_open else ""
    declared = f"{{ {{ ?t a <{OWL_CLASS}> }} UNION {{ ?t a <{RDFS_CLASS}> }} }}"
    edge = (
        f"{{ SELECT DISTINCT ?t WHERE {{ {_context_pattern(declared, ontology_graph_uris)} }} }}\n"
        f"  {g_open} ?t ?p ?o . {g_close}"
        if terms_first
        else f"{g_open} ?t ?p ?o . {g_close}\n  {_term_filter('?t', ontology_graph_uris)}"
    )
    if terms is not None:
        listed = " ".join(f"<{term}>" for term in terms)
        edge = f"VALUES ?t {{ {listed} }}\n  {g_open} ?t ?p ?o . {g_close}"
    return f"""\
SELECT ?t ?p ?kind ?oc ?dt ?object_binding{graph_var} (COUNT(*) AS ?n)
{dataset}
WHERE {{
  {edge}
  FILTER(isIRI(?t))
  {_data_predicate(ontology_graph_uris)}
  OPTIONAL {{
    {{ SELECT DISTINCT ?o WHERE {{
      {_context_pattern(f"{{ ?o a <{OWL_CLASS}> }} UNION {{ ?o a <{RDFS_CLASS}> }}", ontology_graph_uris)}
      FILTER(isIRI(?o))
    }} }}
    BIND(?o AS ?term)
  }}
  OPTIONAL {{
    FILTER(isIRI(?o) && !BOUND(?term))
    {_type_pattern("?o", "?type", type_context_graph_uris)}
    FILTER(isIRI(?type))
  }}
  BIND(IF(isLiteral(?o), "literal", IF(isBlank(?o), "bnode", "iri")) AS ?kind)
  BIND(COALESCE(?term, ?type) AS ?oc)
  BIND(IF(BOUND(?term), "term", "type") AS ?object_binding)
  BIND(IF(isLiteral(?o), DATATYPE(?o), ?unbound) AS ?dt)
}}
GROUP BY ?t ?p ?kind ?oc ?dt ?object_binding{graph_var}
ORDER BY ?t ?p ?kind ?oc ?dt ?object_binding{graph_var}"""


def _row_count(row: Mapping[str, Any]) -> int | None:
    try:
        return int(row["n"]["value"])
    except (KeyError, TypeError, ValueError):
        return None


# Declared terms per subject-count query when the query of every term is refused.
TERM_BATCH = 500
# More batches than this are not sent: the subject counts are read over a sample instead.
MAX_TERM_BATCHES = 200


def _read(
    query: str,
    purpose: str,
    helper: SparqlHelper,
    collect: Collect,
    chunk_size: int,
    outcome: QueryOutcome,
    *,
    unit: str,
    rows_of: Callable[[], list[dict[str, Any]] | None] | None = None,
) -> list[dict[str, Any]]:
    """Read a grouped term count in pages; a refused one over a sample, else a gap.

    *rows_of* is a fallback tried between the two (term batches). The counts of ontology terms
    are enrichment: a refusal that no sample answers is recorded as a measurement gap of
    *outcome*, never as a failure of the source.
    """
    from rdfsolve.mining.query_fallbacks import collect_outcome
    from rdfsolve.mining.sampling import refusal, sample_query, sampled_select

    found = collect_outcome(
        SparqlHelper.prepare_paginated_query(query), purpose, collect, chunk_size
    )
    if found.state == "complete":
        return found.rows
    if refusal(found) is not None and rows_of is not None:
        batched = rows_of()
        if batched is not None:
            return batched
    found = sampled_select(
        lambda size: sample_query(query, size), purpose, helper, found, unit=unit
    )
    outcome.samples.extend(found.samples)
    if found.state != "complete":
        outcome.gaps.extend(found.failures)
    return found.rows


def _declared_terms(
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    ontology_graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None,
    collect: Collect,
    chunk_size: int,
) -> list[str] | None:
    """List the declared terms (owl:Class, rdfs:Class) in scope; None when refused."""
    from rdfsolve.sparql_helper import SparqlHelperError

    dataset, _, _ = _graph_scope(
        graph_uris, (ontology_graph_uris or []) + (type_context_graph_uris or [])
    )
    declared = f"{{ ?t a <{OWL_CLASS}> }} UNION {{ ?t a <{RDFS_CLASS}> }}"
    query = (
        f"SELECT DISTINCT ?t {dataset} WHERE {{ "
        f"{_context_pattern(declared, ontology_graph_uris)} FILTER(isIRI(?t)) }}"
    )
    try:
        rows = collect(
            SparqlHelper.prepare_paginated_query(query), "ontology-terms/declared", chunk_size
        )
    except SparqlHelperError as error:
        logger.warning("Declared ontology terms not listed: %s", str(error)[:200])
        return None
    return sorted({r["t"]["value"] for r in rows if r.get("t", {}).get("type") == "uri"})


def _subject_rows_in_batches(
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    ontology_graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None,
    collect: Collect,
    chunk_size: int,
    outcome: QueryOutcome,
) -> list[dict[str, Any]] | None:
    """Count the subject rows of the declared terms in batches; None when not listed.

    A refused batch is split in two; a term refused alone is counted over a sample of its edges
    (rdfsolve.mining.sampling), and one whose samples are refused is a measurement gap.
    """
    from rdfsolve.mining.query_fallbacks import collect_outcome
    from rdfsolve.mining.sampling import refusal, sample_query, sampled_select

    terms = _declared_terms(
        helper, graph_uris, ontology_graph_uris, type_context_graph_uris, collect, chunk_size
    )
    if terms is None or len(terms) > TERM_BATCH * MAX_TERM_BATCHES:
        return None
    logger.info("Ontology terms: subject counts of %d declared terms in batches", len(terms))
    pending = [terms[i : i + TERM_BATCH] for i in range(0, len(terms), TERM_BATCH)]
    rows: list[dict[str, Any]] = []
    while pending:
        batch = pending.pop()
        query = build_term_subject_query(
            graph_uris, ontology_graph_uris, type_context_graph_uris, terms=batch
        )
        purpose = "ontology-terms/subject/batch"
        found = collect_outcome(
            SparqlHelper.prepare_paginated_query(query), purpose, collect, chunk_size
        )
        if found.state != "complete" and refusal(found) is not None and len(batch) > 1:
            pending += [batch[len(batch) // 2 :], batch[: len(batch) // 2]]
            continue
        if found.state != "complete":

            def bounded(size: int, query: str = query) -> str:
                """Bound the batch's query to its first *size* solutions."""
                return sample_query(query, size)

            found = sampled_select(
                bounded,
                purpose,
                helper,
                found,
                unit="edges",
                classes=batch,
            )
            outcome.samples.extend(found.samples)
            if found.state != "complete":
                outcome.gaps.extend(found.failures)
        rows.extend(found.rows)
    return rows


def probe_term_patterns(
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect: Collect,
    chunk_size: int,
    ontology_graph_uris: list[str] | None = None,
    type_context_graph_uris: list[str] | None = None,
    typed: Iterable[SchemaPattern] | None = None,
    classes: Mapping[str, str] | None = None,
    outcome: QueryOutcome | None = None,
) -> list[SchemaPattern]:
    """Return every observed pattern that uses an ontology term as value or subject.

    Each pattern carries its triple count. With the *typed* patterns, and no separate ontology
    graphs, the terms that are values are read only for the (class, property) pairs whose
    object class is owl:Class or rdfs:Class (the filter over every triple can need more memory
    than the server has); *classes* gives the class objects (groups of ontology terms) by name. Results are
    paged to completion.

    The counts are enrichment of the typed schema, so a refusal never fails the source: the
    subject counts of every term, when refused (a gateway can cut them at its time limit), are
    read for batches of the declared terms; a refused query is read over a sample, whose
    rows are flagged sampled with lower-bound counts (rdfsolve.mining.sampling); what no sample
    answers is a measurement gap. Samples and gaps are added to *outcome*.
    """
    from rdfsolve.mining.sampling import flag, sample_of

    outcome = outcome if outcome is not None else QueryOutcome()
    patterns: dict[tuple[str, str, str, str | None, str, str], SchemaPattern] = {}

    def add(pattern: SchemaPattern, row: Mapping[str, Any]) -> None:
        """Combine graph rows while retaining each edge count."""
        graph = row.get("_g", {}).get("value")
        pattern.count = _row_count(row)
        pattern.count_semantics = (
            "quad_occurrences"
            if len(graph_uris or []) > 1
            else "triples_in_graph"
            if graph_uris
            else "endpoint_default"
        )
        if graph and pattern.count is not None:
            pattern.graphs = {graph: pattern.count}
        provenance = sample_of(dict(row))
        if provenance is not None:
            flag(pattern, provenance, ["patterns", "counts"])
        key = (
            pattern.subject_class,
            pattern.property_uri,
            pattern.object_class,
            pattern.datatype,
            pattern.subject_binding,
            pattern.object_binding,
        )
        previous = patterns.get(key)
        if previous is None:
            patterns[key] = pattern
            return
        previous.count = (
            previous.count + pattern.count
            if previous.count is not None and pattern.count is not None
            else None
        )
        previous.graphs = {**(previous.graphs or {}), **(pattern.graphs or {})} or None
        if provenance is not None:
            flag(previous, provenance, ["patterns", "counts"])

    if typed is not None and not ontology_graph_uris:
        pairs = sorted(
            {
                (p.subject_class, p.property_uri)
                for p in typed
                if p.object_class in (OWL_CLASS, RDFS_CLASS) and _is_data_predicate(p.property_uri)
            }
        )
        for sc, p in pairs:
            query = build_term_object_pair_query(
                (classes or {}).get(sc, sc), p, graph_uris, type_context_graph_uris
            )
            for row in _read(
                query, "ontology-terms/object", helper, collect, chunk_size, outcome, unit="edges"
            ):
                t = row.get("t", {}).get("value")
                if t:
                    add(
                        SchemaPattern(
                            subject_class=sc, property_uri=p, object_class=t, object_binding="term"
                        ),
                        row,
                    )
    else:
        rows = _read(
            build_term_object_query(graph_uris, ontology_graph_uris, type_context_graph_uris),
            "ontology-terms/object",
            helper,
            collect,
            chunk_size,
            outcome,
            unit="edges",
        )
        for row in rows:
            sc = row.get("sc", {}).get("value")
            p = row.get("p", {}).get("value")
            t = row.get("t", {}).get("value")
            if sc and p and t:
                add(
                    SchemaPattern(
                        subject_class=sc, property_uri=p, object_class=t, object_binding="term"
                    ),
                    row,
                )
    terms_first = str(getattr(helper, "sparql_engine", "")).lower() == "qlever"
    rows = _read(
        build_term_subject_query(
            graph_uris, ontology_graph_uris, type_context_graph_uris, terms_first=terms_first
        ),
        "ontology-terms/subject",
        helper,
        collect,
        chunk_size,
        outcome,
        unit="edges",
        rows_of=lambda: _subject_rows_in_batches(
            helper,
            graph_uris,
            ontology_graph_uris,
            type_context_graph_uris,
            collect,
            chunk_size,
            outcome,
        ),
    )
    for row in rows:
        t = row.get("t", {}).get("value")
        p = row.get("p", {}).get("value")
        kind = row.get("kind", {}).get("value")
        if not t or not p or kind == "bnode":
            continue
        literal = kind == "literal"
        add(
            SchemaPattern(
                subject_class=t,
                subject_binding="term",
                object_binding="term"
                if row.get("object_binding", {}).get("value") == "term"
                else "type",
                property_uri=p,
                object_class="Literal" if literal else row.get("oc", {}).get("value") or "Resource",
                datatype=row.get("dt", {}).get("value") or None if literal else None,
            ),
            row,
        )
    logger.info("Probed %d term-level ontology patterns", len(patterns))
    return list(patterns.values())


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
    seen: set[frozenset[str]] = set()
    while size() > budget:
        representatives = frozenset(current.values())
        if representatives in seen:
            result.over_budget = True
            break
        seen.add(representatives)
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


# Fewest terms without a parent that mark a namespace used for typing (see group_by_shape).
NAMESPACE_GROUP_MIN_TERMS = 100


def parentless_candidates(
    chosen: Subsumption, parents: Mapping[str, set[str]], *, min_terms: int | None = None
) -> list[str]:
    """Return the terms that no ancestor can take, from namespaces used for typing.

    A term is a candidate when it has no parent and stands only for itself; a representative
    with members keeps its place. Only namespaces with at least *min_terms* such terms count: a
    namespace with few is more likely the vocabulary of the data than an ontology used for
    typing.
    """
    if not chosen.over_budget:
        return []
    if min_terms is None:
        min_terms = NAMESPACE_GROUP_MIN_TERMS
    used: dict[str, int] = defaultdict(int)
    for rep in chosen.representative.values():
        used[rep] += 1
    by_namespace: dict[str, list[str]] = defaultdict(list)
    for term, rep in chosen.representative.items():
        if rep == term and not parents.get(term) and used[term] == 1:
            by_namespace[namespace(term)].append(term)
    return sorted(t for terms in by_namespace.values() if len(terms) >= min_terms for t in terms)


def shape_group_iri(properties: Iterable[str]) -> str:
    """Return the rdfsolve IRI of the group of terms whose instances have these properties."""
    from rdfsolve.config import mint

    digest = hashlib.sha256("\n".join(sorted(set(properties))).encode("utf-8")).hexdigest()
    return mint("term-shape", digest[:16])


@dataclass
class Shape:
    """The properties that the instances of a term use, their number and one of them."""

    properties: frozenset[str]
    instances: int = 0
    example: str | None = None


def fetch_shapes(
    helper: SparqlHelper,
    terms: Iterable[str],
    *,
    batch_size: int = 100,
    graph_uris: list[str] | None = None,
    purpose: str = "ontology-terms/shapes",
) -> tuple[dict[str, Shape], list[str]]:
    """Return the shape of the instances of each term, and the terms that could not be read.

    On QLever the properties come from the property set of each instance (ql:has-predicate),
    which holds its properties in the whole index; elsewhere from the triples of the scope. A
    batch that the endpoint refuses is halved, and a term refused alone is returned as
    unreadable.
    """
    from rdfsolve.sparql_helper import EndpointError

    dataset, _, _ = _graph_scope(graph_uris)
    qlever = helper.sparql_engine == "qlever"
    shapes: dict[str, Shape] = {}
    unreadable: list[str] = []

    def rows(query: str) -> list[dict[str, Any]]:
        """Return the rows of a query."""
        result = helper.select(query, purpose=purpose)
        bindings: list[dict[str, Any]] = result.get("results", {}).get("bindings", [])
        return bindings

    def read(batch: list[str]) -> None:
        """Read the shapes of a batch of terms; halve the batch when it is refused."""
        values = " ".join(f"<{iri}>" for iri in batch)
        if qlever:
            props = (
                "PREFIX ql: <http://qlever.cs.uni-freiburg.de/builtin-functions/>\n"
                f"SELECT ?c ?p WHERE {{ VALUES ?c {{ {values} }} ?s {membership_path()} ?c . "
                "?s ql:has-predicate ?p } GROUP BY ?c ?p"
            )
        else:
            props = (
                f"SELECT DISTINCT ?c ?p {dataset} WHERE {{ VALUES ?c {{ {values} }} "
                f"?s {membership_path()} ?c . ?s ?p ?o }}"
            )
        counts = (
            f"SELECT ?c (COUNT(DISTINCT ?s) AS ?n) (SAMPLE(?s) AS ?x) {dataset} "
            f"WHERE {{ VALUES ?c {{ {values} }} ?s {membership_path()} ?c }} GROUP BY ?c"
        )
        try:
            prop_rows, count_rows = rows(props), rows(counts)
        except EndpointError as error:
            logger.warning("Shape query refused for %d terms: %s", len(batch), error)
            if len(batch) == 1:
                unreadable.append(batch[0])
                return
            half = len(batch) // 2
            read(batch[:half])
            read(batch[half:])
            return
        found: dict[str, set[str]] = {iri: set() for iri in batch}
        for row in prop_rows:
            term, prop = row.get("c", {}).get("value"), row.get("p", {}).get("value")
            if term in found and prop and prop not in MEMBERSHIP.get():
                found[term].add(prop)
        for iri in batch:
            shapes[iri] = Shape(frozenset(found[iri]))
        for row in count_rows:
            term = row.get("c", {}).get("value")
            if term in shapes:
                shapes[term].instances = int(row.get("n", {}).get("value", 0))
                shapes[term].example = row.get("x", {}).get("value")

    ordered = sorted(set(terms))
    for start in range(0, len(ordered), batch_size):
        read(ordered[start : start + batch_size])
    return shapes, unreadable


def group_by_shape(
    chosen: Subsumption,
    parents: Mapping[str, set[str]],
    shapes: Mapping[str, Iterable[str]],
    *,
    min_terms: int | None = None,
) -> dict[str, frozenset[str]]:
    """Group the terms that no ancestor can take by the shape of their instances; return the groups.

    Used before mining when lifting leaves more classes than the budget. *shapes* gives the set
    of properties (rdf:type left out) that the instances of each candidate use; terms with the
    same set form one group, named by a hash of the set, and a term whose shape no other term has
    stays its own class. A group is a shared unknown type, to be named later from links or from
    the use of its terms, not a class of an ontology. *chosen* is changed in place.
    """
    by_shape: dict[frozenset[str], list[str]] = defaultdict(list)
    for term in parentless_candidates(chosen, parents, min_terms=min_terms):
        if term in shapes:
            by_shape[frozenset(shapes[term])].append(term)
    groups: dict[str, frozenset[str]] = {}
    for shape, terms in by_shape.items():
        if len(terms) < 2:
            continue
        group = shape_group_iri(shape)
        groups[group] = shape
        for term in terms:
            chosen.representative[term] = group
    chosen.classes_after = len(set(chosen.representative.values()))
    return dict(sorted(groups.items()))


# Most terms without a parent whose shapes are read through an endpoint (fetch_shapes: two
# queries for every 100 terms). Above it the terms are grouped by namespace, which needs no
# query: a source can type its records with millions of identifier IRIs.
SHAPE_READ_MAX_TERMS = 20_000
# Member terms of a namespace group named in the queries that mine it: the queries name every
# member (VALUES (?_member ?class)), and a namespace can hold very many terms. A larger
# group is mined over evenly spaced members and its rows are marked sampled.
NAMESPACE_GROUP_MINED_MEMBERS = 1000


def namespace_group_iri(key: str) -> str:
    """Return the rdfsolve IRI of the group of the terms of one namespace."""
    from rdfsolve.config import mint

    return mint("term-namespace", hashlib.sha256(key.encode("utf-8")).hexdigest()[:16])


def _bioregistry_prefix(iri: str) -> str | None:
    """Return the Bioregistry prefix of a term IRI, or None when Bioregistry does not know it."""
    try:
        import bioregistry

        parsed = bioregistry.parse_iri(iri)
    except Exception:  # Bioregistry unavailable or the IRI not parsed
        return None
    return parsed[0] if parsed and parsed[0] else None


def group_by_namespace(
    chosen: Subsumption,
    parents: Mapping[str, set[str]],
    *,
    min_terms: int | None = None,
) -> dict[str, dict[str, Any]]:
    """Group the terms that no ancestor can take by their namespace; return the groups.

    Used before mining instead of group_by_shape when the candidates are too many to read
    their shapes through an endpoint (SHAPE_READ_MAX_TERMS). The terms of one namespace
    (ontology.terms.namespace) form a group, and namespaces that Bioregistry gives the same
    prefix are one group (two URI forms of one prefix). A group is
    named by its key, not by its members, so it is the same group in every run; a namespace
    with one term keeps it as its own class. Each group lists its namespaces and its prefix;
    the members are recorded by the caller. *chosen* is changed in place.
    """
    by_namespace: dict[str, list[str]] = defaultdict(list)
    for term in parentless_candidates(chosen, parents, min_terms=min_terms):
        by_namespace[namespace(term)].append(term)
    by_key: dict[str, list[str]] = defaultdict(list)
    prefixes: dict[str, str | None] = {}
    spaces: dict[str, list[str]] = defaultdict(list)
    for space, terms in sorted(by_namespace.items()):
        prefix = _bioregistry_prefix(terms[0])
        key = f"bioregistry:{prefix}" if prefix else space
        prefixes[key] = prefix
        spaces[key].append(space)
        by_key[key].extend(terms)
    groups: dict[str, dict[str, Any]] = {}
    for key, terms in sorted(by_key.items()):
        if len(terms) < 2:
            continue
        group = namespace_group_iri(key)
        groups[group] = {
            "key": key,
            "bioregistry_prefix": prefixes[key],
            "namespaces": spaces[key],
            "terms": len(terms),
        }
        for term in terms:
            chosen.representative[term] = group
    chosen.classes_after = len(set(chosen.representative.values()))
    return groups


def spread(terms: list[str], size: int) -> list[str]:
    """Return *size* evenly spaced terms of the sorted *terms* (all of them when fewer)."""
    ordered = sorted(terms)
    if len(ordered) <= size:
        return ordered
    step = len(ordered) / size
    return [ordered[int(i * step)] for i in range(size)]


def _merge(group: list[SchemaPattern], subject: str, obj: str) -> SchemaPattern:
    """Merge patterns that map to the same subsumed pattern."""
    first = group[0]
    counts = [p.count for p in group]
    graphs: dict[str, int] = defaultdict(int)
    for pattern in group:
        for graph, count in (pattern.graphs or {}).items():
            graphs[graph] += count
    # A member counted in a sample (a lower bound) makes the sum no bound at all: no count.
    lower = any(p.count_bound == "lower_bound" for p in group)
    if lower:
        counts, graphs = [None], defaultdict(int)
    sampled = next((p.sampled for p in group if p.sampled is not None), None)
    if sampled is not None:
        covers = sorted({c for p in group if p.sampled is not None for c in p.sampled.covers})
        sampled = sampled.model_copy(update={"covers": covers})
    return first.model_copy(
        update={
            "sampled": sampled,
            "count_bound": None,
            "subject_class": subject,
            "object_class": obj,
            # Sum over member terms; an instance typed with two members of one
            # representative contributes twice, so this is an upper bound.
            "count": None if None in counts else sum(c for c in counts if c is not None),
            "graphs": dict(graphs) or None,
            "count_semantics": "upper_bound",
            "distinct_subjects": None,
            "distinct_objects": None,
            "graph_distinct_subjects": None,
            "graph_distinct_objects": None,
            "evidence_source": "inferred",
        }
    )


def subsume_patterns(
    patterns: Iterable[SchemaPattern], representative: Mapping[str, str]
) -> list[SchemaPattern]:
    """Replace subject and object classes by their representatives and merge duplicates."""
    groups: dict[tuple[str, str, str, str | None, str], list[SchemaPattern]] = defaultdict(list)
    changed: set[tuple[str, str, str, str | None, str]] = set()
    for pattern in patterns:
        if pattern.subject_binding == "term" or pattern.object_binding == "term":
            raise ValueError("Exact term bindings cannot be replaced by class representatives")
        # Untyped subjects have no class to replace; their typed objects are grouped.
        subject = (
            pattern.subject_class
            if pattern.untyped_subject
            else representative.get(pattern.subject_class, pattern.subject_class)
        )
        obj = pattern.object_class
        if obj not in _SENTINEL_OBJECTS:
            obj = representative.get(obj, obj)
        key: tuple[str, str, str, str | None, str] = (
            subject,
            pattern.property_uri,
            obj,
            pattern.datatype,
            pattern.subject_binding,
        )
        groups[key].append(pattern)
        if subject != pattern.subject_class or obj != pattern.object_class:
            changed.add(key)
    out = []
    for grouped, group in groups.items():
        if grouped in changed or len(group) > 1:
            out.append(_merge(group, grouped[0], grouped[2]))
        else:
            out.append(group[0])
    return out


def pattern_classes(patterns: Iterable[SchemaPattern]) -> set[str]:
    """Return the subject and non-sentinel object classes of *patterns*."""
    classes: set[str] = set()
    for pattern in patterns:
        if not pattern.untyped_subject:
            classes.add(pattern.subject_class)
        if pattern.object_class not in _SENTINEL_OBJECTS:
            classes.add(pattern.object_class)
    return classes


__all__ = [
    "Subsumption",
    "build_term_object_query",
    "build_term_subject_query",
    "choose_representatives",
    "group_by_namespace",
    "namespace_group_iri",
    "pattern_classes",
    "probe_term_patterns",
    "spread",
    "subsume_patterns",
]
