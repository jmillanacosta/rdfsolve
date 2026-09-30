"""Mine exact ontology-term bindings and group typed patterns by ancestry."""

from __future__ import annotations

import gzip
import hashlib
import logging
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rdfsolve.mining.query_builders import _bound, _context_pattern, _graph_scope, _type_pattern
from rdfsolve.mining.types import ONTOLOGY_METACLASSES
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

Collect = Callable[[str, str, int | None], list[dict[str, Any]]]

OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
RDFS_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class"
RDFS_SUBCLASS_OF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

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
  ?s a ?sc .
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
    return not iri.startswith(_NON_DATA_PREDICATE_NAMESPACES)


def build_term_subject_query(
    graph_uris: list[str] | None,
    ontology_graph_uris: list[str] | None = None,
    type_context_graph_uris: list[str] | None = None,
    *,
    terms_first: bool = False,
) -> str:
    """Ontology terms described with data properties, grouped per term and value kind.

    With *terms_first* (QLever) the set of terms is read before their edges; the filter form
    reads every triple of the graph.
    """
    dataset, g_open, g_close = _graph_scope(
        graph_uris, (ontology_graph_uris or []) + (type_context_graph_uris or [])
    )
    graph_var = " ?_g" if g_open else ""
    terms = f"{{ {{ ?t a <{OWL_CLASS}> }} UNION {{ ?t a <{RDFS_CLASS}> }} }}"
    edge = (
        f"{{ SELECT DISTINCT ?t WHERE {_context_pattern(terms, ontology_graph_uris)} }}\n"
        f"  {g_open} ?t ?p ?o . {g_close}"
        if terms_first
        else f"{g_open} ?t ?p ?o . {g_close}\n  {_term_filter('?t', ontology_graph_uris)}"
    )
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


def probe_term_patterns(
    helper: SparqlHelper,
    graph_uris: list[str] | None,
    collect: Collect,
    chunk_size: int,
    ontology_graph_uris: list[str] | None = None,
    type_context_graph_uris: list[str] | None = None,
    typed: Iterable[SchemaPattern] | None = None,
    classes: Mapping[str, str] | None = None,
) -> list[SchemaPattern]:
    """Return every observed pattern that uses an ontology term as value or subject.

    Each pattern carries its triple count. With the *typed* patterns, and no separate ontology
    graphs, the terms that are values are read only for the (class, property) pairs whose
    object class is owl:Class or rdfs:Class (Bgee: none; the filter over every triple asked for
    54 GB); *classes* gives the class objects (groups of ontology terms) by name. Results are
    paged to completion.
    """
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
            for row in collect(
                SparqlHelper.prepare_paginated_query(query), "ontology-terms/object", chunk_size
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
        rows = collect(
            SparqlHelper.prepare_paginated_query(
                build_term_object_query(graph_uris, ontology_graph_uris, type_context_graph_uris)
            ),
            "ontology-terms/object",
            chunk_size,
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
    rows = collect(
        SparqlHelper.prepare_paginated_query(
            build_term_subject_query(
                graph_uris, ontology_graph_uris, type_context_graph_uris, terms_first=terms_first
            )
        ),
        "ontology-terms/subject",
        chunk_size,
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


def fetch_superclasses(
    helper: SparqlHelper,
    terms: Iterable[str],
    *,
    batch_size: int = 500,
    purpose: str = "ontology-terms/superclasses",
    graph_uris: list[str] | None = None,
) -> dict[str, set[str]]:
    """Return named ``rdfs:subClassOf`` parents for *terms* and all their ancestors.

    Read the RDF merge of the selected graphs, or the endpoint default dataset.
    """
    dataset, _, _ = _graph_scope(graph_uris)
    parents: dict[str, set[str]] = {}
    frontier = sorted(set(terms))
    while frontier:
        for start in range(0, len(frontier), batch_size):
            batch = frontier[start : start + batch_size]
            values = " ".join(f"<{iri}>" for iri in batch)
            query = f"""\
SELECT ?c ?parent
{dataset}
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


def read_hierarchy(paths: Iterable[str | Path]) -> dict[str, set[str]]:
    """Read (child, parent) IRI pairs from tab-separated files, plain or gzip-compressed.

    Lines that start with # are comments. The files give parents to terms whose ontology is not
    in the data: PubChem types records with NCIt and PR terms, but the index holds no hierarchy
    for them. A term is not its own parent.
    """
    parents: dict[str, set[str]] = defaultdict(set)
    for path in paths:
        opener = gzip.open if str(path).endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as lines:
            for line in lines:
                if not line.strip() or line.startswith("#"):
                    continue
                child, parent = line.rstrip("\n").split("\t")[:2]
                if child != parent:
                    parents[child].add(parent)
    return dict(parents)


# Fewest terms without a parent that mark a namespace used for typing (see group_by_shape).
NAMESPACE_GROUP_MIN_TERMS = 100


def term_namespace(iri: str) -> str:
    """Return the ontology namespace of a term: an OBO prefix (obo/MONDO_), else the IRI base."""
    match = re.match(r"(.*/obo/[A-Za-z][A-Za-z0-9]*_)", iri)
    if match:
        return match.group(1)
    return re.sub(r"[^/#:]*$", "", iri)


def parentless_candidates(
    chosen: Subsumption, parents: Mapping[str, set[str]], *, min_terms: int | None = None
) -> list[str]:
    """Return the terms that no ancestor can take, from namespaces used for typing.

    A term is a candidate when it has no parent and stands only for itself; a representative
    with members keeps its place. Only namespaces with at least *min_terms* such terms count: a
    namespace with few is more likely the vocabulary of the data (PubChem: 55 classes of its own
    vocabulary) than an ontology used for typing.
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
            by_namespace[term_namespace(term)].append(term)
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
    unreadable (PubChem, shapes-3: 9 of 31,675 terms).
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
                f"SELECT ?c ?p WHERE {{ VALUES ?c {{ {values} }} ?s a ?c . "
                "?s ql:has-predicate ?p } GROUP BY ?c ?p"
            )
        else:
            props = (
                f"SELECT DISTINCT ?c ?p {dataset} WHERE {{ VALUES ?c {{ {values} }} "
                "?s a ?c . ?s ?p ?o }"
            )
        counts = (
            f"SELECT ?c (COUNT(DISTINCT ?s) AS ?n) (SAMPLE(?s) AS ?x) {dataset} "
            f"WHERE {{ VALUES ?c {{ {values} }} ?s a ?c }} GROUP BY ?c"
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
            if term in found and prop and prop != RDF_TYPE:
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
            "count_semantics": "upper_bound",
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
        if pattern.subject_binding == "term" or pattern.object_binding == "term":
            raise ValueError("Exact term bindings cannot be replaced by class representatives")
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
