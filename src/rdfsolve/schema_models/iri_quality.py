"""Terms of a source that are not RDF IRIs.

A term such as ``<http://example.org/a b>`` has a character that RDF IRIs exclude (RDF 1.1
Concepts, RFC 3987). Lenient engines keep such terms, and mining queries them with
``IRI("...")`` (rdfsolve.sparql_terms). They are data-quality findings of the source: the JSON
schema keeps them as they are, and the RDF outputs (VoID, SHACL, LinkML), which cannot write
them as IRIs, leave them out and the findings say so.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import TYPE_CHECKING, Any

from rdflib import Graph, URIRef

from rdfsolve.schema_models.paths import is_rdf_iri

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema

__all__ = ["findings", "rdf_terms_only", "rdf_writable"]

SENTINELS = frozenset({"Literal", "Resource", "BlankNode"})
_TEST = '!REGEX(STR(?term), "^[^\\\\s<>\\"{}|^`\\\\\\\\]*$")'
QUERIES = {
    "property": f"SELECT ?term (COUNT(*) AS ?triples) WHERE {{ ?s ?term ?o FILTER({_TEST}) }} "
    "GROUP BY ?term",
    "subject": f"SELECT ?term (COUNT(*) AS ?triples) WHERE {{ ?term ?p ?o "
    f"FILTER(isIRI(?term) && {_TEST}) }} GROUP BY ?term",
    "object": f"SELECT ?term (COUNT(*) AS ?triples) WHERE {{ ?s ?p ?term "
    f"FILTER(isIRI(?term) && {_TEST}) }} GROUP BY ?term",
}


def _roles(schema: MinedSchema) -> tuple[dict[str, set[str]], Counter[str], Counter[str]]:
    """Return the roles, patterns and counted triples of each term that is not an RDF IRI."""
    roles: dict[str, set[str]] = defaultdict(set)
    patterns: Counter[str] = Counter()
    triples: Counter[str] = Counter()
    rows = [*schema.patterns, *(schema.term_patterns or [])]
    for p in rows:
        for term, role in (
            (p.subject_class, "class"),
            (p.property_uri, "property"),
            (p.object_class, "class"),
        ):
            if term not in SENTINELS and not is_rdf_iri(term):
                roles[term].add(role)
                patterns[term] += 1
                triples[term] += p.count or 0
    for s in schema.structural_patterns or []:
        for term in {s.property_uri, *s.subject_properties, *s.object_properties}:
            if not is_rdf_iri(term):
                roles[term].add("property")
                patterns[term] += 1
                triples[term] += s.count if term == s.property_uri else 0
    return roles, patterns, triples


def findings(schema: MinedSchema, statistics: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the terms that are not RDF IRIs, their roles and counts, and the queries that list
    every such term of the data by position.

    The triples of a property come from the dataset statistics where these were counted, else
    from the counted patterns.
    """
    roles, patterns, triples = _roles(schema)
    partitions = (statistics or {}).get("property_partitions") or {}
    for prop in partitions:
        if not is_rdf_iri(prop):
            roles[prop].add("property")
    terms = [
        {
            "iri": iri,
            "roles": sorted(roles[iri]),
            "patterns": patterns[iri],
            "triples": partitions.get(iri, {}).get("triples", triples[iri]),
            "in_rdf_outputs": False,
        }
        for iri in sorted(roles)
    ]
    return {
        "rule": 'RDF 1.1 Concepts: IRIs conform to RFC 3987, which excludes spaces and <>"{}|^`\\',
        "terms": terms,
        "queries": QUERIES,
    }


def rdf_writable(schema: MinedSchema) -> MinedSchema:
    """Return *schema* without the rows that hold a term that is not an RDF IRI."""

    def kept(p: Any) -> bool:
        """Return whether a schema row holds RDF IRIs only."""
        return all(
            t in SENTINELS or is_rdf_iri(t)
            for t in (p.subject_class, p.property_uri, p.object_class)
        )

    def structural(s: Any) -> bool:
        """Return whether a structural row holds RDF IRIs only."""
        return all(
            is_rdf_iri(t) for t in (s.property_uri, *s.subject_properties, *s.object_properties)
        )

    update: dict[str, Any] = {"patterns": [p for p in schema.patterns if kept(p)]}
    for name in ("raw_patterns", "term_patterns"):
        rows = getattr(schema, name)
        if rows is not None:
            update[name] = [p for p in rows if kept(p)]
    if schema.structural_patterns is not None:
        update["structural_patterns"] = [s for s in schema.structural_patterns if structural(s)]
    return schema.model_copy(update=update)


def rdf_terms_only(graph: Graph) -> Graph:
    """Remove the triples of *graph* that hold a term that is not an RDF IRI."""
    for triple in list(graph):
        if any(isinstance(t, URIRef) and not is_rdf_iri(t) for t in triple):
            graph.remove(triple)
    return graph
