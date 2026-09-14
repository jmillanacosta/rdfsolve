"""F03: Conjunction, disjunction, and multiplicity.

Data:
    ex:pAuthor a ex:Paper; ex:author ex:alice .
    ex:pVenue a ex:Paper; ex:venue ex:j1 .
    ex:pBoth a ex:Paper; ex:author ex:alice; ex:venue ex:j1 .
    ex:pNeither a ex:Paper .
    ex:pMany a ex:Paper; ex:author ex:alice, ex:bob; ex:venue ex:j1, ex:j2 .
    ex:alice a ex:Person . ex:bob a ex:Person .
    ex:j1 a ex:Venue . ex:j2 a ex:Venue .

Labels:
    ex:author: "author"
    ex:venue: "publication venue"
"""

from collections import Counter

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .
ex:pAuthor a ex:Paper; ex:author ex:alice .
ex:pVenue a ex:Paper; ex:venue ex:j1 .
ex:pBoth a ex:Paper; ex:author ex:alice; ex:venue ex:j1 .
ex:pNeither a ex:Paper .
ex:pMany a ex:Paper; ex:author ex:alice, ex:bob; ex:venue ex:j1, ex:j2 .
ex:alice a ex:Person . ex:bob a ex:Person .
ex:j1 a ex:Venue . ex:j2 a ex:Venue .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.Paper), "label": "Paper"},
        {"iri": str(EX.Person), "label": "Person"},
        {"iri": str(EX.Venue), "label": "Venue"},
    ],
    "predicates": [
        {"iri": str(EX.author), "label": "author"},
        {"iri": str(EX.venue), "label": "publication venue"},
    ],
    "patterns": [
        {"subject": str(EX.Paper), "predicate": str(EX.author), "object": str(EX.Person)},
        {"subject": str(EX.Paper), "predicate": str(EX.venue), "object": str(EX.Venue)},
    ],
}


def create_graph() -> Graph:
    """Create the F03 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.Paper, RDFS.label, Literal("Paper")))
    g.add((EX.Person, RDFS.label, Literal("Person")))
    g.add((EX.Venue, RDFS.label, Literal("Venue")))
    g.add((EX.author, RDFS.label, Literal("author")))
    g.add((EX.venue, RDFS.label, Literal("publication venue")))


# Oracle queries

# Q05: Conjunction (ALL) - papers with both author and venue
Q05_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?p WHERE {
    ?p a ex:Paper; ex:author ?a; ex:venue ?v .
    ?a a ex:Person . ?v a ex:Venue .
}
"""

Q05_EXPECTED = {(URIRef(str(EX.pBoth)),), (URIRef(str(EX.pMany)),)}

# Q06: Disjunction (ANY) - papers with author OR venue
Q06_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?p WHERE {
    { ?p a ex:Paper; ex:author ?a . ?a a ex:Person . }
    UNION
    { ?p a ex:Paper; ex:venue ?v . ?v a ex:Venue . }
}
"""

Q06_EXPECTED = {
    (URIRef(str(EX.pAuthor)),),
    (URIRef(str(EX.pVenue)),),
    (URIRef(str(EX.pBoth)),),
    (URIRef(str(EX.pMany)),),
}

# Q07: Non-distinct bag - pBoth once, pMany 4 times (2 authors * 2 venues)
Q07_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?p WHERE {
    ?p a ex:Paper; ex:author ?a; ex:venue ?v .
    ?a a ex:Person . ?v a ex:Venue .
}
"""

# Expected as a multiset (Counter)
Q07_EXPECTED_BAG = Counter([
    (URIRef(str(EX.pBoth)),),
    (URIRef(str(EX.pMany)),),
    (URIRef(str(EX.pMany)),),
    (URIRef(str(EX.pMany)),),
    (URIRef(str(EX.pMany)),),
])


def verify_oracle_q05(g: Graph) -> bool:
    """Verify Q05: conjunction of author and venue."""
    result = list(g.query(Q05_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q05_EXPECTED


def verify_oracle_q06(g: Graph) -> bool:
    """Verify Q06: disjunction of author or venue."""
    result = list(g.query(Q06_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q06_EXPECTED


def verify_oracle_q07(g: Graph) -> bool:
    """Verify Q07: bag semantics with multiplicities."""
    result = list(g.query(Q07_SPARQL))
    actual = Counter(tuple(row) for row in result)
    return actual == Q07_EXPECTED_BAG
