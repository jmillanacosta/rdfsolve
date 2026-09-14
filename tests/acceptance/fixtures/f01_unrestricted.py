"""F01: Unrestricted selection, isolated target, and source isolation.

Data:
    ex:a a ex:A; ex:links ex:b .
    ex:b a ex:B .
    ex:c a ex:C; ex:otherLink ex:b .
    ex:isolated a ex:D .

Labels:
    ex:links: "links"
    ex:otherLink: "other link"
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .
ex:a a ex:A; ex:links ex:b .
ex:b a ex:B .
ex:c a ex:C; ex:otherLink ex:b .
ex:isolated a ex:D .
"""

# Schema metadata for class/predicate labels
SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.A), "label": "A"},
        {"iri": str(EX.B), "label": "B"},
        {"iri": str(EX.C), "label": "C"},
        {"iri": str(EX.D), "label": "D"},
    ],
    "predicates": [
        {"iri": str(EX.links), "label": "links"},
        {"iri": str(EX.otherLink), "label": "other link"},
    ],
    "patterns": [
        {"subject": str(EX.A), "predicate": str(EX.links), "object": str(EX.B)},
        {"subject": str(EX.C), "predicate": str(EX.otherLink), "object": str(EX.B)},
    ],
}


def create_graph() -> Graph:
    """Create the F01 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations to the graph."""
    g.add((EX.A, RDFS.label, Literal("A")))
    g.add((EX.B, RDFS.label, Literal("B")))
    g.add((EX.C, RDFS.label, Literal("C")))
    g.add((EX.D, RDFS.label, Literal("D")))
    g.add((EX.links, RDFS.label, Literal("links")))
    g.add((EX.otherLink, RDFS.label, Literal("other link")))


# Oracle queries and expected answers

Q01_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?a ?b WHERE { ?a a ex:A; ex:links ?b . ?b a ex:B . }
"""

Q01_EXPECTED = [(URIRef(str(EX.a)), URIRef(str(EX.b)))]

Q02_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?c ?b WHERE { ?c a ex:C; ex:otherLink ?b . ?b a ex:B . }
"""

Q02_EXPECTED = [(URIRef(str(EX.c)), URIRef(str(EX.b)))]


def verify_oracle_q01(g: Graph) -> bool:
    """Verify Q01 against fixture data."""
    result = list(g.query(Q01_SPARQL))
    expected = {tuple(row) for row in Q01_EXPECTED}
    actual = {tuple(row) for row in result}
    return actual == expected


def verify_oracle_q02(g: Graph) -> bool:
    """Verify Q02 against fixture data."""
    result = list(g.query(Q02_SPARQL))
    expected = {tuple(row) for row in Q02_EXPECTED}
    actual = {tuple(row) for row in result}
    return actual == expected
