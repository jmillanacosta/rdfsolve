"""F02: Ambiguous semantics with opaque predicate IRIs.

Data:
    ex:alice a ex:Person; ex:P001 ex:instX; ex:P002 ex:instY .
    ex:bob a ex:Person; ex:P001 ex:instY; ex:P002 ex:instX .
    ex:instX a ex:Organization .
    ex:instY a ex:Organization .

Labels:
    ex:P001: "current employer", description "Organization currently employing the person."
    ex:P002: "publication affiliation", description "Institution recorded for the person on a publication."
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .
ex:alice a ex:Person; ex:P001 ex:instX; ex:P002 ex:instY .
ex:bob a ex:Person; ex:P001 ex:instY; ex:P002 ex:instX .
ex:instX a ex:Organization .
ex:instY a ex:Organization .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.Person), "label": "Person"},
        {"iri": str(EX.Organization), "label": "Organization"},
    ],
    "predicates": [
        {
            "iri": str(EX.P001),
            "label": "current employer",
            "description": "Organization currently employing the person.",
        },
        {
            "iri": str(EX.P002),
            "label": "publication affiliation",
            "description": "Institution recorded for the person on a publication.",
        },
    ],
    "patterns": [
        {"subject": str(EX.Person), "predicate": str(EX.P001), "object": str(EX.Organization)},
        {"subject": str(EX.Person), "predicate": str(EX.P002), "object": str(EX.Organization)},
    ],
}


def create_graph() -> Graph:
    """Create the F02 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.Person, RDFS.label, Literal("Person")))
    g.add((EX.Organization, RDFS.label, Literal("Organization")))
    g.add((EX.P001, RDFS.label, Literal("current employer")))
    g.add((EX.P002, RDFS.label, Literal("publication affiliation")))
    g.add((EX.P001, RDFS.comment, Literal("Organization currently employing the person.")))
    g.add((EX.P002, RDFS.comment, Literal("Institution recorded for the person on a publication.")))


# Oracle queries

Q03_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?p WHERE { ?p a ex:Person; ex:P001 ex:instX . }
"""

Q03_EXPECTED = [(URIRef(str(EX.alice)),)]

Q04_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?p WHERE { ?p a ex:Person; ex:P002 ex:instX . }
"""

Q04_EXPECTED = [(URIRef(str(EX.bob)),)]


def verify_oracle_q03(g: Graph) -> bool:
    """Verify Q03: current employer of instX is alice."""
    result = list(g.query(Q03_SPARQL))
    expected = {tuple(row) for row in Q03_EXPECTED}
    actual = {tuple(row) for row in result}
    return actual == expected


def verify_oracle_q04(g: Graph) -> bool:
    """Verify Q04: publication affiliation of instX is bob."""
    result = list(g.query(Q04_SPARQL))
    expected = {tuple(row) for row in Q04_EXPECTED}
    actual = {tuple(row) for row in result}
    return actual == expected
