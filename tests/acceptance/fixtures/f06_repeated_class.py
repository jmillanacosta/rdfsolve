"""F06: Two Person roles joined through one Work.

Tests that repeated classes can have different bindings.

Data:
    ex:w1 a ex:Work; ex:author ex:alice, ex:bob .
    ex:w2 a ex:Work; ex:author ex:alice, ex:carol .
    ex:w3 a ex:Work; ex:author ex:dana .
    ex:alice a ex:Person . ex:bob a ex:Person .
    ex:carol a ex:Person . ex:dana a ex:Person .

Labels:
    ex:author: "author"
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .

ex:w1 a ex:Work; ex:author ex:alice, ex:bob .
ex:w2 a ex:Work; ex:author ex:alice, ex:carol .
ex:w3 a ex:Work; ex:author ex:dana .
ex:alice a ex:Person .
ex:bob a ex:Person .
ex:carol a ex:Person .
ex:dana a ex:Person .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.Work), "label": "Work"},
        {"iri": str(EX.Person), "label": "Person"},
    ],
    "predicates": [
        {"iri": str(EX.author), "label": "author"},
    ],
    "patterns": [
        {"subject": str(EX.Work), "predicate": str(EX.author), "object": str(EX.Person)},
    ],
}


def create_graph() -> Graph:
    """Create the F06 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.Work, RDFS.label, Literal("Work")))
    g.add((EX.Person, RDFS.label, Literal("Person")))
    g.add((EX.author, RDFS.label, Literal("author")))


# Q11: Coauthors of alice (excluding alice) - requires different(left, right)
Q11_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?other WHERE {
    ?w a ex:Work; ex:author ex:alice; ex:author ?other .
    ?other a ex:Person .
    FILTER(?other != ex:alice)
}
"""

Q11_EXPECTED = {
    (URIRef(str(EX.bob)),),
    (URIRef(str(EX.carol)),),
}

# Q12: Coauthors of alice (including alice) - no inequality constraint
Q12_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?other WHERE {
    ?w a ex:Work; ex:author ex:alice; ex:author ?other .
    ?other a ex:Person .
}
"""

Q12_EXPECTED = {
    (URIRef(str(EX.alice)),),
    (URIRef(str(EX.bob)),),
    (URIRef(str(EX.carol)),),
}


def verify_oracle_q11(g: Graph) -> bool:
    """Verify Q11: Coauthors excluding alice."""
    result = list(g.query(Q11_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q11_EXPECTED


def verify_oracle_q12(g: Graph) -> bool:
    """Verify Q12: Coauthors including alice."""
    result = list(g.query(Q12_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q12_EXPECTED
