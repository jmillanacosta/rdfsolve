"""F11: Complementary gene-link relations.

Tests that ANY composition includes both relations.

Data:
    ex:a a ex:A; ex:pNER ex:g1; ex:pRegex ex:g2 .
    ex:g1 a ex:Gene . ex:g2 a ex:Gene .

Labels:
    ex:pNER: "NER gene link"
    ex:pRegex: "regex gene link"
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .

ex:a a ex:A; ex:pNER ex:g1; ex:pRegex ex:g2 .
ex:g1 a ex:Gene .
ex:g2 a ex:Gene .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.A), "label": "A"},
        {"iri": str(EX.Gene), "label": "Gene"},
    ],
    "predicates": [
        {"iri": str(EX.pNER), "label": "NER gene link"},
        {"iri": str(EX.pRegex), "label": "regex gene link"},
    ],
    "patterns": [
        {"subject": str(EX.A), "predicate": str(EX.pNER), "object": str(EX.Gene)},
        {"subject": str(EX.A), "predicate": str(EX.pRegex), "object": str(EX.Gene)},
    ],
}


def create_graph() -> Graph:
    """Create the F11 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.A, RDFS.label, Literal("A")))
    g.add((EX.Gene, RDFS.label, Literal("Gene")))
    g.add((EX.pNER, RDFS.label, Literal("NER gene link")))
    g.add((EX.pRegex, RDFS.label, Literal("regex gene link")))


# Q17: Both genes via ANY composition
Q17_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?g WHERE {
    { ex:a ex:pNER ?g . ?g a ex:Gene . }
    UNION
    { ex:a ex:pRegex ?g . ?g a ex:Gene . }
}
"""

Q17_EXPECTED = {
    (URIRef(str(EX.g1)),),
    (URIRef(str(EX.g2)),),
}

# Counter-oracle: Only NER link
Q17_NER_ONLY_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?g WHERE {
    ex:a ex:pNER ?g . ?g a ex:Gene .
}
"""

Q17_NER_ONLY_EXPECTED = {(URIRef(str(EX.g1)),)}


def verify_oracle_q17(g: Graph) -> bool:
    """Verify Q17: Both genes from ANY composition."""
    result = list(g.query(Q17_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q17_EXPECTED


def verify_counter_oracle_q17(g: Graph) -> bool:
    """Counter-oracle: NER only gives one gene."""
    result = list(g.query(Q17_NER_ONLY_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q17_NER_ONLY_EXPECTED and actual != Q17_EXPECTED
