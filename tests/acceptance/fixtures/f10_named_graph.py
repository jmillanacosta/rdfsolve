"""F10: Named-graph scope.

Tests that graph scope prevents cross-graph joins.

Data (in TriG format):
    ex:g1 {
        ex:pSplit a ex:Paper; ex:author ex:alice . ex:alice a ex:Person .
        ex:pWhole a ex:Paper; ex:author ex:alice; ex:venue ex:j1 . ex:j1 a ex:Venue .
    }
    ex:g2 {
        ex:pSplit a ex:Paper; ex:venue ex:j1 . ex:j1 a ex:Venue .
    }

Same labels as F03.
"""

from rdflib import Graph, Dataset, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from . import EX

TRIG_DATA = """
@prefix ex: <https://fixture.invalid/> .

ex:g1 {
    ex:pSplit a ex:Paper; ex:author ex:alice .
    ex:alice a ex:Person .
    ex:pWhole a ex:Paper; ex:author ex:alice; ex:venue ex:j1 .
    ex:j1 a ex:Venue .
}
ex:g2 {
    ex:pSplit a ex:Paper; ex:venue ex:j1 .
    ex:j1 a ex:Venue .
}
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
    "graphs": [str(EX.g1), str(EX.g2)],
}


def create_dataset() -> Dataset:
    """Create the F10 fixture dataset with named graphs."""
    ds = Dataset()
    ds.parse(data=TRIG_DATA, format="trig")
    return ds


def create_g1_graph() -> Graph:
    """Create graph containing only g1 data."""
    ds = create_dataset()
    return ds.graph(EX.g1)


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.Paper, RDFS.label, Literal("Paper")))
    g.add((EX.Person, RDFS.label, Literal("Person")))
    g.add((EX.Venue, RDFS.label, Literal("Venue")))
    g.add((EX.author, RDFS.label, Literal("author")))
    g.add((EX.venue, RDFS.label, Literal("publication venue")))


# Q16: Within g1 scope only - only pWhole has both author and venue in g1
Q16_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?p WHERE {
    GRAPH ex:g1 {
        ?p a ex:Paper; ex:author ?a; ex:venue ?v .
        ?a a ex:Person . ?v a ex:Venue .
    }
}
"""

Q16_EXPECTED = {(URIRef(str(EX.pWhole)),)}

# Counter-oracle: Union of g1 and g2 would incorrectly admit pSplit
Q16_WRONG_UNION_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT DISTINCT ?p WHERE {
    { GRAPH ex:g1 { ?p a ex:Paper; ex:author ?a . ?a a ex:Person . } }
    { GRAPH ?g { ?p ex:venue ?v . ?v a ex:Venue . } }
}
"""


def verify_oracle_q16(ds: Dataset) -> bool:
    """Verify Q16: Only pWhole within g1 scope."""
    result = list(ds.query(Q16_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q16_EXPECTED


def verify_counter_oracle_q16(ds: Dataset) -> bool:
    """Counter-oracle: Union incorrectly admits pSplit."""
    result = list(ds.query(Q16_WRONG_UNION_SPARQL))
    actual = {tuple(row) for row in result}
    return actual != Q16_EXPECTED and URIRef(str(EX.pSplit)) in {r[0] for r in result}
