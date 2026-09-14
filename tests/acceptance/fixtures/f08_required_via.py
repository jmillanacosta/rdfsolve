"""F08: Required intermediate role versus a shorter direct route.

Tests that via constraints distinguish routes with the same meaning.

Data:
    ex:a a ex:A; ex:direct ex:bDirect; ex:step ex:v .
    ex:v a ex:V; ex:next ex:bVia .
    ex:bDirect a ex:B . ex:bVia a ex:B .
    ex:u a ex:U .

Both routes have label "related to" but the via constraint requires going
through V, which leads to bVia not bDirect.

Labels:
    ex:direct: "related to"
    Shape path (step/next): "related to"
    ex:step: "first step"
    ex:next: "second step"
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SH

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .

ex:a a ex:A; ex:direct ex:bDirect; ex:step ex:v .
ex:v a ex:V; ex:next ex:bVia .
ex:bDirect a ex:B .
ex:bVia a ex:B .
ex:u a ex:U .
"""

SHACL_SHAPES = """
@prefix ex: <https://fixture.invalid/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .

ex:AShape a sh:NodeShape;
    sh:targetClass ex:A;
    sh:name "A";
    sh:property [
        sh:path (ex:step ex:next);
        sh:name "related to";
        sh:class ex:B
    ] .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.A), "label": "A"},
        {"iri": str(EX.B), "label": "B"},
        {"iri": str(EX.V), "label": "V"},
        {"iri": str(EX.U), "label": "U"},
    ],
    "predicates": [
        {"iri": str(EX.direct), "label": "related to"},
        {"iri": str(EX.step), "label": "first step"},
        {"iri": str(EX.next), "label": "second step"},
    ],
    "patterns": [
        {"subject": str(EX.A), "predicate": str(EX.direct), "object": str(EX.B)},
        {"subject": str(EX.A), "predicate": str(EX.step), "object": str(EX.V)},
        {"subject": str(EX.V), "predicate": str(EX.next), "object": str(EX.B)},
    ],
    "shape_paths": [
        {
            "source": str(EX.A),
            "target": str(EX.B),
            "label": "related to",
            "path": ["ex:step", "ex:next"],
            "via": [str(EX.V)],
        }
    ],
}


def create_graph() -> Graph:
    """Create the F08 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def create_shapes_graph() -> Graph:
    """Create the SHACL shapes graph."""
    g = Graph()
    g.parse(data=SHACL_SHAPES, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.A, RDFS.label, Literal("A")))
    g.add((EX.B, RDFS.label, Literal("B")))
    g.add((EX.V, RDFS.label, Literal("V")))
    g.add((EX.U, RDFS.label, Literal("U")))
    g.add((EX.direct, RDFS.label, Literal("related to")))
    g.add((EX.step, RDFS.label, Literal("first step")))
    g.add((EX.next, RDFS.label, Literal("second step")))


# Q13: Via V - only returns bVia
Q13_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?b WHERE {
    ex:a a ex:A; ex:step ?v .
    ?v a ex:V; ex:next ?b .
    ?b a ex:B .
}
"""

Q13_EXPECTED = {(URIRef(str(EX.bVia)),)}

# Counter-oracle: Direct route - returns bDirect
Q13_DIRECT_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?b WHERE {
    ex:a ex:direct ?b .
    ?b a ex:B .
}
"""

Q13_DIRECT_EXPECTED = {(URIRef(str(EX.bDirect)),)}


def verify_oracle_q13(g: Graph) -> bool:
    """Verify Q13: Via V returns bVia."""
    result = list(g.query(Q13_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q13_EXPECTED


def verify_counter_oracle_q13(g: Graph) -> bool:
    """Verify counter-oracle: direct route returns bDirect (different)."""
    result = list(g.query(Q13_DIRECT_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q13_DIRECT_EXPECTED and actual != Q13_EXPECTED
