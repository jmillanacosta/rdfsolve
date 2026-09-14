"""F05: Shape-only inverse sequence with untyped intermediate nodes.

Data:
    ex:a1 a ex:A; ex:p ex:x1 . ex:b1 a ex:B; ex:q ex:x1 . ex:x1 ex:score 12 .
    ex:a2 a ex:A; ex:p ex:x2 . ex:b2 a ex:B; ex:q ex:x2 . ex:x2 ex:score 1 .
    ex:a3 a ex:A; ex:p ex:x3 . ex:b3 a ex:B; ex:q ex:x4 .  # Different records
    ex:x3 ex:score 99 . ex:x4 ex:score 99 .

The shape path (ex:p [sh:inversePath ex:q]) connects A to B via shared untyped
intermediate nodes. x1-x4 have no rdf:type.

Labels:
    Shape path: "linked B via shared record"
    ex:p: "record"
    ex:q: "record"
    ex:score: "score" (integer)
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD, SH

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:a1 a ex:A; ex:p ex:x1 .
ex:b1 a ex:B; ex:q ex:x1 .
ex:x1 ex:score 12 .

ex:a2 a ex:A; ex:p ex:x2 .
ex:b2 a ex:B; ex:q ex:x2 .
ex:x2 ex:score 1 .

ex:a3 a ex:A; ex:p ex:x3 .
ex:b3 a ex:B; ex:q ex:x4 .
ex:x3 ex:score 99 .
ex:x4 ex:score 99 .
"""

SHACL_SHAPES = """
@prefix ex: <https://fixture.invalid/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:AShape a sh:NodeShape;
    sh:targetClass ex:A;
    sh:name "A";
    sh:property [
        sh:path (ex:p [sh:inversePath ex:q]);
        sh:name "linked B via shared record";
        sh:class ex:B
    ];
    sh:property [sh:path ex:p; sh:node ex:RecordShape; sh:name "record"] .

ex:BShape a sh:NodeShape;
    sh:targetClass ex:B;
    sh:name "B";
    sh:property [sh:path ex:q; sh:node ex:RecordShape; sh:name "record"] .

ex:RecordShape a sh:NodeShape;
    sh:property [sh:path ex:score; sh:name "score"; sh:datatype xsd:integer] .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.A), "label": "A"},
        {"iri": str(EX.B), "label": "B"},
    ],
    "predicates": [
        {"iri": str(EX.p), "label": "record"},
        {"iri": str(EX.q), "label": "record"},
        {"iri": str(EX.score), "label": "score", "datatype": str(XSD.integer)},
    ],
    # No direct A-to-B pattern - must be discovered through shape
    "patterns": [],
    "shape_paths": [
        {
            "source": str(EX.A),
            "target": str(EX.B),
            "label": "linked B via shared record",
            "path": ["ex:p", "^ex:q"],  # Forward p, inverse q
        }
    ],
}


def create_graph() -> Graph:
    """Create the F05 fixture graph."""
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
    g.add((EX.p, RDFS.label, Literal("record")))
    g.add((EX.q, RDFS.label, Literal("record")))
    g.add((EX.score, RDFS.label, Literal("score")))


# Q09: A to B via shared intermediate (no score filter)
Q09_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?a ?b WHERE {
    ?a a ex:A; ex:p ?x .
    ?b a ex:B; ex:q ?x .
}
"""

Q09_EXPECTED = {
    (URIRef(str(EX.a1)), URIRef(str(EX.b1))),
    (URIRef(str(EX.a2)), URIRef(str(EX.b2))),
}

# Q10: With score filter >= 10
Q10_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?a ?b WHERE {
    ?a a ex:A; ex:p ?x .
    ?b a ex:B; ex:q ?x .
    ?x ex:score ?score .
    FILTER(?score >= 10)
}
"""

Q10_EXPECTED = {(URIRef(str(EX.a1)), URIRef(str(EX.b1)))}

# Counter-oracle: Score constraint on independent record
Q10_WRONG_INDEPENDENT_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?a ?b WHERE {
    ?a a ex:A; ex:p ?x .
    ?b a ex:B; ex:q ?y .
    ?z ex:score ?score .
    FILTER(?score >= 10)
}
"""


def verify_oracle_q09(g: Graph) -> bool:
    """Verify Q09: A-B pairs via shared intermediate."""
    result = list(g.query(Q09_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q09_EXPECTED


def verify_oracle_q10(g: Graph) -> bool:
    """Verify Q10: Only a1-b1 with score >= 10."""
    result = list(g.query(Q10_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q10_EXPECTED


def verify_counter_oracle_q10(g: Graph) -> bool:
    """Counter-oracle: independent score gives wrong answer."""
    result = list(g.query(Q10_WRONG_INDEPENDENT_SPARQL))
    actual = {tuple(row) for row in result}
    # Wrong query returns more results
    return actual != Q10_EXPECTED and len(actual) > len(Q10_EXPECTED)
