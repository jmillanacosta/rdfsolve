"""F09: Integer and date boundaries.

Tests filter comparisons with boundary values.

Data:
    ex:old a ex:Paper; ex:year 2019; ex:date "2019-12-31"^^xsd:date .
    ex:boundary a ex:Paper; ex:year 2020; ex:date "2020-01-01"^^xsd:date .
    ex:new a ex:Paper; ex:year 2021; ex:date "2021-01-01"^^xsd:date .

Labels:
    ex:year: "publication year" (integer)
    ex:date: "publication date" (XSD date)
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:old a ex:Paper; ex:year 2019; ex:date "2019-12-31"^^xsd:date .
ex:boundary a ex:Paper; ex:year 2020; ex:date "2020-01-01"^^xsd:date .
ex:new a ex:Paper; ex:year 2021; ex:date "2021-01-01"^^xsd:date .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.Paper), "label": "Paper"},
    ],
    "predicates": [
        {"iri": str(EX.year), "label": "publication year", "datatype": str(XSD.integer)},
        {"iri": str(EX.date), "label": "publication date", "datatype": str(XSD.date)},
    ],
}


def create_graph() -> Graph:
    """Create the F09 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.Paper, RDFS.label, Literal("Paper")))
    g.add((EX.year, RDFS.label, Literal("publication year")))
    g.add((EX.date, RDFS.label, Literal("publication date")))


# Q14: Year >= 2020
Q14_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?p WHERE { ?p a ex:Paper; ex:year ?y . FILTER(?y >= 2020) }
"""

Q14_EXPECTED = {
    (URIRef(str(EX.boundary)),),
    (URIRef(str(EX.new)),),
}

# Q15: Date >= 2020-01-01
Q15_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?p WHERE { ?p a ex:Paper; ex:date ?d . FILTER(?d >= "2020-01-01"^^xsd:date) }
"""

Q15_EXPECTED = {
    (URIRef(str(EX.boundary)),),
    (URIRef(str(EX.new)),),
}

# Parameterized expected results for different thresholds
YEAR_THRESHOLD_EXPECTED = {
    2019: {
        (URIRef(str(EX.old)),),
        (URIRef(str(EX.boundary)),),
        (URIRef(str(EX.new)),),
    },
    2020: {
        (URIRef(str(EX.boundary)),),
        (URIRef(str(EX.new)),),
    },
    2021: {
        (URIRef(str(EX.new)),),
    },
    2022: set(),
}


def year_query(threshold: int) -> str:
    """Generate year filter query for threshold."""
    return f"""
PREFIX ex: <https://fixture.invalid/>
SELECT ?p WHERE {{ ?p a ex:Paper; ex:year ?y . FILTER(?y >= {threshold}) }}
"""


def verify_oracle_q14(g: Graph) -> bool:
    """Verify Q14: year >= 2020."""
    result = list(g.query(Q14_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q14_EXPECTED


def verify_oracle_q15(g: Graph) -> bool:
    """Verify Q15: date >= 2020-01-01."""
    result = list(g.query(Q15_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q15_EXPECTED


def verify_year_threshold(g: Graph, threshold: int) -> bool:
    """Verify parameterized year threshold."""
    query = year_query(threshold)
    result = list(g.query(query))
    actual = {tuple(row) for row in result}
    return actual == YEAR_THRESHOLD_EXPECTED.get(threshold, set())
