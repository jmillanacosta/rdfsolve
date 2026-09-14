"""F04: Same employment record versus split witnesses.

Data shows people with employment records where constraints must share the same
record to avoid false positives.

Data:
    ex:good - employed at instX 2018-2022 (satisfies 2020 constraint)
    ex:split - has two employment records, neither satisfies on its own
    ex:gap - employed at instX but with a gap around 2020

Labels:
    ex:employment: "employment record"
    ex:employer: "employer"
    ex:startYear: "employment start year" (integer)
    ex:endYear: "employment end year" (integer)
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

from . import EX

TURTLE_DATA = """
@prefix ex: <https://fixture.invalid/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:good a ex:Person; ex:employment ex:eGood .
ex:split a ex:Person; ex:employment ex:eOldX, ex:eCurrentY .
ex:gap a ex:Person; ex:employment ex:eBeforeX, ex:eAfterX .

ex:eGood a ex:Employment; ex:employer ex:instX;
    ex:startYear 2018; ex:endYear 2022 .

ex:eOldX a ex:Employment; ex:employer ex:instX;
    ex:startYear 2010; ex:endYear 2012 .

ex:eCurrentY a ex:Employment; ex:employer ex:instY;
    ex:startYear 2019; ex:endYear 2021 .

ex:eBeforeX a ex:Employment; ex:employer ex:instX;
    ex:startYear 2017; ex:endYear 2018 .

ex:eAfterX a ex:Employment; ex:employer ex:instX;
    ex:startYear 2021; ex:endYear 2024 .

ex:instX a ex:Organization .
ex:instY a ex:Organization .
"""

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.Person), "label": "Person"},
        {"iri": str(EX.Employment), "label": "Employment"},
        {"iri": str(EX.Organization), "label": "Organization"},
    ],
    "predicates": [
        {"iri": str(EX.employment), "label": "employment record"},
        {"iri": str(EX.employer), "label": "employer"},
        {"iri": str(EX.startYear), "label": "employment start year", "datatype": str(XSD.integer)},
        {"iri": str(EX.endYear), "label": "employment end year", "datatype": str(XSD.integer)},
    ],
    "patterns": [
        {"subject": str(EX.Person), "predicate": str(EX.employment), "object": str(EX.Employment)},
        {"subject": str(EX.Employment), "predicate": str(EX.employer), "object": str(EX.Organization)},
    ],
}


def create_graph() -> Graph:
    """Create the F04 fixture graph."""
    g = Graph()
    g.parse(data=TURTLE_DATA, format="turtle")
    return g


def add_labels(g: Graph) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.Person, RDFS.label, Literal("Person")))
    g.add((EX.Employment, RDFS.label, Literal("Employment")))
    g.add((EX.Organization, RDFS.label, Literal("Organization")))
    g.add((EX.employment, RDFS.label, Literal("employment record")))
    g.add((EX.employer, RDFS.label, Literal("employer")))
    g.add((EX.startYear, RDFS.label, Literal("employment start year")))
    g.add((EX.endYear, RDFS.label, Literal("employment end year")))


# Q08: People employed at instX with a single record spanning 2020
Q08_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?p WHERE {
    ?p a ex:Person; ex:employment ?e .
    ?e a ex:Employment; ex:employer ex:instX; ex:startYear ?s; ex:endYear ?t .
    FILTER(?s <= 2020 && ?t >= 2020)
}
"""

Q08_EXPECTED = {(URIRef(str(EX.good)),)}

# Counter-oracle: Using separate employment variables - returns split, gap too
Q08_WRONG_SPLIT_SPARQL = """
PREFIX ex: <https://fixture.invalid/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?p WHERE {
    ?p a ex:Person; ex:employment ?e1; ex:employment ?e2; ex:employment ?e3 .
    ?e1 a ex:Employment; ex:employer ex:instX .
    ?e2 a ex:Employment; ex:startYear ?s . FILTER(?s <= 2020)
    ?e3 a ex:Employment; ex:endYear ?t . FILTER(?t >= 2020)
}
"""


def verify_oracle_q08(g: Graph) -> bool:
    """Verify Q08: only 'good' satisfies same-record constraint."""
    result = list(g.query(Q08_SPARQL))
    actual = {tuple(row) for row in result}
    return actual == Q08_EXPECTED


def verify_counter_oracle_q08(g: Graph) -> bool:
    """Verify counter-oracle: split variables give wrong answer."""
    result = list(g.query(Q08_WRONG_SPLIT_SPARQL))
    actual = {tuple(row) for row in result}
    # Should be different from expected (admits split/gap)
    return actual != Q08_EXPECTED and len(actual) > len(Q08_EXPECTED)
