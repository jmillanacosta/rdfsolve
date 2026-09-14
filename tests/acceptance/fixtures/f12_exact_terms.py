"""F12: Exact RDF terms, not syntax interpolation.

Tests that special characters in literals are handled correctly.

Data is constructed with RDFLib term constructors, not concatenated Turtle.

Resources ex:term0 through ex:term3 are ex:Record instances with rdfs:label:
- term0: '"} UNION { ?s ?p ?o } #'  (injection attempt)
- term1: 'a\\b\\n"c'  (escape sequences)
- term2: 'color'@en  (language tag)
- term3: 'color'@nl  (different language)

Also tests datatype distinction: "2020"^^xsd:string vs "2020"^^xsd:integer
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

from . import EX

# Literals with special characters
TERM_VALUES = [
    Literal('"} UNION { ?s ?p ?o } #'),  # SPARQL injection attempt
    Literal('a\\b\n"c'),  # Escape sequences
    Literal('color', lang='en'),  # English
    Literal('color', lang='nl'),  # Dutch
]

# Datatype distinction test values
DATATYPE_STRINGS = [
    Literal("2020", datatype=XSD.string),
    Literal("2020", datatype=XSD.integer),
]

SCHEMA_METADATA = {
    "classes": [
        {"iri": str(EX.Record), "label": "Record"},
    ],
    "predicates": [
        {"iri": str(RDFS.label), "label": "label"},
    ],
}


def create_graph() -> Graph:
    """Create the F12 fixture graph with RDFLib constructors."""
    g = Graph()

    # Add special literal records
    for i, value in enumerate(TERM_VALUES):
        term = EX[f"term{i}"]
        g.add((term, RDF.type, EX.Record))
        g.add((term, RDFS.label, value))

    # Add datatype distinction records
    g.add((EX.stringRecord, RDF.type, EX.Record))
    g.add((EX.stringRecord, RDFS.label, DATATYPE_STRINGS[0]))

    g.add((EX.intRecord, RDF.type, EX.Record))
    g.add((EX.intRecord, RDFS.label, DATATYPE_STRINGS[1]))

    return g


def exact_match_query(value: Literal) -> str:
    """Generate exact match query using sameTerm for a literal value."""
    # Use parameter binding for safe literal handling
    return """
PREFIX ex: <https://fixture.invalid/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?r WHERE {
    ?r a ex:Record; rdfs:label ?label .
    FILTER(sameTerm(?label, %s))
}
""" % value.n3()


def verify_exact_match(g: Graph, term_index: int) -> bool:
    """Verify exact match for term at index."""
    expected_uri = EX[f"term{term_index}"]
    value = TERM_VALUES[term_index]
    query = exact_match_query(value)
    result = list(g.query(query))
    actual = {row[0] for row in result}
    return actual == {expected_uri}


def verify_all_exact_matches(g: Graph) -> list[bool]:
    """Verify all term exact matches."""
    return [verify_exact_match(g, i) for i in range(len(TERM_VALUES))]


def verify_datatype_distinction(g: Graph) -> bool:
    """Verify string and integer literals are distinct."""
    # Query for string "2020"
    string_query = """
PREFIX ex: <https://fixture.invalid/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?r WHERE {
    ?r a ex:Record; rdfs:label ?label .
    FILTER(sameTerm(?label, "2020"^^xsd:string))
}
"""
    string_result = list(g.query(string_query))
    string_records = {row[0] for row in string_result}

    # Query for integer 2020
    int_query = """
PREFIX ex: <https://fixture.invalid/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?r WHERE {
    ?r a ex:Record; rdfs:label ?label .
    FILTER(sameTerm(?label, "2020"^^xsd:integer))
}
"""
    int_result = list(g.query(int_query))
    int_records = {row[0] for row in int_result}

    # String record should only match string query
    # Integer record should only match integer query
    return (
        string_records == {EX.stringRecord}
        and int_records == {EX.intRecord}
        and string_records != int_records
    )


def verify_language_distinction(g: Graph) -> bool:
    """Verify language tags are distinguished."""
    # Query for 'color'@en
    en_query = """
PREFIX ex: <https://fixture.invalid/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?r WHERE {
    ?r a ex:Record; rdfs:label ?label .
    FILTER(sameTerm(?label, "color"@en))
}
"""
    en_result = list(g.query(en_query))
    en_records = {row[0] for row in en_result}

    # Query for 'color'@nl
    nl_query = """
PREFIX ex: <https://fixture.invalid/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?r WHERE {
    ?r a ex:Record; rdfs:label ?label .
    FILTER(sameTerm(?label, "color"@nl))
}
"""
    nl_result = list(g.query(nl_query))
    nl_records = {row[0] for row in nl_result}

    return (
        en_records == {EX.term2}
        and nl_records == {EX.term3}
        and en_records != nl_records
    )
