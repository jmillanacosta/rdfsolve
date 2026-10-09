"""rdfsolve.ontology.manchester: class expressions in RDF written in Manchester syntax."""

from rdflib import RDF, BNode, Graph

from rdfsolve.ontology.manchester import EXPRESSION_PREDICATES, UnsupportedExpressionError, render

TTL = """
@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <urn:ex:> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:some a [ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Heart ] .
ex:nested a [ a owl:Class ; owl:intersectionOf ( ex:Cell [ a owl:Restriction ;
    owl:onProperty ex:partOf ; owl:allValuesFrom [ a owl:Class ; owl:unionOf ( ex:A ex:B ) ] ] ) ] .
ex:qualified a [ a owl:Restriction ; owl:onProperty ex:hasPart ;
    owl:qualifiedCardinality "2"^^xsd:nonNegativeInteger ; owl:onClass ex:Leg ] .
ex:not a [ a owl:Class ; owl:complementOf ex:A ] .
ex:value a [ a owl:Restriction ; owl:onProperty [ owl:inverseOf ex:p ] ; owl:hasValue ex:v ] .
ex:max a [ a owl:Restriction ; owl:onProperty ex:p ; owl:maxCardinality 1 ] .
ex:one a [ a owl:Class ; owl:oneOf ( ex:x ex:y ) ] .
ex:range a [ a owl:Restriction ; owl:onProperty ex:age ; owl:someValuesFrom [
    owl:onDatatype xsd:integer ; owl:withRestrictions ( [ xsd:minInclusive 18 ] ) ] ] .
ex:broken a [ a owl:Restriction ; owl:onProperty ex:p ] .
"""


def _term(node):
    return f"_:{node}" if isinstance(node, BNode) else node.n3()


def _name(iri: str) -> str:
    return iri.replace("urn:ex:", "ex:").replace("http://www.w3.org/2001/XMLSchema#", "xsd:")


def test_class_expressions_are_written_in_manchester_syntax():
    graph = Graph().parse(data=TTL, format="turtle")
    outgoing: dict[str, list[tuple[str, str]]] = {}
    for s, p, o in graph:
        if isinstance(s, BNode) and str(p) in EXPRESSION_PREDICATES:
            outgoing.setdefault(_term(s), []).append((str(p), _term(o)))
    found = {}
    for s, o in graph.subject_objects(RDF.type):
        if isinstance(o, BNode):
            try:
                found[_name(str(s))] = render(_term(o), outgoing, _name)
            except UnsupportedExpressionError:
                found[_name(str(s))] = None
    assert found == {
        "ex:some": "ex:partOf some ex:Heart",
        "ex:nested": "ex:Cell and (ex:partOf only (ex:A or ex:B))",
        "ex:qualified": "ex:hasPart exactly 2 ex:Leg",
        "ex:not": "not ex:A",
        "ex:value": "inverse ex:p value ex:v",
        "ex:max": "ex:p max 1",
        "ex:one": "{ex:x, ex:y}",
        "ex:range": 'ex:age some xsd:integer[>= "18"^^xsd:integer]',
        "ex:broken": None,
    }
