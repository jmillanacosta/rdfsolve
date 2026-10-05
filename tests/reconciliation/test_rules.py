"""rdfsolve.reconciliation.rules: a conversion or check rule is plain SPARQL whose input records
are a declared parameter; bound to records, it reads only those; it is kept as a SHACL SPARQL
executable, also in a nanopublication; a rule that is not portable SPARQL is refused."""

import pytest
from rdflib import Dataset, Graph, Literal, Namespace, URIRef

from rdfsolve.reconciliation.nanopubs import load, save
from rdfsolve.reconciliation.rules import Rule

SH = Namespace("http://www.w3.org/ns/shacl#")
DATA = """
<urn:a> <urn:name> "A" . <urn:b> <urn:name> "B" . <urn:c> <urn:name> "" .
"""
CONVERT = Rule(
    "urn:rule/label",
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
    "CONSTRUCT { ?record rdfs:label ?name } WHERE { ?record <urn:name> ?name }",
    comment="Copy each record's name to its label",
)
CHECK = Rule("urn:rule/empty", 'SELECT ?record WHERE { ?record <urn:name> "" }')


def test_a_rule_bound_to_records_reads_only_those():
    data = Dataset().parse(data=DATA, format="turtle")
    converted = data.query(CONVERT.bound(["urn:a"])).graph
    assert {(str(s), str(o)) for s, _, o in converted} == {("urn:a", "A")}
    assert {str(r.record) for r in data.query(CHECK.bound(["urn:b", "urn:c"]))} == {"urn:c"}
    assert "VALUES" not in CONVERT.query, "The rule itself holds no input records"


def test_a_rule_is_kept_as_a_sparql_executable_and_in_a_nanopublication(tmp_path):
    graph = CONVERT.to_graph()
    rule = URIRef(CONVERT.iri)
    assert (rule, None, SH.SPARQLConstructExecutable) in graph
    assert (rule, SH.construct, Literal(CONVERT.query)) in graph
    (parameter,) = graph.objects(rule, SH.parameter)
    assert (parameter, SH.name, Literal("record")) in graph
    assert (URIRef(CHECK.iri), None, SH.SPARQLSelectExecutable) in CHECK.to_graph()
    assert Rule.from_graph(graph, CONVERT.iri) == CONVERT
    saved = load(
        save(CONVERT.nanopublication(attributed_to="urn:tool", created="2026-10-05"), tmp_path)
    )
    assert Rule.from_graph(saved.assertion, CONVERT.iri) == CONVERT


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", "parameter"),
        ("SELECT ?record WHERE { VALUES ?record { <urn:a> } ?record ?p ?o }", "VALUES"),
        ("SELECT ?record WHERE { ?record ?p ?o", "SPARQL"),
        ("ASK { ?record ?p ?o }", "CONSTRUCT or SELECT"),
    ],
)
def test_a_rule_that_is_not_portable_is_refused(query, message):
    with pytest.raises(ValueError, match=message):
        Rule("urn:rule/bad", query)
