"""Queries from a model get missing prefixes, checks, and notes on empty results."""

import pytest
from rdfsolve.client.query_fragments import QuerySyntaxError
from rdfsolve.mcp.sparql import (
    add_prefixes,
    diagnose,
    has_limit,
    loose_optionals,
    parse,
    required_triples,
    undeclared_prefixes,
    terms,
    with_graphs,
)

PREFIXES = {"ex": "urn:ex:", "rdfs": "http://www.w3.org/2000/01/rdf-schema#"}


def test_only_used_and_undeclared_known_prefixes_are_added():
    text = 'PREFIX ex: <urn:other:>\nSELECT ?s WHERE { ?s ex:p "no:prefix" ; rdfs:label ?l . ?s <urn:x:y> un:known }'
    completed, added = add_prefixes(text, PREFIXES)
    assert added == ["rdfs"], "Declared, quoted, bracketed and unknown prefixes are left alone"
    assert completed.startswith("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n")
    assert add_prefixes("SELECT ?s WHERE { ?s ?p ?o }", PREFIXES) == ("SELECT ?s WHERE { ?s ?p ?o }", [])
    assert undeclared_prefixes(text) == ["rdfs", "un"]


def test_parse_accepts_select_only_on_this_source():
    parse("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o FILTER NOT EXISTS { ?s a ?t } }")
    with pytest.raises(QuerySyntaxError) as syntax:
        parse("SELECT ?s WHERE {\n ?s ?p }")
    assert syntax.value.detail["line"] == 2
    for text, message in [
        ("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", "Use a SELECT query"),
        ("SELECT ?s FROM <urn:g> WHERE { ?s ?p ?o }", "Leave out FROM"),
        ("SELECT ?s WHERE { SERVICE <urn:e> { ?s ?p ?o } }", "Leave out SERVICE"),
        ("SELECT ?s WHERE { ?s un:known ?o }", "Declare it with PREFIX"),
    ]:
        with pytest.raises(ValueError, match=message):
            parse(text)


def test_limits_terms_and_required_patterns_are_read_from_the_algebra():
    assert has_limit(parse("SELECT ?s WHERE { ?s ?p ?o } LIMIT 3"))
    assert not has_limit(parse("SELECT ?s WHERE { ?s ?p ?o } OFFSET 3"))
    query = parse(
        "SELECT ?s WHERE { ?s a <urn:C> ; <urn:p>/^<urn:q> ?o . OPTIONAL { ?o <urn:r> ?x } "
        "{ ?s <urn:u1> ?y } UNION { ?s <urn:u2> ?y } FILTER EXISTS { ?s <urn:e> ?z } }"
    )
    assert terms(query) == ({"urn:C"}, {"urn:p", "urn:q", "urn:r", "urn:u1", "urn:u2", "urn:e"})
    required = [str(p) for _, p, _ in required_triples(query.algebra)]
    assert len(required) == 2 and "urn:r" not in " ".join(required), "OPTIONAL and UNION are left out"


def test_diagnosis_names_the_first_pattern_that_fails():
    query = parse("SELECT * WHERE { ?a <urn:p> ?b . ?b <urn:q> ?c . ?c <urn:r> ?d }")
    asked = []

    def select(text, empty=("urn:r",)):
        asked.append(text)
        return [] if any(f"<{e}>" in text for e in empty) else [{}]

    assert diagnose(query, select, str) == ["No data matches ?c <urn:r> ?d ."]
    joined = diagnose(query, lambda t: [] if "<urn:p>" in t and "<urn:q>" in t else [{}], str)
    assert "?a <urn:p> ?b ." in joined[0] and "?b <urn:q> ?c ." in joined[0], "RDFLib can reorder"
    assert "match no data together" in joined[0]
    assert "FILTER" in diagnose(query, lambda t: [{}], str)[0]
    assert all(t.endswith("LIMIT 1") for t in asked)


def test_graphs_become_from_clauses_before_where():
    text = "PREFIX ex: <urn:{x}>\nSELECT ?s WHERE { ?s ?p ?o }"
    assert with_graphs(text, ["urn:g"]) == "PREFIX ex: <urn:{x}>\nSELECT ?s FROM <urn:g> WHERE { ?s ?p ?o }"
    assert with_graphs("SELECT * { ?s ?p ?o }", ["urn:g"]) == "SELECT * FROM <urn:g> { ?s ?p ?o }"
    assert with_graphs(text, []) == text


def test_an_optional_bound_only_by_another_optional_is_named():
    loose = parse(
        "SELECT * WHERE { ?e <urn:a> ?b OPTIONAL { ?b <urn:p> ?p } OPTIONAL { ?p <urn:label> ?name } }"
    )
    assert loose_optionals(loose) == ["p"]
    nested = parse("SELECT * WHERE { ?e <urn:a> ?b OPTIONAL { ?b <urn:p> ?p OPTIONAL { ?p <urn:label> ?n } } }")
    assert loose_optionals(nested) == []
    shared = parse("SELECT * WHERE { ?e <urn:a> ?b OPTIONAL { ?b <urn:p> ?p } OPTIONAL { ?b <urn:q> ?p } }")
    assert loose_optionals(shared) == [], "Joined on a required variable"
