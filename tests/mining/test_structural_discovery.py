"""rdfsolve.mining.structural_strategy discovery: untyped subjects and their edges, node kinds, a
refused property during discovery, and recounts of language-tagged literals."""

import re
from types import SimpleNamespace

from rdflib import Dataset

from rdfsolve.evidence.observed import build_node_kind_query
from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.structural_strategy import _discovery_query, structural_queries
from rdfsolve.schema_models.structural import StructuralPattern
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError, QueryError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> ; <urn:q> "x" .
<urn:b> a <urn:B> .
<urn:u> <urn:p> <urn:b> ; <urn:q> "y" .
<urn:w> <urn:q> "z" .
"""


def test_uncovered_edges_of_untyped_subjects_are_found_without_the_typed_keys(monkeypatch):
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        sent = []

        def refuse(run):
            def call(query, *args, purpose="", **kwargs):
                sent.append(query)
                if purpose != "structural/coverage" and "?_subjectType" in query:
                    raise EndpointError(
                        "HTTP 500: Virtuoso 42000 Error SQ200: Stack Overflow in cost model"
                    )
                return run(query, *args, purpose=purpose, **kwargs)

            return call

        monkeypatch.setattr(miner.helper, "select", refuse(miner.helper.select))
        monkeypatch.setattr(
            miner.helper, "select_with_fallback", refuse(miner.helper.select_with_fallback)
        )
        result = miner.mine("untyped")
        (entry,) = miner.last_report.config["structural_coverage"]
    assert entry["state"] == "complete" and entry["uncovered_triples"] == 3
    found = sorted(
        (p.property_uri, p.subject_selection, p.count) for p in result.structural_patterns
    )
    assert found == [("urn:p", "untyped", 1), ("urn:q", "untyped", 1), ("urn:q", "untyped", 1)]
    for pattern in result.structural_patterns:
        assert f"FILTER NOT EXISTS {{ ?s <{pattern.property_uri}> ?o ." in pattern.recount_query
    assert not any("NOT EXISTS { ?s a ?_type" in q for q in sent), (
        "The group repeats the edge (QLever)"
    )


def kind(query: str, variable: str) -> str:
    """Return the expression that a query binds to *variable*."""
    return re.search(rf"BIND\((.*?) AS \?{variable}\)", query)[1]


def test_node_kinds_are_read_with_isblank_first():
    discovery = _discovery_query(None, [], "")
    observed = build_node_kind_query(["urn:A"], None)
    for expression in (kind(discovery, "sk"), kind(discovery, "ok"), kind(observed, "kind")):
        assert expression.startswith("IF(isBlank("), expression
        assert "isIRI" not in expression, "Virtuoso: isIRI is true for blank nodes in a BIND"


def test_discovery_groups_only_the_nodes_of_uncovered_edges():
    """The property sets are grouped for the subjects and objects of the uncovered edges only,
    with the same answer as grouping every node (UberGraph: 47 properties refused at 10 min each)."""
    import json

    from rdflib import Graph

    data = Graph().parse(
        format="turtle",
        data="""<urn:a> <urn:p> <urn:b> ; <urn:q> "x" . <urn:b> <urn:r> <urn:c> .
        <urn:c> <urn:q> "y" . _:n <urn:p> "z" ; <urn:r> <urn:a> .""",
    )
    residual = "VALUES ?p { <urn:p> }"
    own = _discovery_query(None, [], residual)
    assert "WHERE { { SELECT DISTINCT ?s WHERE { ?s ?p ?o . VALUES ?p { <urn:p> } } }" in own
    # The earlier form, which grouped every node of the graph.
    whole = own.replace(
        "{ SELECT DISTINCT ?s WHERE { ?s ?p ?o . VALUES ?p { <urn:p> } } } ", ""
    ).replace("{ SELECT DISTINCT ?o WHERE { ?s ?p ?o . VALUES ?p { <urn:p> } } } ", "")
    assert "SELECT DISTINCT ?s WHERE" not in whole

    def rows(query):
        """Return the answer rows, with the property sets in order (GROUP_CONCAT has none)."""
        found = json.loads(data.query(query).serialize(format="json"))["results"]["bindings"]
        for row in found:
            for key in ("ss", "os"):
                if key in row:
                    row[key]["value"] = " ".join(sorted(row[key]["value"].split()))
        return sorted(json.dumps(r, sort_keys=True) for r in found)

    assert rows(own) == rows(whole) and len(rows(own)) == 2


def test_a_refused_property_does_not_fail_the_source(monkeypatch):
    outcomes = []
    context = SimpleNamespace(report=SimpleNamespace(record_outcome=outcomes.append))

    def select(context, query, purpose, **options):
        if "<urn:p:slow>" in query:
            raise EndpointTimeoutError("Operation timed out")
        return [{"p": {"value": "urn:p:fast"}}]

    monkeypatch.setattr(structural_strategy, "_select", select)
    entry = {
        "census_properties": {
            "urn:p:slow": {"uncoveredTriples": 5},
            "urn:p:fast": {"uncoveredTriples": 2},
            "urn:p:covered": {"uncoveredTriples": 0},
        }
    }
    rows = structural_strategy._patterns_discovery(context, None, [], entry)
    assert rows == [{"p": {"value": "urn:p:fast"}}]
    assert "discovery_refused" in entry["census_properties"]["urn:p:slow"]
    assert len(outcomes) == 1 and outcomes[0].state == "partial"
    assert "urn:p:slow" in outcomes[0].failures[0].message


def test_triples_of_refused_properties_are_counted_as_not_discovered():
    entry = {
        "census_properties": {
            "urn:p:slow": {"uncoveredTriples": 5, "discovery_refused": "timed out"},
            "urn:p:fast": {"uncoveredTriples": 2},
        }
    }
    assert structural_strategy._undiscovered_triples(entry) == 5
    assert structural_strategy._undiscovered_triples({}) == 0


def test_a_refused_property_is_recorded_when_each_property_is_discovered_alone(monkeypatch):
    """The discovery with one query for each property has the same rule: a refused query is
    recorded and does not fail the source."""
    outcomes = []
    context = SimpleNamespace(
        report=SimpleNamespace(record_outcome=outcomes.append),
        graph_uris=None,
        type_context_graph_uris=None,
    )

    def select(context, query, purpose, **options):
        if "<urn:p:slow>" in query:
            raise QueryError("Cannot paginate volatile expressions without changing their meaning")
        return [{"p": {"value": "urn:p:fast"}}]

    monkeypatch.setattr(structural_strategy, "_select", select)
    entry = {
        "census_properties": {
            "urn:p:slow": {"uncoveredTriples": 5, "untypedTriples": 5},
            "urn:p:fast": {"uncoveredTriples": 2, "untypedTriples": 2},
        }
    }
    rows, untyped = structural_strategy._property_discovery(context, None, [], [], entry)
    assert rows == [{"p": {"value": "urn:p:fast"}}] and untyped == {"urn:p:slow", "urn:p:fast"}
    assert "discovery_refused" in entry["census_properties"]["urn:p:slow"]
    assert len(outcomes) == 1 and outcomes[0].state == "partial"


XSD = "http://www.w3.org/2001/XMLSchema#"
LANG_STRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"


def pattern(datatype, language):
    return StructuralPattern(
        subject_properties=["urn:p"],
        object_properties=[],
        subject_kind="IRI",
        object_kind="Literal",
        property_uri="urn:p",
        datatype=datatype,
        language=language,
        graph_uri=None,
        type_graph_uris=[],
        object_type_graph_uris=[],
        covered_types=[],
        subject_selection="untyped",
        count=1,
        distinct_subjects=1,
        distinct_objects=1,
        witness_query="",
        recount_query="",
    )


def test_the_language_is_tested_only_for_a_language_string():
    for datatype in (XSD + "integer", XSD + "dateTime", XSD + "string"):
        witness, recount = structural_queries(pattern(datatype, ""))
        assert f"DATATYPE(?o) = <{datatype}>" in recount and "LANG(?o)" not in recount + witness
    witness, recount = structural_queries(pattern(LANG_STRING, "en"))
    assert 'FILTER(LANG(?o) = "en")' in recount and 'FILTER(LANG(?o) = "en")' in witness
