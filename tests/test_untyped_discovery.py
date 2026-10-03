"""When the uncovered edges of a property are as many as the edges of its untyped subjects, they
are the same edges: a subject without a type has no typed profile. They are then discovered and
recounted with the test of an untyped subject, without the typed keys of the property, whose
filter Virtuoso refused (SIBiLS pattern#contains: "SQ200: Stack Overflow in cost model")."""

from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointError

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
                    raise EndpointError("HTTP 500: Virtuoso 42000 Error SQ200: Stack Overflow in cost model")
                return run(query, *args, purpose=purpose, **kwargs)

            return call

        monkeypatch.setattr(miner.helper, "select", refuse(miner.helper.select))
        monkeypatch.setattr(miner.helper, "select_with_fallback", refuse(miner.helper.select_with_fallback))
        result = miner.mine("untyped")
        (entry,) = miner.last_report.config["structural_coverage"]
    assert entry["state"] == "complete" and entry["uncovered_triples"] == 3
    found = sorted((p.property_uri, p.subject_selection, p.count) for p in result.structural_patterns)
    assert found == [("urn:p", "untyped", 1), ("urn:q", "untyped", 1), ("urn:q", "untyped", 1)]
    for pattern in result.structural_patterns:
        assert f"FILTER NOT EXISTS {{ ?s <{pattern.property_uri}> ?o ." in pattern.recount_query
    assert not any("NOT EXISTS { ?s a ?_type" in q for q in sent), "The group repeats the edge (QLever)"
