"""rdfsolve.reconciliation.applications: a rule applied to records in batches records the rule,
the source release, the mapping sets, each batch's records and query, and whether each batch
was answered completely (DQV); a failed batch is recorded as incomplete and the others are
kept."""

from rdflib import PROV, Dataset, Literal, Namespace, URIRef

from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.reconciliation.applications import apply
from rdfsolve.reconciliation.rules import Rule
from rdfsolve.sparql_helper import EndpointError

DQV = Namespace("http://www.w3.org/ns/dqv#")
DATA = '<urn:a> <urn:name> "A" . <urn:b> <urn:name> "B" . <urn:c> <urn:name> "C" .'
RULE = Rule(
    "urn:rule/label", "CONSTRUCT { ?record <urn:label> ?n } WHERE { ?record <urn:name> ?n }"
)
RECORDS = ["urn:a", "urn:b", "urn:c"]


def helper():
    return LocalGraphHelper("urn:local", Dataset().parse(data=DATA, format="turtle"))


def run(source_helper):
    return apply(
        RULE,
        RECORDS,
        source_helper,
        source="urn:dataset/names",
        release="urn:dataset/names/2026-10-05",
        mapping_sets=["urn:mappings/one"],
        batch_size=2,
    )


def test_an_application_records_what_was_run_on_what():
    result, application = run(helper())
    assert len(result) == 3 and application.complete
    graph = application.to_graph()
    activity = URIRef(application.iri)
    used = set(graph.objects(activity, PROV.used))
    assert {
        URIRef(RULE.iri),
        URIRef("urn:dataset/names/2026-10-05"),
        URIRef("urn:mappings/one"),
    } <= used
    measured = list(graph.subjects(DQV.computedOn, None))
    assert len(measured) == 2 and all((m, DQV.value, Literal(True)) in graph for m in measured)
    members = {
        str(m)
        for b in graph.objects(None, DQV.computedOn)
        for m in graph.objects(b, PROV.hadMember)
    }
    assert members == set(RECORDS)
    assert any("VALUES ?record" in str(q) for q in graph.objects(None, PROV.value)), (
        "Each batch keeps the query that was sent"
    )


def test_a_failed_batch_is_recorded_as_incomplete_and_the_others_are_kept(monkeypatch):
    failing = helper()
    construct = failing.construct

    def refuse(query):
        if "<urn:c>" in query:
            raise EndpointError("Query cost/time limit")
        return construct(query)

    monkeypatch.setattr(failing, "construct", refuse)
    result, application = run(failing)
    assert {str(s) for s in result.subjects()} == {"urn:a", "urn:b"}
    assert not application.complete
    graph = application.to_graph()
    values = sorted(bool(v.toPython()) for v in graph.objects(None, DQV.value))
    assert values == [False, True]
