"""On QLever, the counts of a class and property are read per object first, with the edges and
distinct subjects of (class, property) in one query. A group whose edges are all the edges of
(class, property) has the distinct subjects of (class, property), exactly; the distinct
subjects of a group with part of the edges are counted for that group alone (Bgee
hasSequenceUnit: 709,482,280 edges, all 7 object types with all edges; the count with distinct
subjects of every group reached the time limit of 3,600 s). RDFLib stands in for QLever here."""

from rdflib import Dataset

from rdfsolve.mining.miner import SchemaMiner

ALL = """
<urn:a1> a <urn:A> ; <urn:p> <urn:o1>, <urn:o2> .  <urn:a2> a <urn:A> ; <urn:p> <urn:o1> .
<urn:o1> a <urn:T1> .  <urn:o2> a <urn:T1> .
"""
PART = ALL + '<urn:o1> a <urn:T2> . <urn:a1> <urn:q> 1, "x" . <urn:a2> <urn:q> "y" .\n'


def counts(monkeypatch, data):
    with SchemaMiner.from_graph(Dataset().parse(data=data, format="turtle"), delay=0, class_batch_size=1) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        sent = []
        select = miner.helper.select

        def record(query, *args, purpose="", **kwargs):
            sent.append(purpose)
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", record)
        schema = miner.mine("counts")
        state = miner.last_report.completion_state
    found = {
        (p.subject_class, p.object_class, p.datatype): (p.count, p.distinct_subjects)
        for p in schema.patterns
        if p.property_uri in ("urn:p", "urn:q")
    }
    return found, sent, state


def test_distinct_subjects_follow_from_a_group_with_all_edges(monkeypatch):
    found, sent, state = counts(monkeypatch, ALL)
    assert found[("urn:A", "urn:T1", None)] == (3, 2) and state == "complete"
    heavy = "counts/typed-object/property/urn:p"
    assert heavy + "/per-object" in sent and heavy + "/total" in sent and heavy not in sent
    found, sent, state = counts(monkeypatch, PART)
    assert found[("urn:A", "urn:T1", None)] == (3, 2) and found[("urn:A", "urn:T2", None)] == (2, 2)
    xsd = "http://www.w3.org/2001/XMLSchema#"
    assert found[("urn:A", "Literal", xsd + "integer")] == (1, 1)
    assert found[("urn:A", "Literal", xsd + "string")] == (2, 2)
    assert "counts/literal/property/urn:q/group" in sent
    assert heavy + "/group" in sent and heavy not in sent and state == "complete", (
        "A group with part of the edges counts its own distinct subjects (Bgee ExpressionCondition: 93 s)"
    )
