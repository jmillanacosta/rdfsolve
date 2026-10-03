"""On QLever, when the exact instance count of each class is refused, the batches are planned
with the rdf:type triples of each class in the whole index, an upper bound (PubChem: COUNT
DISTINCT with 26 FROM clauses asked 68.8 GB and 133 classes were mined in fixed batches, with 51
timeouts; the bound took 1.6 s). The weights only plan batches; a match of _same_members is
confirmed by a count of the scope, so the patterns are those of the exact weights."""

from rdflib import Dataset

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointError

DATA = """
<urn:data> { <urn:a> a <urn:A> ; <urn:p> <urn:b> . <urn:b> a <urn:B> ; <urn:q> "x" .
             <urn:c> a <urn:A>, <urn:C> ; <urn:p> <urn:b> . }
<urn:more> { <urn:a> a <urn:A> ; <urn:q> "y" . }
"""


def mine(monkeypatch, refuse):
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="trig"), delay=0,
                                graph_uris=["urn:data", "urn:more"]) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        sent = []

        def answer(run):
            def call(query, *args, purpose="", **kwargs):
                if purpose == "two-phase/class-weights":
                    sent.append(query)
                    if refuse and "DISTINCT" in query:
                        raise EndpointError("Tried to allocate 68.8 GB, but only 30.7 GB were available")
                return run(query, *args, purpose=purpose, **kwargs)

            return call

        monkeypatch.setattr(miner.helper, "select", answer(miner.helper.select))
        schema = miner.mine("weights")
        assert miner.last_report.completion_state == "complete", "A refused plan is not a gap"
        return schema, miner.last_report.config.get("class_weights"), sent


def test_a_refused_weight_count_plans_batches_with_an_upper_bound(monkeypatch):
    exact, state, sent = mine(monkeypatch, refuse=False)
    assert state == "exact" and len(sent) == 1
    bound, state, sent = mine(monkeypatch, refuse=True)
    assert state == "upper_bound" and "DISTINCT" not in sent[-1] and "FROM" not in sent[-1]
    rows = sorted((p.subject_class, p.property_uri, p.object_class, p.count) for p in bound.patterns)
    assert rows == sorted((p.subject_class, p.property_uri, p.object_class, p.count) for p in exact.patterns)
