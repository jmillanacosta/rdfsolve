"""A property whose census is refused and whose objects carry few triples each is not counted in
batches of objects: a batch would name as many objects as it has triples (SIBiLS
pattern#contains: 27,554,728 triples, 27,554,728 objects). Its triples are recorded as not
checked, with the reason, and the source is partial, not failed."""

from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointTimeoutError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> ; <urn:c> <urn:x1>, <urn:x2>, <urn:x3>, <urn:x4> .
<urn:b> a <urn:B> .
<urn:u> <urn:p> <urn:b> .
"""


def test_a_refused_property_with_one_triple_per_object_is_not_checked(monkeypatch):
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    monkeypatch.setattr(structural_strategy, "CENSUS_MIN_TRIPLES_PER_OBJECT", 2)
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        select, paged = miner.helper.select, miner.helper.select_with_fallback
        sent = []

        def limited(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            if purpose == "structural/coverage" and "<urn:c>" in query:
                raise EndpointTimeoutError("Query cost/time limit: Virtuoso ANYTIME timeout")
            return select(query, *args, purpose=purpose, **kwargs)

        def limited_pages(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            return paged(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        monkeypatch.setattr(miner.helper, "select_with_fallback", limited_pages)
        result = miner.mine("refused")
        (entry,) = miner.last_report.config["structural_coverage"]
        state = miner.last_report.completion_state
    counted = entry["census_properties"]["urn:c"]
    assert counted["triples"] == 4 and "per object" in counted["refused"]
    assert entry["unchecked_triples"] == 4 and entry["census_refused"] == ["urn:c"]
    assert entry["covered_triples"] + entry["uncovered_triples"] + 4 == entry["triple_count"]
    assert entry["state"] == "partial" and state == "partial", "Partial, not failed"
    assert not any("GROUP BY ?o" in q and "<urn:c>" in q for _, q in sent), "No object listing"
    assert {p.property_uri for p in result.structural_patterns} == {"urn:p"}, "urn:u is found"
