"""A class listing that the endpoint refuses is paged. On a shared node, the class listing of
PubChem reached the 600 s limit of QLever, which cut the answer off at 22 MB; the helper reads
that as a cost limit, and mining pages the listing instead of stopping."""

from rdflib import Graph

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointTimeoutError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> ; <urn:q> "x" .
"""


def test_a_refused_class_listing_is_paged(monkeypatch):
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        select = miner.helper.select
        refused = []

        def limited(query, *args, purpose="", **kwargs):
            if purpose == "two-phase/classes" and "LIMIT" not in query.upper():
                refused.append(query)
                raise EndpointTimeoutError("JSON decode error (non-retriable): Invalid control character")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        result = miner.mine("listing")
    assert refused, "The unpaged listing was tried first"
    assert {p.subject_class for p in result.patterns} >= {"urn:A", "urn:B"}
