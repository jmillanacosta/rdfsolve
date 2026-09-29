"""On QLever, the census reads the triples of each property and the triples of untyped subjects in
two grouped queries, with the precomputed patterns (ql:has-predicate): Bgee in about 4 min, not
about 10 h. When typed mining covered every class of the data, an uncovered edge is an edge of
an untyped subject, so the numbers and the structural patterns equal those of the exact census.
RDFLib stands in for QLever here: ?s ql:has-predicate ?p is read as ?s ?p ?_value."""

import re
from itertools import count

import pytest
from rdflib import Dataset

from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> ; <urn:q> "y" .
"""


def translate(query):
    """Write ql:has-predicate as a plain triple pattern for RDFLib."""
    numbers = count()
    query = query.replace(structural_strategy.QLEVER_PREFIX, "")
    return re.sub(r"(\?\w+) ql:has-predicate (<[^>]+>|\?\w+)", lambda m: f"{m[1]} {m[2]} ?_hp{next(numbers)}", query)


def mine(monkeypatch, data=DATA, *, engine="qlever", patterns=True):
    with monkeypatch.context() as patch, SchemaMiner.from_graph(
        Dataset().parse(data=data, format="turtle"), delay=0
    ) as miner:
        patch.setattr(structural_strategy, "LocalGraphHelper", type("Endpoint", (), {}))
        patch.setattr(miner.helper, "sparql_engine", engine)
        select, paged = miner.helper.select, miner.helper.select_with_fallback
        sent = []

        def answer(run):
            def call(query, *args, purpose="", **kwargs):
                sent.append(query)
                if "ql:" in query and not patterns:
                    raise EndpointError("HTTP 400: unknown predicate ql:has-predicate")
                return run(translate(query), *args, purpose=purpose, **kwargs)

            return call

        patch.setattr(miner.helper, "select", answer(select))
        patch.setattr(miner.helper, "select_with_fallback", answer(paged))
        result = miner.mine("patterns")
        (entry,) = miner.last_report.config["structural_coverage"]
    return entry, result, sent


def shapes(result):
    return sorted(
        (p.property_uri, tuple(p.subject_properties), tuple(p.object_properties), p.object_kind, p.count)
        for p in result.structural_patterns
    )


def test_the_pattern_census_equals_the_exact_census(monkeypatch):
    fast, fast_result, sent = mine(monkeypatch)
    exact, exact_result, _ = mine(monkeypatch, engine="")
    assert fast["census"] == "qlever_patterns" and exact["census"] == "per_property"
    for field in ("triple_count", "untyped_subject_triples", "covered_triples", "uncovered_triples"):
        assert fast[field] == exact[field], field
    assert fast["census_properties"] == exact["census_properties"]
    assert shapes(fast_result) == shapes(exact_result) and shapes(fast_result)
    assert not any("FILTER(!(" in q for q in sent), "No test of each edge"


@pytest.mark.parametrize(
    ("data", "patterns"),
    [(DATA, False), (DATA + "<urn:d> a [ ] ; <urn:p> <urn:b> .", True)],
    ids=["no patterns in the index", "a type value that is not an IRI"],
)
def test_the_exact_census_is_kept_when_the_patterns_cannot_decide(monkeypatch, data, patterns):
    entry, _, _ = mine(monkeypatch, data, patterns=patterns)
    assert entry["census"] == "per_property"
