"""rdfsolve.mining.structural_strategy on QLever: the census reads grouped triples with the
precomputed patterns, discovery answers one row per structural pattern with a witness edge, and the
counts of a class and property are read per object first."""

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
    return re.sub(
        r"(\?\w+) ql:has-predicate (<[^>]+>|\?\w+)",
        lambda m: f"{m[1]} {m[2]} ?_hp{next(numbers)}",
        query,
    )


def mine(monkeypatch, data=DATA, *, engine="qlever", patterns=True):
    with (
        monkeypatch.context() as patch,
        SchemaMiner.from_graph(Dataset().parse(data=data, format="turtle"), delay=0) as miner,
    ):
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
        (
            p.property_uri,
            tuple(p.subject_properties),
            tuple(p.object_properties),
            p.object_kind,
            p.count,
        )
        for p in result.structural_patterns
    )


def test_the_pattern_census_equals_the_exact_census(monkeypatch):
    fast, fast_result, sent = mine(monkeypatch)
    exact, exact_result, _ = mine(monkeypatch, engine="")
    assert fast["census"] == "qlever_patterns" and exact["census"] == "per_property"
    for field in (
        "triple_count",
        "untyped_subject_triples",
        "covered_triples",
        "uncovered_triples",
    ):
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


MORE = (
    DATA
    + """
<urn:d> <urn:p> <urn:b> ; <urn:q> "z" .
<urn:e> <urn:p> <urn:c> .
"""
)


def test_discovery_answers_one_row_for_each_pattern(monkeypatch):
    _, exact, _ = mine(monkeypatch, MORE, patterns=False)
    entry, result, sent = mine(monkeypatch, MORE)
    assert entry["census"] == "qlever_patterns" and shapes(result) == shapes(exact)
    discovery = [q for q in sent if "ql:has-predicate ?sp" in q]
    assert discovery and all(
        "COUNT(DISTINCT ?s)" in q and "GROUP BY ?p ?ss" in q for q in discovery
    )
    witness = [q for q in sent if q.rstrip().endswith("LIMIT 1") and "ql:has-predicate" not in q]
    assert not witness, "No witness queries"
    exact_examples = {
        (p.property_uri, tuple(p.subject_properties)): p for p in exact.structural_patterns
    }
    for pattern in result.structural_patterns:
        (example,) = pattern.examples
        assert {"s", "o"} <= set(example), "A witness edge of the pattern"
        other = exact_examples[pattern.property_uri, tuple(pattern.subject_properties)]
        assert example["o"]["type"] == other.examples[0]["o"]["type"]


ALL = """
<urn:a1> a <urn:A> ; <urn:p> <urn:o1>, <urn:o2> .  <urn:a2> a <urn:A> ; <urn:p> <urn:o1> .
<urn:o1> a <urn:T1> .  <urn:o2> a <urn:T1> .
"""
PART = ALL + '<urn:o1> a <urn:T2> . <urn:a1> <urn:q> 1, "x" . <urn:a2> <urn:q> "y" .\n'


def counts(monkeypatch, data):
    with SchemaMiner.from_graph(
        Dataset().parse(data=data, format="turtle"), delay=0, class_batch_size=1
    ) as miner:
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
        "A group with part of the edges counts its own distinct subjects"
    )
