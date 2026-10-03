"""rdfsolve.mining.property_queries: the properties of a class, listed and counted with bounded
queries, also for groups of ontology terms; a refused class listing is paged, and refused exact
counts fall back to type triples to plan the batches."""

import re
from itertools import count

from rdflib import Dataset, Graph

from rdfsolve.mining import query_fallbacks
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.query_builders import Representative
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError

PROPERTY_LISTING_DATA = """
<urn:s1> a <urn:T1> ; <urn:p1> "a" .
<urn:s2> a <urn:T2> ; <urn:p2> "b" .
<urn:s3> a <urn:Group> ; <urn:p3> "c" .
"""
GROUP = Representative("urn:Group", ["urn:Group", "urn:T1", "urn:T2"])


def listing(monkeypatch, engine):
    with SchemaMiner.from_graph(
        Graph().parse(data=PROPERTY_LISTING_DATA, format="turtle"), delay=0
    ) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", engine)
        select, sent = miner.helper.select, []

        def translate(query, *args, **kwargs):
            sent.append(query)
            numbers = count()
            query = query.replace(
                "PREFIX ql: <http://qlever.cs.uni-freiburg.de/builtin-functions/>\n", ""
            )
            query = re.sub(
                r"(\?\w+) ql:has-predicate (\?\w+)",
                lambda m: f"{m[1]} {m[2]} ?_hp{next(numbers)}",
                query,
            )
            return select(query, *args, **kwargs)

        monkeypatch.setattr(miner.helper, "select", translate)
        found = query_fallbacks.enumerate_properties_for_class(GROUP, None, "test", miner.helper)
    return (
        {r["p"]["value"] for r in found.rows} - {"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"},
        found.state,
        sent,
    )


def test_the_properties_of_every_member_are_listed(monkeypatch):
    for engine in ("virtuoso", "qlever"):
        found, state, sent = listing(monkeypatch, engine)
        assert found == {"urn:p1", "urn:p2", "urn:p3"} and state == "complete", engine
        assert any("ql:has-predicate" in q for q in sent) == (engine == "qlever")


def test_property_queries_preserve_typed_literal_and_untyped_counts():
    graph = Graph().parse(
        data="""
        @prefix e: <urn:e:> .
        e:a a e:A; e:link e:b; e:text "one", "two"; e:ref e:untyped .
        e:c a e:A; e:link e:b; e:text "one" .
        e:b a e:B; e:text "target" .
    """,
        format="turtle",
    )
    with SchemaMiner.from_graph(graph, class_batch_size=1, delay=0) as miner:
        miner.helper.sparql_engine = "qlever"
        select, sent = miner.helper.select, []
        miner.helper.select = lambda query, **kw: (
            sent.append(kw.get("purpose")) or select(query, **kw)
        )
        schema = miner.mine("mixed-fields")
        assert miner.last_report.completion_state == "complete"
    rows = {(p.subject_class, p.property_uri, p.object_class): p for p in schema.patterns}
    link = rows["urn:e:A", "urn:e:link", "urn:e:B"]
    text = rows["urn:e:A", "urn:e:text", "Literal"]
    ref = rows["urn:e:A", "urn:e:ref", "Resource"]
    assert (link.count, link.distinct_subjects, link.distinct_objects) == (2, 2, 1)
    assert (text.count, text.distinct_subjects, text.distinct_objects) == (3, 2, 2)
    assert ref.count == 1
    assert not schema.structural_patterns, "Typed discovery covers every subject edge"
    for purpose in ("two-phase/literal", "counts/literal", "two-phase/blank-node"):
        assert f"{purpose}/property/urn:e:link" not in sent, (
            "IRI-only fields need no literal/blank scans"
        )
    for purpose in ("two-phase/typed-object", "two-phase/untyped-uri", "counts/typed-object"):
        assert f"{purpose}/property/urn:e:text" not in sent, (
            "Literal-only fields need no object scans"
        )
    assert (
        "two-phase/literal/property/urn:e:text" in sent
        and "two-phase/typed-object/property/urn:e:link" in sent
    )

    from rdfsolve.sparql_helper import EndpointTimeoutError

    with SchemaMiner.from_graph(graph, class_batch_size=1, delay=0) as miner:
        miner.helper.sparql_engine, select = "qlever", miner.helper.select

        def slow_subjects(query, **kw):
            if "?subjects" in query and kw.get("purpose", "").startswith("counts/"):
                raise EndpointTimeoutError("Operation timed out")  # distinct subjects too costly
            return select(query, **kw)

        miner.helper.select = slow_subjects
        limited = {
            (p.subject_class, p.property_uri, p.object_class): p for p in miner.mine("x").patterns
        }
        assert miner.last_report.completion_state == "partial", "Unmeasured subjects stay visible"
    link, text = (
        limited["urn:e:A", "urn:e:link", "urn:e:B"],
        limited["urn:e:A", "urn:e:text", "Literal"],
    )
    assert (link.count, link.distinct_subjects, link.distinct_objects) == (2, None, 1)
    assert (text.count, text.distinct_subjects) == (3, None), "Keep triples when subjects time out"


CLASS_LISTING_FALLBACK_DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> .
<urn:b> a <urn:B> ; <urn:q> "x" .
"""


def test_a_refused_class_listing_is_paged(monkeypatch):
    with SchemaMiner.from_graph(
        Graph().parse(data=CLASS_LISTING_FALLBACK_DATA, format="turtle"), delay=0
    ) as miner:
        select = miner.helper.select
        refused = []

        def limited(query, *args, purpose="", **kwargs):
            if purpose == "two-phase/classes" and "LIMIT" not in query.upper():
                refused.append(query)
                raise EndpointTimeoutError(
                    "JSON decode error (non-retriable): Invalid control character"
                )
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", limited)
        result = miner.mine("listing")
    assert refused, "The unpaged listing was tried first"
    assert {p.subject_class for p in result.patterns} >= {"urn:A", "urn:B"}


DATA = """
<urn:data> { <urn:a> a <urn:A> ; <urn:p> <urn:b> . <urn:b> a <urn:B> ; <urn:q> "x" .
             <urn:c> a <urn:A>, <urn:C> ; <urn:p> <urn:b> . }
<urn:more> { <urn:a> a <urn:A> ; <urn:q> "y" . }
"""


def mine(monkeypatch, refuse):
    with SchemaMiner.from_graph(
        Dataset().parse(data=DATA, format="trig"), delay=0, graph_uris=["urn:data", "urn:more"]
    ) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        sent = []

        def answer(run):
            def call(query, *args, purpose="", **kwargs):
                if purpose == "two-phase/class-weights":
                    sent.append(query)
                    if refuse and "DISTINCT" in query:
                        raise EndpointError(
                            "Tried to allocate 68.8 GB, but only 30.7 GB were available"
                        )
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
    rows = sorted(
        (p.subject_class, p.property_uri, p.object_class, p.count) for p in bound.patterns
    )
    assert rows == sorted(
        (p.subject_class, p.property_uri, p.object_class, p.count) for p in exact.patterns
    )
