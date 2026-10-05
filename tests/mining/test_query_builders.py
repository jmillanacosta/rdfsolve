"""rdfsolve.mining.query_builders: the queries of mining are valid SPARQL, subject counts are taken
from total counts where these suffice, and the census of a local graph is counted."""

from rdflib import Dataset, Graph

from rdfsolve import SchemaMiner
from rdfsolve._outcomes import QueryOutcome
from rdfsolve.evidence.observed import build_property_usage_query
from rdfsolve.mining import property_queries
from rdfsolve.mining.query_builders import (
    _build_batched_literal_count_query,
    _build_batched_literal_objects_query,
    _build_batched_typed_count_query,
    _build_class_weight_query,
    _build_properties_for_class_query,
)

C, P = "urn:ex:Gene", "urn:ex:expressedIn"


def test_property_usage_takes_distinct_subjects_from_the_class_and_property(monkeypatch):
    answers = iter(
        [
            QueryOutcome([{"class": {"value": C}, "p": {"value": P}, "triples": {"value": "7"}}]),
            QueryOutcome([{"cnt": {"value": "7"}, "subjects": {"value": "3"}}]),
        ]
    )
    monkeypatch.setattr(
        "rdfsolve.mining.query_fallbacks.select_outcome", lambda *args, **kwargs: next(answers)
    )
    outcome = property_queries._subjects_from_total(
        C, P, None, None, build_property_usage_query, "property-usage", helper=None
    )
    assert outcome is not None and outcome.state == "complete"
    assert outcome.rows == [
        {
            "class": {"value": C},
            "p": {"value": P},
            "triples": {"value": "7"},
            "subjects": {"value": "3"},
        }
    ]


def test_distinct_subjects_of_object_groups_are_counted_within_a_budget(monkeypatch):
    """Each object group that holds part of the edges needs its own query; beyond the budget,
    the largest groups have their distinct subjects and the others keep edges and objects."""
    group = [("urn:ex:Y", "3"), ("urn:ex:X", "5"), ("urn:ex:Z", "1")]
    answers = iter(
        [
            QueryOutcome(
                [
                    {
                        "class": {"value": C},
                        "p": {"value": P},
                        "oc": {"type": "uri", "value": oc},
                        "cnt": {"value": n},
                    }
                    for oc, n in group
                ]
            ),
            QueryOutcome([{"cnt": {"value": "9"}, "subjects": {"value": "4"}}]),
            QueryOutcome([{"cnt": {"value": "5"}, "subjects": {"value": "2"}}]),
        ]
    )
    sent = []
    monkeypatch.setattr(
        "rdfsolve.mining.query_fallbacks.select_outcome",
        lambda query, purpose, *args, **kwargs: sent.append(purpose) or next(answers),
    )
    monkeypatch.setattr(property_queries, "GROUP_QUERIES", 1)
    outcome = property_queries._subjects_from_total(
        C, P, None, None, _build_batched_typed_count_query, "counts", helper=None
    )
    assert sent == ["counts/per-object", "counts/total", "counts/group"], "One group query"
    subjects = {row["oc"]["value"]: row.get("subjects", {}).get("value") for row in outcome.rows}
    assert subjects == {"urn:ex:X": "2", "urn:ex:Y": None, "urn:ex:Z": None}, "The largest first"
    assert outcome.state == "partial"
    (failure,) = outcome.failures
    assert failure.category == "budget" and "2 of 3" in failure.message


def test_queries_count_edges_in_the_selected_graph():
    data = Dataset()
    data.parse(
        data='@prefix e: <urn:> .\n        e:edges { e:a e:p e:b; e:text "x", "y". e:c e:text "x". }\n        e:types { e:a a e:A. e:c a e:A. e:b a e:B. }\n        e:other { e:a e:text "excluded". }',
        format="trig",
    )
    scope = ["urn:edges", "urn:types"]
    queries = {
        "typed edges": (_build_batched_typed_count_query, {"cnt": 1, "subjects": 1, "objects": 1}),
        "literal edges": (_build_batched_literal_count_query, {"cnt": 3, "subjects": 2}),
        "literal values": (_build_batched_literal_objects_query, {"objects": 2}),
    }
    for name, (build, counts) in queries.items():
        query = build(["urn:A"], scope, paginated=True).format(limit=10, offset=0)
        rows = [
            {str(key): str(value) for key, value in row.asdict().items()}
            for row in data.query(query)
        ]
        assert len(rows) == 1, f"{name}: {rows}"
        assert {key: rows[0].get(key) for key in ("_g", *counts)} == {
            "_g": "urn:edges",
            **{key: str(value) for key, value in counts.items()},
        }, name

    from rdfsolve.mining.query_builders import (
        _build_batched_typed_object_query,
        _build_typed_object_for_class_property_query,
        _build_typed_object_query_plain,
    )

    data.parse(
        data="""@prefix e: <urn:> .
        e:edges { e:c e:p e:b. e:a e:p _:x. }
        e:types { _:x a e:B. }
        e:context { e:b a e:B, e:C. }
        e:other { e:a e:p e:outside. e:outside a e:Wrong. e:b a e:Wrong. }
        """,
        format="trig",
    )
    queries = {
        "all classes": _build_typed_object_query_plain(scope, ["urn:context"]),
        "class batch": _build_batched_typed_object_query(
            ["urn:A"], scope, type_context_graph_uris=["urn:context"]
        ),
        "class property": _build_typed_object_for_class_property_query(
            "urn:A", "urn:p", scope, type_context_graph_uris=["urn:context"]
        ),
    }
    for name, query in queries.items():
        rows = [row.asdict() for row in data.query(query)]
        assert len(rows) == 2, f"{name}: repeated objects or types changed the row count: {rows}"
        assert {str(row["oc"]) for row in rows} == {"urn:B", "urn:C"}, name

    from rdfsolve.mining.property_queries import PROPERTY_BUILDERS

    def answer(query):
        rows = [sorted((str(k), str(v)) for k, v in r.asdict().items()) for r in data.query(query)]
        return sorted(r for r in rows if dict(r).get("class"))  # empty groups are not rows

    found = 0
    for build in PROPERTY_BUILDERS:
        for prop in ("urn:p", "urn:text"):
            context = {"type_context_graph_uris": ["urn:context"]}
            bound = build(["urn:A"], scope, property_uri=prop, **context)
            assert "VALUES ?class" not in bound and "VALUES ?p" not in bound, (
                "Engines must see constants"
            )
            batch = answer(build(["urn:A", "urn:Z"], scope, **context))
            expected = [r for r in batch if ("class", "urn:A") in r and ("p", prop) in r]
            assert answer(bound) == expected, (build.__name__, prop)
            found += len(expected)
    assert found, "The comparison must cover returned rows"


def test_census_counts_data_subjects_in_each_scope():
    plain = Graph().parse(
        data="""
        <urn:a> a <urn:A>, <urn:B>; <urn:p> "one", "two" .
        <urn:b> a <urn:A>; <urn:p> "three" .
    """,
        format="turtle",
    )
    scoped = Dataset().parse(
        data="""
        <urn:data> { <urn:a> <urn:p> "one", "two" . <urn:b> <urn:p> "three" . }
        <urn:types> { <urn:a> a <urn:A>, <urn:B> . <urn:b> a <urn:A> .
                      <urn:outside> a <urn:A> . }
    """,
        format="trig",
    )
    for data, graphs, context in ((plain, None, None), (scoped, ["urn:data"], ["urn:types"])):
        with SchemaMiner.from_graph(
            data, graph_uris=graphs, type_context_graph_uris=context, delay=0
        ) as miner:
            query = _build_class_weight_query(graphs, context).format(offset=0, limit=100)
            rows = miner.helper.select(query)["results"]["bindings"]
            assert {r["class"]["value"]: int(r["n"]["value"]) for r in rows} == {
                "urn:A": 2,
                "urn:B": 1,
            }, "Counts exclude companion-only subjects and retain multiple types"

            properties = miner.helper.select(
                _build_properties_for_class_query("urn:A", graphs, type_context_graph_uris=context)
            )["results"]["bindings"]
            expected = (
                {"urn:p"}
                if context
                else {"urn:p", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}
            )
            assert {r["p"]["value"] for r in properties} == expected
            assert len(properties) == len(expected), "One row per observed scoped property"
