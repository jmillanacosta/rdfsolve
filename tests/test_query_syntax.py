from rdflib import Dataset
from rdfsolve.mining.query_builders import (
    _build_batched_literal_count_query,
    _build_batched_literal_objects_query,
    _build_batched_typed_count_query,
)


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

    data.parse(data="""@prefix e: <urn:> .
        e:edges { e:c e:p e:b. e:a e:p _:x. }
        e:types { _:x a e:B. }
        e:context { e:b a e:B, e:C. }
        e:other { e:a e:p e:outside. e:outside a e:Wrong. e:b a e:Wrong. }
        """, format="trig")
    queries = {
        "all classes": _build_typed_object_query_plain(scope, ["urn:context"]),
        "class batch": _build_batched_typed_object_query(
            ["urn:A"], scope, type_context_graph_uris=["urn:context"]),
        "class property": _build_typed_object_for_class_property_query(
            "urn:A", "urn:p", scope, type_context_graph_uris=["urn:context"]),
    }
    for name, query in queries.items():
        rows = [row.asdict() for row in data.query(query)]
        assert len(rows) == 2, f"{name}: repeated objects or types changed the row count: {rows}"
        assert {str(row["oc"]) for row in rows} == {"urn:B", "urn:C"}, name
