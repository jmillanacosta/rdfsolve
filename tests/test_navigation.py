"""Check route counting, bounds, and the gap between schema and instance joins."""

from rdflib import Graph, SH

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


def edge(subject, predicate, obj):
    return SchemaPattern(subject_class=subject, property_uri=predicate, object_class=obj, count=1)


def test_bounded_walks_keep_cycles_but_not_literal_continuations():
    patterns = [edge("urn:A", "urn:p", "urn:B"), edge("urn:B", "urn:q", "urn:C"),
                edge("urn:C", "urn:r", "urn:A"), edge("urn:B", "urn:label", "Literal")]
    schema = MinedSchema(about=AboutMetadata.build(), patterns=patterns + patterns[:1])
    result = schema.discover_paths(max_hops=3, max_paths_per_length=1)
    assert result.edge_count == 4
    assert result.walk_counts == {1: 4, 2: 4, 3: 4}
    assert [len(route.steps) for route in result.paths] == [2, 3]
    assert result.truncated_lengths == [2, 3]
    assert MinedSchema.from_dict(schema.to_dict()).navigation == result


def test_positive_edge_counts_do_not_prove_a_path(caplog):
    schema = MinedSchema(about=AboutMetadata.build(), patterns=[
        edge("urn:A", "urn:p", "urn:B"), edge("urn:B", "urn:q", "urn:C")
    ])
    routes = schema.discover_paths(max_hops=3)
    data = Graph().parse(data="""
        <urn:a> a <urn:A>; <urn:p> <urn:b1> .
        <urn:b1> a <urn:B> .
        <urn:b2> a <urn:B>; <urn:q> <urn:c> .
        <urn:c> a <urn:C> .
    """, format="turtle")
    assert not bool(data.query("ASK { ?s <urn:p>/<urn:q> ?o }"))
    assert routes.walk_counts == {1: 2, 2: 1, 3: 0}
    assert routes.paths[0].instance_support == "not_checked"
    graph = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert not list(graph.triples((None, SH.minCount, None)))
    assert {int(count) for count in graph.objects(None, SH.qualifiedMinCount)} == {0}
    namespace = {}
    exec(schema.to_pydantic(), namespace)
    assert namespace["RDF_NAVIGATION"]["urn:A"][0]["instance_support"] == "not_checked"
    assert namespace["RDF_NAVIGATION"]["urn:A"][0]["sparql_path"] == "(<urn:p>/<urn:q>)"
    schema.to_void_graph()
    assert "does not encode" in caplog.text
