"""Check bounded routes and descriptive exports using the saved AOPWiki schema."""

import json
from collections import Counter
from pathlib import Path

import pytest
from rdflib import RDF, SH, Graph

from rdfsolve.mining.navigation import discover_paths_with_fallback
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


def chain_schema(length: int) -> MinedSchema:
    """Build a schema whose classes chain exactly *length* edges deep."""
    return MinedSchema(
        about=AboutMetadata.build(dataset_name="chain"),
        patterns=[
            SchemaPattern(
                subject_class=f"urn:C{step}",
                property_uri=f"urn:p{step}",
                object_class=f"urn:C{step + 1}",
                count=1,
            )
            for step in range(length)
        ],
    )


def aop_schema():
    path = Path(__file__).parent / "test_data" / "aopwikirdf_schema.json"
    triples = json.loads(path.read_text())["triples"]
    return MinedSchema(
        about=AboutMetadata.build(dataset_name="aopwikirdf"),
        patterns=[
            SchemaPattern(subject_class=s, property_uri=p, object_class=o) for s, p, o in triples
        ],
    )


def test_aop_routes_cover_start_classes_and_account_for_omissions():
    schema = aop_schema()
    result = schema.discover_paths(max_hops=3, max_paths_per_length=100)
    starts = {p.subject_class for p in schema.patterns}
    eligible = {p.subject_class for p in schema.patterns if p.object_class in starts}
    selected = Counter(r.steps[0].subject_class for r in result.paths if len(r.steps) == 2)
    assert len(eligible) > 1
    assert set(selected) == eligible
    for hops in (2, 3):
        saved = sum(len(r.steps) == hops for r in result.paths)
        assert saved <= 100
        assert saved + sum(result.omitted_by_class[hops].values()) == result.walk_counts[hops]
    assert MinedSchema.from_dict(schema.to_dict()).navigation == result


def test_aop_shacl_has_no_generated_counts_or_duplicate_paths():
    schema = aop_schema()
    schema.discover_paths(max_hops=3)
    before = schema.to_dict()
    compact = Graph().parse(data=schema.to_shacl(trim_descriptions=20), format="turtle")
    assert all(len(str(text)) <= 20 for text in compact.objects(None, SH.description))
    assert schema.to_dict() == before
    graph = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert (
        MinedSchema.from_shacl(compact.serialize(format="turtle")).patterns
        == MinedSchema.from_shacl(graph.serialize(format="turtle")).patterns
    )
    for predicate in (SH.minCount, SH.maxCount, SH.qualifiedMinCount, SH.qualifiedMaxCount):
        assert not list(graph.objects(None, predicate))
    navigation = [n for n in graph.subjects(RDF.type, SH.NodeShape) if "navigation-" in str(n)]
    for node in navigation:
        paths = [
            tuple(graph.items(graph.value(p, SH.path))) for p in graph.objects(node, SH.property)
        ]
        assert len(paths) == len(set(paths))
    templates = [n for n in graph.subjects(RDF.type, SH.NodeShape) if n not in navigation]
    assert templates and all(bool(graph.value(n, SH.deactivated)) for n in templates)
    restored = MinedSchema.from_shacl(schema.to_shacl())
    assert all(s.deactivated for s in restored.shapes.node_shapes if "navigation-" not in s.uri)
    active = Graph().parse(data=schema.to_shacl(activate_observed=True), format="turtle")
    assert not list(active.objects(None, SH.deactivated))
    namespace = {}
    exec(schema.to_pydantic(), namespace)
    first = schema.navigation.paths[0]
    exported = namespace["RDF_NAVIGATION"][first.steps[0].subject_class][0]
    assert exported["bindings"]["focus"]["class_iri"] == first.steps[0].subject_class
    assert exported["triples"][-1]["object"] == "value"
    assert exported["bindings"]["step_1"]["class_iri"] == first.steps[0].object_class
    assert exported["execution"] == "not_checked"
    assert namespace["RDF_NAVIGATION_SUMMARY"]["omitted_by_class"]
    models = [
        value
        for value in namespace.values()
        if isinstance(value, type)
        and getattr(value, "rdf_class_iri", None) == first.steps[0].subject_class
    ]
    exported_schema = models[0].model_json_schema()
    view = (
        exported_schema["$defs"][exported_schema["$ref"].rsplit("/", 1)[-1]]
        if "$ref" in exported_schema
        else exported_schema
    )
    assert view["rdf_navigation"][0] == exported
    assert any("rdf_patterns" in field for field in view["properties"].values())
    json.dumps(exported_schema)


def test_path_probes_measure_joins_and_keep_zero_degree_sources():
    from rdfsolve import SchemaMiner
    from rdfsolve.client.api import Client

    graph = Graph().parse(
        data="""@prefix e: <urn:route:> .
        e:a1 a e:A; e:p e:b1 . e:a2 a e:A; e:p e:b2 .
        e:b1 a e:B . e:b2 a e:B; e:q e:c . e:c a e:C .
        e:orphan a e:B; e:r e:d . e:d a e:D .""",
        format="turtle",
    )
    with SchemaMiner.from_graph(graph) as miner:
        schema = miner.mine()
        nav = schema.discover_paths(
            max_hops=2, max_paths_per_length=10, helper=miner.helper, probe_limit=10
        )
    routes = {
        p.steps[-1].property_uri: p for p in nav.paths if p.steps[0].subject_class == "urn:route:A"
    }
    assert (routes["urn:route:q"].matched_sources, routes["urn:route:q"].source_count) == (1, 2)
    assert (routes["urn:route:q"].min_count, routes["urn:route:q"].max_count) == (0, 1)
    assert routes["urn:route:r"].instance_support == "no_match"
    restored = MinedSchema.from_dict(schema.to_dict())
    assert restored.navigation == nav
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    profiles = [s for s in shapes.subjects(RDF.type, SH.NodeShape) if "observed-route-" in str(s)]
    assert profiles and all(bool(shapes.value(s, SH.deactivated)) for s in profiles)
    assert list(shapes.objects(None, SH.qualifiedValueShape))
    roundtrip = MinedSchema.from_shacl(schema.to_shacl()).to_shacl()
    assert list(
        Graph().parse(data=roundtrip, format="turtle").objects(None, SH.qualifiedValueShape)
    )
    client = Client(schema, graph)
    table = client.navigation(observed_only=True)
    query = client.prepare_path(table.loc[table.Target == "urn:route:C"].iloc[0]["Reference"])
    assert client.select(query).row_count == 1


def test_failed_path_probe_does_not_become_negative_evidence():
    from unittest.mock import Mock

    schema = aop_schema()
    helper = Mock()
    helper.select_with_fallback.side_effect = TimeoutError("budget")
    routes = schema.discover_paths(
        max_hops=2, max_paths_per_length=2, helper=helper, probe_limit=1
    ).paths
    assert routes[0].instance_support == "timeout"
    assert routes[0].matched_sources is None and routes[1].instance_support == "not_checked"


def test_navigation_names_do_not_change_shape_identity():
    from rdflib import RDFS

    schema = aop_schema()
    route = schema.discover_paths(max_hops=2, max_paths_per_length=1).paths[0]
    route.instance_support, route.matched_sources, route.source_count = "matched", 1, 1
    first = Graph().parse(data=schema.to_shacl(), format="turtle")
    profile = next(s for s in first.subjects(RDF.type, SH.NodeShape) if "observed-route-" in str(s))
    route.steps[0].subject_label = "Start"
    route.steps[0].object_label = "Middle"
    route.steps[-1].object_label = "End"
    route.steps[0].property_label = "connects"
    second = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert str(second.value(profile, RDFS.label)) == "Start → End via Middle"
    child = second.value(profile, SH.property)
    assert str(second.value(child, SH.name)) == "connects"
    restored = MinedSchema.from_shacl(second.serialize(format="turtle"))
    assert (
        next(s for s in restored.shapes.node_shapes if s.uri == str(profile)).name == route.label()
    )


def test_hop_fallback_steps_down_to_the_deepest_reachable_length():
    result = discover_paths_with_fallback(chain_schema(3), max_hops=5, min_hops=3)
    assert result.max_hops == 3
    assert max(len(route.steps) for route in result.paths) == 3


def test_hop_fallback_keeps_the_requested_bound_when_routes_reach_it():
    result = discover_paths_with_fallback(chain_schema(6), max_hops=5, min_hops=3)
    assert result.max_hops == 5
    assert any(len(route.steps) == 5 for route in result.paths)


def test_hop_fallback_returns_the_lowest_attempt_when_nothing_chains():
    result = discover_paths_with_fallback(chain_schema(1), max_hops=5, min_hops=3)
    assert result.max_hops == 3
    assert result.paths == []
    assert result.walk_counts[1] == 1


@pytest.mark.parametrize("bounds", [(7, 3), (5, 1), (3, 5)])
def test_hop_fallback_rejects_an_impossible_range(bounds):
    max_hops, min_hops = bounds
    with pytest.raises(ValueError, match="2..6 hops"):
        discover_paths_with_fallback(chain_schema(3), max_hops=max_hops, min_hops=min_hops)
