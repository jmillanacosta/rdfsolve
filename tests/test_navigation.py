"""Check bounded routes and descriptive exports using the saved AOPWiki schema."""

import json
from collections import Counter
from pathlib import Path

from rdflib import Graph, RDF, SH

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


def aop_schema():
    path = Path(__file__).parent / "test_data" / "aopwikirdf_schema.json"
    triples = json.loads(path.read_text())["triples"]
    return MinedSchema(about=AboutMetadata.build(dataset_name="aopwikirdf"), patterns=[
        SchemaPattern(subject_class=s, property_uri=p, object_class=o) for s, p, o in triples
    ])


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
    graph = Graph().parse(data=schema.to_shacl(), format="turtle")
    for predicate in (SH.minCount, SH.maxCount, SH.qualifiedMinCount, SH.qualifiedMaxCount):
        assert not list(graph.objects(None, predicate))
    navigation = [n for n in graph.subjects(RDF.type, SH.NodeShape) if "navigation-" in str(n)]
    for node in navigation:
        paths = [tuple(graph.items(graph.value(p, SH.path))) for p in graph.objects(node, SH.property)]
        assert len(paths) == len(set(paths))
    templates = [n for n in graph.subjects(RDF.type, SH.NodeShape) if n not in navigation]
    assert templates and all(bool(graph.value(n, SH.deactivated)) for n in templates)
    restored = MinedSchema.from_shacl(schema.to_shacl())
    assert all(s.deactivated for s in restored.shapes.node_shapes if "navigation-" not in s.uri)
    active = Graph().parse(data=schema.to_shacl(activate_observed=True), format="turtle")
    assert not list(active.objects(None, SH.deactivated))
