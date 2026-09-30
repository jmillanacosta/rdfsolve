"""Paths over several steps are tested on the data one step at a time, and only the paths that
some instance follows are kept (the owner decision of 2026-09-30). A path is extended only from a
matched shorter path, so no candidate is skipped that could match; the time budget of a dataset
is recorded with the lengths that were tested completely."""

from rdflib import RDF, SH, Graph

from rdfsolve import SchemaMiner
from rdfsolve.mining.navigation import find_tested_paths
from rdfsolve.schema_models import MinedSchema

E = "urn:route:"
DATA = f"""@prefix e: <{E}> .
e:a1 a e:A; e:p e:b1 . e:a2 a e:A; e:p e:b2 .
e:b1 a e:B . e:b2 a e:B; e:q e:c . e:c a e:C; e:s e:a1 .
e:orphan a e:B; e:r e:d . e:d a e:D ."""


def _mine(**options):
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine()
        schema.navigation = find_tested_paths(schema, miner.helper, **options)
    return schema


def _signature(route):
    return tuple((s.subject_class[len(E):], s.property_uri[len(E):], s.object_class[len(E):])
                 for s in route.steps)


def test_only_paths_that_instances_follow_are_kept():
    schema = _mine(max_hops=4, budget_s=600)
    nav = schema.navigation
    kept = {_signature(route): route for route in nav.paths}
    assert (("A", "p", "B"), ("B", "q", "C")) in kept
    assert (("B", "q", "C"), ("C", "s", "A")) in kept
    assert (("A", "p", "B"), ("B", "q", "C"), ("C", "s", "A")) in kept
    assert (("A", "p", "B"), ("B", "r", "D")) not in kept, "No instance follows this path"
    assert all(route.instance_support == "matched" for route in nav.paths)
    route = kept[("A", "p", "B"), ("B", "q", "C")]
    assert (route.matched_sources, route.source_count) == (1, 2)
    assert route.evidence == "instance_tested" and route.query
    assert nav.strategy == "tested" and nav.stop_reason is None
    assert nav.complete_lengths == [2, 3, 4]
    assert nav.matched_by_length[2] == 3 and nav.tested_by_length[2] == 4
    assert nav.walk_counts[2] >= 4, "The paths of the schema are still counted"
    assert MinedSchema.from_dict(schema.to_dict()).navigation == nav


def test_a_path_does_not_repeat_an_edge():
    nav = _mine(max_hops=5, budget_s=600).navigation
    for route in nav.paths:
        steps = [(s.subject_class, s.property_uri, s.object_class) for s in route.steps]
        assert len(steps) == len(set(steps))


def test_an_exhausted_budget_is_recorded():
    nav = _mine(max_hops=4, budget_s=0).navigation
    assert nav.paths == [] and nav.complete_lengths == []
    assert nav.stop_reason == "budget"


def test_the_shacl_holds_only_matched_paths():
    schema = _mine(max_hops=3, budget_s=600)
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    text = schema.to_shacl()
    assert "Schema-composed" not in text and "Candidate" not in text
    profiles = [s for s in shapes.subjects(RDF.type, SH.NodeShape) if "observed-route-" in str(s)]
    assert len(profiles) == len(schema.navigation.paths)
    assert all(bool(shapes.value(s, SH.deactivated)) for s in profiles)
    from rdflib.collection import Collection

    sequences = [Collection(shapes, p) for p in shapes.objects(None, SH.path) if (p, RDF.first, None) in shapes]
    assert sequences and all(f"{E}r" not in {str(i) for i in items} for items in sequences)


def test_the_pipeline_tests_paths_within_a_budget(monkeypatch):
    import sys

    import pytest

    from scripts.pipeline_stages import cli

    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--navigation-budget", "900"])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].navigation_budget == 900
    assert not hasattr(seen["config"], "navigation_probes")
