"""rdfsolve.mining.navigation tested paths: paths over several steps are tested on the data one step
at a time, only the paths some instance follows are kept, and they are written compactly."""

import json

from rdflib import RDF, SH, Graph

from rdfsolve import SchemaMiner
from rdfsolve.mining.navigation import find_tested_paths, support_query
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
    return tuple(
        (s.subject_class[len(E) :], s.property_uri[len(E) :], s.object_class[len(E) :])
        for s in route.steps
    )


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
    assert route.evidence == "instance_tested"
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
    from rdflib.collection import Collection

    schema = _mine(max_hops=3, budget_s=600)
    text = schema.to_shacl(paths="only")
    shapes = Graph().parse(data=text, format="turtle")
    assert "Schema-composed" not in text and "Candidate" not in text
    assert "1 of 2 start instances" in text, "Each path with its counts"
    sequences = [
        Collection(shapes, p)
        for p in shapes.objects(None, SH.path)
        if (p, RDF.first, None) in shapes
    ]
    assert sequences and all(f"{E}r" not in {str(i) for i in items} for items in sequences)
    assert not [s for s in shapes.subjects(RDF.type, SH.NodeShape) if "observed-route-" in str(s)]
    classes_only = Graph().parse(data=schema.to_shacl(paths="without"), format="turtle")
    assert not [
        p for p in classes_only.objects(None, SH.path) if (p, RDF.first, None) in classes_only
    ]
    assert len(set(classes_only.subjects(RDF.type, SH.NodeShape))) == 3, (
        "One shape for each class with properties"
    )


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


def test_each_query_has_the_rest_of_the_budget_and_no_pages():
    """A query that does not answer within the budget fails at once: it is not retried, nor
    recovered in pages, which could take far longer than the budget."""
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine()
        helper = miner.helper
        seen = []
        select = helper.select_with_fallback

        def watched(query, purpose=""):
            seen.append((helper.timeout, helper.max_retries, helper.page_recovery))
            return select(query, purpose=purpose)

        helper.select_with_fallback = watched
        settings = (helper.timeout, helper.max_retries, helper.page_recovery)
        find_tested_paths(schema, helper, max_hops=3, budget_s=600)
    assert seen and all(t <= 600 and tries == 1 and not pages for t, tries, pages in seen)
    assert (helper.timeout, helper.max_retries, helper.page_recovery) == settings


def _schema():
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine()
        schema.navigation = find_tested_paths(schema, miner.helper, max_hops=4, budget_s=600)
    return schema


def test_paths_are_written_as_edge_numbers():
    schema = _schema()
    written = schema.to_dict()["schema"]["navigation"]
    paths = written["paths"]
    assert set(paths) == {"format", "edges", "rows", "graph_uris", "type_context_graph_uris"}
    assert paths["format"] == "edges-1"
    assert all(set(row) == {"edges", "matched", "sources", "at"} for row in paths["rows"])
    assert len(paths["rows"]) == len(schema.navigation.paths)
    assert len(paths["edges"]) <= len(schema.patterns), "Each step is written once"
    assert "SELECT" not in json.dumps(paths), "No query text"
    restored = MinedSchema.from_dict(json.loads(json.dumps(schema.to_dict())))
    assert restored.navigation == schema.navigation


def test_the_query_of_a_path_is_made_again():
    nav = _schema().navigation
    route = next(r for r in nav.paths if len(r.steps) == 2 and r.steps[0].subject_class == E + "A")
    assert route.query is None
    query = support_query(route, nav)
    assert query.startswith("SELECT (COUNT(*) AS ?sources)") and f"<{E}q>" in query


def test_the_earlier_list_form_is_still_read():
    schema = _schema()
    document = schema.to_dict()
    nav = document["schema"]["navigation"]
    rows, edges = nav["paths"]["rows"], nav["paths"]["edges"]
    nav["paths"] = [
        {
            "steps": [edges[i] for i in row["edges"]],
            "evidence": "instance_tested",
            "instance_support": "matched",
            "matched_sources": row["matched"],
            "source_count": row["sources"],
            "observed_at": row["at"],
        }
        for row in rows
    ]
    assert MinedSchema.from_dict(document).navigation == schema.navigation
