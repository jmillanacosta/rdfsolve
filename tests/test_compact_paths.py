"""Tested paths are written compactly: the steps once, in a table of edges, and each path as the
numbers of its edges with its two counts. The query of a path is not written; it is made again
from the steps. (AOP-Wiki, 338,317 triples: 23,924 paths gave a schema file of 101 MB with the
full steps and the query of each path, 2026-09-30.)"""

import json

from rdflib import Graph

from rdfsolve import SchemaMiner
from rdfsolve.mining.navigation import find_tested_paths, support_query
from rdfsolve.schema_models import MinedSchema
from tests.test_tested_paths import DATA, E


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
        {"steps": [edges[i] for i in row["edges"]], "evidence": "instance_tested",
         "instance_support": "matched", "matched_sources": row["matched"],
         "source_count": row["sources"], "observed_at": row["at"]}
        for row in rows
    ]
    assert MinedSchema.from_dict(document).navigation == schema.navigation
