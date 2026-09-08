"""Reject unsafe queries and misleading comparisons before paid model trials."""

import json
from pathlib import Path
import runpy

import pytest
from rdflib import Graph

EXPERIMENT = runpy.run_path(str(Path(__file__).parents[1] / "notebooks/pydantic_ai/experiment.py"))


def test_experiment_rejects_remote_access_and_updates():
    validate = EXPERIMENT["validate_select"]
    validate('SELECT ?s WHERE { ?s ?p "SERVICE is just text" }')
    for query in (
        "INSERT DATA { <urn:s> <urn:p> <urn:o> }",
        "SELECT * FROM <https://example.org/data> WHERE { ?s ?p ?o }",
        "SELECT * WHERE { { SELECT * WHERE { SERVICE <https://example.org/> { ?s ?p ?o } } } }",
    ):
        with pytest.raises(Exception):
            validate(query)


def test_scores_preserve_rdf_types_and_duplicate_rows():
    graph = Graph().parse(Path(__file__).parent / "test_data/aopwikirdf_phenobarbital_excerpt.ttl")
    expected = json.loads(graph.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 2").serialize(format="json"))
    actual = json.loads(json.dumps(expected))
    actual["results"]["bindings"].append(actual["results"]["bindings"][0])
    score = EXPERIMENT["score"]
    result = score(expected, actual, ["s", "p", "o"], "rows")
    assert not result["exact"] and result["recall"] == 1
    actual["results"]["bindings"] = []
    assert score(expected, actual, ["s", "p", "o"], "rows")["recall"] == 0
    with pytest.raises(ValueError, match="columns"):
        score(expected, actual, ["wrong"], "rows")
    actual = json.loads(json.dumps(expected))
    actual["results"]["bindings"][0]["s"]["type"] = "literal"
    assert not score(expected, actual, ["s", "p", "o"], "rows")["exact"]
