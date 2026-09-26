"""The server checks tool arguments and gives each failure as a JSON error."""

from rdfsolve.mcp.server import CONTRACTS, dispatch


def test_contracts_are_the_five_tools():
    assert list(CONTRACTS) == ["schema", "find", "paths", "run", "answer"]
    assert all(model.__doc__ for model in CONTRACTS.values())


def test_failures_are_json_errors_with_repair_facts(toolbox):
    assert dispatch(toolbox, "rdf_find", {})["error"]["code"] == "unknown_tool"
    extra = dispatch(toolbox, "find", {"text": "x", "kind": "y"})["error"]
    assert extra["code"] == "invalid_arguments" and extra["details"][0]["type"] == "extra_forbidden"
    syntax = dispatch(toolbox, "run", {"sparql": "SELECT ?s WHERE {\n ?s ?p }"})["error"]
    assert (syntax["code"], syntax["line"]) == ("sparql_syntax", 2)
    outputs = dispatch(toolbox, "answer", {"sparql": "SELECT ?aop WHERE { ?aop a ex:Pathway }"})
    assert outputs["error"]["code"] == "missing_outputs"
    wrong = dispatch(toolbox, "paths", {"source": "x", "target": "ex:Gene"})["error"]
    assert wrong["code"] == "invalid_request" and "does not name one class" in wrong["message"]
    assert toolbox.final is None


def test_results_are_json_values(toolbox):
    rows = dispatch(toolbox, "run", {"sparql": "SELECT ?s WHERE { ?s a ex:Gene }", "limit": 5})
    assert rows["rows"] == [["ex:tpo"]] and "error" not in rows
    text = dispatch(toolbox, "schema", {"classes": ["ex:Gene"]})["text"]
    assert text.startswith("ex:Gene, instances: 1")
