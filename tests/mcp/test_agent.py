"""Real SDK and subprocess checks use a deterministic local model."""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
from conftest import E, snapshot_endpoint
from mcp import Client as MCPClient
from mcp import MCPError, StdioServerParameters
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel

from rdfsolve.api import ask_rdf
from rdfsolve.mcp.server import CONTRACTS
from rdfsolve.mcp.workflow import launch_config


def test_final_receipt_preserves_distinct_semantic_warnings():
    from rdfsolve.mcp.agent import receipt_text

    text = receipt_text(
        {
            "state": "complete",
            "rows": 10,
            "warnings": [
                "Species mapping is uncertain.",
                "Species mapping is uncertain.",
                "Measurement methods were mapped to gene extraction.",
                "Mixed RDF values.",
            ],
        }
    )
    assert text.count("Species mapping") == 1
    assert "gene extraction" in text and "Mixed RDF values" in text
    from rdfsolve.mcp.workflow import Answer

    answer = Answer(calls=[{"result": {"state": "complete", "warnings": ["Review mapping"]}}])
    assert answer.diagnostics()["warnings"] == ["Review mapping"]


def files(session, folder):
    schema, data = folder / "schema.json", folder / "data.ttl"
    schema.write_text(json.dumps(session.client._schema.to_dict()))
    session.client.source.serialize(destination=data, format="turtle")
    return schema, data


def test_generic_api_and_real_stdio_preserve_data_boundary(session, tmp_path):
    schema, data = files(session, tmp_path)
    seen = []

    def model(messages, info):
        encoded = json.dumps([str(m) for m in messages])
        assert "Assay A" not in encoded and "Assay B" not in encoded
        seen.append(messages)
        step = len(seen)
        if step == 1:
            name, args = (
                "rdf_schema",
                {
                    "goals": [
                        {
                            "clause": "Return pathway identities",
                            "kind": "output",
                            "concept": "Adverse Outcome Pathway",
                            "owner": "a",
                        }
                    ],
                    "concepts": ["Adverse Outcome Pathway"],
                },
            )
        elif step == 2:
            name, args = (
                "rdf_prepare",
                {
                    "patterns": [{"reference": session.catalogue.type_refs[str(E.AOP)], "bindings": ["a"]}],
                    "outputs": ["a"],
                    "grounding": {
                        "g1": {
                            "project": ["a"],
                            "evidence": [session.catalogue.type_refs[str(E.AOP)]],
                        }
                    },
                },
            )
        elif step == 3:
            results = [
                p.content
                for m in messages
                for p in m.parts
                if getattr(p, "tool_name", "") == "rdf_prepare" and hasattr(p, "content")
            ]
            name, args = "rdf_finish", {"query_ref": results[-1]["query_ref"]}
        else:
            raise AssertionError("Unexpected model request after final execution")
        return ModelResponse(parts=[ToolCallPart(name, args, tool_call_id=str(step))])

    answer = asyncio.run(
        ask_rdf(
            "List pathways",
            schema=schema,
            data_file=data,
            source_id="independent-local-database",
            output_variables=["a"],
            model=FunctionModel(model),
            output_dir=tmp_path,
        )
    )
    assert answer.state == "complete", answer.error
    assert {r["a"]["value"] for r in answer.bindings} == {
        str(E.humanAOP),
        str(E.mouseAOP),
        str(E.noChemicalAOP),
    }
    assert len(seen) == 3 and answer.usage.requests == 3
    assert "3 rows" in answer.text
    assert answer.package["source_queries"] == 1
    assert len(list(tmp_path.glob("*.answer.json"))) == 1
    assert "aopwikirdf" not in json.dumps(launch_config(schema, source_id="other"))


def test_real_transport_reports_errors_and_refuses_dataset_resources(session, tmp_path):
    schema, data = files(session, tmp_path)

    async def check():
        config = launch_config(schema, data_file=data)
        async with MCPClient(StdioServerParameters(**config), read_timeout_seconds=30) as server:
            assert {tool.name for tool in (await server.list_tools()).tools} == set(CONTRACTS)
            error = await server.call_tool("rdf_probe", {"query_ref": "missing", "limit": 0})
            assert (
                error.is_error and error.structured_content["error"]["code"] == "invalid_arguments"
            )
            with pytest.raises(MCPError):
                await server.read_resource("rdfsolve://artifacts/results")

    asyncio.run(check())


def test_model_prose_cannot_claim_completed_execution(session, tmp_path):
    from pydantic_ai.messages import TextPart

    schema, data = files(session, tmp_path)
    model = FunctionModel(lambda messages, info: ModelResponse(parts=[TextPart("Ready.")]))
    answer = asyncio.run(ask_rdf("List pathways", schema=schema, data_file=data, model=model))
    assert answer.state == "blocked" and not answer.bindings
    assert answer.text.startswith("No final query was executed.")


def test_subprocess_paths_use_this_checkout_from_any_directory(tmp_path, monkeypatch):
    schema = tmp_path / "other.schema.json"
    schema.write_text("{}")
    monkeypatch.chdir(tmp_path)
    config = launch_config(
        schema, endpoint="https://example.invalid/sparql", graph_uris=["urn:scope"]
    )
    assert config["command"] == sys.executable
    import subprocess

    import rdfsolve.mcp.workflow as installed

    child = subprocess.check_output(
        [config["command"], "-c", "import rdfsolve.mcp.workflow as m; print(m.__file__)"],
        env=config["env"],
        text=True,
    )
    assert Path(child.strip()).resolve() == Path(installed.__file__).resolve()
    assert str(schema) in config["args"] and '["urn:scope"]' in config["args"]


def test_working_context_retains_goals_and_complete_recent_tool_pairs():
    from pydantic_ai.messages import ModelRequest, ToolReturnPart, UserPromptPart

    from rdfsolve.mcp.agent import Bridge

    bridge = Bridge(None)
    messages = [ModelRequest(parts=[UserPromptPart("Original question")])]
    for i in range(30):
        value = {
            "requirements": {"g1": {"clause": "Return events"}},
            "items": [{"ref": "f1", "kind": "field", "label": "event path"}],
        }
        bridge.calls.append({"name": "rdf_schema", "result": value})
        messages.extend(
            [
                ModelResponse(
                    parts=[ToolCallPart("rdf_schema", {"concepts": [str(i)]}, tool_call_id=str(i))]
                ),
                ModelRequest(parts=[ToolReturnPart("rdf_schema", value, tool_call_id=str(i))]),
            ]
        )
    compact = bridge.context(messages)
    assert len(compact) == 14 and "Original question" in str(compact[0])
    assert "Return events" in str(compact[1]) and "event path" in str(compact[1])
    calls = {p.tool_call_id for m in compact for p in m.parts if isinstance(p, ToolCallPart)}
    returns = {p.tool_call_id for m in compact for p in m.parts if isinstance(p, ToolReturnPart)}
    assert calls == returns == {str(i) for i in range(24, 30)}
    messages += [
        ModelResponse(parts=[ToolCallPart("rdf_schema", {}, tool_call_id="30")]),
        ModelRequest(parts=[ToolReturnPart("rdf_schema", {}, tool_call_id="30")]),
    ]
    next_context = bridge.context(messages)
    assert next_context[:2] == compact[:2] and len(next_context) == len(compact) + 2


def test_repeated_blocker_stops_even_when_query_text_changes():
    from types import SimpleNamespace

    from rdfsolve.mcp.agent import Bridge, NoProgressError

    class Server:
        async def call_tool(self, name, arguments):
            return SimpleNamespace(
                structured_content={
                    "error": {"code": "goal_owner", "message": "Select the field subject"},
                    "trace": {"step": arguments["attempt"]},
                }
            )

    async def run():
        bridge = Bridge(Server())
        for attempt in range(2):
            await bridge.call(
                "rdf_prepare", {"sparql": "SELECT " + " " * attempt, "attempt": attempt}
            )
        with pytest.raises(NoProgressError, match="Select the field subject"):
            await bridge.call("rdf_prepare", {"sparql": "Changed query", "attempt": 2})
        assert len(bridge.calls) == 3

    asyncio.run(run())


@pytest.mark.parametrize("truncated", [False, True])
def test_endpoint_control_has_no_schema_and_logs_failed_queries(
    session, tmp_path, monkeypatch, truncated
):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    import time

    import mcp_experiment
    from mcp_experiment import ask_endpoint, evaluate, read_query
    from pydantic_ai.usage import UsageLimits

    from rdfsolve.mcp.agent import Answer

    _, data = files(session, tmp_path)
    final = "SELECT DISTINCT ?a WHERE { ?a a <" + str(E.AOP) + "> }"
    calls = []
    active, peak = 0, 0

    def checked(query):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            time.sleep(0.03)
            return read_query(query)
        finally:
            active -= 1

    monkeypatch.setattr(mcp_experiment, "read_query", checked)

    def model(messages, info):
        assert {t.name for t in info.function_tools} == {"sparql_query"}
        assert info.model_settings["max_tokens"] == 4096
        calls.append(messages)
        if len(calls) == 1:
            assert "schema.json" not in str(messages)
            assert str(E.AOP) not in str(messages)
            return ModelResponse(
                parts=[
                    ToolCallPart("sparql_query", {"query": "SELECT broken"}, tool_call_id="bad"),
                    ToolCallPart(
                        "sparql_query",
                        {"query": "SELECT ?type WHERE {?s a ?type} LIMIT 2"},
                        tool_call_id="types",
                    ),
                    ToolCallPart(
                        "sparql_query",
                        {"query": "SELECT ?p WHERE {?s ?p ?o} LIMIT 2"},
                        tool_call_id="predicates",
                    ),
                ]
            )
        assert "sparql_syntax" in str(messages)
        if len(calls) == 2 and not truncated:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        info.output_tools[0].name,
                        {"sparql": final.replace("?a WHERE", "?missing WHERE")},
                    )
                ]
            )
        if not truncated:
            assert "missing_outputs" in str(messages)
        return ModelResponse(
            parts=[ToolCallPart(info.output_tools[0].name, {"sparql": final})],
            finish_reason="length" if truncated else "stop",
        )

    with snapshot_endpoint(data, tmp_path / "endpoint.jsonl") as endpoint:
        answer = asyncio.run(
            ask_endpoint(
                "List pathways.",
                endpoint=endpoint,
                model=FunctionModel(model),
                model_settings={"max_tokens": 65536},
                usage_limits=UsageLimits(request_limit=5),
                output_dir=tmp_path,
                output_variables=["a"],
            )
        )
    if truncated:
        assert answer.state == "blocked" and not answer.bindings
        assert answer.error["code"] == "model_generation_limit"
        assert answer.package["source_queries"] == 2
    else:
        assert answer.state == "complete" and len(answer.bindings) == 3
    assert peak == 1
    journal = [json.loads(line) for line in (tmp_path / "calls.jsonl").read_text().splitlines()]
    assert journal[0]["arguments"]["query"] == "SELECT broken"
    assert len(journal) == (3 if truncated else 4)
    assert all("error" not in c["result"] for c in journal[1:3])
    if not truncated:
        assert journal[-1]["name"] == "final_query"
        assert journal[-1]["result"]["error"]["code"] == "missing_outputs"
    assert answer.calls[0]["result"]["error"]["code"] == "sparql_syntax"
    assert (tmp_path / "helper.json").is_file()
    assert evaluate(Answer(), {("x",)}, ["a"])["f1"] == 0
    for text in (
        "SELECT * FROM <http://outside/> WHERE {?s ?p ?o}",
        "SELECT * WHERE { SERVICE SILENT ?endpoint {?s ?p ?o} }",
    ):
        with pytest.raises(ValueError, match="outside"):
            read_query(text)


def test_examples_and_reference_use_typed_shacl(session, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    from mcp_experiment import evaluate, load_examples, rdf_tuples, run_reference

    from rdfsolve.api import QueryCollection
    from rdfsolve.mcp.agent import Answer

    _, data = files(session, tmp_path)
    examples = QueryCollection()
    examples.add(
        "Pathways",
        "SELECT DISTINCT ?a WHERE { ?a a ex:AOP }",
        description="List all pathways.",
        prefixes={"ex": str(E)},
    )
    examples.add("Ask", "ASK { ?s ?p ?o }")
    examples.to_turtle(tmp_path / "examples.ttl")
    cases, exclusions = load_examples(tmp_path / "examples.ttl")
    assert len(cases) == len(exclusions) == 1
    case = cases[0]
    assert case["question"] == "List all pathways." and case["columns"] == ["a"]
    with snapshot_endpoint(data, tmp_path / "endpoint.jsonl") as endpoint:
        rows = asyncio.run(
            run_reference(
                case["query"],
                endpoint,
                tmp_path / "reference.jsonl",
                tmp_path / "reference",
                max_seconds=20,
            )
        )
    expected = rdf_tuples(rows, ["a"])
    assert len(expected) == 3
    assert evaluate(Answer(state="complete", bindings=rows), expected, ["a"])["f1"] == 1
    assert evaluate(Answer(), expected, ["a"])["f1"] == 0
    assert evaluate(Answer(state="complete", bindings=rows[:1]), expected, ["a"])["f1"] == 0.5
    assert json.loads((tmp_path / "reference.jsonl").read_text())["status"] == "complete"


@pytest.mark.parametrize("with_tool", [False, True])
@pytest.mark.parametrize("limit", [4096, 8192, None])
def test_generation_limit_stops_before_another_tool(session, tmp_path, with_tool, limit):
    from pydantic_ai.messages import ThinkingPart
    from pydantic_ai.usage import RequestUsage, UsageLimits

    schema, data = files(session, tmp_path)
    expected_limit = limit or 65536
    seen = []

    def model(messages, info):
        seen.append(info.model_settings)
        part = (
            ToolCallPart("rdf_schema", {"concepts": ["Event"]})
            if with_tool
            else ThinkingPart("Unfinished reasoning")
        )
        return ModelResponse(
            parts=[part], finish_reason="length", usage=RequestUsage(output_tokens=expected_limit)
        )

    answer = asyncio.run(
        ask_rdf(
            "List events",
            schema=schema,
            data_file=data,
            model=FunctionModel(model),
            model_settings={"max_tokens": 65536},
            max_response_tokens=limit,
            usage_limits=UsageLimits(request_limit=2),
            output_dir=tmp_path,
        )
    )
    assert len(seen) == 1 and seen[0]["max_tokens"] == expected_limit
    assert answer.state == "blocked" and not answer.calls and not answer.bindings
    assert answer.error["code"] == "model_generation_limit"
    assert "Increase" not in answer.text and answer.usage.output_tokens == expected_limit
    assert (
        json.loads(Path(answer.files["answer"]).read_text())["response_token_limit"]
        == expected_limit
    )
    assert answer.max_response_tokens == limit


def test_response_limit_configuration_is_consistent():
    import inspect

    from rdfsolve.mcp import ask_rdf as mcp_ask
    from rdfsolve.mcp.agent import ask, bounded_model_settings
    from rdfsolve.mcp.workflow import ask_rdf as wrapper

    for fn in [ask_rdf, mcp_ask, wrapper, ask]:
        assert inspect.signature(fn).parameters["max_response_tokens"].default == 4096
    assert bounded_model_settings(max_response_tokens=8192)["max_tokens"] == 8192
    assert "max_tokens" not in bounded_model_settings(max_response_tokens=None)
    assert bounded_model_settings({"max_tokens": 1000}, 4096)["max_tokens"] == 1000
    for invalid in [0, -1, True, "none"]:
        with pytest.raises(ValueError):
            bounded_model_settings(max_response_tokens=invalid)


def test_attempt_deadline_stops_blocking_worker_and_detached_children(tmp_path, monkeypatch):
    from time import monotonic

    import psutil

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    from mcp_experiment import run_worker

    pid_file = tmp_path / "child.pid"
    code = """import subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)
Path(sys.argv[1]).write_text(str(child.pid))
time.sleep(60)
"""
    started = monotonic()
    result = asyncio.run(run_worker([sys.executable, "-c", code, str(pid_file)], {}, tmp_path, 2))
    assert monotonic() - started < 8
    assert result["state"] == "failed" and result["error"]["code"] == "attempt_timeout"
    assert not result["bindings"]
    pid = int(pid_file.read_text())
    assert not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    assert json.loads((tmp_path / "result.json").read_text())["error"] == result["error"]


def test_trials_start_with_the_same_private_ontology_cache(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    monkeypatch.setenv("RDFSOLVE_MODEL", "fixture")
    monkeypatch.setenv("RDFSOLVE_MODEL_BASE_URL", "http://127.0.0.1:1/v1")
    from mcp_experiment import _attempt

    from rdfsolve.mcp.agent import Answer

    seed = tmp_path / "seed.json"
    seed.write_text('{"initial": 1}')
    seen = []

    async def investigate(**kwargs):
        cache = kwargs["ontology_cache"]
        seen.append(json.loads(cache.read_text()))
        cache.write_text('{"learned": 2}')
        return Answer(state="complete")

    monkeypatch.setattr("rdfsolve.api.ask_rdf", investigate)
    for index in range(2):
        directory = tmp_path / str(index)
        directory.mkdir()
        asyncio.run(
            _attempt(
                dict(
                    condition="rdfsolve_ols",
                    question="Fixture",
                    schema="schema.json",
                    output_dir=str(directory),
                    ontology_seed=str(seed),
                    usage_limits={},
                )
            )
        )
    assert seen == [{"initial": 1}, {"initial": 1}]
    assert json.loads(seed.read_text()) == {"initial": 1}


def test_control_final_execution_failure_is_in_the_call_report(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    from mcp_experiment import ask_endpoint
    from rdfsolve.sparql_helper import PaginationTruncatedError, SparqlHelper

    query = "SELECT ?s WHERE { ?s a <urn:Type> }"
    def model(messages, info):
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, {"sparql": query})])
    def fail(helper, text, **kwargs):
        assert text == query and kwargs["exhaustive"]
        helper.last_select_execution = {"status": "failed", "rows": 10000}
        raise PaginationTruncatedError("Endpoint sort limit", offset=10000)
    monkeypatch.setattr(SparqlHelper, "select_with_fallback", fail)
    answer = asyncio.run(ask_endpoint("List resources", endpoint="https://example.invalid/sparql",
        model=FunctionModel(model), model_settings={}, usage_limits=None, output_dir=tmp_path))
    assert answer.state == "failed" and not answer.bindings
    failed = json.loads((tmp_path / "calls.jsonl").read_text())
    assert failed["name"] == "final_execution"
    assert failed["result"]["error"]["type"] == "PaginationTruncatedError"
    assert answer.execution == {"status": "failed", "rows": 10000}
