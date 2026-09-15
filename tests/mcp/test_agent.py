"""Real SDK and subprocess checks use a deterministic local model."""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
from conftest import E
from mcp import Client as MCPClient
from mcp import MCPError, StdioServerParameters
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel

from rdfsolve.api import ask_rdf
from rdfsolve.mcp.openai import launch_config
from rdfsolve.mcp.server import CONTRACTS


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
    from rdfsolve.mcp.openai import Answer

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
                    "sparql": f"SELECT DISTINCT ?a WHERE {{ ?a a <{E.AOP}> . }}",
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

    import rdfsolve.mcp.openai as installed

    child = subprocess.check_output(
        [config["command"], "-c", "import rdfsolve.mcp.openai as m; print(m.__file__)"],
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
