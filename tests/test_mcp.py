"""Exercise investigation tools over MCP with retained AOPWiki RDF."""

import asyncio
import json
import sys

import pandas as pd
import pytest
from mcp import Client as MCPClient, MCPError, StdioServerParameters
from rdflib import Literal, URIRef

from rdfsolve.mcp import create_server, read_result
from rdfsolve.query_log import QueryLog
from tests.test_client_api import AOP, CHEMICAL, DATA, client


def test_typed_export_keeps_values_while_previews_are_bounded(tmp_path):
    async def run():
        with client() as data:
            session = data.session(source_id="aopwikirdf", preview_rows=1)
            log_path = tmp_path / "session.json"
            async with MCPClient(create_server(session, log_path=log_path)) as wire:
                assert {t.name for t in (await wire.list_tools()).tools} == {"schema", "plan", "search", "paths", "read"}
                tools = {t.name: t for t in (await wire.list_tools()).tools}
                assert tools["schema"].input_schema["properties"]["limit"]["maximum"] == 30
                await wire.call_tool("schema", {"kind": AOP})
                assert not data.queries
                found = await wire.call_tool("search", {"terms": ["carcinomas"], "kind": AOP})
                reference = found.structured_content["reference"]
                await wire.call_tool("read", {"reference": reference, "fields": ["C54571"]})
                before = len(data.queries)
                table = await read_result(wire, reference)
                records = await read_result(wire, reference, output="records")
                assert len(data.queries) == before and len(table) == 2
                assert isinstance(table.iloc[0]["Identifier"], URIRef)
                assert all(isinstance(term, Literal) for term in table.iloc[0]["title"])
                assert pd.isna(table.iloc[1]["c54571"])
                assert table.attrs["evidence"] and table.attrs["coverage"]
                for original, restored in zip(session.result(reference), records, strict=True):
                    assert restored.model_dump(mode="json") == original.model_dump(mode="json")
                    assert set(restored.to_graph()) == set(original.to_graph())
                chemical = await wire.call_tool("search", {"terms": ["Phenobarbital"], "kind": CHEMICAL})
                chemical_ref = chemical.structured_content["reference"]
                page = await wire.call_tool("read", {"reference": chemical_ref, "fields": ["exactMatch"]})
                row = page.structured_content["rows"][0]
                assert len(row["fields"]["exactmatch"]) == 3
                assert row["field_counts"]["exactmatch"] == 10 and row["values_previewed"]
                full = await read_result(wire, chemical_ref)
                assert len(full.iloc[0]["exactmatch"]) == 10
                session.release(reference)
                with pytest.raises(MCPError, match="Unknown result"):
                    await read_result(wire, reference)
                with pytest.raises(ValueError):
                    await read_result(wire, "../missing")
                bad = await wire.call_tool("read", {"reference": chemical_ref, "fields": ["type_label"]})
                assert bad.is_error and "Available fields:" in bad.content[0].text
            assert QueryLog.read(log_path).tools().iloc[-1]["Status"] == "failed"
            assert records[0].to_graph()
    asyncio.run(run())


def test_mcp_serializes_calls_and_reads_a_selected_route():
    async def run():
        with client() as data:
            async with MCPClient(create_server(data.session(source_id="aopwikirdf"))) as wire:
                found, _ = await asyncio.gather(
                    wire.call_tool("search", {"terms": ["thyroxine"]}),
                    wire.call_tool("search", {"terms": ["Phenobarbital"], "kind": CHEMICAL}),
                )
                reference = found.structured_content["reference"]
                routes = await wire.call_tool("paths", {"source": reference, "target": AOP})
                read = await wire.call_tool("read", {
                    "reference": reference, "paths": [p["id"] for p in routes.structured_content["paths"]],
                    "fields": ["title"],
                })
                assert not read.is_error
                assert read.structured_content["rows"][0]["id"] == "https://identifiers.org/aop/162"
                assert read.structured_content["evidence"]
                before = len(data.queries)
                links = await read_result(wire, read.structured_content["reference"], output="connections")
                assert set(links["Target"]) == {URIRef("https://identifiers.org/aop/162")}
                assert links["Source class"].notna().all() and links["Query"].notna().all()
                assert len(data.queries) == before
            calls = data.session_metadata()["tool_calls"]
            assert not set(calls[0]["query_ids"]) & set(calls[1]["query_ids"])
            assert calls[0]["finished_at"] <= calls[1]["started_at"]
    asyncio.run(run())


def test_stdio_opens_saved_schema_without_mining(tmp_path):
    with client() as data:
        path = tmp_path / "schema.json"
        path.write_text(json.dumps(data._schema.to_dict()))
    async def run():
        transport = StdioServerParameters(command=sys.executable, args=[
            "-m", "rdfsolve.mcp", "--schema", str(path), "--data", str(DATA.resolve()),
        ])
        async with MCPClient(transport) as wire:
            result = await wire.call_tool("search", {"terms": ["Phenobarbital"], "kind": CHEMICAL})
            assert not result.is_error
            assert result.structured_content["rows"][0]["id"] == "https://identifiers.org/cas/50-06-6"
    asyncio.run(run())


def test_agent_corrects_unknown_output_references_before_export():
    from pydantic_ai.messages import ModelResponse, ToolCallPart, ToolReturnPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.usage import UsageLimits
    from rdfsolve.pydantic_ai import research_agent

    def model(messages, info):
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart("read", {"reference": "missing"})])
        if len(messages) == 3:
            return ModelResponse(parts=[ToolCallPart("plan", {"source": AOP, "targets": [CHEMICAL],
                "terms": ["carcinomas"], "selection": "Pathways and linked chemicals"})])
        if len(messages) == 5:
            return ModelResponse(parts=[ToolCallPart("search", {"terms": ["carcinomas"], "kind": AOP})])
        payload = next(part.content for message in messages for part in message.parts
                       if isinstance(part, ToolReturnPart) and isinstance(part.content, dict) and "reference" in part.content)
        reference = "0" * 32 if len(messages) == 7 else payload["reference"]
        if len(messages) == 11:
            return ModelResponse(parts=[ToolCallPart("search", {"terms": ["carcinomas"], "kind": AOP})])
        if len(messages) == 13:
            plan = next(part.content for message in messages for part in message.parts
                        if isinstance(part, ToolReturnPart) and part.tool_name == "plan")
            return ModelResponse(parts=[ToolCallPart("read", {"reference": reference,
                "paths": [path["id"] for path in plan["routes"][0]["paths"]]})])
        results = [{"reference": reference}, {"reference": reference}]
        if len(messages) >= 15:
            linked = [part.content for message in messages for part in message.parts
                      if isinstance(part, ToolReturnPart) and part.tool_name == "read"][-1]
            results.append({"reference": linked["reference"]})
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, {
            "text": "Found linked chemicals.", "results": results,
        })])

    async def run():
        with client() as data:
            async with MCPClient(create_server(data.session(source_id="aopwikirdf"))) as wire:
                agent = await research_agent(wire, FunctionModel(model))
                answer = await agent.run("Find pathways and linked chemicals", usage_limits=UsageLimits(request_limit=8))
                before = len(data.queries)
                tables = [await read_result(wire, result.reference) for result in answer.output.results]
                assert answer.output.text == "Found linked chemicals."
                assert len(tables) == 2
                assert str(tables[1].iloc[0]["Identifier"]) == "https://identifiers.org/cas/50-06-6"
                assert len(data.queries) == before
            calls = data.session_metadata()["tool_calls"]
            assert [c["status"] for c in calls] == ["complete", "complete", "complete"]
            assert not calls[0]["query_ids"] and calls[1]["query_ids"]
    asyncio.run(run())
