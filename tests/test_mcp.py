"""Exercise the MCP wire with retained AOPWiki RDF, not a model service."""

import asyncio
import json
import sys

from mcp import Client as MCPClient, StdioServerParameters

from rdfsolve.mcp import create_server
from rdfsolve.query_log import QueryLog
from tests.test_client_api import AOP, CHEMICAL, DATA, client


def test_typed_result_export_preserves_terms_unread_fields_and_source_bindings():
    import pandas as pd
    import pytest
    from rdflib import Literal, URIRef
    from mcp import MCPError
    from rdfsolve.mcp import read_result

    async def run():
        with client() as data:
            session = data.session(source_id="aopwikirdf", preview_rows=1)
            async with MCPClient(create_server(session)) as wire:
                catalogue = await wire.call_tool("catalogue", {"text": "thyroid"})
                assert not data.queries
                assert not catalogue.structured_content["data_queried"]
                assert catalogue.structured_content["operations"]
                found = await wire.call_tool("find", {"text": "carcinomas", "kind": AOP})
                reference = found.structured_content["reference"]
                table = await read_result(wire, reference)
                assert len(table) == 2  # Export all retained rows, not just the preview.
                await wire.call_tool("select", {"reference": reference, "fields": ["C54571"], "limit": 1})
                before = len(data.queries)
                table = await read_result(wire, reference)
                records = await read_result(wire, reference, output="records")
                assert len(data.queries) == before
                assert isinstance(table.iloc[0]["Identifier"], URIRef)
                assert all(isinstance(term, Literal) for term in table.iloc[0]["title"])
                assert all(isinstance(term, URIRef) for term in table.iloc[0]["c54571"])
                assert pd.isna(table.iloc[1]["c54571"])
                assert table.attrs["queries"] == data.session_metadata()["queries"]
                local = session.result(reference)
                assert local.table().iloc[0]["c54571"] == table.iloc[0]["c54571"]
                for original, restored in zip(local, records, strict=True):
                    assert restored.rdf_class_iri == original.rdf_class_iri
                    assert restored.model_dump(mode="json") == original.model_dump(mode="json")
                    assert set(restored.to_graph()) == set(original.to_graph())
                with pytest.raises(ValueError, match="reference"):
                    await read_result(wire, "../not-a-result")
                await wire.call_tool("release", {"reference": reference})
                with pytest.raises(MCPError, match="Unknown result"):
                    await read_result(wire, reference)
            assert records[0].to_graph()  # Values do not need a live server.
    asyncio.run(run())


def test_mcp_reads_records_and_records_errors(tmp_path):
    async def run():
        with client() as data:
            log_path = tmp_path / "session.json"
            session = data.session(source_id="aopwikirdf", preview_rows=1)
            async with MCPClient(create_server(session, log_path=log_path)) as wire:
                tools = await wire.list_tools()
                assert {tool.name for tool in tools.tools} == {
                    "find", "describe", "catalogue", "call", "select", "release",
                }
                await wire.call_tool("describe", {"identifier": "records.get"})
                assert not data.queries
                bad_kind = await wire.call_tool("find", {"text": "Phenobarbital", "kind": "unknown-class"})
                assert bad_kind.is_error and not data.queries
                assert "Available class names:" in bad_kind.content[0].text
                matches = await wire.call_tool("find", {"text": "Phenobarbital", "kind": CHEMICAL})
                assert not matches.is_error
                payload = matches.structured_content
                assert payload["rows"][0]["id"] == "https://identifiers.org/cas/50-06-6"
                assert payload["rows"][0]["type_label"]
                assert "do not establish identity" in payload["basis"]
                page = await wire.call_tool("select", {"reference": payload["reference"], "fields": ["title"]})
                assert page.structured_content["rows"][0]["fields"]["title"][0]["value"] == "Phenobarbital"
                before = len(data.queries)
                invalid_field = await wire.call_tool("select", {"reference": payload["reference"], "fields": ["type_label"]})
                assert invalid_field.is_error and len(data.queries) == before
                assert "Available fields:" in invalid_field.content[0].text
                assert "title" in invalid_field.content[0].text
                bad = await wire.call_tool("call", {"operation": "delete", "arguments": {}})
                assert bad.is_error and len(data.queries) == before
                assert "Unknown operation" in bad.content[0].text
                await wire.call_tool("release", {"reference": payload["reference"]})
                assert (await wire.call_tool("select", {"reference": payload["reference"], "fields": []})).is_error
                assert not session.results
            log = QueryLog.read(log_path)
            assert log.tools().iloc[-1]["Status"] == "failed"
            assert "Phenobarbital" in log._repr_html_()
            assert log.tool_calls[2]["query_ids"] and log.tool_calls[2]["operation_ids"]
    asyncio.run(run())


def test_mcp_serializes_parallel_reads_and_keeps_query_ownership():
    async def run():
        with client() as data:
            async with MCPClient(create_server(data.session(source_id="aopwikirdf"))) as wire:
                results = await asyncio.gather(
                    wire.call_tool("find", {"text": "Phenobarbital", "kind": CHEMICAL}),
                    wire.call_tool("find", {"text": "thyroid", "kind": AOP}),
                )
                assert all(not result.is_error for result in results)
            calls = data.session_metadata()["tool_calls"]
            ids = [call["query_ids"] for call in calls]
            assert all(ids) and not set(ids[0]) & set(ids[1])
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
            result = await wire.call_tool("find", {"text": "Phenobarbital", "kind": CHEMICAL})
            assert not result.is_error
            assert result.structured_content["rows"][0]["id"] == "https://identifiers.org/cas/50-06-6"
    asyncio.run(run())


def test_agent_uses_mcp_contract_and_recovers_from_a_bad_reference():
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.usage import UsageLimits
    from rdfsolve.pydantic_ai import mcp_tools

    def model(messages, info):
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart("select", {"reference": "missing", "fields": []})])
        if len(messages) == 3:
            return ModelResponse(parts=[ToolCallPart("find", {"text": "Phenobarbital", "kind": CHEMICAL})])
        assert "50-06-6" in str(messages[-1])
        return ModelResponse(parts=[TextPart("Found")])

    async def run():
        with client() as data:
            async with MCPClient(create_server(data.session(source_id="aopwikirdf"))) as wire:
                agent = Agent(FunctionModel(model), toolsets=[await mcp_tools(wire)], instructions=wire.instructions)
                await agent.run("Find Phenobarbital", usage_limits=UsageLimits(request_limit=3))
            calls = data.session_metadata()["tool_calls"]
            assert [call["status"] for call in calls] == ["failed", "complete"]
            assert not calls[0]["query_ids"] and calls[1]["query_ids"]
    asyncio.run(run())
