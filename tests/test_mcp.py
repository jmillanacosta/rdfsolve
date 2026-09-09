"""Exercise investigation tools over MCP with retained AOPWiki RDF."""

import asyncio
import json
import sys

import pandas as pd
import pytest
from mcp import Client as MCPClient, MCPError, StdioServerParameters
from rdflib import Literal, URIRef

from rdfsolve.mcp import create_server, query_answer, read_answer, read_result
from rdfsolve.query_log import QueryLog
from tests.test_client_api import AOP, CHEMICAL, DATA, client


def test_typed_export_keeps_values_while_previews_are_bounded(tmp_path):
    async def run():
        with client() as data:
            session = data.session(source_id="aopwikirdf", preview_rows=1)
            log_path = tmp_path / "session.json"
            async with MCPClient(create_server(session, log_path=log_path)) as wire:
                assert {t.name for t in (await wire.list_tools()).tools} == {"schema", "plan", "search", "paths", "read", "answer"}
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
                final = await query_answer(wire, [read.structured_content["reference"]], name="Events and pathways")
                assert len(data.queries) == before + 2
                table = final.table()
                exported = await read_result(wire, final.payload["reference"])
                assert exported.equals(table) and exported.attrs["records"]
                assert len(table) == len(final.bindings) and table["Target title"].notna().all()
                assert "Connection" not in table and "Relationship 1 IRI" in table
                from rdfsolve.query_collection import QueryCollection
                queries = QueryCollection()
                queries.load_shacl(final.to_shacl())
                saved = queries.queries["Events and pathways"]
                assert saved.query == final.query and queries.paths
                rerun = json.loads(data.source.query(saved.query).serialize(format="json"))["results"]["bindings"]
                assert rerun == final.bindings
                assert set(final.to_graph()) <= set(data.source)
                from rdflib import Graph
                restored = Graph()
                for record in final.records():
                    restored += record.to_graph()
                assert set(restored) == set(final.to_graph())
                assert str(table.iloc[0]["Target IRI"]) in final.diagram(instances=True, row=0)
                assert len(data.queries) == before + 2
                from rdfsolve.client_diagram import connection_diagram
                diagram = connection_diagram(links.iloc[:1], instances=True)
                assert str(links.iloc[0]["Source"]) in diagram
                assert str(links.iloc[0]["Target"]) in diagram
                assert "N0_1 -->" in diagram  # The stored RDF edge points back to the search match.
                assert "subgraph R1" not in diagram
                assert "No observed connections" in connection_diagram(links.iloc[:0])
                with pytest.raises(ValueError, match="path evidence"):
                    connection_diagram(pd.DataFrame())
                assert len(data.queries) == before + 2
            calls = data.session_metadata()["tool_calls"]
            assert not set(calls[0]["query_ids"]) & set(calls[1]["query_ids"])
            assert calls[0]["finished_at"] <= calls[1]["started_at"]
    asyncio.run(run())


def test_final_query_reads_unloaded_metadata_without_a_row_cap():
    from pathlib import Path
    from rdfsolve.answer_query import QueryAnswer

    metadata = Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl"
    with client(metadata) as data:
        session = data.session(source_id="aopwikirdf")
        kind = "http://www.w3.org/ns/dcat#Dataset"
        target = "http://rdfs.org/ns/void#Dataset"
        found = session.search(["complete dataset"], kind=kind)
        paths = session.paths(found["reference"], target, max_hops=1)
        linked = session.read(reference=found["reference"], paths=[p["id"] for p in paths["paths"]])
        before = len(data.queries)
        with pytest.raises(ValueError):
            session.answer([linked["reference"]], fields={kind: ["not_a_field"]})
        assert len(data.queries) == before
        final = session.answer([linked["reference"]], name="Dataset descriptions")
        output = QueryAnswer(session.export_result(final["reference"]))
        assert output.table()["Source description"].eq(Literal("AOP-Wiki RDF -- complete dataset")).all()
        assert "Gene mapping enrichment triples" in set(map(str, output.table()["Target description"]))
        assert set(output.to_graph()) <= set(data.source)
        assert "LIMIT" not in output.query and output.coverage["retrieval"] == "complete"


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


def test_path_first_keeps_multivalued_fields_without_multiplying_connections():
    from rdfsolve.answer_query import QueryAnswer
    from rdfsolve.answer_plan import plan_status
    from rdfsolve.rdf_operations import PathFilter

    with client() as data:
        session = data.session(source_id="aopwikirdf")
        plan = session.plan(AOP, [CHEMICAL], ["Phenobarbital"], "Pathways and chemicals")
        routes = [path["id"] for group in plan["routes"] for path in group["paths"]]
        assert not data.queries
        final = session.answer(paths=routes, where=[PathFilter(kind=CHEMICAL, terms=["Phenobarbital"])],
                               fields={AOP: ["title"], CHEMICAL: ["title", "exactmatch"]})
        result = QueryAnswer(session.export_result(final["reference"]))
        table = result.table()
        assert len(table) == 2
        assert all(len(values) == 10 for values in table["Target exactmatch"])
        assert set(table["Source IRI"]) == {URIRef("https://identifiers.org/aop/107"), URIRef("https://identifiers.org/aop/162")}
        assert not plan_status(session)["pending"]
        assert set(result.to_graph()) <= set(data.source)
        assert not any(op["operation"] in {"search", "read"} for op in data._operations)
        before = len(data.queries)
        page = session.read(reference=final["reference"], detail=True)
        assert len(page["preview"][0]["Target exactmatch"]) == 10
        assert AOP in session._kinds(final["reference"]) and CHEMICAL in session._kinds(final["reference"])
        assert len(data.queries) == before
        with pytest.raises(ValueError, match="Available fields"):
            session.answer(paths=routes, where=[PathFilter(kind=CHEMICAL, fields=["missing"], terms=["x"])])
        assert len(data.queries) == before
        empty = session.answer(paths=routes, where=[PathFilter(kind=CHEMICAL, terms=["Phenobarbital"]),
                                                   PathFilter(kind=AOP, terms=["unrelated nonexistent phrase"])])
        assert empty["rows"] == 0 and empty["status"] == "complete"


def test_local_blank_nodes_keep_fields_and_identity(tmp_path):
    from rdflib import BNode, DCTERMS, RDF, SH
    from rdfsolve.answer_query import QueryAnswer
    from rdfsolve.rdf_operations import PathFilter
    from rdfsolve.miner import SchemaMiner
    from rdfsolve.mining.local_graph import LocalGraphHelper
    from rdflib import Dataset

    with client() as data:
        session = data.session(source_id="aopwikirdf")
        paths = session.paths(AOP, CHEMICAL, max_hops=2)
        found = session.answer(paths=[p["id"] for p in paths["paths"]],
                               where=[PathFilter(kind=CHEMICAL, terms=["Phenobarbital"])])
        graph = QueryAnswer(session.export_result(found["reference"])).to_shacl()
        path = tmp_path / "actual-query-export.ttl"
        graph.serialize(path, format="turtle")
    with client(path) as data:
        session = data.session(source_id="queries")
        anchor = next(data.source.subjects(DCTERMS.references, None))
        paths = session.paths(str(SH.SPARQLExecutable), str(SH.PropertyShape), max_hops=1)
        found = session.answer(paths=[p["id"] for p in paths["paths"]],
            where=[PathFilter(kind=str(SH.SPARQLExecutable), iris=[str(anchor)])],
            fields={str(SH.SPARQLExecutable): [], str(SH.PropertyShape): ["name", "path"]})
        result = QueryAnswer(session.export_result(found["reference"]))
        assert all(isinstance(node, BNode) for node in result.table()["Target IRI"])
        assert result.table()["Target name"].notna().all()
        assert set(result.to_graph()) <= set(data.source)
        assert all(set(record.to_graph()) <= set(data.source) for record in result.records())
        dataset = Dataset()
        dataset.default_graph += data.source
        with SchemaMiner("https://example.org/sparql", strategy="single-pass", counts=False,
                         pagination="cursor", delay=0, chunk_size=10) as miner:
            miner._helper = LocalGraphHelper(miner.endpoint_url, dataset)
            schema = miner.mine("queries")
            blank = [p for p in schema.patterns if p.property_uri == str(DCTERMS.references) and p.object_class == "BlankNode"]
            assert blank and str(SH.path) in blank[0].blank_node_predicates
            assert all(p.object_class == "BlankNode" or not p.object_class.startswith("BlankNode[") for p in schema.patterns)


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
                joined = await read_answer(wire, [item.reference for item in answer.output.results])
                assert len(joined) == 2
                assert all("has_chemical_entity" in value for value in joined["Relationship type"])
                assert joined.attrs["title"] and joined.attrs["records"]
                assert all(len(match["nodes"]) == 3 for match in joined.attrs["connections"].values())
                assert len(data.queries) == before
            calls = data.session_metadata()["tool_calls"]
            assert [c["status"] for c in calls] == ["complete", "complete", "complete"]
            assert not calls[0]["query_ids"] and calls[1]["query_ids"]
    asyncio.run(run())
