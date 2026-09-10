"""Exercise investigation tools over MCP with retained AOPWiki RDF."""

import asyncio
import json
import sys

import pandas as pd
import pytest
from mcp import Client as MCPClient, MCPError, StdioServerParameters
from rdflib import Literal, URIRef

from rdfsolve.mcp import create_server, query_answer, read_result
from rdfsolve.query_log import QueryLog
from tests.test_client_api import AOP, CHEMICAL, DATA, client


def column(answer, variable, route=None):
    info = answer.column_info()
    if route is not None:
        info = info.loc[info["Route"].eq(route)]
    return info.loc[info["Variable"].eq(variable), "Column"].iloc[0]


def test_typed_export_keeps_values_while_previews_are_bounded(tmp_path):
    async def run():
        with client() as data:
            session = data.session(source_id="aopwikirdf", preview_rows=1)
            log_path = tmp_path / "session.json"
            async with MCPClient(create_server(session, log_path=log_path)) as wire:
                assert {t.name for t in (await wire.list_tools()).tools} == {"schema", "plan", "search", "paths", "read", "answer"}
                tools = {t.name: t for t in (await wire.list_tools()).tools}
                with pytest.raises(ValueError, match="No answer result"):
                    await query_answer(wire, [])
                assert not data.queries
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


@pytest.mark.parametrize("failure", [RuntimeError("Broken execution"), AttributeError("Broken client")])
def test_execution_fault_is_not_an_agent_retry(monkeypatch, failure):
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelResponse, ToolCallPart
    from pydantic_ai.models.function import FunctionModel
    from rdfsolve.pydantic_ai import mcp_tools

    calls = []
    def model(messages, info):
        calls.append(True)
        return ModelResponse(parts=[ToolCallPart("search", {"terms": ["Phenobarbital"]})])
    def fail(*args, **kwargs):
        raise failure
    async def run():
        with client() as data:
            monkeypatch.setattr(data, "search", fail)
            async with MCPClient(create_server(data.session(source_id="aopwikirdf"))) as wire:
                agent = Agent(FunctionModel(model), toolsets=[await mcp_tools(wire)])
                with pytest.raises(RuntimeError, match=str(failure)):
                    await agent.run("Find Phenobarbital")
        assert len(calls) == 1
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
                assert len(table) == len(final.bindings) and table[column(final, "target_field_title")].notna().all()
                assert not any(name.startswith(("Source ", "Target ", "Via", "Relationship ")) for name in table)
                assert "?relationship" not in final.query and "AS ?source_class" not in final.query
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
                assert str(table.iloc[0][column(final, "target")]) in final.diagram(instances=True, row=0)
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
        assert output.table()[column(output, "source_field_description")].eq(Literal("AOP-Wiki RDF -- complete dataset")).all()
        assert "Gene mapping enrichment triples" in set(map(str, output.table()[column(output, "target_field_description")]))
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
        plan = session.plan(AOP, [CHEMICAL], [PathFilter(kind=CHEMICAL, terms=["Phenobarbital"])], "Pathways and chemicals")
        routes = [path["id"] for group in plan["routes"] for path in group["paths"]]
        assert not data.queries
        final = session.answer(paths=routes, where=[PathFilter(kind=CHEMICAL, terms=["Phenobarbital"])],
                               fields={AOP: ["title"], CHEMICAL: ["title", "exactmatch"]})
        result = QueryAnswer(session.export_result(final["reference"]))
        table = result.table()
        assert len(table) == 2
        assert sum(route["rows"] for route in result.summary()["routes"]) == len(table)
        assert {item["class"]: item["count"] for item in result.summary()["entities"]}[AOP] == 2
        before = len(data.queries)
        repeated = session.answer(paths=routes, where=[PathFilter(kind=CHEMICAL, terms=["Phenobarbital"])],
                                  fields={AOP: ["title"], CHEMICAL: ["title", "exactmatch"]})
        assert repeated["reference"] == final["reference"] and len(data.queries) == before
        for i, binding in enumerate(result.bindings):
            heading = column(result, "target_field_exactmatch", int(binding["_route"]["value"]) + 1)
            assert len(table.iloc[i][heading]) == 10
        assert set(table[column(result, "source")]) == {URIRef("https://identifiers.org/aop/107"), URIRef("https://identifiers.org/aop/162")}
        assert column(result, "source") == "Aopo adverse outcome pathway · IRI"
        assert "→" in column(result, "target") or "←" in column(result, "target")
        assert not plan_status(session)["pending"]
        assert set(result.to_graph()) <= set(data.source)
        assert not any(op["operation"] in {"search", "read"} for op in data._operations)
        before = len(data.queries)
        page = session.read(reference=final["reference"], detail=True)
        heading = column(result, "target_field_exactmatch", int(result.bindings[0]["_route"]["value"]) + 1)
        assert len(page["preview"][0][heading]) == 10
        assert AOP in session._kinds(final["reference"]) and CHEMICAL in session._kinds(final["reference"])
        assert len(data.queries) == before
        with pytest.raises(ValueError, match="Available fields"):
            session.answer(paths=routes, where=[PathFilter(kind=CHEMICAL, fields=["missing"], terms=["x"])])
        assert len(data.queries) == before
        empty = session.answer(paths=routes, where=[PathFilter(kind=CHEMICAL, terms=["Phenobarbital"]),
                                                   PathFilter(kind=AOP, terms=["unrelated nonexistent phrase"])])
        assert empty["rows"] == 0 and empty["status"] == "complete"
        session.plan(AOP, [CHEMICAL], [
            PathFilter(kind=AOP, terms=["thyroid"]),
            PathFilter(kind=AOP, terms=["mouse", "human"]),
        ], "Thyroid pathways in mouse or human")
        for extra in ([], [PathFilter(kind=AOP, terms=["thyroid", "mouse", "human"])]):
            found = session.answer(paths=routes, where=extra)
            filtered = QueryAnswer(session.export_result(found["reference"]))
            assert {row["source"]["value"] for row in filtered.bindings} == {"https://identifiers.org/aop/162"}
            assert filtered.query.count("EXISTS") >= 2


def test_object_field_extension_keeps_the_selected_source_route():
    from rdfsolve.answer_query import QueryAnswer

    event = "http://aopkb.org/aop_ontology#KeyEvent"
    gene = "http://edamontology.org/data_1025"
    predicate = "https://aopwiki.rdf.bigcat-bioinformatics.org/geneDetectedByNER"
    with client(DATA.with_name("aopwikirdf_thyroid_genes_excerpt.ttl")) as data:
        session = data.session(source_id="aopwikirdf")
        plan = session.plan(AOP, [event], [
            {"kind": AOP, "terms": ["thyroid"]},
            {"kind": AOP, "terms": ["human", "mammal"]},
        ], "Thyroid-related pathways and their linked genes", max_hops=1)
        paths = [p["id"] for p in plan["routes"][0]["paths"]]
        final = session.answer(paths=paths, fields={event: [predicate]}, expand_links=True)
        result = QueryAnswer(session.export_result(final["reference"]))
        assert len(result.bindings) == 1
        row = result.bindings[0]
        assert row["source"]["value"] == "https://identifiers.org/aop/300"
        assert row["via1"]["value"] == "https://identifiers.org/aop.events/1656"
        assert row["target"]["value"] == "https://identifiers.org/hgnc/11782"
        assert [node["type"] for node in result.execution["branches"][0]["nodes"]] == [AOP, event, gene]
        assert set(result.to_graph()) <= set(data.source)
        assert not list(result.to_graph().triples((URIRef("https://identifiers.org/aop.events/277"), None, None)))


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
        assert all(isinstance(node, BNode) for node in result.table()[column(result, "target")])
        assert result.table()[column(result, "target_field_name")].notna().all()
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


@pytest.mark.parametrize("invalid", ["unknown", "search"])
def test_agent_rejects_unknown_or_unjoined_results(tmp_path, invalid):
    from pydantic_ai.messages import ModelResponse, ToolCallPart, RetryPromptPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.usage import UsageLimits
    from rdfsolve.pydantic_ai import research_agent, save_answer

    def model(messages, info):
        retried = any(isinstance(part, RetryPromptPart) for message in messages for part in message.parts)
        answer_tool = next(tool for tool in info.function_tools if tool.name == "answer")
        assert set(answer_tool.parameters_json_schema["properties"]) == {"name", "paths", "fields"}
        reference = final["reference"] if retried else bad_reference
        results = [{"reference": reference}, {"reference": reference}]
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, {
            "text": "Found connected pathways.", "results": results,
        })])

    async def run():
        nonlocal final, bad_reference
        with client() as data:
            log_path = tmp_path / "session.json"
            session = data.session(source_id="aopwikirdf")
            found = session.search(["Phenobarbital"], kind=CHEMICAL)
            routes = session.paths(CHEMICAL, AOP)
            final = session.answer(references=[found["reference"]], paths=[p["id"] for p in routes["paths"]])
            bad_reference = "0" * 32 if invalid == "unknown" else found["reference"]
            async with MCPClient(create_server(session, log_path=log_path)) as wire:
                agent = await research_agent(wire, FunctionModel(model))
                before = len(data.queries)
                answer = await agent.run("Find connected pathways", usage_limits=UsageLimits(request_limit=2))
                tables = [await read_result(wire, result.reference) for result in answer.output.results]
                assert len(tables) == 1
                assert len(tables[0]) == 2
                assert len(data.queries) == before
            data.save_session(log_path)
            save_answer(answer, log_path, question="Find Phenobarbital")
            saved = QueryLog.read(log_path).agent
            assert saved["output"]["text"] == answer.output.text
            assert saved["usage"]["requests"] == answer.usage.requests
    final, bad_reference = {}, ""
    asyncio.run(run())
