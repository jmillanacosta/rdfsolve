"""Behavioral checks for the 64fdc4e notebook patch.

No monkeypatching of the planner, compiler, executor, or resource reader. Expected
answers are independent fixture facts. SDK tests run only with real dependencies.
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest
from rdflib import Dataset, Graph, Literal, RDF, URIRef, XSD
from rdflib.plugins.sparql import prepareQuery

from rdfsolve.client_api import Client
from rdfsolve.mcp.query_service import QueryService
from rdfsolve.mcp.server import CONTRACTS, dispatch
from rdfsolve.notebook import launch_config, model_config, resolve_schema
from tests.acceptance.conftest import _build_client
from tests.acceptance.fixtures import EX
from tests.acceptance.fixtures import f01_unrestricted as f01, f02_ambiguous as f02
from tests.acceptance.fixtures import f03_conjunction as f03, f08_required_via as f08
from tests.acceptance.fixtures import f09_date_integer as f09, f10_named_graph as f10


def role(name, cls=None):
    return {"id": name, **({"class_hint": cls} if cls else {})}


def relation(name, left, right, **kwargs):
    return {"id": name, "op": "relation", "from": left, "to": right, "meaning": "related to", **kwargs}


def intent(roles, clauses=(), select=None, op="all", distinct=True):
    return {"roles": roles, "where": {"op": op, "args": list(clauses)},
            "select": select or [r["id"] for r in roles], "distinct": distinct}


def start(service, spec, operation="start"):
    return service.query_start(operation, "Fixture question", spec)


def run_ready(service, response):
    assert response["state"] == "ready", response
    emitted = service.query_finish(response["session"], response["revision"], {"type": "emit_query"})
    prepareQuery(emitted["query"])
    assert service.read_resource(emitted["query_ref"])["query"] == emitted["query"]
    done = service.query_finish(response["session"], response["revision"], {"type": "execute", "operation_id": "execute"})
    assert done["state"] == "complete", done
    data = service.read_resource(done["result_ref"])
    assert data["row_count"] == done["rows"] == len(data["bindings"])
    # The exact query reaching the real backend is the emitted artifact.
    assert service.client.queries[-1] == emitted["query"]
    return data, done


def values(data, names):
    return Counter(tuple(row.get(n, {}).get("value") for n in names) for row in data["bindings"])


def paper_spec(op="all", distinct=True):
    return intent([role("p", "Paper"), role("a", "Person"), role("v", "Venue")],
                  [relation("author", "p", "a"), relation("venue", "p", "v")], ["p"], op, distinct)


@pytest.mark.parametrize("op,expected", [("all", f03.Q05_EXPECTED), ("any", f03.Q06_EXPECTED)])
def test_exact_public_execution_for_and_or(op, expected):
    service = QueryService(_build_client(f03))
    data, done = run_ready(service, start(service, paper_spec(op)))
    assert values(data, ["p"]) == Counter(tuple(map(str, row)) for row in expected)
    assert all(row["p"]["type"] == "uri" for row in data["bindings"])


def test_non_distinct_multiplicity_survives_execution():
    service = QueryService(_build_client(f03))
    data, _ = run_ready(service, start(service, paper_spec(distinct=False)))
    assert values(data, ["p"]) == Counter({(str(EX.pBoth),): 1, (str(EX.pMany),): 4})


def test_nested_or_with_common_constraint():
    c = _build_client(f03)
    c.source.add((EX.pAuthor, EX.year, Literal(2022)))
    c.source.add((EX.pVenue, EX.year, Literal(2019)))
    c.source.add((EX.pBoth, EX.year, Literal(2020)))
    s = QueryService(c)
    i = paper_spec("any")
    i["where"] = {"op": "all", "args": [i["where"], {"id": "date", "op": "compare", "role": "p", "field": str(EX.year),
        "operator": "ge", "term": {"type": "literal", "value": "2020", "datatype": str(XSD.integer)}}]}
    data, _ = run_ready(s, start(s, i))
    assert values(data, ["p"]) == Counter({(str(EX.pAuthor),): 1, (str(EX.pBoth),): 1})


def test_unrestricted_class_listing_executes_graph_pattern():
    s = QueryService(_build_client(f01))
    o = start(s, intent([role("a", "A")]))
    data, _ = run_ready(s, o)
    assert values(data, ["a"]) == Counter({(str(EX.a),): 1})


def test_zero_rows_is_completed_not_failed():
    s = QueryService(_build_client(f01))
    i = intent([role("a", "A")], [{"id": "bind", "op": "bind", "role": "a", "term": {"type": "iri", "value": str(EX.absent)}}])
    data, done = run_ready(s, start(s, i))
    assert data["bindings"] == [] and done["rows"] == 0 and done["preview"] == []


@pytest.mark.parametrize("term", [
    {"type": "literal", "value": "bonjour", "language": "fr"},
    {"type": "literal", "value": 'quote" backslash\\ newline\n'},
    {"type": "literal", "value": "2020-01-01", "datatype": str(XSD.date)},
    {"type": "iri", "value": str(EX.a1)},
])
def test_rdf_term_metadata_roundtrip(term):
    s = QueryService(_build_client(f01))
    data, _ = run_ready(s, start(s, intent([role("value")], [{"id": "bind", "op": "bind", "role": "value", "term": term}])))
    actual = data["bindings"][0]["value"]
    expected = {"type": "uri" if term["type"] == "iri" else "literal", "value": term["value"]}
    if "language" in term: expected["xml:lang"] = term["language"]
    if "datatype" in term: expected["datatype"] = term["datatype"]
    assert actual == expected


def test_comparison_boundary():
    s = QueryService(_build_client(f09))
    i = intent([role("p", "Paper")], [{"id": "year", "op": "compare", "role": "p", "field": "publication year", "operator": "ge",
            "term": {"type": "literal", "value": "2020", "datatype": str(XSD.integer)}}])
    data, _ = run_ready(s, start(s, i))
    assert values(data, ["p"]) == Counter({(str(EX.boundary),): 1, (str(EX.new),): 1})


def test_compile_does_not_execute_and_replay_executes_once():
    c = _build_client(f01); s = QueryService(c)
    o = start(s, intent([role("a", "A")]))
    s.query_finish(o["session"], o["revision"], {"type": "emit_query"})
    assert c.queries == []
    data, done = run_ready(s, o)
    n = len(c.queries)
    replay = s.query_finish(o["session"], o["revision"], {"type": "execute", "operation_id": "execute"})
    assert replay == done and len(c.queries) == n == 1


def test_backend_failure_never_produces_success():
    # A real backend object intentionally fails; no execution method is patched.
    class BrokenGraph(Graph):
        def query(self, *args, **kwargs):
            raise OSError("fixture endpoint failure")
    c = _build_client(f01); c = Client(c._schema, BrokenGraph(), graph_uris=[]); s = QueryService(c)
    o = start(s, intent([role("a", "A")]))
    r = s.query_finish(o["session"], o["revision"], {"type": "execute", "operation_id": "e"})
    assert r["error"]["code"] == "backend_unavailable" and "fixture endpoint failure" in r["error"]["message"]
    status = s.query_inspect(o["session"], o["revision"], "status")
    assert not status["has_result_artifact"] and status["state"] == "ready"


def test_selected_routes_remain_associated_with_requirements():
    s = QueryService(_build_client(f02))
    i = intent([role("p", "Person"), role("employer", "Organization"), role("affiliation", "Organization")],
               [relation("e", "p", "employer"), relation("a", "p", "affiliation")])
    o = start(s, i)
    for meaning in ["current employer", "publication affiliation"]:
        d = o["decision"]
        option = next(x for x in d["options"] if meaning in x["meaning"])
        o = s.query_decide(o["session"], o["revision"], meaning, {"type": "choose", "decision": d["id"], "option": option["id"]})
    data, _ = run_ready(s, o)
    assert values(data, ["p", "employer", "affiliation"]) == Counter({
        (str(EX.alice), str(EX.instX), str(EX.instY)): 1, (str(EX.bob), str(EX.instY), str(EX.instX)): 1})


def test_required_intermediate_is_used():
    s = QueryService(_build_client(f08))
    i = intent([role("a", "A"), role("b", "B"), role("middle", "V")],
               [relation("r", "a", "b", via=["middle"])], ["b"])
    data, _ = run_ready(s, start(s, i))
    assert values(data, ["b"]) == Counter(tuple(map(str, row)) for row in f08.Q13_EXPECTED)


def test_named_graph_does_not_cross_join_default_union():
    original = _build_client(f03); ds = f10.create_dataset(); ds.default_union = True
    s = QueryService(Client(original._schema, ds, graph_uris=[str(EX.g1)]))
    data, _ = run_ready(s, start(s, paper_spec()))
    assert values(data, ["p"]) == Counter(tuple(map(str, row)) for row in f10.Q16_EXPECTED)


def test_named_graph_variable_does_not_collide_with_role():
    c = _build_client(f03); ds = Dataset(); ds.graph(EX.g1).add((EX.paper, RDF.type, EX.Paper))
    s = QueryService(Client(c._schema, ds, graph_uris=[str(EX.g1)]))
    data, _ = run_ready(s, start(s, intent([role("_graph", "Paper")])))
    assert values(data, ["_graph"]) == Counter({(str(EX.paper),): 1})


def test_inspect_and_emit_reject_stale_revision():
    s = QueryService(_build_client(f01)); o = start(s, intent([role("a", "A")]))
    assert s.query_inspect(o["session"], 0, "status")["error"]["code"] == "stale_revision"
    assert s.query_finish(o["session"], 0, {"type": "emit_query"})["error"]["code"] == "stale_revision"


def test_start_payload_conflict_and_immutable_artifacts():
    s = QueryService(_build_client(f01)); i = intent([role("a", "A")]); o = start(s, i)
    assert s.query_start("start", "Different question", i)["error"]["code"] == "operation_conflict"
    a = s.read_resource(o["query_ref"]); a["query"] = "changed"; a["coverage"]["fake"] = []
    assert s.read_resource(o["query_ref"])["query"] != "changed"
    assert "fake" not in s.read_resource(o["query_ref"])["coverage"]


@pytest.mark.parametrize("mutation", ["unknown_key", "missing_id", "bad_integer", "both_language_datatype", "bad_child"])
def test_invalid_conditions_not_silently_ignored(mutation):
    s = QueryService(_build_client(f01)); i = intent([role("a", "A"), role("b", "B")], [relation("r", "a", "b")])
    if mutation == "unknown_key": i["where"]["args"][0]["pretend_filter"] = True
    if mutation == "missing_id": del i["where"]["args"][0]["id"]
    if mutation == "bad_child": i["where"]["args"].append("not an expression")
    if mutation in {"bad_integer", "both_language_datatype"}:
        term = {"type": "literal", "value": "2020", "datatype": str(XSD.integer)}
        if mutation == "bad_integer": term["value"] = "bad"
        else: term["language"] = "en"
        i["where"]["args"].append({"id": "bind", "op": "bind", "role": "a", "term": term})
    assert start(s, i)["error"]["code"] == "invalid_input"


def test_dispatch_schema_matches_execution_contract():
    import jsonschema
    s = QueryService(_build_client(f01))
    arguments = {"operation_id": "s", "question": "List A", "intent": intent([role("a", "A")])}
    jsonschema.Draft202012Validator(CONTRACTS["query_start"][0].model_json_schema(by_alias=True)).validate(arguments)
    o = dispatch(s, "query_start", arguments)
    assert o["state"] == "ready"
    out = dispatch(s, "query_finish", {"session": o["session"], "revision": o["revision"],
          "action": {"type": "execute", "operation_id": "e"}})
    assert out["rows"] == 1
    assert dispatch(s, "query_start", {**arguments, "made_up": True})["error"]["code"] == "invalid_input"


@pytest.mark.parametrize("cwd", [".", "notebooks", "notebooks/mcp", "temporary"])
def test_schema_resolution_and_child_python(cwd, monkeypatch, tmp_path):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.delenv("RDFSOLVE_SCHEMA", raising=False); monkeypatch.delenv("RDFSOLVE_ROOT", raising=False)
    monkeypatch.chdir(tmp_path if cwd == "temporary" else root / cwd)
    cfg = launch_config()
    assert cfg["command"] == sys.executable
    assert Path(cfg["args"][cfg["args"].index("--schema")+1]) == root / "notebooks/data/aopwikirdf.schema.json"
    assert cfg["env"]["PYTHONPATH"].split(os.pathsep)[0] == str(root / "src")


def test_explicit_bad_schema_does_not_fall_back(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_SCHEMA", str(tmp_path / "missing.json"))
    with pytest.raises(FileNotFoundError): resolve_schema()


def test_slurm_model_exports_are_not_replaced(monkeypatch):
    monkeypatch.delenv("RDFSOLVE_MODEL", raising=False); monkeypatch.delenv("RDFSOLVE_MODEL_BASE_URL", raising=False)
    monkeypatch.setenv("QWEN_BASE_URL", "http://compute-node:8080/v1")
    monkeypatch.setenv("QWEN_MODEL", "my-served-model")
    assert model_config() == ("http://compute-node:8080/v1", "my-served-model")
    assert model_config(base_url="http://other:8000/v1", model_name="different") == ("http://other:8000/v1", "different")


def test_saved_aopwiki_schema_compiles_without_network():
    path = Path(__file__).resolve().parents[1] / "notebooks/data/aopwikirdf.schema.json"
    c = Client.open(path); s = QueryService(c, source_id="aopwikirdf")
    cards = s.query_inspect(target="schema:Adverse Outcome")
    o = start(s, intent([role("aop", cards["types"][0]["id"])]))
    artifact = s.read_resource(o["query_ref"])
    prepareQuery(artifact["query"])
    assert c.queries == [] and "http://aopwiki.org/" in artifact["query"]


def test_real_sdk_in_process():
    mcp = pytest.importorskip("mcp", reason="requires repository mcp==2.2.0 dependency")
    from rdfsolve.mcp.server import create_server
    async def exercise():
        async with mcp.Client(create_server(_build_client(f01))) as client:
            assert {t.name for t in (await client.list_tools()).tools} == set(CONTRACTS)
            o = (await client.call_tool("query_start", {"operation_id": "s", "question": "List A", "intent": intent([role("a", "A")])})).structured_content
            r = (await client.call_tool("query_finish", {"session": o["session"], "revision": o["revision"], "action": {"type": "execute", "operation_id": "e"}})).structured_content
            artifact = await client.read_resource(r["result_ref"])
            data = json.loads(artifact.contents[0].text)
            assert values(data, ["a"]) == Counter({(str(EX.a),): 1})
    asyncio.run(exercise())


def test_real_sdk_stdio_from_unrelated_cwd(tmp_path, monkeypatch):
    mcp = pytest.importorskip("mcp", reason="requires repository mcp==2.2.0 dependency")
    c = _build_client(f01); schema = tmp_path / "fixture.json"; data = tmp_path / "fixture.ttl"
    schema.write_text(c._schema.model_dump_json()); c.source.serialize(data, format="turtle")
    cfg = launch_config(schema)
    cfg["args"].extend(["--data-file", str(data)])
    monkeypatch.chdir(tmp_path)
    async def exercise():
        async with mcp.Client(mcp.StdioServerParameters(**cfg), read_timeout_seconds=30) as client:
            o = (await client.call_tool("query_start", {"operation_id": "s", "question": "List A", "intent": intent([role("a", "A")])})).structured_content
            r = (await client.call_tool("query_finish", {"session": o["session"], "revision": o["revision"], "action": {"type": "execute", "operation_id": "e"}})).structured_content
            artifact = await client.read_resource(r["result_ref"])
            assert json.loads(artifact.contents[0].text)["row_count"] == 1
    asyncio.run(exercise())
