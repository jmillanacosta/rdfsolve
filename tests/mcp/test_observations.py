"""Model observations are bounded; complete RDF remains caller-side."""

import json

import pytest
from conftest import E, declare, field, prepare, values
from rdflib import Literal

from rdfsolve.mcp.server import CONTRACTS, dispatch


def test_long_values_never_enter_probe_or_final_observations(session):
    sentinel = "PRIVATE_LITERAL_" + "x" * 100000
    session.client.source.add((E.ke1, E.method, Literal(sentinel, lang="en")))
    declare(session, "Return event methods", concept="measurement method")
    prepared = prepare(
        session,
        {"g1": {"pattern": field(session, E.Event, E.method, "e", "m"), "project": ["e", "m"]}},
    )
    probe = dispatch(session, "rdf_probe", {"query_ref": prepared["query_ref"]})
    final = dispatch(session, "rdf_finish", {"query_ref": prepared["query_ref"]})
    for observation in (prepared, probe, final, session.inspect(final["result_ref"])):
        encoded = json.dumps(observation)
        assert "PRIVATE_LITERAL_" not in encoded and len(encoded) < 12000
    full = session.export(final["result_ref"])
    assert any(r["m"]["value"] == sentinel and r["m"]["xml:lang"] == "en" for r in full["bindings"])
    assert "sample_rows" not in json.dumps(
        {name: c[0].model_json_schema() for name, c in CONTRACTS.items()}
    )


def test_errors_are_actionable_and_serializable(session):
    bad = dispatch(session, "rdf_probe", {"query_ref": "missing", "limit": "2"})
    assert bad["error"]["code"] == "invalid_arguments"
    json.dumps(bad)
    empty = dispatch(session, "rdf_find", {"text": ""})
    assert "rdf_prepare" in empty["repair"] and not session.client.queries
    declare(session, "Return pathways", concept="Adverse Outcome Pathway")
    bad = dispatch(session, "rdf_prepare", {"sparql": "SELECT broken"})
    assert bad["error"]["code"] == "invalid_arguments"
    ref = session.catalogue.type_refs[str(E.AOP)]
    bad = dispatch(session, "rdf_prepare", {"patterns": [{"reference": ref, "bindings": ["a", "b"]}], "outputs": ["a"]})
    assert "one variable" in bad["error"]["message"]
    repaired = dispatch(session, "rdf_prepare", {"patterns": [{"reference": ref, "bindings": ["a"]}], "outputs": ["a"]})
    assert repaired["state"] == "prepared" and not session.client.queries


def test_small_schema_pages_keep_usable_references(session):
    refs = list(session.catalogue.field_refs.values())
    page = session._page(refs, budget=240)
    assert page["items"] and page["next_offset"] == 1
    assert len(json.dumps(page["items"]).encode()) <= 240
    assert session.inspect(page["items"][0]["ref"])["binding_count"] == 2


def test_route_tool_compiles_and_executes_the_client_path(session):
    declare(session, "Return Key Events", concept="Key Event")
    route = session.paths(str(E.AOP), str(E.Event), max_hops=1)["items"][0]["ref"]
    prepared = dispatch(session, "rdf_prepare", {"patterns": [{"reference": route, "bindings": ["source", "event"]}], "outputs": ["source", "event"]})
    final = dispatch(session, "rdf_finish", {"query_ref": prepared["query_ref"]})
    assert final["rows"] == 3 and final["trace"]["query_ids"]
    assert "bindings" not in final
