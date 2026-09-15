"""Model observations are bounded; complete RDF remains caller-side."""

import json

import pytest
from conftest import E, declare, field, prepare, values
from rdflib import Literal

from rdfsolve.mcp.server import CONTRACTS, dispatch


def test_long_values_never_enter_probe_or_final_observations(session):
    sentinel = "PRIVATE_LITERAL_" + "x" * 100000
    session.client.source.add((E.ke1, E.method, Literal(sentinel, lang="en")))
    declare(session, "Return event methods")
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
    declare(session, "Return pathways")
    bad = dispatch(
        session,
        "rdf_prepare",
        {
            "sparql": f"SELECT ?a WHERE {{ ?a <{E.AOP}> . }}",
            "grounding": {"g1": {"project": ["a"]}},
        },
    )
    assert bad["error"]["code"] == "sparql_syntax"
    assert bad["error"]["line"] and bad["error"]["hints"]
    ref = session.catalogue.type_refs[str(E.AOP)]
    bad = dispatch(
        session,
        "rdf_prepare",
        {
            "sparql": f"SELECT ?a WHERE {{ ?a {ref} }}",
            "grounding": {"g1": {"evidence": [ref]}},
        },
    )
    assert bad["selected_evidence"]["items"][0]["insert"] == "{{" + ref + " ?s}}"
