import json

from conftest import E, declare
from rdfsolve.mcp.server import dispatch


def test_errors_are_actionable_and_serializable(session):
    bad = dispatch(session, "rdf_probe", {"query_ref": "missing", "limit": "2"})
    assert bad["error"]["code"] == "invalid_arguments"
    json.dumps(bad)
    empty = dispatch(session, "rdf_find", {"text": ""})
    assert "rdf_prepare" in empty["repair"] and (not session.client.queries)
    declare(session, "Return pathways", concept="Adverse Outcome Pathway")
    bad = dispatch(session, "rdf_prepare", {"sparql": "SELECT broken"})
    assert bad["error"]["code"] == "invalid_arguments"
    ref = session.catalogue.type_refs[str(E.AOP)]
    bad = dispatch(
        session,
        "rdf_prepare",
        {"patterns": [{"reference": ref, "bindings": ["a", "b"]}], "outputs": ["a"]},
    )
    assert "one variable" in bad["error"]["message"]
    repaired = dispatch(
        session,
        "rdf_prepare",
        {"patterns": [{"reference": ref, "bindings": ["a"]}], "outputs": ["a"]},
    )
    assert repaired["state"] == "prepared" and (not session.client.queries)
