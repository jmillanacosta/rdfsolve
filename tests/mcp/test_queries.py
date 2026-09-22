from conftest import E, event_goals, prepare, values


def test_event_answers_preserve_owner_scope_and_optional_data(session):
    prepared = prepare(session, event_goals(session))
    rows = values(session, prepared)
    actual = {
        (r["event"]["value"], r.get("species", {}).get("value"), r.get("method", {}).get("value"))
        for r in rows
    }
    assert actual == {
        (str(E.ke1), str(E.Mouse), "Assay A"),
        (str(E.ke1), str(E.Mouse), "Assay B"),
        (str(E.ke3), None, None),
    }
    assert {r["method"]["xml:lang"] for r in rows if "method" in r} == {"en"}
    assert (
        str(E.mouseAOP)
        not in session.export(session.finish(prepared["query_ref"])["result_ref"])["query"]
    )
