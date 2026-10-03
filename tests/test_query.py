"""Describe what a SELECT result's rows mean before counting them."""

from rdfsolve.client.query import QueryResult, ResultCell

XSD = "http://www.w3.org/2001/XMLSchema#"


def cell(value, datatype=None):
    kind = "literal" if datatype else "uri"
    return ResultCell(value=value, type=kind, datatype=datatype)


def test_profile_reports_row_multiplicity_gaps_and_datatypes():
    rows = []
    for index in range(10):
        row = {"measurement": cell(f"urn:m{index}"), "protein": cell("urn:p1"),
               "value": cell(str(index), XSD + ("int" if index < 2 else "decimal"))}
        if index < 7:
            row["qualifier"] = cell("=", XSD + "string")
        rows.append(row)
    rows.append({**rows[0], "protein": cell("urn:p2")})  # one measurement, two proteins
    result = QueryResult(query="SELECT", endpoint="", row_count=len(rows), duration_ms=0,
                         variables=["measurement", "protein", "value", "qualifier"], rows=rows)
    profile = result.profile()

    measurement = profile.variable("measurement")
    assert (measurement.bound, measurement.distinct) == (11, 10)
    assert profile.variable("qualifier").bound == 8
    assert profile.variable("value").datatypes == {XSD + "int": 3, XSD + "decimal": 8}
    found = {(m.key, m.other): (m.keys, m.multiple, m.max) for m in profile.multiplicities}
    assert found[("measurement", "protein")] == (10, 1, 2)
    assert ("protein", "measurement") not in {
        (m.key, m.other) for m in profile.multiplicities if m.note
    }, "Most proteins have several measurements: that is the question's one-to-many shape"
    notes = " ".join(profile.notes)
    assert "1 of 10 measurement values have several protein values" in notes
    assert "qualifier is unbound in 3 of 11 rows" in notes
    assert "value has several datatypes" in notes
    assert profile.model_dump(mode="json")["rows"] == 11, "The profile is JSON for MCP clients"
