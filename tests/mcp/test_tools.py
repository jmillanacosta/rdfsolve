"""The tools show the schema, find resources, run queries and keep the final rows."""

import json

import pytest
from conftest import E
from rdfsolve.client.retrieval import QueryValidationError

AOPS = "SELECT ?aop ?label WHERE { ?aop a ex:Pathway ; rdfs:label ?label }"


def test_schema_paths_and_overview_read_the_schema_only(toolbox):
    before = len(toolbox.client.queries)
    assert "ex:Pathway" in toolbox.schema()["text"]
    cards = toolbox.schema(classes=["Key Event", "ex:Chemicals"], search=["cas"])["text"]
    assert "ex:gene → ex:Gene [1]" in cards
    assert "'ex:Chemicals' is not a class of the schema. Similar terms: ex:Chemical" in cards
    assert 'ex:cas "CAS number"' in cards
    route = toolbox.paths("Adverse Outcome Pathway", "ex:Gene")["text"]
    assert "?source ex:event ?n1 . ?n1 ex:gene ?target ." in route
    with pytest.raises(ValueError, match="does not name one class"):
        toolbox.paths("nothing", "ex:Gene")
    assert "Output variables of the answer: ?aop ?label" in toolbox.overview()
    assert len(toolbox.client.queries) == before, "No source request"


def test_find_gives_iris_by_name_text_iri_and_class(toolbox):
    assert toolbox.find("thyroxine")["text"] == 'ex:ke1 a ex:Event: "Decreased thyroxine"'
    assert "ex:ke2" in toolbox.find("fibrosis", in_class="Key Event")["text"]
    assert "Nothing in the source" in toolbox.find("fibrosis", in_class="ex:Chemical")["text"]
    assert toolbox.find("ex:chem1")["text"].startswith("ex:chem1 is in the source, a ex:Chemical")
    assert toolbox.find(f"<{E.tpo}>")["text"].startswith("ex:tpo is in the source, a ex:Gene")
    assert "Nothing in the source" in toolbox.find("ex:absent")["text"]


def test_run_shows_rows_and_notes(toolbox):
    result = toolbox.run(AOPS, limit=1)
    assert result["columns"] == ["aop", "label"]
    assert len(result["rows"]) == 1 and result["more_rows"]
    assert result["notes"] == ["Added PREFIX for ex, rdfs."]
    count = toolbox.run("SELECT (COUNT(*) AS ?n) WHERE { ?s a ex:Event }")
    assert count["rows"] == [["2"]], "Numbers are plain"
    typo = toolbox.run("SELECT ?aop ?label WHERE { ?aop a ex:Pathway ; ex:lable ?label }")
    assert typo["rows"] == []
    assert "ex:lable is not a property of the schema." in typo["notes"][1]
    assert "No data matches ?aop ex:lable ?label ." in typo["notes"]
    optional = toolbox.run("SELECT ?aop ?x WHERE { ?aop a ex:Pathway OPTIONAL { ?aop ex:none ?x } }")
    assert "No value in these rows for ?x." in optional["notes"]
    assert "The answer needs the variables ?label." in optional["notes"]


def test_answer_checks_outputs_runs_on_all_data_and_ends(toolbox):
    with pytest.raises(QueryValidationError, match="label"):
        toolbox.answer("SELECT ?aop WHERE { ?aop a ex:Pathway }")
    done = toolbox.answer(AOPS)
    assert done["state"] == "complete" and done["rows"] == 2
    saved = json.loads((toolbox.artifact_dir / f"{done['result_ref']}.json").read_text())
    assert {row["aop"]["value"] for row in saved["bindings"]} == {str(E.aop1), str(E.aop2)}
    assert saved["query"].startswith("PREFIX ex:")
    assert toolbox.final["bindings"] == saved["bindings"]
    with pytest.raises(ValueError, match="already given"):
        toolbox.answer(AOPS)
    report = toolbox.diagnostics()
    assert report["answered"] and report["source_queries"] == len(report["queries"]) >= 1


def test_a_source_error_in_the_diagnosis_keeps_the_empty_result(toolbox, monkeypatch):
    from rdfsolve.sparql_helper import EndpointError

    real = toolbox.client._select

    def select(query, **options):
        if query.rstrip().endswith("LIMIT 1"):
            raise EndpointError("Virtuoso S1TAT Error: ANYTIME timeout")
        return real(query, **options)

    monkeypatch.setattr(toolbox.client, "_select", select)
    result = toolbox.run("SELECT ?aop ?label WHERE { ?aop ex:none ?label }")
    assert result["rows"] == []
    assert result["notes"][-1] == "The triple patterns could not be checked one by one: source error."


def test_prefixes_of_bioregistry_complete_a_query_with_a_note(toolbox):
    result = toolbox.run("SELECT ?x WHERE { ?x a edam:data_1027 }")
    assert result["rows"] == []
    assert "Namespaces from Bioregistry: edam: <http://edamontology.org/>." in result["notes"]
    with pytest.raises(ValueError, match="Unknown namespace prefix"):
        toolbox.run("SELECT ?x WHERE { ?x a unregistered:thing }")


def test_answer_uses_one_request_when_ordered_pages_cannot_run(toolbox, monkeypatch):
    from rdfsolve.sparql_helper import PaginationTruncatedError

    real = toolbox.client._select

    def select(query, *, exhaustive=False):
        if exhaustive:
            raise PaginationTruncatedError("Virtuoso 42000 Error The estimated execution time exceeds the limit")
        return real(query)

    monkeypatch.setattr(toolbox.client, "_select", select)
    done = toolbox.answer(AOPS)
    assert done["rows"] == 2
    assert any(n.startswith("The query was run in one request, not in pages") for n in done["notes"])
    assert not any("can be incomplete" in n for n in done["notes"]), "2 rows is not a size limit"
