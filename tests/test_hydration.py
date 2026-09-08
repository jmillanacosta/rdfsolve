"""Test hydration against retained AOPWiki RDF, not a mock domain dataset."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from rdflib import Dataset, Graph, URIRef

from rdfsolve import HydrationLimitError, MinedSchema
from rdfsolve.sparql_helper import EndpointError, SparqlHelper

ROOT = "https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF"
DATASET = "http://rdfs.org/ns/void#Dataset"
DESCRIPTION = "http://purl.org/dc/elements/1.1/description"
DATA = Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl"


def schema():
    return MinedSchema.from_shacl("""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix void: <http://rdfs.org/ns/void#> .
        @prefix dc: <http://purl.org/dc/elements/1.1/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <urn:dataset> sh:targetClass void:Dataset; sh:name "Dataset";
          sh:property [sh:path dc:description; sh:datatype xsd:string],
                      [sh:path void:subset; sh:class void:Dataset],
                      [sh:path (<http://rdfs.org/ns/void#subset> dc:description);
                       sh:name "Subset descriptions"].
    """)


def test_generated_classes_hydrate_paths_and_preserve_terms():
    graph = Graph().parse(DATA, format="turtle")
    with schema().hydrator(graph) as client:
        model = client.model(DATASET)
        obj = client.get(model, ROOT)
        assert isinstance(obj, model)
        assert obj.description == [str(graph.value(URIRef(ROOT), URIRef(DESCRIPTION)))]
        expected = {str(value) for subset in graph.objects(URIRef(ROOT), URIRef("http://rdfs.org/ns/void#subset"))
                    for value in graph.objects(subset, URIRef(DESCRIPTION))}
        assert expected and set(obj.subset_descriptions) == expected
        assert obj.rdf_terms["description"][0]["kind"] == "literal"
        assert DATASET in obj.rdf_type
        assert len(client.queries) == 1
        assert obj.model_dump(mode="json")["rdf_source"]["endpoint"] is None
        selected = client.get(model, ROOT, fields=["description"])
        assert selected.subset is None
        assert selected.rdf_loaded_fields == ["description"]
        extended = client.with_paths(model, texts=[ "http://rdfs.org/ns/void#subset", DESCRIPTION])
        assert set(client.get(extended, ROOT, fields=["texts"]).texts) == expected
        assert "texts" not in model.model_fields


def test_graph_scope_budgets_and_missing_subjects():
    data = Dataset()
    data.graph(URIRef("urn:source")).parse(DATA, format="turtle")
    with schema().hydrator(data, graph_uris=["urn:source"], batch_size=1) as client:
        model = client.model(DATASET)
        sample = client.sample(model, limit=2, fields=["description"])
        assert len(sample) == 2
        assert all(obj.rdf_source["graph_uris"] == ["urn:source"] for obj in sample)
        with pytest.raises(LookupError):
            client.get(model, "urn:missing")
        with pytest.raises(ValueError):
            client.get(model, "urn:x> } UNION { ?s ?p ?o")
    with schema().hydrator(data, graph_uris=["urn:source"], max_rows=1) as client:
        with pytest.raises(HydrationLimitError):
            client.get(client.model(DATASET), ROOT)
    with schema().hydrator(data, graph_uris=[]) as client:
        assert client.sample(client.model(DATASET)) == []


def test_endpoint_failures_and_truncation_do_not_return_empty_objects(monkeypatch):
    with SparqlHelper("https://example.org/sparql") as helper:
        select = Mock(side_effect=EndpointError("timeout"))
        monkeypatch.setattr(helper, "select", select)
        with schema().hydrator(helper) as client:
            model = client.model(DATASET)
            with pytest.raises(EndpointError, match="timeout"):
                client.get(model, ROOT)
            select.side_effect = None
            select.return_value = {}
            with pytest.raises(EndpointError, match="response"):
                client.get(model, ROOT)
            with pytest.raises(ValueError, match="path"):
                client.get(model, ROOT, fields=["unknown"])
        # A supplied helper remains usable.
        select.return_value = {"results": {"bindings": []}}
        assert helper.select("SELECT * WHERE {}")["results"]["bindings"] == []


def test_session_keeps_repeated_queries_and_failed_steps(tmp_path):
    graph = Graph().parse(DATA, format="turtle")
    with schema().hydrator(graph, max_rows=1) as client:
        model = client.model(DATASET)
        for _ in range(2):
            with pytest.raises(HydrationLimitError), client.step("Read dataset"):
                client.get(model, ROOT)
        output = tmp_path / "session.json"
        client.save_session(output)
    session = json.loads(output.read_text())
    assert [q["id"] for q in session["queries"]] == [1, 2]
    assert session["queries"][0]["query"] == session["queries"][1]["query"]
    assert all(q["success"] for q in session["queries"])
    assert [step["query_ids"] for step in session["steps"]] == [[1], [2]]
    assert all(step["status"] == "failed" for step in session["steps"])
    assert all(step["error"] == "HydrationLimitError" for step in session["steps"])


def test_session_keeps_helper_history_and_failed_queries(monkeypatch):
    with SparqlHelper("https://example.org/sparql") as helper:
        helper.enable_query_collection()
        monkeypatch.setattr(helper, "_execute_request", Mock(return_value={
            "results": {"bindings": []},
        }))
        helper.select("SELECT * WHERE {}")
        with schema().hydrator(helper) as client:
            monkeypatch.setattr(helper, "_execute_request", Mock(
                side_effect=EndpointError("unavailable")
            ))
            with pytest.raises(EndpointError), client.step("Read dataset"):
                client.get(client.model(DATASET), ROOT)
            session = client.session_metadata()
        assert len(session["queries"]) == 2
        assert session["queries"][0]["success"] is True
        assert session["queries"][1]["success"] is False
        assert session["queries"][1]["error"] == "EndpointError"
        assert session["steps"][0]["query_ids"] == [2]


def test_exploration_follows_real_links_in_both_directions():
    graph = Graph().parse(DATA, format="turtle")
    with schema().client(graph) as client:
        model = client.model(DATASET)
        root = client.get(model, ROOT, fields=["description"])
        assert "subset" in set(client.links(model)["field"])
        assert client.search(model, 'missing" } #', fields=["description"]) == []
        with client.step("Read subsets"):
            subsets = client.follow([root], "subset", model, fields=["description"])
        expected = {str(iri) for iri in graph.objects(URIRef(ROOT), URIRef("http://rdfs.org/ns/void#subset"))}
        assert {obj.uri for obj in subsets} == expected
        assert [obj.uri for obj in client.follow(subsets, "subset", model, inverse=True)] == [ROOT]
        matches = client.evidence()
        assert set(matches["target"]) == expected | {ROOT}
        assert set(client.table(subsets, ["description"])["uri"]) == expected
        with pytest.raises(ValueError, match="Unknown fields"):
            client.table(subsets, ["misspelt"])
        with pytest.raises(ValueError, match="No RDF path"):
            client.follow([root], "misspelt", model)


def test_rdf_output_preserves_source_triples_and_rejects_invented_paths():
    graph = Graph().parse(DATA, format="turtle")
    with schema().hydrator(graph) as client:
        model = client.model(DATASET)
        record = client.get(model, ROOT)
        with pytest.raises(ValueError, match="intermediate triples"):
            record.to_graph()
        output = record.to_graph(fields=["description", "subset"])
        predicates = {URIRef(DESCRIPTION), URIRef("http://rdfs.org/ns/void#subset"),
                      URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")}
        expected = {triple for triple in graph.triples((URIRef(ROOT), None, None))
                    if triple[1] in predicates}
        assert set(output) == expected
        record.description = ["A revised description"]
        with pytest.raises(ValueError, match="Values changed"):
            record.to_graph(fields=["description"])
        del record.rdf_terms["description"]
        assert str(record.to_graph(fields=["description"]).value(
            URIRef(ROOT), URIRef(DESCRIPTION))) == "A revised description"
        created = model(uri=ROOT, description=record.description)
        assert set(created.to_graph().objects(URIRef(ROOT), URIRef(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))) == {URIRef(DATASET)}
        nested = client.get(model, ROOT, fields=["description", "subset"])
        nested.subset = client.follow([nested], "subset", model, fields=["description"]) if hasattr(client, "follow") else [
            client.get(model, iri, fields=["description"]) for iri in nested.subset]
        assert all(triple in graph for triple in nested.to_graph())
        restored = model.model_validate_json(nested.model_dump_json())
        assert set(restored.to_graph()) == set(nested.to_graph())


def test_void_pattern_fields_work_without_shacl_profiles():
    graph = Graph().parse(DATA, format="turtle")
    mined = schema()
    mined.shapes = None
    restored = MinedSchema.from_void(mined.to_void_graph().serialize(format="turtle"))
    with restored.hydrator(graph, graph_uris=[]) as client:
        model = client.model(DATASET)
        obj = client.get(model, ROOT, fields=["description"])
        assert obj.description == [str(graph.value(URIRef(ROOT), URIRef(DESCRIPTION)))]
