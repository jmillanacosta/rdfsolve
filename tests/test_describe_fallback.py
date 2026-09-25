"""Resolve external class labels against scoped source evidence."""
import json
from unittest.mock import patch
import pytest
from rdflib import Dataset
from rdfsolve.client.api import Client
from rdfsolve.client.ontology import OntologyLookup
from rdfsolve.schema_models import MinedSchema

def test_external_label_is_separate_from_source_evidence(tmp_path):
    schema = MinedSchema.from_shacl("""
      @prefix sh: <http://www.w3.org/ns/shacl#> .
      <urn:Shape> a sh:NodeShape; sh:targetClass <urn:Observation>;
        sh:property [sh:path <urn:value>; sh:datatype <http://www.w3.org/2001/XMLSchema#decimal>] .
    """)
    data = Dataset().parse(data="""
      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
      <urn:data> { <urn:m1> a <urn:IC50>; rdfs:label "IC50" .
                   <urn:knownInstance> a <urn:local> .
                   <urn:m2> a <urn:IC50>; rdfs:label "IC50" .
                   <urn:local> a <http://www.w3.org/2002/07/owl#Class>; rdfs:label "Known" . }
      <urn:excluded> { <urn:IC50> rdfs:label "IC50" .
                       <urn:other> a <urn:Absent> . }
    """, format="trig")
    term = {"iri": "urn:IC50", "label": "IC50"}
    lookup = OntologyLookup(offline=True)
    with Client(schema, data, graph_uris=["urn:data"], ontology_grounding=lookup, max_rows=1) as client:
        with patch.object(lookup, "search", return_value=[term]) as search:
            found = client.describe("IC50", ontology_fallback=True)
            assert search.call_count == 1, "Lookup runs after source discovery, once"
        external = found[found.Kind == "ontology"].iloc[0]
        evidence = external["Ontology evidence"][0]
        assert evidence["label_origin"] == "external ontology"
        assert evidence["source_use"]["status"] == "observed_class"
        assert evidence["source_label"]["status"] == "not_found_for_searched_literals"
        assert evidence["source_label"]["graph_uris"] == ["urn:data"]
        assert found.attrs["coverage"]["status"] == "partial", "External evidence must not repair truncated source coverage"
        assert external.Resource == "urn:IC50", "Do not mistake labelled measurements for their class"
        with patch.object(lookup, "search", return_value=[{"iri": "urn:Absent", "label": "Missing"}]):
            absent = client.describe("Missing", ontology_fallback=True)
        assert absent[absent.Kind == "ontology"].iloc[0]["Ontology evidence"][0]["source_use"]["status"] == "not_observed"
        with patch.object(lookup, "search") as search:
            client.describe("Known", ontology_fallback=True)
            search.assert_not_called()
        with patch.object(lookup, "search", return_value=[]) as search:
            client.describe("Observation", ontology_fallback=True)
            client.describe("Observation value", ontology_fallback=True)
            assert search.call_count == 2, "Unused or partly matching schema classes do not suppress"
        with patch.object(client, "_select", side_effect=RuntimeError("source unavailable")), patch.object(lookup, "search") as search:
            with pytest.raises(RuntimeError, match="source unavailable"):
                client.describe("Failure", ontology_fallback=True)
            search.assert_not_called()
        client.save_session(tmp_path / "session.json")
        saved = json.loads((tmp_path / "session.json").read_text())
        assert saved["description_lookups"][0]["strategy"] == "ontology_fallback"
        assert saved["description_lookups"][0]["candidates"][0]["source_use"]["query_ids"]
        assert client._schema == schema, "External names must not change the contract"
