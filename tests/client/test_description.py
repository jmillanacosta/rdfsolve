"""rdfsolve.client description: classes and records are described from the schema and the data, with a
fallback when a description is missing."""

import json
from unittest.mock import patch

import pytest
from rdflib import Dataset, Literal

from rdfsolve.client.api import Client
from rdfsolve.ontology import Ontologies
from rdfsolve.schema_models import MinedSchema


def test_describe_literals_to_query(tmp_path):
    schema = MinedSchema.from_shacl("""
      @prefix sh: <http://www.w3.org/ns/shacl#> .
      @prefix e: <urn:example:> .
      e:Shape a sh:NodeShape; sh:targetClass e:Observation;
        sh:property [sh:path e:subject; sh:nodeKind sh:IRI],
                    [sh:path e:value; sh:datatype <http://www.w3.org/2001/XMLSchema#decimal>] .
    """)
    data = Dataset().parse(
        data="""
      @prefix e: <urn:example:> .
      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
      e:data {
        e:drug a e:UnmodelledChemical; rdfs:label "donepezil", "Donepezil"@en .
        e:metric a <http://www.w3.org/2002/07/owl#Class>; rdfs:label "IC50";
            e:uses <http://id.nlm.nih.gov/mesh/D000001> .
        <http://id.nlm.nih.gov/mesh/D000001> rdfs:label "Example" .
        <http://id.nlm.nih.gov/mesh/D000002> rdfs:label "Example" .
        e:untyped e:description "DONEPEZIL" .
        e:result a e:Observation; e:subject e:drug; e:value 0.011 .
        e:other a e:Observation; e:subject e:another; e:value 99.1 .
      }
      e:excluded { e:wrong rdfs:label "Donepezil" . }
    """,
        format="trig",
    )
    with Client(schema, data, graph_uris=["urn:example:data"]) as client:
        matches = client.describe("donepezil")
        resources = matches[matches.Kind == "resource"]
        assert set(resources.Resource) == {"urn:example:drug", "urn:example:untyped"}, (
            "Find typed and untyped literal subjects only in scope"
        )
        drug = resources[resources.Resource == "urn:example:drug"].iloc[0]
        assert drug.Types == ["urn:example:UnmodelledChemical"], (
            "Retain types absent from the schema"
        )
        assert drug.Graph == "urn:example:data", "Retain the selected graph"
        labelled = client.describe(Literal("Donepezil", lang="en"))
        assert labelled.iloc[0].Literal["language"] == "en", "Keep an exact language-tagged literal"
        assert set(client.describe("DONEPEZIL").Resource) == {
            "urn:example:drug",
            "urn:example:untyped",
        }, "Find common case variants by literal identity"
        metric = client.describe("IC50")
        assert "urn:example:metric" in set(metric.Resource), (
            "Find vocabulary labels without generated models"
        )
        fields = client.describe(owners=["Observation"], source=False)
        relation = fields[fields.Path == "<urn:example:subject>"].iloc[0]
        value = fields[fields.Path == "<urn:example:value>"].iloc[0]
        query = client.prepare_network(
            [
                {"reference": relation.Reference, "bindings": ["observation", "drug"]},
                {"reference": value.Reference, "bindings": ["observation", "value"]},
            ],
            outputs=["value"],
            values={"drug": drug.Reference},
        )
        result = client.select(query)
        assert [float(row["value"].value) for row in result.rows] == [0.011], (
            "A discovered identity must constrain the actual measurement"
        )
        before = len(client.queries)
        client.describe("Observation", source=False)
        assert len(client.queries) == before, "Schema-only inspection must remain offline"
        assert client.describe("absent phrase").attrs["coverage"]["status"] == "complete"
        ambiguous = client.describe("Example")
        assert ambiguous.attrs["resolution"]["status"] == "ambiguous"
        with pytest.raises(ValueError, match="ambiguous"):
            client.connections(ambiguous, metric, max_hops=1)
        identified = client.describe("Example", identifier="MESH:D000001")
        assert set(identified.Resource) == {"http://id.nlm.nih.gov/mesh/D000001"}, (
            "Resolve a registered identifier against matching source evidence"
        )
        paths = client.connections(identified, metric, max_hops=1)
        assert set(paths.From) == {"http://id.nlm.nih.gov/mesh/D000001"} and set(paths.To) == {
            "urn:example:metric"
        }, "Navigate descriptions without choosing a row"
        assert client.describe("Wrong name", identifier="MESH:D000001").empty, (
            "An identifier must not override conflicting name evidence"
        )
        with pytest.raises(ValueError, match="identifier"):
            client.describe("Example", identifier="not-a-registered-prefix:123")
        client.save_session(tmp_path / "session.json")
    with Client(schema, data, graph_uris=["urn:example:data"], max_rows=1) as limited:
        assert limited.describe("donepezil").attrs["coverage"]["status"] == "partial", (
            "Never report a truncated match set as complete"
        )
    with Client(schema, data, graph_uris=["urn:example:data"], max_rows=1) as limited:
        identified = limited.describe("Example", identifier="MESH:D000002")
        assert set(identified.Resource) == {"http://id.nlm.nih.gov/mesh/D000002"}, (
            "Constrain identity before applying the row budget"
        )
        with pytest.raises(ValueError, match="partial"):
            limited.connections(limited.describe("donepezil"), "urn:example:metric", max_hops=1)


def test_external_label_is_separate_from_source_evidence(tmp_path):
    schema = MinedSchema.from_shacl("""
      @prefix sh: <http://www.w3.org/ns/shacl#> .
      <urn:Shape> a sh:NodeShape; sh:targetClass <urn:Observation>;
        sh:property [sh:path <urn:value>; sh:datatype <http://www.w3.org/2001/XMLSchema#decimal>] .
    """)
    data = Dataset().parse(
        data="""
      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
      <urn:data> { <urn:m1> a <urn:IC50>; rdfs:label "IC50" .
                   <urn:knownInstance> a <urn:local> .
                   <urn:m2> a <urn:IC50>; rdfs:label "IC50" .
                   <urn:local> a <http://www.w3.org/2002/07/owl#Class>; rdfs:label "Known" . }
      <urn:excluded> { <urn:IC50> rdfs:label "IC50" .
                       <urn:other> a <urn:Absent> . }
    """,
        format="trig",
    )
    term = {"iri": "urn:IC50", "label": "IC50"}
    lookup = Ontologies(offline=True)
    with Client(
        schema, data, graph_uris=["urn:data"], ontology_grounding=lookup, max_rows=1
    ) as client:
        with patch.object(lookup, "search", return_value=[term]) as search:
            found = client.describe("IC50", ontology_fallback=True)
            assert search.call_count == 1, "Lookup runs after source discovery, once"
        external = found[found.Kind == "ontology"].iloc[0]
        evidence = external["Ontology evidence"][0]
        assert evidence["label_origin"] == "external ontology"
        assert evidence["source_use"]["status"] == "observed_class"
        assert evidence["source_label"]["status"] == "not_found_for_searched_literals"
        assert evidence["source_label"]["graph_uris"] == ["urn:data"]
        assert found.attrs["coverage"]["status"] == "partial", (
            "External evidence must not repair truncated source coverage"
        )
        assert external.Resource == "urn:IC50", (
            "Do not mistake labelled measurements for their class"
        )
        with patch.object(
            lookup, "search", return_value=[{"iri": "urn:Absent", "label": "Missing"}]
        ):
            absent = client.describe("Missing", ontology_fallback=True)
        assert (
            absent[absent.Kind == "ontology"].iloc[0]["Ontology evidence"][0]["source_use"][
                "status"
            ]
            == "not_observed"
        )
        with patch.object(lookup, "search") as search:
            client.describe("Known", ontology_fallback=True)
            search.assert_not_called()
        with patch.object(lookup, "search", return_value=[]) as search:
            client.describe("Observation", ontology_fallback=True)
            client.describe("Observation value", ontology_fallback=True)
            assert search.call_count == 2, (
                "Unused or partly matching schema classes do not suppress"
            )
        with (
            patch.object(client, "_select", side_effect=RuntimeError("source unavailable")),
            patch.object(lookup, "search") as search,
        ):
            with pytest.raises(RuntimeError, match="source unavailable"):
                client.describe("Failure", ontology_fallback=True)
            search.assert_not_called()
        client.save_session(tmp_path / "session.json")
        saved = json.loads((tmp_path / "session.json").read_text())
        assert saved["description_lookups"][0]["strategy"] == "ontology_fallback"
        assert saved["description_lookups"][0]["candidates"][0]["source_use"]["query_ids"]
        assert client._schema == schema, "External names must not change the contract"
