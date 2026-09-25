"""Discover source literals and reuse their identities without a supplied route."""
import pytest
from rdflib import Dataset, Literal
from rdfsolve.client.api import Client
from rdfsolve.schema_models import MinedSchema

def test_describe_literals_to_query(tmp_path):
    schema = MinedSchema.from_shacl("""
      @prefix sh: <http://www.w3.org/ns/shacl#> .
      @prefix e: <urn:example:> .
      e:Shape a sh:NodeShape; sh:targetClass e:Observation;
        sh:property [sh:path e:subject; sh:nodeKind sh:IRI],
                    [sh:path e:value; sh:datatype <http://www.w3.org/2001/XMLSchema#decimal>] .
    """)
    data = Dataset().parse(data="""
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
    """, format="trig")
    with Client(schema, data, graph_uris=["urn:example:data"]) as client:
        matches = client.describe("donepezil")
        resources = matches[matches.Kind == "resource"]
        assert set(resources.Resource) == {"urn:example:drug", "urn:example:untyped"}, "Find typed and untyped literal subjects only in scope"
        drug = resources[resources.Resource == "urn:example:drug"].iloc[0]
        assert drug.Types == ["urn:example:UnmodelledChemical"], "Retain types absent from the schema"
        assert drug.Graph == "urn:example:data", "Retain the selected graph"
        labelled = client.describe(Literal("Donepezil", lang="en"))
        assert labelled.iloc[0].Literal["language"] == "en", "Keep an exact language-tagged literal"
        assert set(client.describe("DONEPEZIL").Resource) == {"urn:example:drug", "urn:example:untyped"}, "Find common case variants by literal identity"
        metric = client.describe("IC50")
        assert "urn:example:metric" in set(metric.Resource), "Find vocabulary labels without generated models"
        fields = client.describe(owners=["Observation"], source=False)
        relation = fields[fields.Path == "<urn:example:subject>"].iloc[0]
        value = fields[fields.Path == "<urn:example:value>"].iloc[0]
        query = client.prepare_network([
            {"reference": relation.Reference, "bindings": ["observation", "drug"]},
            {"reference": value.Reference, "bindings": ["observation", "value"]},
        ], outputs=["value"], values={"drug": drug.Reference})
        result = client.select(query)
        assert [float(row["value"].value) for row in result.rows] == [0.011], "A discovered identity must constrain the actual measurement"
        before = len(client.queries)
        client.describe("Observation", source=False)
        assert len(client.queries) == before, "Schema-only inspection must remain offline"
        assert client.describe("absent phrase").attrs["coverage"]["status"] == "complete"
        ambiguous = client.describe("Example")
        assert ambiguous.attrs["resolution"]["status"] == "ambiguous"
        with pytest.raises(ValueError, match="ambiguous"):
            client.connections(ambiguous, metric, max_hops=1)
        identified = client.describe("Example", identifier="MESH:D000001")
        assert set(identified.Resource) == {"http://id.nlm.nih.gov/mesh/D000001"}, "Resolve a registered identifier against matching source evidence"
        paths = client.connections(identified, metric, max_hops=1)
        assert set(paths.From) == {"http://id.nlm.nih.gov/mesh/D000001"} and set(paths.To) == {"urn:example:metric"}, "Navigate descriptions without choosing a row"
        assert client.describe("Wrong name", identifier="MESH:D000001").empty, "An identifier must not override conflicting name evidence"
        with pytest.raises(ValueError, match="identifier"):
            client.describe("Example", identifier="not-a-registered-prefix:123")
        client.save_session(tmp_path / "session.json")
    with Client(schema, data, graph_uris=["urn:example:data"], max_rows=1) as limited:
        assert limited.describe("donepezil").attrs["coverage"]["status"] == "partial", "Never report a truncated match set as complete"
    with Client(schema, data, graph_uris=["urn:example:data"], max_rows=1) as limited:
        identified = limited.describe("Example", identifier="MESH:D000002")
        assert set(identified.Resource) == {"http://id.nlm.nih.gov/mesh/D000002"}, "Constrain identity before applying the row budget"
        with pytest.raises(ValueError, match="partial"):
            limited.connections(limited.describe("donepezil"), "urn:example:metric", max_hops=1)
