"""Resolve names into class or resource constraints without hiding alternatives."""

import json
from unittest.mock import patch

import pytest
from rdflib import Dataset

from rdfsolve.client.api import Client
from rdfsolve.client.ontology import OntologyLookup
from rdfsolve.client.resolution import ResolutionError
from rdfsolve.schema_models import MinedSchema

SHACL = """
@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix e: <urn:e:> .
e:Person <http://www.w3.org/2000/01/rdf-schema#label> "人物" .
e:P a sh:NodeShape; sh:targetClass e:Person; sh:name "人物";
  sh:property [sh:path e:affiliation; sh:name "member of"; sh:nodeKind sh:IRI],
              [sh:path e:name; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
e:O a sh:NodeShape; sh:targetClass e:Organisation;
  sh:property [sh:path e:title; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
"""
DATA = """
@prefix e: <urn:e:> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
e:data {
  e:one a e:Person, e:External; e:affiliation e:org; e:name "Ada";
        e:about <http://purl.obolibrary.org/obo/CHEBI_53289> .
  e:ext a e:External; e:affiliation e:org .
  e:org a e:Organisation; e:title "Institute" .
  e:Local a <http://www.w3.org/2002/07/owl#Class>; rdfs:label "Researcher" .
  e:loc a e:Local .
}
e:excluded { e:three a e:OtherExternal . }
"""


def test_resolution_reports_candidates_semantics_and_evidence(tmp_path):
    schema = MinedSchema.from_shacl(SHACL)
    lookup = OntologyLookup(offline=True)
    graph = Dataset().parse(data=DATA, format="trig")
    external = [{"iri": "urn:e:External", "label": "Researcher"},
                {"iri": "urn:e:OtherExternal", "label": "Researcher"}]
    with Client(schema, graph, graph_uris=["urn:e:data"]) as client:
        with patch.object(OntologyLookup, "search", return_value=external):
            named = client.resolve("Researcher", kind="class", external_names=True)
        assert named.status == "ambiguous", "A schema, source or external name has no precedence"
        origins = {c.iri: (c.origin, c.use) for c in named.candidates}
        assert origins == {"urn:e:Local": ("source label", "used as class"),
                           "urn:e:External": ("external ontology", "used as class"),
                           "urn:e:OtherExternal": ("external ontology", "not used as class")}
        assert client.ontology is None, "An external lookup must not enable later lookups"

        person = client.resolve("人物", kind="class")
        assert (person.status, person.iri) == ("resolved", "urn:e:Person")
        assert any("subclass" in note for note in person.notes), "State the rdf:type semantics"

        chebi = client.resolve("CHEBI:53289", kind="resource")
        assert chebi.iri == "http://purl.obolibrary.org/obo/CHEBI_53289", "CURIEs are identifiers"
        assert chebi.candidates[0].origin == "registered identifier"
        missing = client.resolve("urn:e:missing", kind="resource")
        assert (missing.status, missing.candidates[0].use) == ("unresolved", "absent")
        assert client.resolve("Ada", kind="resource").iri == "urn:e:one"

        with pytest.raises(ResolutionError) as failure, patch.object(
            OntologyLookup, "search", return_value=external
        ):
            client.prepare_network([{"reference": "Researcher", "bindings": ["r"]}],
                                   outputs=["r"], resolve=True, external_names=True)
        assert failure.value.resolution.status == "ambiguous"
        query = client.prepare_network([
            {"reference": "urn:e:External", "bindings": ["r"]},
            {"reference": "member of", "bindings": ["r", "org"]},
        ], outputs=["r", "org"], resolve=True)
        assert [row["r"].value for row in client.select(query).rows] == ["urn:e:one"]
        assert any("urn:e:Person" in w for w in query.warnings), (
            "Say that a field from another class adds its owner type")

        client.save_session(tmp_path / "session.json")

        def strict(value):
            raise ValueError(value)

        saved = json.loads((tmp_path / "session.json").read_text(), parse_constant=strict)
        assert [r["status"] for r in saved["resolutions"]][:2] == ["ambiguous", "resolved"]
        assert client._schema == schema, "Resolution never rewrites the source contract"
