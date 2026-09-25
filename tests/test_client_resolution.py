"""Resolve graph-scoped names into reusable query constraints."""
import json
from unittest.mock import patch
import pytest
from rdflib import Dataset
from rdfsolve.client.api import Client
from rdfsolve.client.ontology import OntologyLookup
from rdfsolve.schema_models import MinedSchema

def test_resolve_names_and_compose_a_network(tmp_path):
    schema = MinedSchema.from_shacl("""
      @prefix sh: <http://www.w3.org/ns/shacl#> .
      @prefix e: <urn:e:> .
      e:P a sh:NodeShape; sh:targetClass e:Person;
        sh:property [sh:path e:affiliation; sh:name "member of"; sh:nodeKind sh:IRI],
                    [sh:path e:name; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
      e:O a sh:NodeShape; sh:targetClass e:Organisation;
        sh:property [sh:path e:title; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
    """)
    graph = Dataset().parse(data="""
      @prefix e: <urn:e:> .
      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
      e:data {
        e:one a e:Person, e:External; e:affiliation e:org; e:name "Ada" .
        e:two a e:Person; e:affiliation e:other; e:name "Other" .
        e:org a e:Organisation; e:title "Institute" .
        e:other a e:Organisation; e:title "Elsewhere" .
        e:External rdfs:comment "A class without its requested name" .
      }
      e:excluded { e:three a e:External; e:affiliation e:other .
                   e:unused a e:OtherExternal . }
    """, format="trig")
    lookup = OntologyLookup(offline=True)
    with Client(schema, graph, graph_uris=["urn:e:data"], ontology_grounding=lookup) as client:
        with patch.object(lookup, "search", return_value=[{"iri": "urn:e:External", "label": "Researcher"}, {"iri": "urn:e:OtherExternal", "label": "Researcher"}]):
            query = client.prepare_network([
                {"reference": "Researcher", "bindings": ["person"]},
                {"reference": "member of", "bindings": ["person", "organisation"]},
                {"reference": "Organisation", "bindings": ["organisation"]},
                {"reference": "label", "bindings": ["organisation", "name"]},
            ], outputs=["person", "organisation", "name"], resolve=True, ontology_fallback=True)
        result = client.select(query)
        assert [(r["person"].value, r["organisation"].value, r["name"].value) for r in result.rows] == [("urn:e:one", "urn:e:org", "Institute")], "Resolve class names and owner-qualified fields without losing the join or scope"
        assert query.diagnostics["resolutions"][0]["method"] == "external ontology class"
        term = client.resolve("urn:e:External", kind="resource")
        assert client.catalogue.fragments[term["reference"]].kind == "term", "A class IRI may also be queried as a resource"
        found = client.resolve("Ada", kind="resource")
        assert found["iri"] == "urn:e:one"
        with pytest.raises(ValueError, match="ambiguous"):
            client.prepare_network([{"reference": "label", "bindings": ["s", "label"]}], outputs=["s", "label"], resolve=True)
        with pytest.raises(ValueError):
            client.prepare_network([{"reference": "Researcher", "bindings": ["s"]}], outputs=["s"])
        with patch.object(lookup, "search", return_value=[{"iri": "urn:e:External", "label": "Ambiguous"}, {"iri": "urn:e:Person", "label": "Ambiguous"}]):
            with pytest.raises(ValueError, match="ambiguous"):
                client.resolve("Ambiguous", kind="class", ontology_fallback=True)
        client.save_session(tmp_path / "session.json")
        saved = json.loads((tmp_path / "session.json").read_text())
        assert saved["prepared_queries"][query.ref]["diagnostics"]["resolutions"]
        assert client._schema == schema, "Resolutions must not rewrite the source contract"
