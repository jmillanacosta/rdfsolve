"""Retrieve chemical classifications with their source records and graph scopes."""

import pytest
from pyoxigraph import RdfFormat, Store

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.selection import SchemaSelection


def test_retrieve_selected_chemical_classifications():
    store = Store()
    store.load(input="""
        @prefix e: <urn:chemical:> .
        e:data {
            e:c1 a e:Chemical; e:group e:g1, e:g2 .
            e:c2 a e:Chemical; e:group e:untyped .
            e:c3 a e:Chemical .
            e:g1 e:label "PFAS"@en .
        }
        e:labels {
            e:g1 e:label "PFAS"@en .
            e:g2 e:label "Other"@en .
            e:untyped e:label "No group type"@en .
        }
        e:types {
            e:g1 a e:Group . e:g2 a e:Group .
            e:decoy a e:Chemical; e:group e:g1 .
            e:g1 e:label "Context label"@en .
        }
        e:outside { e:g1 e:label "Outside"@en . }
    """, format=RdfFormat.TRIG)
    schema = MinedSchema(
        about=AboutMetadata(graph_uris=["urn:chemical:data", "urn:chemical:labels"],
                            type_context_graph_uris=["urn:chemical:types"]),
        patterns=[
            SchemaPattern(subject_class="urn:chemical:Chemical",
                          property_uri="urn:chemical:group", object_class="urn:chemical:Group"),
            SchemaPattern(subject_class="urn:chemical:Group",
                          property_uri="urn:chemical:label", object_class="Literal",
                          datatype="http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
        ],
    )
    path = schema.discover_paths(max_hops=2).paths[0]
    selected = SchemaSelection.model_validate_json(schema.select(paths=[path]).model_dump_json())
    query = selected.path_query(selected.paths[0])
    rows = list(store.query(query))
    matches = {(r["n0"].value, r["n1"].value, r["n2"].value, r["n2"].language)
               for r in rows if r["n2"] is not None}
    assert matches == {
        ("urn:chemical:c1", "urn:chemical:g1", "PFAS", "en"),
        ("urn:chemical:c1", "urn:chemical:g2", "Other", "en"),
    }, "Keep alternative groups, intermediate identities and language; exclude context edges"
    assert {r["n0"].value for r in rows} == {
        "urn:chemical:c1", "urn:chemical:c2", "urn:chemical:c3"
    }, "Keep missing classifications without adding context-only chemicals"
    assert len(rows) == 4, "Duplicate triples across data graphs must not duplicate output"
    assert len(list(store.query(selected.path_query(path, include_unmatched=False)))) == 2
    assert selected.paths[0].instance_support == "not_checked", "Building a query is not a probe"
    foreign = path.model_copy(deep=True)
    foreign.steps[0].property_uri = "urn:foreign"
    with pytest.raises(ValueError, match="Choose a selected path"):
        selected.path_query(foreign)
