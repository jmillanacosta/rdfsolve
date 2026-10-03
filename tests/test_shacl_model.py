def test_mined_prefixes_survive_json_shacl_and_void():
    from rdflib import SH, XSD, URIRef
    from rdfsolve.schema_models import MinedSchema
    from rdfsolve.schema_models.exporters.shacl import minedschema_to_shacl

    schema = MinedSchema(
        about={},
        prefixes={"mine": "https://example.org/vocabulary/"},
        patterns=[
            {
                "subject_class": "https://example.org/vocabulary/Item",
                "property_uri": "https://example.org/vocabulary/name",
                "object_class": "Literal",
                "datatype": str(XSD.string),
            }
        ],
    )
    schema = MinedSchema.from_dict(schema.to_dict())
    shapes = minedschema_to_shacl(schema, base_uri="urn:shapes")
    graph = shapes.to_rdf()
    declarations = {
        str(graph.value(d, SH.prefix)): graph.value(d, SH.namespace)
        for d in graph.objects(URIRef("urn:shapes"), SH.declare)
    }
    assert str(declarations["mine"]) == schema.prefixes["mine"]
    assert declarations["mine"].datatype == XSD.anyURI
    assert "mine:Item" in schema.to_shacl(base_uri="urn:shapes")
    restored = MinedSchema.from_shacl(schema.to_shacl(base_uri="urn:shapes"))
    assert restored.prefixes["mine"] == schema.prefixes["mine"]
    assert restored.shapes.prefix_declarations["urn:shapes"]
    restored = MinedSchema.from_void(schema.to_void_graph().serialize(format="turtle"))
    assert restored.prefixes["mine"] == schema.prefixes["mine"]
