from rdfsolve.schema_models import AboutMetadata, MinedSchema


def test_version_iri_survives_rdf_exports():
    schema = MinedSchema(
        patterns=[],
        about=AboutMetadata.build(
            dataset_name="test",
            endpoint="https://example.org/sparql",
            source_version_iri="urn:release",
        ),
    )
    assert MinedSchema.from_dict(schema.to_jsonld()).about.source_version_iri == "urn:release"
    assert (
        MinedSchema.from_void(
            schema.to_void_graph().serialize(format="turtle")
        ).about.schema_version
        == "urn:release"
    )
    assert schema.to_linkml().version == "urn:release"
