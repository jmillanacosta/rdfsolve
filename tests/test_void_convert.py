from rdfsolve.schema_models.readers.void import void_to_minedschema


def test_mixed_class_and_datatype_partitions_keep_their_own_counts():
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="mixed"),
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class=kind,
                datatype=datatype,
                count=count,
            )
            for kind, datatype, count in [
                ("urn:B", None, 0),
                ("urn:C", None, None),
                ("Literal", "http://www.w3.org/2001/XMLSchema#string", 2**54 + 1),
                ("Literal", "http://www.w3.org/2001/XMLSchema#integer", 12),
            ]
        ],
    )
    restored = MinedSchema.from_dict(schema.to_jsonld())
    key = lambda p: (p.object_class, p.datatype, p.count)
    assert {key(p) for p in restored.patterns} == {key(p) for p in schema.patterns}
    from_void = void_to_minedschema(schema.to_void_graph().serialize(format="turtle"))
    assert {key(p) for p in from_void.patterns} == {key(p) for p in schema.patterns}
    assert len(MinedSchema.from_shacl(from_void.to_shacl()).patterns) == 4
