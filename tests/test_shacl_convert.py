def test_mixed_values_use_alternatives_not_conflicting_node_kinds():
    """Regress the mixed literal/resource shape produced by mining."""
    from rdflib import Graph, Namespace
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="mixed"),
        patterns=[
            SchemaPattern(
                subject_class="urn:A", property_uri="urn:p", object_class=kind, datatype=datatype
            )
            for kind, datatype in [
                ("urn:B", None),
                ("Resource", None),
                ("BlankNode", None),
                ("Literal", "http://www.w3.org/2001/XMLSchema#string"),
            ]
        ],
    )
    graph = Graph().parse(data=schema.to_shacl(), format="turtle")
    sh = Namespace("http://www.w3.org/ns/shacl#")
    heads = list(graph.objects(None, sh["or"]))
    assert len(heads) == 1
    assert len(list(graph.items(heads[0]))) == 4
    assert all(
        (
            len(list(graph.objects(subject, sh.nodeKind))) == 1
            for subject in graph.subjects(sh.nodeKind, None)
        )
    )
    restored = MinedSchema.from_shacl(schema.to_shacl())
    assert {p.object_class for p in restored.patterns} == {p.object_class for p in schema.patterns}


    from rdflib.compare import isomorphic

    declared = """
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix e: <urn:declared:> .
        e:Profile a sh:NodeShape; sh:targetClass e:Person;
            sh:closed true; sh:ignoredProperties
            (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>);
            sh:property [ sh:path e:name; sh:minCount 1;
                          sh:pattern "^[A-Z]"; sh:message "Use a capital" ] .
    """
    imported = MinedSchema.from_shacl(declared)
    restored = MinedSchema.from_dict(imported.to_dict())
    assert isomorphic(
        Graph().parse(data=declared, format="turtle"),
        restored.get_metadata().to_rdf_graph(),
    ), "Keep the full provider profile, including constraints outside the model"
    assert restored.shapes.node_shapes[0].closed
