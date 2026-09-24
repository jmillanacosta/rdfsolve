def test_shacl_import_storage_and_navigation():
    """Import constraints, preserve their RDF and compose declared paths."""
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


    implicit = MinedSchema.from_shacl("""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix e: <urn:declared:> .
        e:Person a rdfs:Class, sh:NodeShape;
            sh:property [ sh:path e:worksFor; sh:class e:Organization ] .
        e:Organization a rdfs:Class, sh:NodeShape;
            sh:property [ sh:path e:location; sh:class e:Place ] .
    """)
    assert len(implicit.patterns) == 2, "Class shapes supply implicit targets"
    routes = implicit.discover_paths(max_hops=2)
    assert len(routes.paths) == 1
    assert routes.paths[0].instance_support == "not_checked", "Declarations are not witnesses"
