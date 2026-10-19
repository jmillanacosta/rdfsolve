"""rdfsolve.schema_models SHACL: shapes are read and written with their lists, paths and constraints,
a shape without a type constraint is kept, and RDF boundaries are respected."""

import pytest
from pyshacl import validate
from rdflib import RDF, RDFS, SH, BNode, Graph, Literal, Namespace, URIRef
from rdflib.plugins.sparql.parser import parseQuery

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.readers.paths import read_path
from rdfsolve.schema_models.shacl_model import ShaclPropertyShape


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


VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
schema:Thing a rdfs:Class . schema:Person a rdfs:Class; rdfs:subClassOf schema:Thing .
schema:colleague schema:domainIncludes schema:Person; schema:rangeIncludes schema:Person, rdf:List .
"""
DATA = """
@prefix schema: <https://schema.org/> .
<urn:a> a schema:Person ; schema:colleague ( <urn:b> {second} ) .
<urn:b> a schema:Person . <urn:c> a schema:Person . <urn:x> a schema:Thing .
"""


def test_list_members_are_checked_and_the_list_survives_a_round_trip():
    schema = MinedSchema.from_vocabulary(VOCABULARY, ["https://schema.org/Person"])
    shapes = Graph().parse(
        data=schema.to_shacl(activate_observed=True, void=False), format="turtle"
    )
    for second, conforms in (("<urn:c>", True), ("<urn:x>", False)):
        data = Graph().parse(data=DATA.format(second=second), format="turtle")
        assert validate(data, shacl_graph=shapes)[0] is conforms, f"member {second}"
    restored = MinedSchema.from_shacl(shapes.serialize(format="turtle"))
    (profile,) = restored.collections
    assert (profile.subject_class, profile.property_uri, profile.member_types) == (
        "https://schema.org/Person",
        "https://schema.org/colleague",
        ["https://schema.org/Person"],
    )


def test_path_and_cardinality_survive_canonical_storage():
    source = '\n        @prefix sh: <http://www.w3.org/ns/shacl#> .\n        <urn:S> a sh:NodeShape; sh:targetClass <urn:A>;\n            sh:closed true; sh:ignoredProperties (<urn:type>); sh:name "Display name"@en;\n            sh:property [\n                sh:path (<urn:p> [sh:inversePath <urn:q>]);\n                sh:minCount 0; sh:maxCount 2;\n                sh:qualifiedValueShape [sh:class <urn:B>];\n                sh:qualifiedMinCount 0; sh:qualifiedMaxCount 1\n            ] .\n    '
    schema = MinedSchema.from_shacl(source)
    assert schema.patterns == []
    restored = MinedSchema.from_dict(schema.to_dict())
    shape = restored.shapes.node_shapes[0].property_shapes[0]
    assert shape.min_count == shape.qualified_min_count == 0
    assert shape.max_count == 2 and shape.qualified_max_count == 1
    output = Graph().parse(data=restored.to_shacl(), format="turtle")
    repeated = MinedSchema.from_shacl(restored.to_shacl())
    assert len(repeated.shapes.node_shapes) == 1
    prop = output.value(URIRef("urn:S"), SH.property)
    path = read_path(output, output.value(prop, SH.path))
    assert path == shape.path
    assert output.value(URIRef("urn:S"), RDFS.label).language == "en"
    assert list(output.items(output.value(URIRef("urn:S"), SH.ignoredProperties))) == [
        URIRef("urn:type")
    ]
    expression = path_to_sparql(path)
    query = f"SELECT ?value WHERE {{ <urn:one> {expression} ?value }}"
    parseQuery(query)
    data = Graph().parse(
        data="<urn:one> <urn:p> <urn:middle> . <urn:result> <urn:q> <urn:middle> .", format="turtle"
    )
    assert list(data.query(query))[0][0] == URIRef("urn:result")


def test_sparql_property_paths_are_read_as_path_models():
    prefixes = {"ex": "urn:ex:", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}
    path = PropertyPath.from_sparql("^(ex:author/rdf:rest*/rdf:first)|a/ex:name?", prefixes)
    assert path.operator == "alternative" and path.items[0].operator == "inverse"
    assert path.items[1].items[0].iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", (
        "a is rdf:type"
    )
    assert PropertyPath.from_sparql(path_to_sparql(path), prefixes) == path, (
        "Written and read again"
    )
    data = Graph().parse(
        data="@prefix ex: <urn:ex:> . ex:paper ex:author (ex:a ex:me) .", format="turtle"
    )
    query = f"SELECT ?work WHERE {{ <urn:ex:me> {path_to_sparql(path.items[0])} ?work }}"
    assert [row[0] for row in data.query(query)] == [URIRef("urn:ex:paper")], (
        "Found through the list"
    )
    for text, problem in (
        ("ex:a//ex:b", "Expected"),
        ("zz:a", "Unknown prefix"),
        ("(ex:a", "Expected"),
    ):
        with pytest.raises(ValueError, match=problem):
            PropertyPath.from_sparql(text, prefixes)


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
        len(list(graph.objects(subject, sh.nodeKind))) == 1
        for subject in graph.subjects(sh.nodeKind, None)
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


OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"


def test_rdf_type_has_no_property_shape():
    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="fixture"),
        patterns=[
            SchemaPattern(
                subject_class="urn:C", property_uri=str(RDF.type), object_class=OWL_CLASS
            ),
            SchemaPattern(subject_class="urn:C", property_uri="urn:p", object_class="urn:D"),
        ],
    )
    shapes = Graph().parse(
        data=schema.to_shacl(activate_observed=True, void=False), format="turtle"
    )
    paths = set(shapes.objects(None, SH.path))
    assert RDF.type not in paths and len(paths) == 1


def test_shacl_blank_shape_and_zero_cardinality():
    graph = Graph()
    shape = ShaclPropertyShape(path="urn:property", min_count=0, max_count=0)
    node = shape.to_rdf(graph)
    assert isinstance(node, BNode)
    restored = ShaclPropertyShape.from_rdf(graph, node)
    assert restored.uri is None
    assert restored.min_count == restored.max_count == 0
    assert (node, Namespace("http://www.w3.org/ns/shacl#").maxCount, Literal(0)) in graph


def test_node_shapes_name_the_namespaces_of_their_class_iris():
    from rdfsolve.schema_models.enrichment import RdfTerm
    from rdfsolve.targets.shacl import Shacl

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="fixture"),
        patterns=[
            SchemaPattern(subject_class="urn:Gene", property_uri="urn:p", object_class="urn:Chem"),
            SchemaPattern(subject_class="urn:Chem", property_uri="urn:q", object_class="urn:Gene"),
        ],
    )
    schema.enrichment.class_examples = {
        "urn:Gene": [RdfTerm(kind="uri", value="https://identifiers.org/ncbigene/1017")],
        "urn:Chem": [
            RdfTerm(kind="uri", value="https://identifiers.org/chebi/CHEBI:15377"),
            RdfTerm(kind="uri", value="http://purl.obolibrary.org/obo/CHEBI_16236"),
            RdfTerm(kind="uri", value="http://example.org/20260910/chem-1"),  # not registered
        ],
    }
    graph = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    patterns = {
        str(graph.value(shape, SH.targetClass)): str(pattern)
        for shape, pattern in graph.subject_objects(SH.pattern)
    }
    assert patterns["urn:Gene"] == r"^(https://identifiers\.org/ncbigene/)"
    assert {graph.value(shape, SH.severity) for shape, _ in graph.subject_objects(SH.pattern)} == {
        SH.Info
    }, "namespaces read from examples are not an error"
    assert patterns["urn:Chem"] == (
        r"^(http://example\.org/20260910/|http://purl\.obolibrary\.org/obo/CHEBI_|"
        r"https://identifiers\.org/chebi/CHEBI:)"
    )
    assert not any(str(p).isdigit() for p in graph.objects(None, SH.prefix)), "no invented prefix"
    kinds = {k.iri: k.identifiers for k in Shacl(graph).kinds()}
    assert kinds["urn:Gene"] == ("ncbigene",) and kinds["urn:Chem"] == ("chebi",)


def _fixture_schema(**changes) -> MinedSchema:
    """A schema of class urn:A with two members and exact counts of three properties."""
    from rdflib import XSD

    from rdfsolve.schema_models.class_extensions import ClassExtensions

    rows = [("urn:p", 2, 2), ("urn:q", 3, 2), ("urn:r", 1, 1)]
    return MinedSchema(
        about=AboutMetadata.build(dataset_name="fixture"),
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri=prop,
                object_class="Literal",
                datatype=str(XSD.string),
                count=count,
                distinct_subjects=subjects,
                **changes,
            )
            for prop, count, subjects in rows
        ],
        class_extensions=ClassExtensions(members={"urn:A": 2}),
    )


CARDINALITY_DATA = """
<urn:a1> a <urn:A> ; <urn:p> "x" ; <urn:q> "y", "z" ; <urn:r> "w" .
<urn:a2> a <urn:A> ; <urn:p> "x" ; <urn:q> "y" .
"""


def test_exact_counts_give_required_and_single_values():
    from rdfsolve.schema_models.exporters.shacl import counted_cardinalities

    schema = _fixture_schema()
    assert counted_cardinalities(schema) == {
        ("urn:A", "urn:p"): (1, 1),
        ("urn:A", "urn:q"): (1, None),
        ("urn:A", "urn:r"): (None, 1),
    }
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    data = Graph().parse(data=CARDINALITY_DATA, format="turtle")
    assert validate(data, shacl_graph=shapes)[0], "the data the counts come from conforms"
    missing = Graph().parse(data=CARDINALITY_DATA, format="turtle")
    missing.remove((URIRef("urn:a2"), URIRef("urn:p"), None))
    second = Graph().parse(data=CARDINALITY_DATA, format="turtle")
    second.add((URIRef("urn:a1"), URIRef("urn:r"), Literal("v")))
    for changed, component in (
        (missing, SH.MinCountConstraintComponent),
        (second, SH.MaxCountConstraintComponent),
    ):
        conforms, report, _ = validate(changed, shacl_graph=shapes)
        assert not conforms
        assert set(report.objects(None, SH.sourceConstraintComponent)) == {component}


def test_counts_that_are_not_exact_prove_nothing():
    from rdfsolve.schema_models.exporters.shacl import counted_cardinalities

    assert counted_cardinalities(_fixture_schema(count_bound="lower_bound")) == {}
    hierarchy = _fixture_schema()
    hierarchy.patterns.append(
        SchemaPattern(
            subject_class="urn:Class",
            property_uri=str(RDFS.subClassOf),
            object_class="urn:Class",
        )
    )
    assert counted_cardinalities(hierarchy) == {}, "subclass members are not counted"


def test_shapes_are_active_and_named_from_the_enrichment():
    from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation

    schema = _fixture_schema()
    schema.enrichment.labels = [
        TermAnnotation(
            term_iri="urn:p",
            predicate=str(RDFS.label),
            text=RdfTerm(kind="literal", value="name", language="en"),
        )
    ]
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    assert not list(shapes.subjects(SH.deactivated, None))
    assert (
        str(next(shapes.objects(next(shapes.subjects(SH.path, URIRef("urn:p"))), SH.name)))
        == "name"
    )
    inactive = Graph().parse(
        data=schema.to_shacl(activate_observed=False, void=False), format="turtle"
    )
    assert list(inactive.subjects(SH.deactivated, Literal(True)))


def test_a_member_outside_the_example_namespaces_is_reported_at_info():
    from rdfsolve.schema_models.enrichment import RdfTerm

    schema = _fixture_schema()
    schema.enrichment.class_examples = {
        "urn:A": [RdfTerm(kind="uri", value="https://example.org/a/1")]
    }
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    data = Graph().parse(data=CARDINALITY_DATA, format="turtle")
    conforms, report, _ = validate(data, shacl_graph=shapes, allow_warnings=True)
    assert conforms
    assert set(report.objects(None, SH.resultSeverity)) == {SH.Info}


def test_members_written_without_a_scheme_conform_once_read():
    """A parser resolves an IRI written without a scheme against its base: the part is any IRI."""
    from rdfsolve.schema_models.class_parts import ClassPart, ClassParts

    schema = _fixture_schema()
    schema.class_parts = ClassParts(
        by_class={"urn:A": [ClassPart(kind="relative_iri", entities=2)]}
    )
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    data = Graph().parse(data=CARDINALITY_DATA, format="turtle")
    assert validate(data, shacl_graph=shapes)[0]


def test_shapes_can_be_limited_to_some_properties_or_to_the_profile():
    schema = _fixture_schema()
    only = Graph().parse(data=schema.to_shacl(void=False, properties=["urn:p"]), format="turtle")
    assert {str(p) for p in only.objects(None, SH.path)} == {"urn:p"}
    schema.shape_profile = "<urn:s> a <http://www.w3.org/ns/shacl#NodeShape> ; " + (
        "<http://www.w3.org/ns/shacl#targetClass> <urn:A> ; "
        "<http://www.w3.org/ns/shacl#property> [ <http://www.w3.org/ns/shacl#path> <urn:q> ] ."
    )
    profiled = Graph().parse(
        data=schema.to_shacl(void=False, properties="profile"), format="turtle"
    )
    assert {str(p) for p in profiled.objects(None, SH.path)} == {"urn:q"}


def test_record_errors_are_public():
    from rdfsolve.api import RecordError
    from rdfsolve.client.authoring import RecordError as Defined

    assert RecordError is Defined


ADVISORY = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
<urn:profile:A> a sh:NodeShape ; sh:targetClass <urn:A> ;
    sh:property [ sh:path <urn:p> ; sh:minCount 1 ; sh:severity sh:Warning ;
                  sh:message "Add p when there is one." ; sh:name "the p" ] ,
                [ sh:path <urn:q> ; sh:maxCount 9 ; sh:name "the q" ] .
"""


def _results(data: Graph, shapes: Graph) -> set[tuple[str, str]]:
    _, report, _ = validate(data, shacl_graph=shapes, allow_warnings=True)
    return {
        (
            str(report.value(r, SH.sourceConstraintComponent)).rsplit("#", 1)[-1],
            str(report.value(r, SH.resultSeverity)).rsplit("#", 1)[-1],
        )
        for r in report.subjects(SH.resultSeverity, None)
    }


def test_a_profile_severity_and_message_cover_only_the_profile_constraints():
    """The profile's Warning and message stay on its own shape; the generated value types stay
    Violations; a plain profile shape is merged; the annotations go on the generated shape."""
    schema = _fixture_schema()
    schema.shape_profile = ADVISORY
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    p_shapes = set(shapes.subjects(SH.path, URIRef("urn:p")))
    warned = {s for s in p_shapes if shapes.value(s, SH.severity) == SH.Warning}
    assert len(warned) == 1 and shapes.value(next(iter(warned)), SH.message) is not None
    generated = {s for s in p_shapes if (s, SH.datatype, None) in shapes}
    assert len(generated) == 1
    main = next(iter(generated))
    assert shapes.value(main, SH.severity) is None and shapes.value(main, SH.message) is None
    assert str(shapes.value(main, SH.name)) == "the p"
    q = next(
        s for s in shapes.subjects(SH.path, URIRef("urn:q")) if (s, SH.datatype, None) in shapes
    )
    assert shapes.value(q, SH.maxCount).toPython() == 9 and str(shapes.value(q, SH.name)) == "the q"
    text = Graph().parse(data=CARDINALITY_DATA, format="turtle")
    text.remove((URIRef("urn:a1"), URIRef("urn:p"), None))
    text.add((URIRef("urn:a1"), URIRef("urn:p"), URIRef("urn:a-link")))
    assert ("DatatypeConstraintComponent", "Violation") in _results(text, shapes)


def test_a_profile_meets_the_main_shape_not_the_info_shapes_beside_it():
    from rdfsolve.schema_models.enrichment import RdfTerm

    schema = _fixture_schema()
    schema.enrichment.class_examples = {
        "urn:A": [RdfTerm(kind="uri", value="https://example.org/a/1")]
    }
    schema.shape_profile = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
<urn:profile:A> a sh:NodeShape ; sh:targetClass <urn:A> ; sh:closed false ;
    sh:property [ sh:path <urn:r> ; sh:minCount 1 ] .
"""
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    info = {s for s in shapes.subjects(SH.severity, SH.Info)}
    assert info and not any((s, SH.closed, None) in shapes for s in info)
    r = {s for s in shapes.subjects(SH.path, URIRef("urn:r")) if (s, SH.minCount, None) in shapes}
    assert r and all(shapes.value(s, SH.severity) is None for s in r)


HIERARCHY_DATA = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<urn:B> rdfs:subClassOf <urn:A> .
<urn:a1> a <urn:A> ; <urn:p> "text" .
<urn:b1> a <urn:A>, <urn:B> ; <urn:p> <urn:c1> .
<urn:c1> a <urn:C> .
"""


def test_a_class_shape_allows_the_values_of_its_subclasses_members():
    """Minimal types count b1 under B only; SHACL targets it as an instance of A too."""
    from rdflib import XSD

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="fixture"),
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class="Literal",
                datatype=str(XSD.string),
            ),
            SchemaPattern(subject_class="urn:B", property_uri="urn:p", object_class="urn:C"),
        ],
        class_hierarchy={"urn:B": ["urn:A"]},
    )
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    data = Graph().parse(data=HIERARCHY_DATA, format="turtle")
    assert validate(data, shacl_graph=shapes)[0]


def test_mining_records_the_class_hierarchy_of_the_data():
    from rdfsolve.mining.miner import SchemaMiner
    from rdfsolve.ontology.hierarchy import nearest_classes

    graph = Graph().parse(data=HIERARCHY_DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
    assert schema.class_hierarchy.get("urn:B") == ["urn:A"]
    assert nearest_classes({"x": {"y"}, "y": {"z"}}, ["x", "z"]) == {"x": ["z"]}


def test_a_shape_of_two_target_classes_gives_each_class_its_fields():
    source = """
@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix schema: <https://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix ex: <https://example.org/shapes/> .
ex:Chemical a sh:NodeShape ; sh:targetClass schema:MolecularEntity, schema:ChemicalSubstance ;
    sh:property [ sh:path schema:name ; sh:datatype xsd:string ; sh:maxCount 1 ] .
"""
    schema = MinedSchema.from_shacl(source)
    s = Namespace("https://schema.org/")
    assert {(p.subject_class, p.property_uri) for p in schema.patterns} == {
        (str(s.MolecularEntity), str(s.name)),
        (str(s.ChemicalSubstance), str(s.name)),
    }
    (shape,) = schema.shapes.node_shapes
    assert shape.target_class is None
    written = schema.shapes.to_rdf(Graph())
    assert set(written.objects(URIRef(shape.uri), SH.targetClass)) == {
        s.MolecularEntity,
        s.ChemicalSubstance,
    }
