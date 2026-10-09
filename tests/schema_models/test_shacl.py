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
        ],
    }
    graph = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    patterns = {
        str(graph.value(shape, SH.targetClass)): str(pattern)
        for shape, pattern in graph.subject_objects(SH.pattern)
    }
    assert patterns["urn:Gene"] == r"^(https://identifiers\.org/ncbigene/)"
    assert patterns["urn:Chem"] == (
        r"^(http://purl\.obolibrary\.org/obo/CHEBI_|https://identifiers\.org/chebi/CHEBI:)"
    )
    kinds = {k.iri: k.identifiers for k in Shacl(graph).kinds()}
    assert kinds["urn:Gene"] == ("ncbigene",) and kinds["urn:Chem"] == ("chebi",)
