import pytest
from rdflib import RDFS, SH, Graph, URIRef
from rdflib.plugins.sparql.parser import parseQuery
from rdfsolve.schema_models import MinedSchema
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.readers.paths import read_path


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
    assert path.items[1].items[0].iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "a is rdf:type"
    assert PropertyPath.from_sparql(path_to_sparql(path), prefixes) == path, "Written and read again"
    data = Graph().parse(
        data="@prefix ex: <urn:ex:> . ex:paper ex:author (ex:a ex:me) .", format="turtle"
    )
    query = f"SELECT ?work WHERE {{ <urn:ex:me> {path_to_sparql(path.items[0])} ?work }}"
    assert [row[0] for row in data.query(query)] == [URIRef("urn:ex:paper")], "Found through the list"
    for text, problem in (("ex:a//ex:b", "Expected"), ("zz:a", "Unknown prefix"), ("(ex:a", "Expected")):
        with pytest.raises(ValueError, match=problem):
            PropertyPath.from_sparql(text, prefixes)
