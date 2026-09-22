from rdflib import RDFS, SH, Graph, URIRef
from rdflib.plugins.sparql.parser import parseQuery
from rdfsolve.schema_models import MinedSchema
from rdfsolve.schema_models.exporters.paths import path_to_sparql
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
