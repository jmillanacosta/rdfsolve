"""Check paths against RDF and SPARQL, not just serialized strings."""

import pytest
from rdflib import BNode, Graph, RDF, SH, URIRef
from rdflib.plugins.sparql.parser import parseQuery

from rdfsolve.schema_models import MinedSchema
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.readers.paths import read_path


def test_path_and_cardinality_survive_canonical_storage():
    source = """
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:S> a sh:NodeShape; sh:targetClass <urn:A>;
            sh:closed true; sh:ignoredProperties (<urn:type>);
            sh:property [
                sh:path (<urn:p> [sh:inversePath <urn:q>]);
                sh:minCount 0; sh:maxCount 2;
                sh:qualifiedValueShape [sh:class <urn:B>];
                sh:qualifiedMinCount 0; sh:qualifiedMaxCount 1
            ] .
    """
    schema = MinedSchema.from_shacl(source)
    assert schema.patterns == []  # A path is not a predicate or an observation.
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
    assert list(output.items(output.value(URIRef("urn:S"), SH.ignoredProperties))) == [URIRef("urn:type")]
    expression = path_to_sparql(path)
    query = f"SELECT ?value WHERE {{ <urn:one> {expression} ?value }}"
    parseQuery(query)
    data = Graph().parse(data="<urn:one> <urn:p> <urn:middle> . <urn:result> <urn:q> <urn:middle> .", format="turtle")
    assert list(data.query(query))[0][0] == URIRef("urn:result")


@pytest.mark.parametrize("operator", ["alternativePath", "zeroOrMorePath", "oneOrMorePath", "zeroOrOnePath"])
def test_remaining_path_operators_parse(operator):
    obj = "(<urn:p> <urn:q>)" if operator == "alternativePath" else "<urn:p>"
    graph = Graph().parse(data=f"@prefix sh: <http://www.w3.org/ns/shacl#> . <urn:s> sh:path [sh:{operator} {obj}].", format="turtle")
    path = read_path(graph, graph.value(URIRef("urn:s"), SH.path))
    parseQuery(f"SELECT * WHERE {{ ?s {path_to_sparql(path)} ?o }}")


def test_path_cycles_and_query_injection_are_rejected():
    graph = Graph()
    node = BNode()
    graph.add((node, SH.inversePath, node))
    with pytest.raises(ValueError, match="acyclic"):
        read_path(graph, node)
    with pytest.raises(ValueError, match="Invalid character"):
        PropertyPath(operator="predicate", iri="urn:p> } UNION { ?s ?p ?o")


def test_shacl_count_is_not_a_dataset_triple_count():
    from rdfsolve.schema_models import AboutMetadata, SchemaPattern
    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="test", source_version_iri="urn:release"),
        patterns=[SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="Literal",
                                datatype="http://www.w3.org/2001/XMLSchema#string", count=37)],
    )
    output = schema.to_shacl()
    graph = Graph().parse(data=output, format="turtle")
    assert not list(graph.triples((None, SH.minCount, None)))
    assert not list(graph.triples((None, SH.maxCount, None)))
    restored = MinedSchema.from_shacl(output)
    assert restored.patterns[0].count == 37
    assert restored.about.source_version_iri == "urn:release"
