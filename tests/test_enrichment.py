import json
from unittest.mock import Mock

import pytest
from rdflib import RDF, Dataset, Literal, Namespace, URIRef
from rdfsolve.mining.enrichment import query_enrichment
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

EX = Namespace("urn:test:")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")


@pytest.fixture
def source():
    dataset = Dataset()
    graph = dataset.graph(URIRef("urn:chosen"))
    graph.add((EX.A, SKOS.definition, Literal('A "useful" definition.', lang="en")))
    graph.add((EX.p, SKOS.definition, Literal("A measured value.", lang="en")))
    graph.add((EX.one, RDF.type, EX.A))
    graph.add((EX.one, EX.p, Literal("hello", lang="en")))
    graph.add((EX.one, EX.p, EX.two))
    graph.add((EX.two, RDF.type, EX.B))
    dataset.graph(URIRef("urn:other")).add((EX.A, SKOS.definition, Literal("Wrong graph")))
    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="test"),
        patterns=[
            SchemaPattern(
                subject_class=str(EX.A),
                subject_label="Thing",
                property_uri=str(EX.p),
                object_class="Literal",
            ),
            SchemaPattern(
                subject_class=str(EX.A),
                subject_label="Thing",
                property_uri=str(EX.p),
                object_class=str(EX.B),
                object_label="Target",
            ),
        ],
    )
    helper = Mock(endpoint_url="https://example.org/sparql")
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        dataset.query(query).serialize(format="json")
    )
    return (schema, helper)


def test_enrichment_keeps_language_and_scope(source):
    schema, helper = source
    result = query_enrichment(schema, helper, ["urn:chosen"], examples_per_pattern=1)
    assert result.state == "complete"
    assert result.description(str(EX.A)) == 'A "useful" definition.'
    assert result.description(str(EX.p)) == "A measured value."
    assert len(result.examples) == 2
    assert result.query_count == 3
    literal = next(
        (example.value for example in result.examples if example.value.kind == "literal")
    )
    assert literal.language == "en"
    assert literal.value == "hello"
    assert result.class_examples[str(EX.B)][0].value == str(EX.two)
    schema.enrichment = result
    assert MinedSchema.from_dict(schema.to_dict()) == schema
    compact = MinedSchema.from_dict(schema.to_dict(trim_descriptions=4))
    assert compact.enrichment.description(str(EX.A)) == 'A "u'
    assert compact.enrichment.examples == schema.enrichment.examples
    assert compact.enrichment.labels == schema.enrichment.labels
    assert schema.enrichment.description(str(EX.A)) == 'A "useful" definition.'
    graph = schema.to_void_graph(trim_descriptions=4)
    assert (EX.one, EX.p, Literal("hello", lang="en")) in graph
    assert (EX.A, SKOS.definition, Literal('A "u', lang="en")) in graph
    assert all(
        (
            not item.text.value
            for item in MinedSchema.from_dict(
                schema.to_dict(trim_descriptions=0)
            ).enrichment.definitions
        )
    )
    with pytest.raises(ValueError, match="trim_descriptions"):
        schema.to_shacl(trim_descriptions=-1)
