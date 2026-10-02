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


def test_a_failed_example_batch_is_asked_again_one_query_at_a_time(source):
    """One costly example query failed its whole batch of 10 (ChEMBL, 2026-09-30); now only the
    costly query fails, and a batch that fails only as a batch is not a failure."""
    from rdfsolve.sparql_helper import EndpointTimeoutError

    schema, helper = source
    answer = helper.select.side_effect

    def costly(query, **kwargs):
        if "UNION" in query or ("?value" in query and "urn:test:B" in query):
            raise EndpointTimeoutError("Query cost/time limit: fixture")
        return answer(query, **kwargs)

    helper.select.side_effect = costly
    result = query_enrichment(schema, helper, ["urn:chosen"], examples_per_pattern=1)
    assert len(result.examples) == 1 and result.examples[0].value.kind == "literal"
    assert result.state == "partial" and len(result.failures) == 1, "Only the costly query"
    helper.select.side_effect = lambda query, **kwargs: (
        (_ for _ in ()).throw(EndpointTimeoutError("fixture"))
        if "UNION" in query
        else answer(query, **kwargs)
    )
    again = query_enrichment(schema, helper, ["urn:chosen"], examples_per_pattern=1)
    assert again.state == "complete" and len(again.examples) == 2 and not again.failures


def test_examples_filter_a_sample_and_ask_again_when_it_has_none(monkeypatch):
    """A value filter runs on a sample of the pattern (ChEMBL: 300 s on 24 M values, 0.1 s on a
    sample); a sample without a value of the filter is asked again in full."""
    import rdfsolve.mining.enrichment as enrichment

    monkeypatch.setattr(enrichment, "EXAMPLE_SAMPLE", 1)
    dataset = Dataset()
    graph = dataset.graph(URIRef("urn:chosen"))
    for i in range(5):
        graph.add((EX[f"s{i}"], RDF.type, EX.A))
        graph.add((EX[f"s{i}"], EX.p, Literal(f"text {i}")))
    graph.add((EX.s4, EX.p, Literal(7)))
    integer = "http://www.w3.org/2001/XMLSchema#integer"
    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="test"),
        patterns=[
            SchemaPattern(
                subject_class=str(EX.A),
                property_uri=str(EX.p),
                object_class="Literal",
                datatype=integer,
            )
        ],
    )
    asked = []

    def select(query, **kwargs):
        asked.append(query)
        return json.loads(dataset.query(query).serialize(format="json"))

    helper = Mock(endpoint_url="https://example.org/sparql", select=Mock(side_effect=select))
    result = query_enrichment(schema, helper, ["urn:chosen"], examples_per_pattern=1)
    assert [e.value.value for e in result.examples] == ["7"] and result.state == "complete"
    examples = [q for q in asked if str(EX.p) in q and "datatype" in q]
    inner = "{ SELECT ?subject ?value WHERE"
    assert inner in examples[0], "the first query filters a sample"
    assert len(examples) == 2 and inner not in examples[1], (
        "the full query when the sample has none"
    )
