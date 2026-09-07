"""Check source text, RDF terms, query scope, and export annotations."""

import json
from unittest.mock import Mock

import pytest
from rdflib import Dataset, Graph, Literal, Namespace, RDF, URIRef
from rdflib.plugins.sparql.parser import parseQuery

from rdfsolve.miner import SchemaMiner
from rdfsolve.mining.enrichment import (
    class_example_query,
    definition_query,
    example_query,
    query_enrichment,
)
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.sparql_helper import EndpointError

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
    return schema, helper


def test_enrichment_keeps_language_and_scope(source):
    schema, helper = source
    result = query_enrichment(schema, helper, ["urn:chosen"], examples_per_pattern=1)
    assert result.state == "complete"
    assert result.description(str(EX.A)) == 'A "useful" definition.'
    assert result.description(str(EX.p)) == "A measured value."
    assert len(result.examples) == 2
    assert result.query_count == 3  # Definitions, batched classes, batched patterns.
    literal = next(example.value for example in result.examples if example.value.kind == "literal")
    assert literal.language == "en"
    assert literal.value == "hello"
    assert result.class_examples[str(EX.B)][0].value == str(EX.two)
    schema.enrichment = result
    assert MinedSchema.from_dict(schema.to_dict()) == schema


def test_enrichment_failure_is_not_missing_text(source):
    schema, helper = source
    helper.select.side_effect = EndpointError("offline")
    result = query_enrichment(schema, helper, examples_per_pattern=0)
    assert result.state == "failed"
    assert result.failures[0].category == "endpoint"


@pytest.mark.parametrize("scope", [None, ["urn:g"], ["urn:a", "urn:b"]])
@pytest.mark.parametrize("kind", ["Literal", "Resource", "BlankNode", "urn:B"])
def test_enrichment_queries_parse(scope, kind):
    pattern = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class=kind)
    parseQuery(example_query(pattern, scope, 2))
    parseQuery(class_example_query("urn:A", scope, 2))
    parseQuery(definition_query(["urn:A"], scope))


def test_enrichment_exports_do_not_add_validation_defaults(source):
    schema, helper = source
    schema.enrichment = query_enrichment(schema, helper, ["urn:chosen"], examples_per_pattern=1)
    graph = schema.to_void_graph()
    void = Namespace("http://rdfs.org/ns/void#")
    sh = Namespace("http://www.w3.org/ns/shacl#")
    assert list(graph.triples((None, void.exampleResource, EX.one)))
    assert (EX.one, EX.p, Literal("hello", lang="en")) in graph
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert list(shapes.triples((None, sh.description, Literal("A measured value."))))
    assert not list(shapes.triples((None, sh.defaultValue, None)))
    restored = MinedSchema.from_dict(schema.to_jsonld())
    assert restored.enrichment.description(str(EX.A)) == 'A "useful" definition.'
    assert any(example.value.language == "en" for example in restored.enrichment.examples)
    namespace = {}
    exec(schema.to_pydantic(), namespace)
    model = namespace["Thing"]
    assert model.__doc__ == 'A "useful" definition.'
    assert "hello" in model.model_fields["p"].examples
    assert model.model_fields["p"].default is None
    linkml = schema.to_linkml()
    from linkml_runtime.dumpers import json_dumper

    document = json_dumper.to_dict(linkml)
    assert any(
        cls.get("description") == 'A "useful" definition.' for cls in document["classes"].values()
    )
    assert any(slot.get("examples") for slot in document["slots"].values())


def test_miner_enriches_before_finalizing_report(source, monkeypatch):
    schema, helper = source
    miner = SchemaMiner(helper.endpoint_url, graph_uris=["urn:chosen"], enrich=True, delay=0)
    monkeypatch.setattr(miner, "_helper", helper)
    monkeypatch.setattr(miner, "_mine_schema", lambda name: schema)
    result = miner.mine("test")
    assert result.enrichment.state == "complete"
    assert miner.last_report.config["enrich"] is True
    assert any(
        phase.name == "enrichment" and phase.finished_at for phase in miner.last_report.phases
    )


def test_rdf_term_does_not_normalize_lexical_form():
    term = RdfTerm(kind="literal", value="01", datatype="http://www.w3.org/2001/XMLSchema#integer")
    assert str(term.to_rdf()) == "01"
