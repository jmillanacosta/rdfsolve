"""rdfsolve.schema_models VoID: VoID descriptions are read and converted, and exporters write only
registered vocabulary terms."""

import logging
from pathlib import Path

import pytest
from rdflib import Graph

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.schema_models.readers.void import void_to_minedschema
from rdfsolve.schema_models.void_model import (
    VoidClassPartition,
    VoidDataset,
    VoidPropertyPartition,
)
from rdfsolve.vocab import unregistered_terms


def test_nested_partitions():
    """Test nested class and property partitions."""
    dataset = VoidDataset(
        uri="http://example.org/dataset",
        class_partitions=[
            VoidClassPartition(
                uri="http://example.org/cp/1",
                class_uri="http://example.org/Person",
                triples=100,
                property_partitions=[
                    VoidPropertyPartition(
                        uri="http://example.org/pp/1",
                        property_uri="http://example.org/name",
                        triples=100,
                    )
                ],
            )
        ],
    )
    g = Graph()
    dataset.to_rdf(g)
    from rdflib import URIRef

    parsed = VoidDataset.from_rdf(g, URIRef(dataset.uri))
    assert len(parsed.class_partitions) == 1
    assert parsed.class_partitions[0].class_uri == "http://example.org/Person"
    assert len(parsed.class_partitions[0].property_partitions) == 1


def test_mixed_class_and_datatype_partitions_keep_their_own_counts():
    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="mixed"),
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class=kind,
                datatype=datatype,
                count=count,
            )
            for kind, datatype, count in [
                ("urn:B", None, 0),
                ("urn:C", None, None),
                ("Literal", "http://www.w3.org/2001/XMLSchema#string", 2**54 + 1),
                ("Literal", "http://www.w3.org/2001/XMLSchema#integer", 12),
            ]
        ],
    )
    restored = MinedSchema.from_dict(schema.to_jsonld())
    key = lambda p: (p.object_class, p.datatype, p.count)
    assert {key(p) for p in restored.patterns} == {key(p) for p in schema.patterns}
    from_void = void_to_minedschema(schema.to_void_graph().serialize(format="turtle"))
    assert {key(p) for p in from_void.patterns} == {key(p) for p in schema.patterns}
    assert len(MinedSchema.from_shacl(from_void.to_shacl()).patterns) == 4


def test_shapes_can_be_written_without_the_void_description():
    from rdflib import RDF, Graph, Namespace

    from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

    sh, void = Namespace("http://www.w3.org/ns/shacl#"), Namespace("http://rdfs.org/ns/void#")
    pattern = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
    schema = MinedSchema(about=AboutMetadata.build(dataset_name="d"), patterns=[pattern])
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    assert set(shapes.subjects(RDF.type, sh.NodeShape)), "The shapes stay"
    assert not set(shapes.subjects(RDF.type, void.Dataset)), "The VoID is published on its own"


DATA = Path(__file__).parents[1] / "test_data/aopwikirdf_phenobarbital_excerpt.ttl"


@pytest.fixture(scope="module")
def schema():
    logging.disable(logging.WARNING)
    try:
        with SchemaMiner.from_graph(Graph().parse(DATA)) as miner:
            mined = miner.mine()
        mined.discover_paths(max_hops=2)
    finally:
        logging.disable(logging.NOTSET)
    return mined


def source_terms(schema):
    enrichment = [*schema.enrichment.labels, *schema.enrichment.definitions]
    return (
        {item.predicate for item in enrichment}
        | set(schema.get_classes())
        | set(schema.get_properties())
    )


def test_void_and_shacl_exports_use_registered_terms(schema, caplog):
    for graph in (schema.to_void_graph(), Graph().parse(data=schema.to_shacl(), format="turtle")):
        assert unregistered_terms(graph, source_terms(schema)) == set()

    from linkml_runtime.utils.schemaview import SchemaView
    from rdflib import XSD, Literal, Namespace

    from rdfsolve import MinedSchema, SchemaPattern

    sample = MinedSchema(
        about={"dataset_name": "api-check"},
        patterns=[
            SchemaPattern(
                subject_class="urn:Record",
                property_uri="http://purl.org/dc/terms/date",
                property_label="date",
                object_class="Literal",
                datatype=str(XSD.date),
            )
        ],
    )
    linkml = sample.to_linkml()
    slots = SchemaView(linkml).all_slots()
    assert "date" not in slots
    assert (
        len(slots) == 1 and next(iter(slots.values())).slot_uri == "http://purl.org/dc/terms/date"
    )
    sh = Namespace("http://www.w3.org/ns/shacl#")
    caplog.clear()
    observed = Graph().parse(data=sample.to_shacl(), format="turtle")
    assert "1 deactivated" in caplog.text and "activate_observed=True" in caplog.text
    assert list(observed.subjects(sh.deactivated, Literal(True)))
    active = Graph().parse(data=sample.to_shacl(activate_observed=True), format="turtle")
    assert not list(active.subjects(sh.deactivated, Literal(True)))
