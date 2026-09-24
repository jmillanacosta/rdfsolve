import logging
from pathlib import Path

import pytest
from rdflib import Graph
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.vocab import unregistered_terms

DATA = Path(__file__).parent / "test_data/aopwikirdf_phenobarbital_excerpt.ttl"


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


    from rdfsolve import MinedSchema, SchemaPattern
    from rdflib import Literal, Namespace, XSD
    from linkml_runtime.utils.schemaview import SchemaView
    sample = MinedSchema(about={"dataset_name": "api-check"}, patterns=[
        SchemaPattern(subject_class="urn:Record", property_uri="http://purl.org/dc/terms/date",
                      property_label="date", object_class="Literal", datatype=str(XSD.date))
    ])
    linkml = sample.to_linkml()
    slots = SchemaView(linkml).all_slots()
    assert "date" not in slots
    assert len(slots) == 1 and next(iter(slots.values())).slot_uri == "http://purl.org/dc/terms/date"
    sh = Namespace("http://www.w3.org/ns/shacl#")
    caplog.clear()
    observed = Graph().parse(data=sample.to_shacl(), format="turtle")
    assert "1 deactivated" in caplog.text and "activate_observed=True" in caplog.text
    assert list(observed.subjects(sh.deactivated, Literal(True)))
    active = Graph().parse(data=sample.to_shacl(activate_observed=True), format="turtle")
    assert not list(active.subjects(sh.deactivated, Literal(True)))
