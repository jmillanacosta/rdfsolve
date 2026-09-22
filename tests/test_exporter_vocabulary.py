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


def test_void_and_shacl_exports_use_registered_terms(schema):
    for graph in (schema.to_void_graph(), Graph().parse(data=schema.to_shacl(), format="turtle")):
        assert unregistered_terms(graph, source_terms(schema)) == set()
