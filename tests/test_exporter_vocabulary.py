"""rdfsolve exporters write only registered vocabulary terms and source terms."""

import logging
from pathlib import Path

import pytest
from rdflib import Graph

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.query_collection import QueryCollection
from rdfsolve.vocab import GENERATED_TERMS, VOCABULARIES, unregistered_terms

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


def test_registered_terms_belong_to_listed_vocabularies():
    assert all(any(term.startswith(ns) for ns in VOCABULARIES) for term in GENERATED_TERMS)


def test_void_and_shacl_exports_use_registered_terms(schema):
    for graph in (schema.to_void_graph(), Graph().parse(data=schema.to_shacl(), format="turtle")):
        assert unregistered_terms(graph, source_terms(schema)) == set()


def test_query_collection_uses_registered_terms(schema):
    collection = QueryCollection()
    collection.add(
        "Pathways",
        "SELECT ?s WHERE { ?s a ?c }",
        description="All typed resources",
        endpoint="https://example.org/sparql",
        schema=schema,
    )
    graph = Graph().parse(data=collection.to_turtle(), format="turtle")
    assert unregistered_terms(graph, ()) == set()


def test_unregistered_generated_terms_are_reported():
    graph = Graph().parse(
        data="<urn:a> <https://rdfsolve.io/vocab#sourceName> 'x' ; a <urn:Invented> .",
        format="turtle",
    )
    assert unregistered_terms(graph, {"urn:Invented"}) == {"https://rdfsolve.io/vocab#sourceName"}
