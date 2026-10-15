"""SchemaMiner(vocabulary_graph_uris=...): names and descriptions read from a vocabulary graph
that is not mined."""

from __future__ import annotations

import pyoxigraph as ox
from rdflib import SH, Graph, URIRef

from rdfsolve.mining.miner import SchemaMiner

DATA, VOCABULARY = "urn:g:data", "urn:g:vocabulary"
QUADS = f"""
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<{DATA}> {{
  <urn:a1> a <urn:A> ; <urn:name> "one" .
  <urn:a2> a <urn:A> ; <urn:name> "two" .
}}
<{VOCABULARY}> {{
  <urn:A> rdfs:label "Thing"@en .
  <urn:name> rdfs:label "name"@en ; rdfs:comment "What it is called." .
}}
"""


def _mine(**options):
    store = ox.Store()
    store.load(QUADS.encode(), format=ox.RdfFormat.TRIG)
    with SchemaMiner.from_graph(store, delay=0, enrich=True, graph_uris=[DATA], **options) as miner:
        return miner.mine(dataset_name="fixture")


def test_names_come_from_the_vocabulary_graph():
    schema = _mine(vocabulary_graph_uris=[VOCABULARY])
    mined = {p.property_uri for p in schema.patterns}
    assert "urn:name" in mined
    assert "http://www.w3.org/2000/01/rdf-schema#label" not in mined, "the vocabulary is not mined"
    shapes = Graph().parse(data=schema.to_shacl(void=False), format="turtle")
    shape = next(shapes.subjects(SH.path, URIRef("urn:name")))
    assert str(shapes.value(shape, SH.name)) == "name"
    assert str(shapes.value(shape, SH.description)) == "What it is called."
    assert schema.enrichment.label("urn:A") == "Thing"


def test_without_the_option_the_vocabulary_graph_is_not_read():
    schema = _mine()
    assert schema.enrichment.label("urn:name") is None
