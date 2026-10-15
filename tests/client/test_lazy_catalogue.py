"""Writing records builds no discovery catalogue and loads no registry: the first from_table is
as fast as the next one. The catalogue is built on the first discovery call and keeps the
records made before."""

import sys

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mining.miner import SchemaMiner

DATA = """
@prefix ex: <https://example.org/> .
ex:a a ex:Thing ; ex:name "A" .
"""


def test_records_are_made_without_building_the_catalogue():
    data = Dataset().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(data, delay=0) as miner:
        schema = miner.mine("lazy")
    with Client(schema, data) as client:
        made = client.from_table("https://example.org/Thing", ["https://example.org/a"])
        assert "catalogue" not in vars(client), "No catalogue for writing records"
        assert made._references is None
        catalogue = client.catalogue
        assert made._references is not None, "Retained when the catalogue is built"
        assert made.references and all(ref in catalogue.records for ref in made.references)


def test_names_are_split_without_loading_bioregistry(monkeypatch):
    from rdfsolve import naming

    naming._protected.cache_clear()
    monkeypatch.setitem(sys.modules, "bioregistry", None)  # any import of it would fail
    try:
        assert naming.words("hasAOPWikiEvent") == ["has", "AOPWiki", "Event"]
    finally:
        naming._protected.cache_clear()
