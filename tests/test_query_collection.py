from pathlib import Path
from unittest.mock import Mock

import pytest
from rdflib import PROV, Graph, URIRef
from rdflib.compare import isomorphic
from rdfsolve.sparql_helper import SparqlHelper

DATA = Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl"


def test_named_queries_roundtrip_and_session_isolation(monkeypatch):
    data = Graph().parse(DATA, format="turtle")
    query = "SELECT ?dataset WHERE { ?dataset a <http://rdfs.org/ns/void#Dataset> }"
    with (
        SparqlHelper("https://example.org/sparql") as first,
        SparqlHelper("https://example.org/sparql") as second,
    ):
        first.add_query(
            'Datasets "source"\nlist', query, description='Quotes " and backslashes \\ survive.'
        )
        with pytest.raises(ValueError, match="unique"):
            first.add_query('Datasets "source"\nlist', query)
        first.queries.rename('Datasets "source"\nlist', "datasets")
        second.load_shacl(Graph().parse(data=first.export_queries_as_ttl(), format="turtle"))
        assert isomorphic(first.queries.graph, second.queries.graph)
        assert set(second.queries.queries) == {"datasets"}
        result = list(data.query(second.queries.queries["datasets"].query))
        assert result and URIRef("https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF") in {
            row[0] for row in result
        }
        execute = Mock(return_value={"boolean": False})
        monkeypatch.setattr(first, "_execute", execute)
        first.add_query("empty", "ASK { FILTER(false) }")
        assert first.run_query("empty") is False
        assert first.history[-1].success
        execute.return_value = {"boolean": "broken"}
        with pytest.raises(Exception, match="boolean"):
            first.run_query("empty")
        assert not first.history[-1].success
        assert first.history[-1].name == "empty"
        assert not second.history
        assert "empty" not in second.queries.queries
        first.enable_query_collection()
        monkeypatch.undo()
        monkeypatch.setattr(
            first,
            "_get_query",
            Mock(return_value=data.query(query).serialize(format="json").decode()),
        )
        first.select(query)
        assert len(first.get_collected_queries()) == 1
        assert not second.get_collected_queries()

        source = "https://example.org/recipes/mesh"
        conversion = """SELECT ?text ?iri WHERE {
            VALUES ?text { "MESH:D000001" }
            BIND(IRI(REPLACE(?text, "MESH:", "http://id.nlm.nih.gov/mesh/")) AS ?iri)
        }"""
        saved = first.queries.add("mesh conversion", conversion, source=source)
        from rdfsolve.query_collection import QueryCollection
        archive = QueryCollection()
        archive.load_shacl(Graph().parse(data=first.queries.to_turtle(), format="turtle"))
        restored = archive.queries["mesh conversion"]
        assert (restored.node, PROV.wasDerivedFrom, URIRef(source)) in archive.graph
        row = list(Graph().query(restored.query))[0]
        assert str(row.text) == "MESH:D000001"
        assert row.iri == URIRef("http://id.nlm.nih.gov/mesh/D000001")
        assert saved.query == restored.query
