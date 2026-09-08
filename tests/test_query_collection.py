"""Exercise query collections against RDF parsing and local AOPWiki data."""

from pathlib import Path
from unittest.mock import Mock

import pytest
from rdflib import RDF, RDFS, SH, Graph, Literal, URIRef
from rdflib.compare import isomorphic

from rdfsolve.query_collection import QueryCollection
from rdfsolve.sparql_helper import SparqlHelper

DATA = Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl"


def test_named_queries_roundtrip_and_session_isolation(monkeypatch):
    data = Graph().parse(DATA, format="turtle")
    query = 'SELECT ?dataset WHERE { ?dataset a <http://rdfs.org/ns/void#Dataset> }'
    with SparqlHelper("https://example.org/sparql") as first, SparqlHelper("https://example.org/sparql") as second:
        first.add_query('Datasets "source"\nlist', query, description='Quotes " and backslashes \\ survive.')
        with pytest.raises(ValueError, match="unique"):
            first.add_query('Datasets "source"\nlist', query)
        first.queries.rename('Datasets "source"\nlist', "datasets")
        second.load_shacl(Graph().parse(data=first.export_queries_as_ttl(), format="turtle"))
        assert isomorphic(first.queries.graph, second.queries.graph)
        assert set(second.queries.queries) == {"datasets"}
        result = list(data.query(second.queries.queries["datasets"].query))
        assert result and URIRef("https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF") in {row[0] for row in result}
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
        first._record_query(query, "SELECT", first.endpoint_url)
        assert len(first.get_collected_queries()) == 1
        assert not second.get_collected_queries()


def test_prefixes_paths_and_validation_context(monkeypatch):
    # Apply a SHACL path to retained AOPWiki RDF, not an invented domain graph.
    graph = Graph().parse(data='''
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        <urn:query> a sh:SPARQLSelectExecutable; rdfs:label "datasets";
          sh:prefixes <urn:prefixes>; sh:select "SELECT ?s WHERE { ?s a void:Dataset }".
        <urn:prefixes> owl:imports <urn:shared> .
        <urn:shared> sh:declare [sh:prefix "void"; sh:namespace "http://rdfs.org/ns/void#"] .
        <urn:downloads> sh:path (<http://rdfs.org/ns/void#subset> <http://purl.org/dc/elements/1.1/description>) .
        <urn:shape> sh:sparql <urn:validation> .
        <urn:validation> sh:select "SELECT $this WHERE { $this $PATH ?value }" .
    ''', format="turtle")
    with SparqlHelper("https://example.org/sparql") as helper:
        helper.load_shacl(graph)
        data = Graph().parse(DATA, format="turtle")
        assert list(data.query(helper.queries.queries["datasets"].query))
        query = helper.queries.path_query(
            "urn:downloads", "https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF")
        # Compare generated traversal with direct RDF traversal.
        root = URIRef("https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF")
        expected = {value for subset in data.objects(root, URIRef("http://rdfs.org/ns/void#subset"))
                    for value in data.objects(subset, URIRef("http://purl.org/dc/elements/1.1/description"))}
        assert expected
        assert {row[0] for row in data.query(query)} == expected
        execute = Mock()
        monkeypatch.setattr(helper, "_execute", execute)
        with pytest.raises(ValueError, match="context"):
            helper.run_query("urn:validation")
        execute.assert_not_called()
        assert isomorphic(graph, Graph().parse(data=helper.export_queries_as_ttl(), format="turtle"))
        with pytest.raises(ValueError):
            helper.queries.path_query("urn:downloads", "urn:x> } UNION { ?s ?p ?o")


def test_invalid_import_is_atomic():
    collection = QueryCollection()
    collection.add("kept", "ASK {}")
    original = collection.to_turtle()
    graph = Graph()
    node = URIRef("urn:bad")
    graph.add((node, RDF.type, SH.SPARQLSelectExecutable))
    graph.add((node, RDFS.label, Literal("bad")))
    graph.add((node, SH.select, Literal("DELETE WHERE { ?s ?p ?o }")))
    with pytest.raises(Exception):
        collection.load_shacl(graph)
    assert collection.to_turtle() == original
    with pytest.raises(Exception):
        collection.add("update", "INSERT DATA { <urn:s> <urn:p> <urn:o> }")
    graph.remove((node, SH.select, None))
    graph.add((node, SH.select, Literal("SELECT * WHERE { ?s missing:p ?o }")))
    with pytest.raises(Exception):
        collection.load_shacl(graph)
    assert collection.to_turtle() == original
