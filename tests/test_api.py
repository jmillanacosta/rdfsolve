"""Test core API functions."""
from unittest.mock import MagicMock, patch
from rdflib import Graph
import rdfsolve


def test_load_parser_from_file(tmp_path):
    void_file = tmp_path / "test.ttl"
    void_file.write_text("@prefix void: <http://rdfs.org/ns/void#> .\n<http://ex.org/ds> a void:Dataset .")
    parser = rdfsolve.load_parser_from_file(str(void_file))
    assert parser is not None


def test_graph_to_jsonld():
    g = Graph()
    g.parse(data="@prefix void: <http://rdfs.org/ns/void#> .\n<http://ex.org/ds> a void:Dataset .", format="turtle")
    result = rdfsolve.graph_to_jsonld(g)
    assert isinstance(result, dict)


@patch("rdfsolve.miner.SchemaMiner")
def test_mine_schema(mock_cls):
    mock_schema = MagicMock()
    mock_schema.to_jsonld.return_value = {"@graph": []}
    mock_cls.return_value.mine.return_value = mock_schema
    schema = rdfsolve.mine_schema("http://example.org/sparql")
    assert schema is not None


@patch("rdfsolve.miner.SchemaMiner")
def test_query_metadata(mock_cls):
    mock_cls.return_value.query_dataset_metadata.return_value = {"source_license": "http://ex.org/l"}
    result = rdfsolve.query_metadata("http://example.org/sparql")
    assert "source_license" in result


def test_load_sources(tmp_path):
    f = tmp_path / "sources.yaml"
    f.write_text("- name: test1\n  endpoint: http://ex.org/sparql\n")
    sources = rdfsolve.load_sources(f)
    assert len(sources) == 1


def test_execute_sparql():
    with patch("rdfsolve.query.execute_sparql") as mock_exec:
        mock_result = MagicMock()
        mock_result.model_dump.return_value = {"bindings": []}
        mock_exec.return_value = mock_result
        result = rdfsolve.execute_sparql("SELECT * WHERE {?s ?p ?o}", "http://ex.org/sparql")
        assert "bindings" in result
