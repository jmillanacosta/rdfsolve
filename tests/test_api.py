"""Test core API functions."""
from unittest.mock import MagicMock, patch
import rdfsolve



@patch("rdfsolve.mining.miner.SchemaMiner")
def test_mine_schema(mock_cls):
    mock_schema = MagicMock()
    mock_schema.to_jsonld.return_value = {"@graph": []}
    mock_cls.return_value.mine.return_value = mock_schema
    schema = rdfsolve.mine_schema("http://example.org/sparql")
    assert schema is not None


@patch("rdfsolve.metadata.SparqlHelper")
def test_query_metadata(mock_cls):
    helper = mock_cls.return_value.__enter__.return_value
    helper.construct.return_value = ""
    helper.endpoint_url = "http://example.org/sparql"
    result = rdfsolve.query_metadata(helper.endpoint_url)
    assert len(result.graph) == 0
    assert result.project() == {}


def test_load_sources(tmp_path):
    f = tmp_path / "sources.yaml"
    f.write_text("- name: test1\n  endpoint: http://ex.org/sparql\n")
    sources = rdfsolve.load_sources(f)
    assert len(sources) == 1


def test_public_api_reexports_implementations():
    from rdfsolve.client.query import execute_sparql
    from rdfsolve.void_source import discover_void_source

    assert rdfsolve.execute_sparql is execute_sparql
    assert rdfsolve.discover_void_source is discover_void_source
