"""Test SchemaMiner."""
from unittest.mock import MagicMock, patch
from rdfsolve.miner import SchemaMiner


@patch("rdfsolve.miner.SparqlHelper")
def test_miner_init(mock_helper):
    miner = SchemaMiner(endpoint_url="http://example.org/sparql")
    assert miner.endpoint_url == "http://example.org/sparql"
    assert miner.timeout == 120.0


@patch("rdfsolve.miner.SparqlHelper")
def test_miner_query_dataset_metadata(mock_cls):
    mock_helper = MagicMock()
    mock_helper.construct.return_value = ""
    mock_helper.endpoint_url = "http://example.org/sparql"
    mock_cls.return_value = mock_helper
    miner = SchemaMiner(endpoint_url="http://example.org/sparql")
    metadata = miner.query_dataset_metadata()
    assert isinstance(metadata, dict)


@patch("rdfsolve.miner.SparqlHelper")
def test_miner_mine_with_mock_data(mock_cls):
    mock_helper = MagicMock()
    mock_helper.execute_paginated_select.return_value = []
    mock_helper.construct.return_value = ""
    mock_helper.endpoint_url = "http://example.org/sparql"
    mock_cls.return_value = mock_helper
    miner = SchemaMiner(endpoint_url="http://example.org/sparql")
    schema = miner.mine()
    assert schema is not None
