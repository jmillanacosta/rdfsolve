from unittest.mock import MagicMock, patch

import rdfsolve


@patch("rdfsolve.mining.miner.SchemaMiner")
def test_mine_schema(mock_cls):
    mock_schema = MagicMock()
    mock_schema.to_jsonld.return_value = {"@graph": []}
    mock_cls.return_value.mine.return_value = mock_schema
    schema = rdfsolve.mine_schema("http://example.org/sparql")
    assert schema is mock_schema
    mock_cls.return_value.mine.assert_called_once()
