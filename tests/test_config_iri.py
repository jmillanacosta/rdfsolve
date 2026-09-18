"""Mint rdfsolve resource IRIs under one base."""

import pytest

from rdfsolve.config import DEFAULT_BASE_URI, get_base_uri, mint


def test_mint_encodes_each_segment_under_the_base(monkeypatch):
    monkeypatch.delenv("RDFSOLVE_BASE_URI", raising=False)
    assert get_base_uri() == DEFAULT_BASE_URI == "https://w3id.org/rdfsolve/"
    assert mint("dataset", "pubchem.ftp.gene") == "https://w3id.org/rdfsolve/dataset/pubchem.ftp.gene"
    assert mint("graph", "a/b c") == "https://w3id.org/rdfsolve/graph/a%2Fb%20c"
    assert mint("hash", "sha256", "ab") == "https://w3id.org/rdfsolve/hash/sha256/ab"


def test_environment_base_is_normalized(monkeypatch):
    monkeypatch.setenv("RDFSOLVE_BASE_URI", "https://example.test/base")
    assert mint("dataset", "x") == "https://example.test/base/dataset/x"


@pytest.mark.parametrize("parts", [("",), ("a/b",), ("dataset", "")])
def test_mint_rejects_empty_or_nested_segments(parts):
    with pytest.raises(ValueError):
        mint(*parts)
