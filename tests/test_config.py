from rdfsolve.config import DEFAULT_BASE_URI, get_base_uri, mint


def test_mint_encodes_each_segment_under_the_base(monkeypatch):
    monkeypatch.delenv("RDFSOLVE_BASE_URI", raising=False)
    assert get_base_uri() == DEFAULT_BASE_URI == "https://w3id.org/rdfsolve/"
    assert (
        mint("dataset", "pubchem.ftp.gene") == "https://w3id.org/rdfsolve/dataset/pubchem.ftp.gene"
    )
    assert mint("graph", "a/b c") == "https://w3id.org/rdfsolve/graph/a%2Fb%20c"
    assert mint("hash", "sha256", "ab") == "https://w3id.org/rdfsolve/hash/sha256/ab"
