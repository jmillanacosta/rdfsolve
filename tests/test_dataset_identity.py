import pytest
import yaml
from rdfsolve.dataset_identity import read_overrides, resolve_identity

EP = "https://example.org/sparql"


def entry(name, endpoint=EP, graphs=(), **extra):
    return {"name": name, "endpoint": endpoint, "graph_uris": list(graphs), **extra}


def test_overrides_decide_pairs_and_group_aliases(tmp_path):
    path = tmp_path / "overrides.yaml"
    path.write_text(
        yaml.safe_dump(
            [
                {"left": "a", "right": "b", "relation": "same_dataset", "note": "mirror"},
                {"left": "b", "right": "c", "relation": "same_dataset"},
            ]
        )
    )
    overrides = read_overrides(path)
    result = resolve_identity(
        [
            entry("a", graphs=["urn:g1"]),
            entry("b", graphs=["urn:g2"]),
            entry("c", "https://x.org/sparql"),
            entry("d", "https://y.org/sparql"),
            entry("service", source_role="service"),
        ],
        overrides,
    )
    assert result.datasets == {"a": ["a", "b", "c"], "d": ["d"]}
    assert all((item.decided_by == "override" for item in result.relations))
    assert result.entries[-1].source_role == "service", "Service provenance"
    assert result.candidates == []
    assert result.review_complete is True
    assert result.canonical_dataset_count == 2
    unresolved = resolve_identity([entry("a"), entry("b")])
    assert unresolved.candidates and unresolved.canonical_dataset_count is None, (
        "Unreviewed identity"
    )
    with pytest.raises(ValueError, match="more than once"):
        resolve_identity([entry("a"), entry("b")], [overrides[0], overrides[0]])
