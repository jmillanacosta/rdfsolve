from rdfsolve.mappings.sssom import create_sssom_mappings
from sssom import Mapping
from sssom.context import ensure_converter


def test_custom_prefix_updates_converter(monkeypatch):
    monkeypatch.setattr("sssom.context.get_converter", lambda: ensure_converter(use_defaults=False))
    mapping = Mapping(
        subject_id="rdfsolve:A",
        predicate_id="skos:relatedMatch",
        object_id="rdfsolve:B",
        mapping_justification="semapv:ManualMappingCuration",
    )
    result = create_sssom_mappings([mapping], "https://example.org/mappings")
    assert result.converter.expand("rdfsolve:A") == "https://w3id.org/rdfsolve/A"
    assert result.converter.compress("https://w3id.org/rdfsolve/B") == "rdfsolve:B"


def test_no_creator_is_assumed(monkeypatch):
    monkeypatch.setattr("sssom.context.get_converter", lambda: ensure_converter(use_defaults=False))
    mapping = Mapping(
        subject_id="rdfsolve:A",
        predicate_id="skos:relatedMatch",
        object_id="rdfsolve:B",
        mapping_justification="semapv:ManualMappingCuration",
    )
    anonymous = create_sssom_mappings([mapping], "https://example.org/mappings")
    assert "creator_id" not in anonymous.metadata, "The creator comes from the caller only"
    named = create_sssom_mappings(
        [mapping], "https://example.org/mappings", creator_id="https://orcid.org/0000-0000-0000-0000"
    )
    assert named.metadata["creator_id"] == ["https://orcid.org/0000-0000-0000-0000"]
