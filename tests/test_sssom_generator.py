"""Check mapping-set prefix registration."""

from sssom import Mapping
from sssom.context import ensure_converter

from rdfsolve.sssom_generator import create_sssom_mappings


def test_custom_prefix_updates_converter(monkeypatch):
    monkeypatch.setattr(
        "sssom.context.get_converter",
        lambda: ensure_converter(use_defaults=False),
    )
    mapping = Mapping(
        subject_id="rdfsolve:A",
        predicate_id="skos:relatedMatch",
        object_id="rdfsolve:B",
        mapping_justification="semapv:ManualMappingCuration",
    )
    result = create_sssom_mappings([mapping], "https://example.org/mappings")
    assert result.converter.expand("rdfsolve:A") == "https://rdfsolve.bigcat-bioinformatics.nl/A"
    assert result.converter.compress("https://rdfsolve.bigcat-bioinformatics.nl/B") == "rdfsolve:B"
