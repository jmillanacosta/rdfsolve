"""One reader of identifiers (rdfsolve.identifiers.parse): IRIs of registered URI formats and
CURIEs, standardized, with their validity. Six readers with different validity rules preceded it
(link signatures, declared identities, identity checks, term keys, client spellings, candidates)."""

import pytest

from rdfsolve.identifiers import candidates, canonical_iri, curie, parse

OBO = "http://purl.obolibrary.org/obo/"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (OBO + "CHEBI_15377", ("chebi", "15377", True)),
        ("CHEBI:15377", ("chebi", "15377", True)),
        ("https://identifiers.org/chebi/72297", ("chebi", "72297", True)),
        ("http://identifiers.org/mgi/101757", ("mgi", "101757", True)),
        ("http://purl.uniprot.org/uniprot/P53_HUMAN", ("uniprot", "P53_HUMAN", False)),
        ("vega:OTTHUMG1", ("vega", "OTTHUMG1", None)),
    ],
)
def test_iris_and_curies_are_read_with_their_validity(value, expected):
    found = parse(value)
    assert (found.prefix, found.local, found.valid) == expected


def test_what_is_not_a_registered_identifier_is_none():
    assert parse("hello") is None and parse("urn:x:y") is None
    assert parse("http://example.org/thing") is None


def test_forms_keys_and_candidates_are_built_on_parse():
    assert curie(OBO + "GO_0006915") == "go:0006915"
    assert curie("http://example.org/thing") == "http://example.org/thing"
    assert canonical_iri("https://identifiers.org/GO:0006915") == OBO + "GO_0006915"
    iris, coverage = candidates("CHEBI:15377")
    assert OBO + "CHEBI_15377" in iris and coverage["basis"] == "registered namespace candidates"
    assert candidates("http://example.org/thing")[0] == ["http://example.org/thing"]
    with pytest.raises(ValueError, match="Invalid"):
        candidates("uniprot:P53_HUMAN")
