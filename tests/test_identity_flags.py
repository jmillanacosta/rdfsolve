"""Checks of a declared identity between two identifiers: an identifier that does not match the
Bioregistry pattern of the namespace it is filed under. Nothing is hardcoded about the kind of
entity that an identifier names (owner, 2026-10-02)."""

from rdfsolve.mappings.identity import identity_flags


def test_an_identifier_that_does_not_match_its_namespace_is_flagged():
    assert identity_flags("cas:182431-12-5", "kegg.compound:D09637") == [
        "namespace:kegg.compound:D09637 does not match the kegg.compound pattern"
    ], "AOP-Wiki files a KEGG DRUG identifier as a KEGG compound"
    assert identity_flags("hgnc:1001", "uniprot:P53_HUMAN") == [
        "namespace:uniprot:P53_HUMAN does not match the uniprot pattern"
    ], "An entry name is not an accession"


def test_no_kind_of_entity_is_assumed():
    assert identity_flags("hgnc:1001", "uniprot:A0A0C4DH53") == [], "Gene and protein: not read"
    assert identity_flags("hgnc:1001", "ensembl:ENSG00000113916") == []
    assert identity_flags("cas:182431-12-5", "wikidata:Q1268941") == []


def test_standard_forms_and_unknown_namespaces_are_not_flagged():
    assert identity_flags("hgnc:1001", "mgi:MGI:101757") == [], "The banana is standardized"
    assert identity_flags("cas:182431-12-5", "chebi:72297") == []
    assert identity_flags("lab:x1", "lab2:y") == [], "No pattern, no check"
