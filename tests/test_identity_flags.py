"""Checks of a declared identity between two identifiers: a value filed under another namespace,
and two identifiers of different kinds of entity (seen in HGNC x-ensembl and AOP-Wiki exactMatch)."""

from rdfsolve.mappings.identity import identifier_kind, identity_flags


def test_kinds_are_read_from_the_identifier_pattern():
    assert identifier_kind("ensembl:ENSG00000113916") == ("gene", "ensembl")
    assert identifier_kind("ensembl:ENSP00000000233") == ("protein", "ensembl")
    assert identifier_kind("ensembl:NM_001095") == ("transcript", "refseq"), "A RefSeq accession"
    assert identifier_kind("uniprot:A0A0C4DH53") == ("protein", "uniprot")
    assert identifier_kind("omim:100100") is None, "OMIM names genes and phenotypes"


def test_an_identity_is_flagged_by_what_it_would_join():
    assert identity_flags("hgnc:1001", "ensembl:ENSG00000113916") == []
    assert identity_flags("hgnc:100", "ensembl:NM_001095") == [
        "namespace:ensembl:NM_001095 is a refseq identifier",
        "kind:gene-transcript",
    ]
    assert identity_flags("hgnc:1001", "uniprot:A0A0C4DH53") == ["kind:gene-protein"]
    assert identity_flags("hgnc:1001", "omim:100100") == ["kind:unknown"]
    assert identity_flags("hgnc:7471", "ncbiprotein:NC_012920") == ["kind:gene-genomic region"], (
        "Bioregistry reads Bio2RDF refseq IRIs as ncbiprotein: the same namespace"
    )
