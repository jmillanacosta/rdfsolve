"""rdfsolve.mappings.identity: a declared identity between two identifiers is flagged when an
identifier does not match the Bioregistry pattern of the namespace it is filed under. Nothing is
assumed about the kind of entity an identifier names."""

import pytest

from rdfsolve.mappings.identity import identity_flags


@pytest.mark.parametrize(
    ("subject", "target", "flags"),
    [
        # a KEGG DRUG identifier filed as a KEGG compound
        (
            "cas:182431-12-5",
            "kegg.compound:D09637",
            ["namespace:kegg.compound:D09637 does not match the kegg.compound pattern"],
        ),
        # an entry name is not an accession
        (
            "hgnc:1001",
            "uniprot:P53_HUMAN",
            ["namespace:uniprot:P53_HUMAN does not match the uniprot pattern"],
        ),
        # no kind of entity is read: gene and protein, gene and gene, chemical and item
        ("hgnc:1001", "uniprot:A0A0C4DH53", []),
        ("hgnc:1001", "ensembl:ENSG00000113916", []),
        ("cas:182431-12-5", "wikidata:Q1268941", []),
        # standard forms (a banana) and namespaces without a pattern are not flagged
        ("hgnc:1001", "mgi:MGI:101757", []),
        ("cas:182431-12-5", "chebi:72297", []),
        ("lab:x1", "lab2:y", []),
    ],
)
def test_identity_flags(subject, target, flags):
    assert identity_flags(subject, target) == flags
