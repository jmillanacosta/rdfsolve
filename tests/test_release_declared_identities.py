"""The release lists the declared identities of a dataset with the result of their checks, so
that a file with flagged statements is marked as such in the manifest."""

from rdfsolve.mappings.declared import declared_identities, write_declared_identities
from rdfsolve.release.build import inventory_artifacts

XREF = "http://bio2rdf.org/hgnc_vocabulary:x-ensembl"


def test_declared_identities_carry_their_check(tmp_path):
    bindings = [
        {"s": {"value": "http://bio2rdf.org/hgnc:5"}, "p": {"value": XREF}, "o": {"value": obj}}
        for obj in ("http://bio2rdf.org/ensembl:ENSG00000121410", "http://bio2rdf.org/uniprot:P53_HUMAN")
    ]
    write_declared_identities(
        declared_identities(bindings, "hgnc"), tmp_path / "hgnc", "hgnc", license_uri="https://spdx.org/licenses/CC0-1.0"
    )
    (tmp_path / "hgnc" / "hgnc_schema.json").write_text("{}")
    by_role = {a.role: a for a in inventory_artifacts(tmp_path)}
    table = by_role["declared_identities"]
    assert table.dataset_id == "hgnc"
    assert table.identity_check == "flagged_statements"
    assert table.identity_check_note == "1 of 2 declared identities are flagged by the identity checks"
    assert by_role["declared_identities_summary"].identity_check == "flagged_statements"
    assert by_role["canonical_schema"].identity_check is None
