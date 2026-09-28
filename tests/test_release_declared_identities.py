"""The release lists the declared identities of a dataset with the verdict of their checks, so
that a file whose statements fail the checks is marked bad in the manifest."""

from rdfsolve.mappings.declared import declared_identities, write_declared_identities
from rdfsolve.release.build import inventory_artifacts

XREF = "http://bio2rdf.org/hgnc_vocabulary:x-ensembl"


def test_declared_identities_carry_their_verdict(tmp_path):
    bindings = [
        {"s": {"value": "http://bio2rdf.org/hgnc:5"}, "p": {"value": XREF}, "o": {"value": obj}}
        for obj in ("http://bio2rdf.org/ensembl:ENSG00000121410", "http://bio2rdf.org/ensembl:NM_130786")
    ]
    write_declared_identities(
        declared_identities(bindings, "hgnc"), tmp_path / "hgnc", "hgnc", license_uri="https://spdx.org/licenses/CC0-1.0"
    )
    (tmp_path / "hgnc" / "hgnc_schema.json").write_text("{}")
    by_role = {a.role: a for a in inventory_artifacts(tmp_path)}
    table = by_role["declared_identities"]
    assert table.dataset_id == "hgnc"
    assert table.quality == "bad"
    assert table.quality_note == "1 of 2 declared identities fail the identity checks"
    assert by_role["declared_identities_summary"].quality == "bad"
    assert by_role["canonical_schema"].quality is None
