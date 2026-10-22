"""rdfsolve.mappings.declared: declared identities (cross-references, exactMatch, sameAs) are
written as SSSOM with the problems of each statement; a file with statements that fail the checks
is marked so, also in the release manifest."""

import json

from rdfsolve.mappings.declared import (
    declared_identities,
    is_declared_property,
    write_declared_identities,
)
from rdfsolve.release.build import inventory_artifacts

XREF = "http://bio2rdf.org/hgnc_vocabulary:x-ensembl"
EXACT = "http://www.w3.org/2004/02/skos/core#exactMatch"
GENE = "http://bio2rdf.org/hgnc:5"
CC0 = "https://spdx.org/licenses/CC0-1.0"


def binding(subject, prop, obj):
    return {"s": {"value": subject}, "p": {"value": prop}, "o": {"value": obj}}


def test_properties_that_declare_identity():
    for prop in (XREF, EXACT, "http://www.w3.org/2002/07/owl#sameAs"):
        assert is_declared_property(prop)
    assert not is_declared_property("http://bio2rdf.org/hgnc_vocabulary:symbol")


def test_statements_are_read_flagged_and_counted(tmp_path):
    rows = declared_identities(
        [
            binding(GENE, XREF, "http://bio2rdf.org/ensembl:ENSG00000121410"),
            binding(GENE, XREF, "http://bio2rdf.org/uniprot:P53_HUMAN"),
            binding("http://bio2rdf.org/ensembl:ENSG00000121410", EXACT, GENE),
            binding(GENE, EXACT, "http://bio2rdf.org/hgnc:6"),
            binding(GENE, EXACT, "not an iri"),
        ],
        "hgnc",
    )
    assert len(rows) == 2, (
        "The repeated pair, the same-namespace pair and the unread value are left out"
    )
    clean, wrong = rows
    assert clean.flags == [] and clean.comment == "hgnc x-ensembl"
    assert wrong.flags == ["namespace:uniprot:P53_HUMAN does not match the uniprot pattern"]
    summary = write_declared_identities(
        rows, tmp_path, "hgnc", license_uri="https://spdx.org/licenses/CC-BY-SA-4.0"
    )
    assert (summary["check"], summary["statements"], summary["flagged"]) == (
        "flagged_statements",
        2,
        1,
    )
    assert summary["flags"] == {"clean": 1, "namespace": 1}
    assert json.loads((tmp_path / "hgnc_declared_identities.json").read_text()) == summary
    table = (tmp_path / "hgnc_declared_identities.sssom.tsv").read_text()
    assert (
        "skos:exactMatch" in table
        and "does not match the uniprot pattern" in table
        and "mapping_set_id:" in table
    )
    assert "license: https://spdx.org/licenses/CC-BY-SA-4.0" in table, "The licence of the source"
    only_clean = declared_identities(
        [binding(GENE, XREF, "http://bio2rdf.org/ensembl:ENSG00000121410")], "hgnc"
    )
    assert (
        write_declared_identities(only_clean, tmp_path / "c", "hgnc", license_uri=CC0)["check"]
        == "no_flagged_statements"
    )


def test_an_identifier_restated_as_a_resolver_iri_is_not_a_cross_namespace_identity():
    resolver = binding(
        "http://bio2rdf.org/mgi:101757",
        "http://bio2rdf.org/hgnc_vocabulary:x-identifiers.org",
        "http://identifiers.org/mgi/101757",
    )
    assert declared_identities([resolver], "hgnc") == []


def test_the_release_lists_declared_identities_with_their_check(tmp_path):
    bindings = [
        binding(GENE, XREF, o)
        for o in (
            "http://bio2rdf.org/ensembl:ENSG00000121410",
            "http://bio2rdf.org/uniprot:P53_HUMAN",
        )
    ]
    write_declared_identities(
        declared_identities(bindings, "hgnc"), tmp_path / "hgnc", "hgnc", license_uri=CC0
    )
    (tmp_path / "hgnc" / "hgnc_schema.json").write_text("{}")
    by_role = {a.role: a for a in inventory_artifacts(tmp_path)}
    table = by_role["declared_identities"]
    assert (table.dataset_id, table.identity_check) == ("hgnc", "flagged_statements")
    assert (
        table.identity_check_note == "1 of 2 declared identities are flagged by the identity checks"
    )
    assert by_role["declared_identities_summary"].identity_check == "flagged_statements"
    assert by_role["canonical_schema"].identity_check is None
