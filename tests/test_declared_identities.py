"""Declared identities (cross-references, exactMatch, sameAs) are written as SSSOM with the
problems of each statement, and a file that holds statements that fail the checks is marked bad."""

import json

from rdfsolve.mappings.declared import (
    declared_identities,
    is_declared_property,
    write_declared_identities,
)

XREF = "http://bio2rdf.org/hgnc_vocabulary:x-ensembl"
EXACT = "http://www.w3.org/2004/02/skos/core#exactMatch"


def binding(subject, prop, obj):
    return {"s": {"value": subject}, "p": {"value": prop}, "o": {"value": obj}}


def test_properties_that_declare_identity():
    assert is_declared_property(XREF)
    assert is_declared_property(EXACT)
    assert is_declared_property("http://www.w3.org/2002/07/owl#sameAs")
    assert not is_declared_property("http://bio2rdf.org/hgnc_vocabulary:symbol")


def test_statements_are_read_flagged_and_counted(tmp_path):
    gene = "http://bio2rdf.org/hgnc:5"
    rows = declared_identities(
        [
            binding(gene, XREF, "http://bio2rdf.org/ensembl:ENSG00000121410"),
            binding(gene, XREF, "http://bio2rdf.org/ensembl:NM_130786"),
            binding("http://bio2rdf.org/ensembl:ENSG00000121410", EXACT, gene),
            binding(gene, EXACT, "http://bio2rdf.org/hgnc:6"),
            binding(gene, EXACT, "not an iri"),
        ],
        "hgnc",
    )
    assert len(rows) == 2, "The repeated pair, the same-namespace pair and the unread value are left out"
    clean, wrong = rows
    assert clean.flags == [] and clean.comment == "hgnc x-ensembl"
    assert wrong.flags == ["namespace:ensembl:NM_130786 is a refseq identifier", "kind:gene-transcript"]
    summary = write_declared_identities(rows, tmp_path, "hgnc")
    assert summary["verdict"] == "bad"
    assert summary["statements"] == 2 and summary["failing"] == 1
    assert summary["flags"] == {"clean": 1, "namespace": 1, "kind:gene-transcript": 1}
    assert json.loads((tmp_path / "hgnc_declared_identities.json").read_text()) == summary
    table = (tmp_path / "hgnc_declared_identities.sssom.tsv").read_text()
    assert "skos:exactMatch" in table and "kind:gene-transcript" in table
    assert "mapping_set_id:" in table


def test_a_file_without_failing_statements_is_not_bad(tmp_path):
    rows = declared_identities(
        [binding("http://bio2rdf.org/hgnc:5", XREF, "http://bio2rdf.org/ensembl:ENSG00000121410")], "hgnc"
    )
    assert write_declared_identities(rows, tmp_path, "hgnc")["verdict"] == "good"
