"""Class associations are written as a table: the link, the number of entity pairs, and the
members of each class that take part, with their coverage."""

import csv

from rdfsolve.mappings.signatures import ClassAssociation, Link, write_associations


def test_associations_are_written_with_their_coverage(tmp_path):
    link = Link("join", "hgnc", "http://x/Gene", "http://x/uniprot", "uniprot", "uniprot", "http://y/Protein")
    association = ClassAssociation(link, [("g1", "p1"), ("g1", "p2"), ("g2", "p2")], 2, 4, 2, 0)
    path = tmp_path / "class_associations.tsv"
    write_associations(path, [association])
    (row,) = csv.DictReader(path.open(), delimiter="\t")
    assert row["source"] == "hgnc" and row["target_class"] == "http://y/Protein"
    assert row["pairs"] == "3"
    assert row["source_subjects"] == "2" and row["source_members"] == "4"
    assert row["source_coverage"] == "0.5"
    assert row["target_coverage"] == "", "No members: no coverage"
