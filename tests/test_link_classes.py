"""A class association is read along a verified link: the entity pairs that the link joins, and
how many members of each class take part."""

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings.signatures import Link, class_association
from tests.test_link_signatures import GENES, PROTEINS, UP

DATA = f"""
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P38398> .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}P99999> .
"""
TARGET = f"<{UP}P04637> a <urn:Protein> . <{UP}P38398> a <urn:Protein> . <{UP}P00001> a <urn:Protein> ."


def test_the_pairs_and_the_coverage_of_both_classes():
    link = Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")
    genes = Dataset().parse(format="turtle", data=DATA)
    proteins = Dataset().parse(format="turtle", data=TARGET)
    with Client(GENES, genes) as source, Client(PROTEINS, proteins) as target:
        association = class_association(link, source, target)
    assert sorted(association.pairs) == [("urn:gene/1", UP + "P04637"), ("urn:gene/2", UP + "P38398")]
    assert (association.source_subjects, association.source_members) == (2, 3)
    assert (association.target_subjects, association.target_members) == (2, 3)
    assert association.source_coverage == 2 / 3 and association.target_coverage == 2 / 3
