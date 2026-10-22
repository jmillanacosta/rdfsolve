"""rdfsolve.mappings.signatures: links between datasets inferred from the identifier types of their
schema examples, verified on the data, resolved through replacement sets, read as class
associations, and written and read back."""

import csv
import hashlib

import pytest
from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings import signatures
from rdfsolve.mappings.signatures import (
    ClassAssociation,
    Link,
    LinkEvidence,
    class_association,
    describe_replacement_sets,
    infer_links,
    read_links,
    read_replacements,
    verify,
    write_associations,
    write_links,
)
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.sparql_helper import PaginationTruncatedError
from tests.mappings.data import GENE_DATA, GENES, JOIN, PROTEIN_DATA, PROTEINS, UP, clients

IDORG_GENES = """
<urn:gene/1> a <urn:Gene> ; <urn:xref> <https://identifiers.org/uniprot:P04637> .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <https://identifiers.org/uniprot:P99999> ."""
ONE_PROTEIN = f"<{UP}P04637> a <urn:Protein> ."


def check(source_data, target_data, link=JOIN, **options):
    source, target = clients(source_data, target_data)
    with source, target:
        return verify(link, source, target, **options)


def test_values_of_one_dataset_join_the_subjects_of_another():
    found = signatures.signatures(GENES)
    assert found.values == {
        "uniprot": {("urn:Gene", "urn:xref")},
        "chebi": {("urn:Gene", "urn:ligand")},
    }
    assert "xsd" not in found.values, "A vocabulary term is not an identifier"
    assert signatures.signatures(PROTEINS).subjects == {"uniprot": {"urn:Protein"}}
    links = infer_links({"genes": GENES, "proteins": PROTEINS})
    joins = {
        (x.source, x.source_class, x.property, x.target, x.target_class)
        for x in links
        if x.kind == "join"
    }
    assert joins == {("genes", "urn:Gene", "urn:xref", "proteins", "urn:Protein")}
    shared = {(x.source, x.target, x.identifier_type) for x in links if x.kind == "shared"}
    assert shared == {("genes", "proteins", "chebi")}, (
        "A CURIE literal and an OBO IRI name one entity"
    )


def test_a_join_is_verified_on_its_values_and_gives_the_target_form():
    evidence = check(IDORG_GENES, ONE_PROTEIN, sample=10)
    assert (evidence.sampled, evidence.found) == (2, 1), "One of two identifiers is in the target"
    assert evidence.target_forms == {UP + "{id}": 1}, (
        "The target writes UniProt as purl.uniprot.org"
    )
    assert evidence.examples == [("https://identifiers.org/uniprot:P04637", UP + "P04637")]


def test_every_value_gives_an_exact_share_and_a_sample_an_interval(tmp_path):
    exact = check(GENE_DATA, PROTEIN_DATA, sample=None)
    assert (exact.sampled, exact.population, exact.found) == (3, 3, 2)
    assert exact.interval() == (2 / 3, 2 / 3), "All values were looked up"
    sampled = check(GENE_DATA, PROTEIN_DATA, sample=2)
    assert (sampled.sampled, sampled.population) == (2, 3)
    low, high = sampled.interval()
    assert 0 <= low <= sampled.share <= high <= 1 and high - low > 0, "A sample has an interval"
    write_links(tmp_path / "links.tsv", [exact, sampled])
    assert [e.population for e in read_links(tmp_path / "links.tsv")] == [3, 3]


def test_a_link_with_many_examples_is_read_back(tmp_path):
    """A cell larger than the csv module's default field limit (131,072 characters) is read."""
    link = Link("join", "a", "urn:A", "urn:p", "hgnc", "b", "urn:B", None)
    pairs = [(f"HGNC:{i}", f"http://identifiers.org/hgnc/{i}") for i in range(5000)]
    exact = LinkEvidence(link, 5000, 5000, {"iri": 5000}, pairs, population=5000, complete=True)
    write_links(tmp_path / "links.tsv", [exact])
    (back,) = read_links(tmp_path / "links.tsv")
    assert back.examples == pairs


READ_SOURCE = f"""
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> ; <urn:ref> "P04637" .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P38398> ; <urn:ref> "uniprot:P38398" .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}P99999> ; <urn:ref> "P99999" .
"""
READ_TARGET = f"""
<{UP}P04637> a <urn:Protein> ; <urn:acc> "P04637" .
<{UP}P38398> a <urn:Protein> ; <urn:acc> "UniProt:P38398" .
<urn:other> a <urn:Protein> ; <urn:acc> "P38398" .
[] <urn:acc> "P04637" .
"""
SHARED = Link(
    "shared", "genes", "urn:Gene", "urn:ref", "uniprot", "proteins", "urn:Protein", "urn:acc"
)


def read_check(link, read_target, refuse_counts=False):
    """Verify with the target's queries recorded; a local read is one response."""
    source, target = clients(READ_SOURCE, READ_TARGET)
    sent = []
    select = target._select

    def send(query, *args, **kwargs):
        sent.append(query)
        assert not kwargs.get("exhaustive"), "One response, checked against its count"
        if refuse_counts and "COUNT(DISTINCT ?t)" in query:
            raise PaginationTruncatedError(
                "Tried to allocate 3.3 GB, but only 3.3 GB were available"
            )
        return select(query, *args, **kwargs)

    target._select = send
    with source, target:
        return verify(link, source, target, sample=None, read_target=read_target), sent


def test_reading_the_target_finds_what_the_lookups_find(monkeypatch):
    """Between two local indexes the target's terms are read once and each spelling matched."""
    for link in (JOIN, SHARED):
        read, sent = read_check(link, read_target=True)
        looked, _ = read_check(link, read_target=False)
        assert (read.sampled, read.found, read.population) == (
            looked.sampled,
            looked.found,
            looked.population,
        )
        assert read.target_forms == looked.target_forms and read.examples == looked.examples
        assert len(sent) <= 2, "One count and one read of the target"
        assert not any("DISTINCT ?t ?x" in q for q in sent), (
            "Terms only: a blank node cannot be paged"
        )
    monkeypatch.setattr(signatures, "TARGET_READ_LIMIT", 1)
    read, sent = read_check(JOIN, read_target=True)
    assert read.found == 2 and any("VALUES (?key ?t)" in q for q in sent), (
        "A large target is looked up"
    )
    read, _ = read_check(JOIN, read_target=True, refuse_counts=True)
    assert read.found == 2, "A target whose terms cannot be counted is looked up"


def test_replacements_keep_one_to_one_rows_with_bioregistry_prefixes(tmp_path):
    """A secondary-to-primary mapping set: a withdrawn identifier and a split are left out."""
    path = tmp_path / "sec2pri.sssom.tsv"
    path.write_text(
        "# curie_map:\n#   IAO: http://purl.obolibrary.org/obo/IAO_\n"
        "#   UniProtKB: http://purl.uniprot.org/uniprot/\n#   semapv: https://w3id.org/semapv/vocab/\n"
        "#   sssom: https://w3id.org/sssom/\n# license: https://creativecommons.org/publicdomain/zero/1.0/\n"
        "# mapping_set_id: https://example.org/sec2pri\n"
        "subject_id\tpredicate_id\tobject_id\tmapping_justification\n"
        + "".join(
            f"UniProtKB:{s}\tIAO:0100001\t{o}\tsemapv:BackgroundKnowledgeBasedMatching\n"
            for s, o in [
                ("P99999", "UniProtKB:P04637"),
                ("P00001", "sssom:NoTermFound"),
                ("P00002", "UniProtKB:P11111"),
                ("P00002", "UniProtKB:P22222"),
            ]
        )
    )
    assert read_replacements(path) == {"uniprot:P99999": "uniprot:P04637"}


def test_a_secondary_identifier_is_found_through_its_primary_identifier():
    plain = check(IDORG_GENES, ONE_PROTEIN)
    resolved = check(IDORG_GENES, ONE_PROTEIN, replacements={"uniprot:P99999": "uniprot:P04637"})
    assert (plain.found, plain.replaced) == (1, 0)
    assert (resolved.found, resolved.replaced) == (2, 1), "The replaced identifier is counted"
    assert resolved.target_forms == {UP + "{id}": 2}
    assert ("https://identifiers.org/uniprot:P99999", UP + "P04637") in resolved.examples


def test_a_value_that_is_not_a_valid_identifier_of_the_type_is_not_sampled():
    """purl.uniprot.org also names entries (P53_HUMAN); such a value is not an accession."""
    data = f"<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> . <urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P53_HUMAN> ."
    evidence = check(data, ONE_PROTEIN)
    assert (evidence.sampled, evidence.found) == (1, 1)


def test_replacement_sets_are_described(tmp_path):
    text = (
        "#curie_map:\n#  IAO: http://purl.obolibrary.org/obo/IAO_\n#  HGNC: https://identifiers.org/hgnc:\n"
        '#mapping_set_id: https://example.org/pysec2pri/hgnc.sssom.tsv\n#mapping_set_version: "2026-09-01"\n'
        "#license: https://creativecommons.org/publicdomain/zero/1.0/\n"
        "subject_id\tpredicate_id\tobject_id\tmapping_justification\nHGNC:1\tIAO:0100001\tHGNC:2\tsemapv:ManualMappingCuration\n"
    )
    path = tmp_path / "hgnc.sssom.tsv"
    path.write_text(text)
    assert describe_replacement_sets([path]) == [
        {
            "file": "hgnc.sssom.tsv",
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
            "mapping_set_id": "https://example.org/pysec2pri/hgnc.sssom.tsv",
            "mapping_set_version": "2026-09-01",
            "citation": None,
        }
    ]


def test_a_class_association_has_its_pairs_and_the_coverage_of_both_classes(tmp_path):
    source, target = clients(GENE_DATA, PROTEIN_DATA + f" <{UP}P00001> a <urn:Protein> .")
    with source, target:
        association = class_association(JOIN, source, target)
    assert sorted(association.pairs) == [
        ("urn:gene/1", UP + "P04637"),
        ("urn:gene/2", UP + "P38398"),
    ]
    assert (association.source_subjects, association.source_members) == (2, 3)
    assert (association.target_subjects, association.target_members) == (2, 3)
    assert association.source_coverage == 2 / 3 and association.target_coverage == 2 / 3
    link = Link(
        "join",
        "hgnc",
        "http://x/Gene",
        "http://x/uniprot",
        "uniprot",
        "uniprot",
        "http://y/Protein",
    )
    write_associations(
        tmp_path / "a.tsv",
        [ClassAssociation(link, [("g1", "p1"), ("g1", "p2"), ("g2", "p2")], 2, 4, 2, 0)],
    )
    (row,) = csv.DictReader((tmp_path / "a.tsv").open(), delimiter="\t")
    assert (row["source"], row["target_class"], row["pairs"]) == ("hgnc", "http://y/Protein", "3")
    assert (row["source_subjects"], row["source_members"], row["source_coverage"]) == (
        "2",
        "4",
        "0.5",
    )
    assert row["target_coverage"] == "", "No members: no coverage"


GENE, SAME = "https://identifiers.org/ncbigene/", "http://www.w3.org/2002/07/owl#sameAs"
FLAG_SOURCE = f"""
<{GENE}7157> a <urn:Gene> ; <{SAME}> <{UP}P04637>, <{UP}P53_HUMAN> ; <urn:xref> <{UP}P04637> .
<{GENE}672> a <urn:Gene> ; <{SAME}> <{UP}P38398>, <{UP}BRCA1_HUMAN> ; <urn:xref> <{UP}P38398> .
"""
FLAG_TARGET = f"<{UP}P04637> a <urn:Protein> . <{UP}P38398> a <urn:Protein> ; <urn:in> <urn:t/1> . <urn:t/1> a <urn:Taxon> ."
IN = SchemaPattern(subject_class="urn:Protein", property_uri="urn:in", object_class="urn:Taxon")
FLAG_GENES = MinedSchema(
    about=AboutMetadata.build(dataset_name="genes"),
    patterns=[
        SchemaPattern(subject_class="urn:Gene", property_uri=p, object_class="Resource")
        for p in (SAME, "urn:xref")
    ],
)
FLAG_PROTEINS = MinedSchema(about=AboutMetadata.build(dataset_name="proteins"), patterns=[IN])
STATED = Link("join", "genes", "urn:Gene", SAME, "uniprot", "proteins", "urn:Protein")


def flagged(link):
    with (
        Client(FLAG_GENES, Dataset().parse(format="turtle", data=FLAG_SOURCE)) as s,
        Client(FLAG_PROTEINS, Dataset().parse(format="turtle", data=FLAG_TARGET)) as t,
    ):
        return verify(link, s, t, sample=None)


@pytest.fixture(scope="module")
def identity_links():
    """A link over owl:sameAs (an identity) and one over a cross-reference, both verified."""
    return flagged(STATED), flagged(
        Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")
    )


def test_an_identity_link_with_identifiers_that_fail_the_check_is_flagged_and_kept(
    tmp_path, identity_links
):
    stated, referred = identity_links
    assert stated.found == 2 and stated.flags == {"namespace": 2}, "The sample has the accessions"
    assert stated.flag_checked == 4 and stated.flagged
    assert referred.found == 2 and referred.flags == {} and not referred.flagged, (
        "A cross-reference states no identity"
    )
    write_links(tmp_path / "links.tsv", [stated, referred])
    read = read_links(tmp_path / "links.tsv")
    assert [e.flags for e in read] == [{"namespace": 2}, {}] and read[0].flag_checked == 4
