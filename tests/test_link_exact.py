"""A link is verified on every value when that is affordable (an exact share), or on a sample
with a Wilson interval; the number of distinct values of the type is recorded in both cases."""

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings.signatures import Link, LinkEvidence, read_links, verify, write_links
from tests.test_link_signatures import GENES, PROTEINS, UP

DATA = f"""
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P38398> .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}P99999> .
"""
TARGET = f"<{UP}P04637> a <urn:Protein> . <{UP}P38398> a <urn:Protein> ."
LINK = Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")


def check(sample):
    genes = Dataset().parse(format="turtle", data=DATA)
    proteins = Dataset().parse(format="turtle", data=TARGET)
    with Client(GENES, genes) as source, Client(PROTEINS, proteins) as target:
        return verify(LINK, source, target, sample=sample)


def test_every_value_gives_an_exact_share_and_a_sample_an_interval(tmp_path):
    exact = check(sample=None)
    assert (exact.sampled, exact.population, exact.found) == (3, 3, 2)
    assert exact.interval() == (2 / 3, 2 / 3), "All values were looked up"
    sampled = check(sample=2)
    assert (sampled.sampled, sampled.population) == (2, 3)
    low, high = sampled.interval()
    assert 0 <= low <= sampled.share <= high <= 1 and high - low > 0, "A sample has an interval"
    write_links(tmp_path / "links.tsv", [exact, sampled])
    assert [e.population for e in read_links(tmp_path / "links.tsv")] == [3, 3]


def test_a_link_with_many_examples_is_read_back(tmp_path):
    """An exact link keeps every found pair (VoID counts its direct links from them). The local
    HGNC to AOP-Wiki links of the rehearsal wrote 136,477 characters in one cell, above the
    default field limit of the csv module (131,072)."""
    link = Link("join", "a", "urn:A", "urn:p", "hgnc", "b", "urn:B", None)
    pairs = [(f"HGNC:{i}", f"http://identifiers.org/hgnc/{i}") for i in range(5000)]
    exact = LinkEvidence(link, 5000, 5000, {"iri": 5000}, pairs, population=5000, complete=True)
    write_links(tmp_path / "links.tsv", [exact])
    (back,) = read_links(tmp_path / "links.tsv")
    assert back.examples == pairs
