"""Between two local indexes, the terms of the target are read once and each spelling of an
identifier is matched against them, instead of one lookup query for every 200 spellings (the
HGNC to AOP-Wiki link: 34,453 identifiers). The match is the term equality of the lookup."""

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings import signatures
from rdfsolve.mappings.signatures import Link, verify
from tests.test_link_signatures import GENES, PROTEINS, UP

DATA = f"""
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> ; <urn:ref> "P04637" .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P38398> ; <urn:ref> "uniprot:P38398" .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}P99999> ; <urn:ref> "P99999" .
"""
TARGET = f"""
<{UP}P04637> a <urn:Protein> ; <urn:acc> "P04637" .
<{UP}P38398> a <urn:Protein> ; <urn:acc> "UniProt:P38398" .
<urn:other> a <urn:Protein> ; <urn:acc> "P38398" .
[] <urn:acc> "P04637" .
"""
JOIN = Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")
SHARED = Link("shared", "genes", "urn:Gene", "urn:ref", "uniprot", "proteins", "urn:Protein", "urn:acc")


def check(link, read_target):
    genes = Dataset().parse(format="turtle", data=DATA)
    proteins = Dataset().parse(format="turtle", data=TARGET)
    with Client(GENES, genes) as source, Client(PROTEINS, proteins) as target:
        sent = {"source": [], "target": []}
        for name, client in (("source", source), ("target", target)):
            client._select = recorded(client._select, sent[name])
        return verify(link, source, target, sample=None, read_target=read_target), sent["target"]


def recorded(select, sent):
    """Record the queries of a client; a local read is one response, checked against its count."""

    def send(query, *args, **kwargs):
        sent.append(query)
        assert not kwargs.get("exhaustive"), "One response, checked against its count"
        return select(query, *args, **kwargs)

    return send


def test_reading_the_target_finds_what_the_lookups_find(monkeypatch):
    for link in (JOIN, SHARED):
        read, sent = check(link, read_target=True)
        looked, _ = check(link, read_target=False)
        assert (read.sampled, read.found, read.population) == (looked.sampled, looked.found, looked.population)
        assert read.target_forms == looked.target_forms and read.examples == looked.examples
        assert len(sent) <= 2, "One count and one read of the target"
        assert not any("DISTINCT ?t ?x" in q for q in sent), "Terms only: a blank node cannot be paged"
    monkeypatch.setattr(signatures, "TARGET_READ_LIMIT", 1)
    read, sent = check(JOIN, read_target=True)
    assert read.found == 2 and any("VALUES (?key ?t)" in q for q in sent), "A large target is looked up"


def test_a_target_that_cannot_be_read_at_once_is_looked_up(monkeypatch):
    """The count of the distinct terms of HGNC reached the query memory of QLever (routes of
    AOP-Wiki to HGNC, 2026-09-30); the identifiers are then looked up, as for a large target."""
    from rdfsolve.sparql_helper import PaginationTruncatedError

    genes = Dataset().parse(format="turtle", data=DATA)
    proteins = Dataset().parse(format="turtle", data=TARGET)
    with Client(GENES, genes) as source, Client(PROTEINS, proteins) as target:
        select = target._select

        def refuse_counts(query, *args, **kwargs):
            if "COUNT(DISTINCT ?t)" in query:
                raise PaginationTruncatedError("Tried to allocate 3.3 GB, but only 3.3 GB were available")
            return select(query, *args, **kwargs)

        target._select = refuse_counts
        read = verify(JOIN, source, target, sample=None, read_target=True)
    assert read.found == 2
