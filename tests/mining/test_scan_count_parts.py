"""Counting in parts (rows by subject and object id, the type table by id) gives the counts of
one pass, for skewed objects (one class or constant shared by most rows), subjects with several
classes, untyped IRI subjects, blank nodes, literals and graph scopes.
"""

from __future__ import annotations

import pytest
from rdflib import RDF, BNode, Dataset, Literal, Namespace, URIRef
from rdflib.namespace import XSD

from rdfsolve.mining import scan

EX = Namespace("http://example.org/")
G1, G2 = "http://example.org/g1", "http://example.org/g2"


def _dataset() -> Dataset:
    data = Dataset(default_union=True)
    one, two = data.graph(URIRef(G1)), data.graph(URIRef(G2))
    conditions = [EX[f"condition{i}"] for i in range(3)]
    for c in conditions:
        one.add((c, RDF.type, EX.Condition))
    two.add((conditions[0], RDF.type, EX.Anatomy))
    for i in range(40):
        e = EX[f"expression{i}"]
        g = one if i % 3 else two
        g.add((e, RDF.type, EX.Expression))
        if i % 5 == 0:
            g.add((e, RDF.type, EX.Observation))
        g.add((e, EX.level, EX.high if i % 7 else EX.low))  # one hot object, untyped
        g.add((e, EX.condition, conditions[i % 3]))
        g.add((e, EX.score, Literal(i / 3, datatype=XSD.double)))
        g.add((e, EX.label, Literal(f"e{i % 4}")))
        if i % 4 == 0:
            node = BNode(f"b{i}")
            g.add((e, EX.detail, node))
            g.add((node, EX.note, Literal("n")))
            if i % 8 == 0:
                g.add((node, RDF.type, EX.Detail))
        if i % 6 == 0:
            two.add((e, EX.condition, conditions[(i + 1) % 3]))  # a triple in both graphs
            one.add((e, EX.condition, conditions[(i + 1) % 3]))
    for i in range(5):  # untyped subjects pointing to a typed hub
        one.add((EX[f"untyped{i}"], EX.condition, conditions[0]))
    return data


def _counts(store) -> list[dict]:
    return [p.model_dump() for p in scan.count_patterns(store)]


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    return scan.store_from_graph(_dataset(), tmp_path_factory.mktemp("store") / "store")


@pytest.mark.parametrize("scope", [None, [G1], [G1, G2]])
@pytest.mark.parametrize(("rows", "types"), [(5, 10**9), (10**9, 20), (7, 30), (50, 13)])
def test_counting_in_parts_gives_the_counts_of_one_pass(store, monkeypatch, scope, rows, types):
    counted = store.view(scope) if scope else store
    whole = _counts(counted)
    assert whole
    monkeypatch.setattr(scan, "PARTITION_ROWS", rows)
    monkeypatch.setattr(scan, "TYPE_PARTITION_ROWS", types)
    monkeypatch.setattr(scan, "BATCH_ROWS", rows)
    assert _counts(counted) == whole
    assert not list(store.path.glob(".count-*")), "The scratch directory is removed"


def test_the_memory_budget_sets_the_parts(monkeypatch):
    monkeypatch.delenv("RDFSOLVE_SCAN_COUNT_GB", raising=False)
    assert scan.count_limits() == (scan.PARTITION_ROWS, scan.TYPE_PARTITION_ROWS)
    monkeypatch.setenv("RDFSOLVE_SCAN_COUNT_GB", "60")
    rows, types = scan.count_limits()
    assert rows == 100_000_000 and types == 750_000_000
