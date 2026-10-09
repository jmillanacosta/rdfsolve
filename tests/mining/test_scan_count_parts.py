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
    assert rows == 133_333_333 and types == 66_666_666


def test_bgee_types_are_split_under_its_budget(monkeypatch):
    """Bgee (job 115704, RDFSOLVE_SCAN_COUNT_GB=80): 725,494,945 type rows stayed in one part
    (the old split allowed 1,000,000,000) and the job used 251.6 GB. Under 80 GB they are
    joined in parts, and a smaller budget gives more and smaller parts of both."""
    monkeypatch.setenv("RDFSOLVE_SCAN_COUNT_GB", "80")
    rows, types = scan.count_limits()
    assert -(-725_494_945 // types) >= 8
    monkeypatch.setenv("RDFSOLVE_SCAN_COUNT_GB", "20")
    smaller = scan.count_limits()
    assert smaller[0] < rows and smaller[1] < types


def test_parts_are_scaled_by_the_classified_rows_of_a_batch(store, monkeypatch):
    """A row whose subject has k classes and object m gives k * m classified rows: SAWGraph's
    hasResultQualifier gives 364 a row, and 110 parts sized by rows ran out of memory (job
    115895). The parts of a batch are sized by its classified rows, estimated from a sample;
    the counts stay those of one pass."""
    whole = _counts(store)
    found = scan._batch_expansion(store, ["http://example.org/condition"], 10**6)
    assert found > 1.0, "Expressions with two classes point to conditions with two classes"
    sizes = []
    original = scan._spill

    def spill(frame, directory, parts, column):
        sizes.append(parts)
        return original(frame, directory, parts, column)

    monkeypatch.setattr(scan, "_spill", spill)
    monkeypatch.setattr(scan, "EXPANSION_MIN_ROWS", 1)
    monkeypatch.setattr(scan, "_batch_expansion", lambda store, batch, size: 50.0)
    monkeypatch.setattr(scan, "PARTITION_ROWS", 400)
    monkeypatch.setattr(scan, "BATCH_ROWS", 400)
    assert _counts(store) == whole
    assert sizes and max(sizes) > 1, "Batches of few rows are split by their classified rows"


def test_a_scope_with_too_many_classes_is_refused_with_its_setting(store, monkeypatch):
    """BioGateway has 65,884,730 classes and GO-CAM 1,718,529: a count gives a pattern for each
    class and property, and its final table ran out of memory. Above the limit the count is
    refused with a message that names the grouping setting."""
    monkeypatch.setenv("RDFSOLVE_SCAN_MAX_CLASSES", "2")
    with pytest.raises(scan.TooManyClassesError, match="ontology_group_before_mining"):
        scan.count_patterns(store)
    monkeypatch.delenv("RDFSOLVE_SCAN_MAX_CLASSES")
    assert scan.count_patterns(store)


@pytest.mark.parametrize("scope", [None, [G1, G2]])
def test_the_per_term_rows_are_written_as_the_patterns_give_them(store, tmp_path, scope):
    """The per-term layer of a source grouped before counting is written batch by batch, never
    held whole: the rows are those of term_rows over the patterns of one count."""
    import polars as pl

    from rdfsolve.mining.term_release import term_rows

    counted = store.view(scope) if scope else store
    expected = term_rows(scan.count_patterns(counted))
    assert scan.count_patterns(counted, rows_path=tmp_path / "rows.parquet") == []
    written = pl.read_parquet(tmp_path / "rows.parquet").sort(expected.columns, nulls_last=True)
    assert written.select(expected.columns).equals(expected.sort(expected.columns, nulls_last=True))


def test_a_predicate_without_rows_is_batched():
    """A store whose smallest predicate has no rows (a slice of a store) is counted."""
    from types import SimpleNamespace

    from rdfsolve.mining.scan import _batches

    store = SimpleNamespace(
        predicates={"a": "a.parquet", "b": "b.parquet"}, manifest={"rows": {"a": 0, "b": 5}}
    )
    assert _batches(store) == [["a", "b"]]
