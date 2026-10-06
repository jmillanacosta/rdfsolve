"""Large predicates are read in slices (LIMIT/OFFSET) at once and joined in order."""

import polars as pl
import pytest

from rdfsolve.mining import scan
from tests.mining.test_scan_export import endpoint


def _rows(store: scan.RowStore) -> dict[str, pl.DataFrame]:
    return {p: pl.read_parquet(f) for p, f in store.predicates.items()}


def test_slices_give_the_store_of_one_query(endpoint, tmp_path, monkeypatch):  # noqa: F811
    whole = scan.export_index(endpoint.url, tmp_path / "whole", index={"name": "t"}, workers=1)
    monkeypatch.setattr(scan, "SLICE_ROWS", 1)
    sliced = scan.export_index(endpoint.url, tmp_path / "sliced", index={"name": "t"}, workers=3)
    assert sliced.manifest["predicates"] == whole.manifest["predicates"]
    for predicate, frame in _rows(whole).items():
        assert _rows(sliced)[predicate].equals(frame), predicate
    assert pl.read_parquet(sliced.path / "types.parquet").equals(
        pl.read_parquet(whole.path / "types.parquet")
    )
    assert not list((sliced.path / "rows").glob("*.slice-*"))


def test_the_last_slice_reads_the_rows_beyond_the_count(monkeypatch):
    monkeypatch.setattr(scan, "SLICE_ROWS", 10)
    assert scan._slices(25) == [(0, 10), (10, 10), (20, None)]
    assert scan._slices(20) == [(0, 10), (10, None)]
    assert scan._slices(0) == [(0, None)]
    monkeypatch.setattr(scan, "MAX_SLICES", 2)
    assert scan._slices(25) == [(0, 13), (13, None)]


def test_the_ids_are_read_without_the_datatype():
    query = scan._row_query("urn:p", datatype=False)
    assert "DATATYPE" not in query
    assert scan._width(query) == 2
    assert scan._width(scan._graph_row_query("urn:p", datatype=False)) == 3
    assert scan._width(scan._unnamed_row_query("urn:p", datatype=False)) == 2
    with pytest.raises(ValueError, match="variables only"):
        scan._width(scan._row_query("urn:p"))


def test_export_workers_follow_the_environment(monkeypatch):
    monkeypatch.setenv("RDFSOLVE_SCAN_WORKERS", "7")
    assert scan.export_workers() == 7
    monkeypatch.delenv("RDFSOLVE_SCAN_WORKERS")
    assert scan.export_workers() >= 4
