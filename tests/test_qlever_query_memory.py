"""QLever gets a share of the SLURM allocation for queries; the share can be raised."""

import pytest

from rdfsolve.qlever.lifecycle import _query_memory


def test_the_query_memory_is_a_share_of_the_allocation(monkeypatch):
    monkeypatch.setenv("SLURM_MEM_PER_NODE", "757760")  # 740 GB, in MB
    monkeypatch.delenv("RDFSOLVE_QLEVER_MEMORY_SHARE", raising=False)
    assert _query_memory("680G") == "454656MB", "0.6 of the allocation by default"
    monkeypatch.setenv("RDFSOLVE_QLEVER_MEMORY_SHARE", "0.85")
    assert _query_memory("680G") == "644096MB", "A larger share when asked"
    assert _query_memory("100G") == "102400MB", "The Qleverfile limit when it is smaller"


@pytest.mark.parametrize("share", ["0", "1", "x"])
def test_a_share_outside_zero_to_one_is_refused(monkeypatch, share):
    monkeypatch.setenv("SLURM_MEM_PER_NODE", "757760")
    monkeypatch.setenv("RDFSOLVE_QLEVER_MEMORY_SHARE", share)
    with pytest.raises(ValueError, match="RDFSOLVE_QLEVER_MEMORY_SHARE"):
        _query_memory("680G")
