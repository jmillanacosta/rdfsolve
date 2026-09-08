"""Check request exclusion across separate processes without network access."""

import multiprocessing
import os
from queue import Empty

import pytest

from rdfsolve._host_gate import host_request


def hold_slot(queue, release):
    with host_request("test.invalid", timeout=10):
        queue.put("entered")
        release.wait(5)


@pytest.mark.skipif(os.name != "posix", reason="Shared file locks require POSIX")
def test_processes_cannot_overlap_requests(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    release = context.Event()
    first = context.Process(target=hold_slot, args=(queue, release))
    second = context.Process(target=hold_slot, args=(queue, release))
    first.start()
    try:
        assert queue.get(timeout=5) == "entered"
        second.start()
        with pytest.raises(Empty):
            queue.get(timeout=0.3)
        release.set()
        assert queue.get(timeout=5) == "entered"
    finally:
        release.set()
        first.join(timeout=10)
        if second.pid is not None:
            second.join(timeout=10)
    assert first.exitcode == second.exitcode == 0
