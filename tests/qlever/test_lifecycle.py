"""rdfsolve.qlever.lifecycle: QLever servers are started and stopped, with their memory, images and a
pool of servers; how a server ended is described, and the port of a server that died is waited for."""

import json
import socket
import subprocess
import sys
from unittest.mock import Mock

import pytest

from rdfsolve.qlever import lifecycle
from rdfsolve.qlever.lifecycle import (
    ServerPool,
    _query_memory,
    _wait_for_port,
    exit_status,
    image_for_index,
    index_build,
)


def test_failed_start_reaps_process_and_preserves_old_log(tmp_path, monkeypatch):
    image = tmp_path / "qlever.sif"
    image.touch()
    old_log = tmp_path / "server.log"
    old_log.write_text("previous run")
    process = Mock(returncode=2)
    process.poll.return_value = 2
    monkeypatch.setattr(lifecycle.subprocess, "Popen", Mock(return_value=process))
    with pytest.raises(RuntimeError, match="status 2; see"):
        lifecycle.start_server(image, tmp_path, "test", 0)
    process.wait.assert_called_once_with()
    process.terminate.assert_not_called()
    assert old_log.read_text() == "previous run"


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


def test_the_least_recently_used_server_is_stopped(tmp_path, monkeypatch):
    started, stopped = [], []

    def start(image, workdir, name, port, **kwargs):
        started.append((name, port))
        return name

    monkeypatch.setattr(lifecycle, "start_server", start)
    monkeypatch.setattr(lifecycle, "stop_server", stopped.append)
    monkeypatch.setattr(
        lifecycle, "image_for_index", lambda data_dir, workdir, name: tmp_path / "q.sif"
    )
    monkeypatch.setattr(lifecycle, "index_name", lambda workdir, name: name)
    pool = ServerPool(tmp_path, size=2, base_port=30000)
    assert pool.endpoint("a") == "http://localhost:30000"
    pool.endpoint("b")
    assert pool.endpoint("a") == "http://localhost:30000", "A running server is reused"
    pool.endpoint("c")
    assert stopped == ["b"]
    assert [name for name, _ in started] == ["a", "b", "c"]
    pool.close()
    assert sorted(stopped) == ["a", "b", "c"]


def workdir(tmp_path, build):
    path = tmp_path / "workdirs" / "src"
    path.mkdir(parents=True)
    (path / "src.meta-data.json").write_text(json.dumps({"git-hash": build}))
    return path


def test_the_image_of_the_index_build_is_chosen(tmp_path):
    (tmp_path / "qlever.sif").write_text("old")
    (tmp_path / "fixed.sif").write_text("new")
    index = workdir(tmp_path, "388f365")
    assert index_build(index, "src") == "388f365"
    assert image_for_index(tmp_path, index, "src") == tmp_path / "qlever.sif", "No catalogue"
    (tmp_path / "qlever_images.yaml").write_text(
        "- image: qlever.sif\n  git_hash: 9ec88a0\n- image: fixed.sif\n  git_hash: 388f365\n"
    )
    assert image_for_index(tmp_path, index, "src") == tmp_path / "fixed.sif"
    old = workdir(tmp_path / "other", "9ec88a")
    assert image_for_index(tmp_path, old, "src") == tmp_path / "qlever.sif", "Short hashes match"
    unknown = workdir(tmp_path / "third", "abc1234")
    with pytest.raises(ValueError, match="abc1234"):
        image_for_index(tmp_path, unknown, "src")


def test_an_index_not_yet_built_gets_the_default_image(tmp_path):
    (tmp_path / "qlever_images.yaml").write_text("- image: fixed.sif\n  git_hash: 388f365\n")
    new = tmp_path / "workdirs" / "src"
    new.mkdir(parents=True)
    assert image_for_index(tmp_path, new, "src") == tmp_path / "qlever.sif"


def test_a_running_server_has_no_exit_status():
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        assert exit_status(process) is None
    finally:
        process.kill()
        process.wait()


@pytest.mark.parametrize(
    ("code", "text"),
    [
        ("import os, signal; os.kill(os.getpid(), signal.SIGSEGV)", "status -11 (SIGSEGV)"),
        ("raise SystemExit(137)", "status 137 (SIGKILL)"),
        ("raise SystemExit(1)", "status 1"),
    ],
)
def test_the_exit_status_names_the_signal(code, text):
    process = subprocess.Popen([sys.executable, "-c", code])
    process.wait()
    assert exit_status(process) == f"exited with {text}"


def test_a_port_in_use_is_refused_after_the_wait():
    with socket.socket() as held:
        held.bind(("0.0.0.0", 0))  # noqa: S104 - The port that the check binds.
        held.listen()
        with pytest.raises(RuntimeError, match="in use"):
            _wait_for_port(held.getsockname()[1], 1)
