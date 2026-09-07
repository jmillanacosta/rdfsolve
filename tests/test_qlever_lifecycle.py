"""Check process ownership and startup failures without loading an index."""

import socket
import subprocess
from unittest.mock import Mock

import pytest

from rdfsolve.qlever import lifecycle


def test_busy_port_does_not_start_or_kill_a_process(tmp_path, monkeypatch):
    image = tmp_path / "qlever.sif"
    image.touch()
    launch = Mock()
    monkeypatch.setattr(lifecycle.subprocess, "Popen", launch)
    with socket.socket() as occupied:
        occupied.bind(("0.0.0.0", 0))
        with pytest.raises(RuntimeError, match="in use"):
            lifecycle.start_server(image, tmp_path, "test", occupied.getsockname()[1])
    launch.assert_not_called()


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


def test_stop_escalates_only_for_owned_process():
    process = Mock()
    process.poll.return_value = None
    process.wait.side_effect = [subprocess.TimeoutExpired("qlever", 10), 0]
    lifecycle.stop_server(process)
    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert process.wait.call_count == 2
