from unittest.mock import Mock

import pytest
from rdfsolve.qlever import lifecycle


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
