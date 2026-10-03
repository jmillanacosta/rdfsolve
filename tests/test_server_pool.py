"""A pool serves local indexes on demand and keeps at most a given number of servers running,
stopping the one used least recently."""

from rdfsolve.qlever import lifecycle
from rdfsolve.qlever.lifecycle import ServerPool


def test_the_least_recently_used_server_is_stopped(tmp_path, monkeypatch):
    started, stopped = [], []

    def start(image, workdir, name, port, **kwargs):
        started.append((name, port))
        return name

    monkeypatch.setattr(lifecycle, "start_server", start)
    monkeypatch.setattr(lifecycle, "stop_server", stopped.append)
    monkeypatch.setattr(lifecycle, "image_for_index", lambda data_dir, workdir, name: tmp_path / "q.sif")
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
