"""With --update-downloads, the folder of a local source whose files changed on the server is
kept under another name and an empty folder is made, so that the source is downloaded and
indexed again. A source without an update, and every source in a run without the option, keeps
its folder and its index."""

from rdfsolve.qlever import downloads
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.local import LocalMiningStage

URL = "https://example.org/data.ttl.gz"


def _stage(tmp_path, update):
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path, repo_dir=tmp_path, sources_file=registry, output_dir=tmp_path / "run",
        update_downloads=update,
    )
    workdir = tmp_path / "fixture"
    (workdir / "rdf").mkdir(parents=True)
    (workdir / "rdf" / "data.ttl").write_text("<urn:a> <urn:p> <urn:b> .")
    (workdir / "fixture.meta-data.json").write_text("{}")
    return LocalMiningStage(config), workdir, Source.from_dict({"name": "fixture", "download_ttl": [URL]})


def _folders(tmp_path):
    return sorted(p.name.split("-update-")[0] for p in tmp_path.iterdir() if p.is_dir() and p.name != "run")


def test_a_source_with_an_update_is_set_aside_and_made_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(downloads, "updated_urls", lambda urls, record, built_at: [URL])
    stage, workdir, source = _stage(tmp_path, update=True)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture", "fixture.before"]
    assert not any(workdir.iterdir()), "The source is downloaded and indexed again"
    kept = next(p for p in tmp_path.iterdir() if ".before-update-" in p.name)
    assert (kept / "rdf" / "data.ttl").exists(), "The old folder is kept"


def test_a_source_without_an_update_keeps_its_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(downloads, "updated_urls", lambda urls, record, built_at: [])
    stage, workdir, source = _stage(tmp_path, update=True)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture"] and (workdir / "rdf" / "data.ttl").exists()


def test_a_changed_registry_entry_is_an_update(tmp_path, monkeypatch):
    def not_asked(urls, record, built_at):
        raise AssertionError("The server is not asked when the entry changed")

    monkeypatch.setattr(downloads, "updated_urls", not_asked)
    stage, workdir, source = _stage(tmp_path, update=True)
    downloads.write_record(workdir, [URL, "https://example.org/dropped.ttl"], lambda url: None)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture", "fixture.before"]


def test_without_the_option_the_server_is_not_asked(tmp_path, monkeypatch):
    def not_asked(urls, record, built_at):
        raise AssertionError("A run without the option asks nothing of the server")

    monkeypatch.setattr(downloads, "updated_urls", not_asked)
    stage, workdir, source = _stage(tmp_path, update=False)
    stage._set_aside_when_updated(workdir, source)
    assert _folders(tmp_path) == ["fixture"]
