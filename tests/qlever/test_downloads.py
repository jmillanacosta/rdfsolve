"""rdfsolve.qlever.downloads: the record of a source's downloads."""

import hashlib
import json
import sys

import pytest

from rdfsolve.qlever import downloads as downloads_module
from rdfsolve.qlever.downloads import (
    MARKER,
    download_paths,
    find_metalinks,
    needs_download,
    read_inputs,
    read_pins,
    read_record,
    record_inputs,
    unpinned_inputs,
    updated_urls,
    write_record,
)

URLS = ["https://example.org/a.ttl.gz", "https://example.org/b.ttl.gz"]
STATE = {"last_modified": "Mon, 01 Sep 2026 10:00:00 GMT", "content_length": "10"}


def test_files_are_downloaded_when_missing_unfinished_or_of_another_entry(tmp_path):
    assert needs_download(tmp_path, URLS, has_inputs=False)
    assert not needs_download(tmp_path, URLS, has_inputs=True), "An old folder without a record"
    (tmp_path / MARKER).touch()
    assert needs_download(tmp_path, URLS, has_inputs=True), "An earlier download did not end"
    write_record(tmp_path, URLS, lambda url: STATE)
    assert not (tmp_path / MARKER).exists() and read_record(tmp_path)["urls"] == URLS
    assert not needs_download(tmp_path, URLS, has_inputs=True)
    assert needs_download(tmp_path, URLS[:1], has_inputs=True), "The registry entry changed"


def test_an_update_on_the_server_is_seen_from_the_record(tmp_path):
    write_record(tmp_path, URLS, lambda url: STATE)
    record = read_record(tmp_path)
    assert updated_urls(URLS, record, built_at=0, head=lambda url: STATE) == []
    newer = {**STATE, "content_length": "11"}
    changed = updated_urls(
        URLS, record, built_at=0, head=lambda url: newer if "b." in url else STATE
    )
    assert changed == [URLS[1]]
    assert updated_urls(URLS, record, built_at=0, head=lambda url: None) == [], (
        "A server that does not answer is not an update"
    )


def test_without_a_record_the_date_of_the_index_is_compared():
    september = 1788256800.0  # 2026-09-01 10:00:00 GMT
    assert updated_urls(URLS, None, built_at=september + 60, head=lambda url: STATE) == []
    assert updated_urls(URLS, None, built_at=september - 60, head=lambda url: STATE) == URLS


def test_the_option_is_read_from_the_command_line(monkeypatch):
    from scripts.pipeline_stages import cli

    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--update-downloads"])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].update_downloads is True


def test_an_update_is_told_by_last_modified_and_size_only(tmp_path):
    write_record(tmp_path, URLS, lambda url: {**STATE, "etag": '"1"'})
    record = read_record(tmp_path)
    again = {**STATE, "etag": '"2"', "final_url": "https://mirror.example.org/a.ttl.gz"}
    assert updated_urls(URLS, record, built_at=0, head=lambda url: again) == []


METALINK_3 = """<metalink xmlns="http://www.metalinker.org/" version="3.0">
<version>2026_03</version>
<files><file name="a.ttl.gz"><size>{size}</size>
<verification><hash type="md5">{md5}</hash></verification></file></files></metalink>"""
METALINK_4 = """<metalink xmlns="urn:ietf:params:xml:ns:metalink"><file name="b.ttl.gz">
<version>142</version><hash type="sha-256">ABC</hash></file></metalink>"""


def test_a_metalink_beside_the_files_names_their_release_and_hashes():
    seen = []

    def fetch(url):
        seen.append(url)
        if url == "https://example.org/RELEASE.metalink":
            return METALINK_3.format(size=3, md5="x").encode()
        if url == "https://example.org/b.meta4":
            return METALINK_4.encode()
        return None

    states = {URLS[1]: {"link": '<b.meta4>; rel=describedby; type="application/metalink4+xml"'}}
    releases = find_metalinks(URLS, states, fetch=fetch)
    assert releases[URLS[0]] == {
        "metalink": "https://example.org/RELEASE.metalink",
        "version": "2026_03",
        "size": 3,
        "hashes": {"md5": "x"},
    }
    assert releases[URLS[1]]["version"] == "142", "The server names the metalink (RFC 6249)"
    assert releases[URLS[1]]["hashes"] == {"sha-256": "abc"}
    assert seen.count("https://example.org/RELEASE.metalink") == 1, "A metalink is read once"
    assert find_metalinks(URLS, {}, fetch=lambda url: b"not xml") == {}


def _downloaded(workdir, content=b"<urn:a> <urn:p> <urn:b> .\n"):
    (workdir / "rdf").mkdir(exist_ok=True)
    path = workdir / "rdf" / "a.ttl.gz"
    path.write_bytes(content)
    md5 = hashlib.md5(content).hexdigest()  # noqa: S324 - the hash a publisher gives
    write_record(
        workdir,
        URLS[:1],
        lambda url: {**STATE, "etag": '"e1"', "final_url": "https://mirror.example.org/a.ttl.gz"},
        lambda urls, states: {
            URLS[0]: {
                "metalink": "m",
                "version": "2026_03",
                "size": len(content),
                "hashes": {"md5": md5},
            }
        },
    )
    return path


def test_the_inputs_of_an_index_are_pinned_with_their_download(tmp_path):
    path = _downloaded(tmp_path)
    entry = {"name": "demo", "download_ttl": URLS[:1]}
    downloads = download_paths(tmp_path, entry)
    assert downloads == {URLS[0]: tmp_path / "rdf" / "a.ttl.gz"}
    manifest = record_inputs(tmp_path, downloads, [path], source="demo")
    assert read_inputs(tmp_path) == manifest
    [item] = manifest["files"]
    assert item["path"] == "rdf/a.ttl.gz" and item["index_input"] and item["url"] == URLS[0]
    assert item["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    [download] = manifest["downloads"]
    assert download["etag"] == '"e1"' and download["final_url"].startswith("https://mirror.")
    assert download["publisher_check"] == "match"
    assert manifest["release_versions"] == ["2026_03"]
    assert unpinned_inputs(manifest, manifest) == []
    write_record(tmp_path, URLS[:1], lambda url: STATE)
    assert read_inputs(tmp_path) is None, "A new download is pinned anew"


def test_a_file_that_did_not_change_is_not_read_again(tmp_path, monkeypatch):
    path = _downloaded(tmp_path)
    record_inputs(tmp_path, {URLS[0]: path}, [path])
    monkeypatch.setattr(
        downloads_module, "digest_file", lambda *args: pytest.fail("The file was read again")
    )
    assert record_inputs(tmp_path, {URLS[0]: path}, [path])["files"][0]["sha256"]


def test_a_pinned_run_refuses_other_missing_and_added_files(tmp_path):
    path = _downloaded(tmp_path)
    pinned = record_inputs(tmp_path, {URLS[0]: path}, [path])
    path.write_bytes(b"<urn:a> <urn:p> <urn:c> .\n")
    extra = tmp_path / "rdf" / "extra.nt"
    extra.write_text("<urn:a> <urn:p> <urn:d> .\n")
    current = record_inputs(tmp_path, {URLS[0]: path}, [path, extra])
    assert current["downloads"][0]["publisher_check"] == "mismatch"
    problems = unpinned_inputs(current, pinned)
    assert any("rdf/a.ttl.gz: sha256" in p for p in problems)
    assert any("rdf/extra.nt: an index input" in p for p in problems)
    path.unlink()
    current = record_inputs(tmp_path, {URLS[0]: None}, [extra])
    assert any("rdf/a.ttl.gz: missing" in p for p in unpinned_inputs(current, pinned))
    moved = {**pinned, "urls": URLS}
    assert "other URLs" in unpinned_inputs(pinned, moved)[0]


def test_the_pins_are_read_from_a_run_directory_or_a_manifest(tmp_path):
    (tmp_path / "demo").mkdir()
    pinned = {"source": "demo", "urls": URLS, "files": []}
    (tmp_path / "demo" / "demo_inputs.json").write_text(json.dumps(pinned))
    assert read_pins(tmp_path, "demo") == pinned
    assert read_pins(tmp_path, "other") is None
    assert read_pins(tmp_path / "demo" / "demo_inputs.json", "demo") == pinned
    assert read_pins(tmp_path / "demo" / "demo_inputs.json", "other") is None


def test_the_local_file_of_each_download_is_found(tmp_path):
    from rdfsolve.qlever.inputs import graph_input_directory
    from rdfsolve.qlever.utils import graph_download_name

    (tmp_path / "rdf").mkdir()
    (tmp_path / "rdf" / "onto.ttl").write_text("")  # served as .owl, renamed by its content
    entry = {
        "name": "demo",
        "download_ttl": ["https://example.org/onto.owl", "https://example.org/folder/"],
    }
    assert download_paths(tmp_path, entry) == {
        "https://example.org/onto.owl": tmp_path / "rdf" / "onto.ttl",
        "https://example.org/folder/": None,
    }
    graph = graph_input_directory(tmp_path, "urn:g") / "rdf"
    graph.mkdir(parents=True)
    url = "https://example.org/core.rdf.xz"
    (graph / graph_download_name(url, "download_rdf")).write_bytes(b"")
    mapped = {"name": "demo", "graph_sources": {"urn:g": {"download_rdf": [url]}}}
    assert download_paths(tmp_path, mapped) == {
        url: graph / graph_download_name(url, "download_rdf")
    }
