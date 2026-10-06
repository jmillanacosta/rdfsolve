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


def test_a_checksum_file_beside_a_download_gives_its_hash_and_is_checked(tmp_path):
    content = b"<urn:a> <urn:p> <urn:b> .\n"
    sha1 = hashlib.sha1(content).hexdigest()  # noqa: S324 - the hash a publisher gives
    files = {
        URLS[0] + ".sha1": f"{sha1.upper()}  a.ttl.gz\n".encode(),
        URLS[1] + ".sha1": f"{sha1}  other.ttl.gz\n".encode(),
    }
    releases = find_metalinks(URLS, {}, fetch=files.get, checksum_files=["sha1"])
    assert releases[URLS[0]] == {
        "version": None,
        "hashes": {"sha1": sha1},
        "checksum_files": [URLS[0] + ".sha1"],
    }
    assert URLS[1] not in releases, "A checksum file of another file gives no hash"
    assert find_metalinks(URLS, {}, fetch=files.get) == {}, "Checksum files are read on request"
    (tmp_path / "rdf").mkdir()
    path = tmp_path / "rdf" / "a.ttl.gz"
    for body, check in ((content, "match"), (b"changed", "mismatch")):
        path.write_bytes(body)
        write_record(
            tmp_path, URLS[:1], lambda url: STATE,
            lambda urls, states: find_metalinks(urls, states, fetch=files.get, checksum_files=["sha1"]),
        )
        manifest = record_inputs(tmp_path, {URLS[0]: path}, [path], source="demo")
        assert manifest["downloads"][0]["publisher_check"] == check
        assert manifest["files"][0]["sha1"] == hashlib.sha1(body).hexdigest()  # noqa: S324


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
    import os
    import time

    path = _downloaded(tmp_path)
    old = time.time_ns() - 60 * 10**9  # changed a minute before it is pinned
    os.utime(path, ns=(old, old))
    record_inputs(tmp_path, {URLS[0]: path}, [path])
    monkeypatch.setattr(
        downloads_module, "digest_file", lambda *args: pytest.fail("The file was read again")
    )
    assert record_inputs(tmp_path, {URLS[0]: path}, [path])["files"][0]["sha256"]


def test_a_file_rewritten_within_one_tick_is_read_again(tmp_path):
    """A file rewritten with the same size and modification time is not taken from the cache."""
    import os

    path = _downloaded(tmp_path)
    first = record_inputs(tmp_path, {URLS[0]: path}, [path])["files"][0]
    path.write_bytes(b"<urn:a> <urn:p> <urn:c> .\n")  # the same size
    os.utime(path, ns=(first["modified_ns"], first["modified_ns"]))
    second = record_inputs(tmp_path, {URLS[0]: path}, [path])["files"][0]
    assert second["sha256"] != first["sha256"]


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


def _folder(tmp_path, *names):
    (tmp_path / "rdf").mkdir(exist_ok=True)
    for name in names:
        (tmp_path / "rdf" / name).write_text("<urn:a> <urn:p> <urn:b> .\n")
    return [tmp_path / "rdf" / name for name in names]


def test_an_index_of_other_downloads_differs_from_the_registry_entry(tmp_path):
    """L19: the URLs recorded with the index (pin, else download record) must be the entry's."""
    from rdfsolve.qlever.downloads import downloads_differ

    entry = {"name": "demo", "download_ttl": URLS}
    inputs = _folder(tmp_path, "a.ttl.gz", "b.ttl.gz")
    write_record(tmp_path, URLS, lambda url: STATE)
    assert downloads_differ(tmp_path, URLS, entry, inputs) is None
    assert downloads_differ(tmp_path, URLS[::-1], entry, inputs) is None, "Order is no change"
    changed = [URLS[0], "https://example.org/c.ttl.gz"]
    reason = downloads_differ(tmp_path, changed, {"name": "demo", "download_ttl": changed}, inputs)
    assert reason and "1 URLs no longer listed, 1 not downloaded" in reason
    # The pin of the index holds the URLs of its download record; it is read first.
    record_inputs(tmp_path, download_paths(tmp_path, entry), inputs)
    (tmp_path / "downloads.json").unlink()
    reason = downloads_differ(tmp_path, URLS[:1], {"name": "demo", "download_ttl": URLS[:1]}, [])
    assert reason and "other downloads" in reason


def test_files_that_the_entry_does_not_list_are_found_among_the_index_inputs(tmp_path):
    """L19 (ALLIE): a work folder held every file of the provider; the entry lists one."""
    from rdfsolve.qlever.downloads import downloads_differ, unlisted_inputs

    url = "https://example.org/data/allie_rdf_nt_latest.gz"
    entry = {"name": "demo", "download_nt": [url]}
    inputs = _folder(tmp_path, "allie_rdf_nt_latest.nt.gz", "mesh_rdf_nt_latest.nt", "pubmed.nt")
    write_record(tmp_path, [url], lambda u: STATE)
    reason = downloads_differ(tmp_path, [url], entry, inputs)
    assert reason and reason.startswith("2 index inputs come from no download")
    names = {url: "allie_rdf_nt_latest.nt.gz"}
    assert unlisted_inputs(names, inputs[:1]) == []
    # Decompressed, converted, or set apart by N__: the same download.
    derived = [tmp_path / n for n in ("allie_rdf_nt_latest.nt", "1__allie_rdf_nt_latest.nq")]
    assert unlisted_inputs(names, derived) == []
    # Members of an archive, or a download named by the server, cannot be told apart.
    assert unlisted_inputs({"https://example.org/all.zip": "all.zip"}, inputs) is None
    assert unlisted_inputs({"https://example.org/folder/": None}, inputs) is None


def test_a_folder_without_a_record_that_lacks_a_listed_download_differs(tmp_path):
    """L19 (OpenBioDiv, HPA): an index from before download records, of other files."""
    from rdfsolve.qlever.downloads import downloads_differ

    urls = ["https://example.org/onto.ttl", "https://example.org/data.nq.gz"]
    entry = {"name": "demo", "download_ttl": urls[:1], "download_nq": urls[1:]}
    inputs = _folder(tmp_path, "onto.ttl")
    reason = downloads_differ(tmp_path, urls, entry, inputs)
    assert reason and "no download record and lacks 1" in reason
    assert downloads_differ(tmp_path, urls, entry, []) is None, "Inputs deleted: no evidence"
    inputs += _folder(tmp_path, "data.nq")
    assert downloads_differ(tmp_path, urls, entry, inputs) is None


def test_an_entry_names_only_the_checksum_kinds_that_are_read():
    from rdfsolve.models.source_model import SourceModel

    assert SourceModel.model_validate({"name": "x", "checksum_files": ["sha1"]}).checksum_files == ["sha1"]
    with pytest.raises(ValueError, match="checksum_files"):
        SourceModel.model_validate({"name": "x", "checksum_files": ["crc32"]})


def test_a_checksum_file_sent_as_gzip_by_its_suffix_is_read_as_sent(monkeypatch):
    import requests

    body = b"c9ef004de88b9201b84f90aad2966bfd067af799  mesh.nt.gz\n"

    class Raw:
        def stream(self, size, decode_content=True):
            assert decode_content is False
            yield body

    class Answer:
        status_code = 200
        raw = Raw()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def iter_content(self, size):
            raise requests.exceptions.ContentDecodingError("incorrect header check")
            yield b""

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: Answer())
    assert downloads_module._fetch_text("https://example.org/mesh.nt.gz.sha1") == body


def test_the_size_of_a_file_is_asked_without_a_transfer_encoding(monkeypatch):
    """L97: raw.githubusercontent.com gave the Content-Length of a gzip transfer, so unchanged
    GitHub files looked changed. HEAD asks for identity encoding; a size that is still of an
    encoded transfer is not the file's, and the ETag is compared instead."""
    import requests

    sent = {}

    class Answer:
        status_code = 200
        url = "https://raw.githubusercontent.com/o/r/main/onto.owl"
        headers = requests.structures.CaseInsensitiveDict(
            {
                "Content-Length": "1200",
                "Content-Encoding": "gzip",
                "ETag": '"abc"',
                "Last-Modified": "Mon, 01 Sep 2026 10:00:00 GMT",
            }
        )

    def head(url, **options):
        sent.update(options)
        return Answer()

    monkeypatch.setattr(requests, "head", head)
    state = downloads_module.server_state(Answer.url)
    assert sent["headers"]["Accept-Encoding"] == "identity"
    assert state["content_length"] is None and state["transfer_length"] == "1200"
    assert state["content_encoding"] == "gzip"
    # The record of the download holds the file's size (an identity answer then).
    record = {
        "files": {
            Answer.url: {
                **STATE,
                "last_modified": Answer.headers["Last-Modified"],
                "content_length": "9000",
                "etag": '"abc"',
            }
        }
    }
    unchanged = updated_urls([Answer.url], record, built_at=0, head=lambda url: state)
    assert unchanged == [], "The transfer's size is not compared with the file's"
    moved = {**state, "etag": '"def"'}
    assert updated_urls([Answer.url], record, built_at=0, head=lambda url: moved) == [Answer.url]
    plain = {**STATE, "content_length": "11"}
    write_record_state = {"files": {URLS[0]: STATE}}
    assert updated_urls(URLS[:1], write_record_state, built_at=0, head=lambda url: plain) == [
        URLS[0]
    ], "Without an encoding the sizes are compared as before"
