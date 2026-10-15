"""rdfsolve.qlever.hdt: an HDT download is written as gzip N-Triples beside it and indexed from
those; the HDT stays the pinned download, and a hash the registry gives for it is checked."""

import configparser
import gzip
import hashlib
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from rdfsolve.qlever import build_qleverfile
from rdfsolve.qlever.downloads import (
    find_metalinks,
    record_inputs,
    unlisted_inputs,
    write_record,
)
from rdfsolve.qlever.hdt import HDT_JAVA_URL, convert_hdt_steps
from rdfsolve.qlever.inputs import index_inputs, qlever_format
from rdfsolve.qlever.utils import analyse_source, download_file_names

TINY = Path(__file__).parents[1] / "test_data" / "tiny.hdt"
# A DVC remote serves a file under its MD5, without an extension (BioBricks' public remote).
MD5 = "583ce780c4139a0e3e7ac8f0f222ab25"
URL = f"https://ins-dvc.s3.amazonaws.com/insdvc/files/md5/{MD5[:2]}/{MD5[2:]}"
ENTRY = {"name": "okn-fixture", "download_hdt": [URL]}


def _run(steps, folder, *, java=None):
    """Run the steps; *java* is a JAVA_HOME, or "" for none (PATH without java either)."""
    env = dict(os.environ)
    if java is not None:
        env.pop("JAVA_HOME", None)
        if java:
            env["JAVA_HOME"] = str(java)
        else:
            tools = folder / "no-java-bin"
            tools.mkdir()
            for tool in ("gzip", "mv", "rm", "tar", "wget", "sha256sum"):
                found = shutil.which(tool)
                if found:
                    (tools / tool).symlink_to(found)
            env["PATH"] = str(tools)
    return subprocess.run(
        ["/bin/bash"], input=" && ".join(steps), text=True, cwd=folder, env=env,
        capture_output=True,
    )


def test_an_hdt_download_is_saved_by_its_format_and_converted_before_indexing(tmp_path):
    name = download_file_names(ENTRY)[URL]
    assert name == f"{MD5[2:]}.hdt", "A download without an extension is named by its field"
    assert analyse_source(ENTRY).needs_hdt_conversion
    parsed = configparser.ConfigParser(interpolation=None)
    parsed.read_string(build_qleverfile(ENTRY, tmp_path, 7000, "singularity"))
    command = parsed["data"]["GET_DATA_CMD"]
    assert f'-O "{name}" "{URL}"' in command
    assert HDT_JAVA_URL in command and "sha256sum -c" in command, "The converter is pinned"
    assert "hdt2rdf.sh" in command and "gzip -1" in command
    assert parsed["data"]["FORMAT"] == "nt" and parsed["index"]["INPUT_FILES"] == "rdf/*.hdt.nt.gz"
    converted = tmp_path / f"{name}.nt.gz"
    assert unlisted_inputs({URL: name}, [converted]) == [], "The converted file is the download's"


def _fake_converter(folder, body):
    """Put a stand-in hdt-java package in the folder (hdt2rdf.sh runs BODY) and a stand-in
    Java; return the JAVA_HOME."""
    tools = folder / "hdt-java-package-3.0.10" / "bin"
    tools.mkdir(parents=True)
    (tools / "hdt2rdf.sh").write_text(f"#!/bin/bash\n{body}\n")
    java = folder / "jdk"
    (java / "bin").mkdir(parents=True)
    (java / "bin" / "java").write_text("#!/bin/sh\n")
    (java / "bin" / "java").chmod(0o755)
    return java


def test_a_failed_hdt_conversion_stops_the_step_and_leaves_no_input(tmp_path):
    (tmp_path / "graph.hdt").write_bytes(b"not hdt")
    java = _fake_converter(
        tmp_path, "echo '<urn:a> <urn:p> <urn:b> .' > \"$2\"; echo broken >&2; exit 3"
    )
    done = _run(convert_hdt_steps(), tmp_path, java=java)
    assert done.returncode != 0 and "HDT conversion failed: graph.hdt" in done.stderr
    assert not list(tmp_path.glob("graph.hdt.nt.gz*")), "No partial output is left"
    assert index_inputs(tmp_path) == []


def test_the_conversion_stops_first_without_java(tmp_path):
    (tmp_path / "graph.hdt").write_bytes(b"hdt")
    done = _run(convert_hdt_steps(), tmp_path, java="")
    assert done.returncode != 0 and "hdt2rdf needs Java" in done.stderr
    assert not list(tmp_path.glob("graph.hdt.nt.gz*"))


def test_a_converted_hdt_is_an_index_input_and_converted_once(tmp_path):
    (tmp_path / "graph.hdt").write_bytes(b"hdt")
    java = _fake_converter(tmp_path, "echo '<urn:a> <urn:p> <urn:b> .' > \"$2\"")
    assert _run(convert_hdt_steps(), tmp_path, java=java).returncode == 0
    output = tmp_path / "graph.hdt.nt.gz"
    assert gzip.decompress(output.read_bytes()) == b"<urn:a> <urn:p> <urn:b> .\n"
    assert index_inputs(tmp_path) == [output] and qlever_format(output) == "nt"
    assert not (tmp_path / "hdt-java-package-3.0.10").exists(), "The tools are removed"
    output.write_bytes(gzip.compress(b"kept\n"))
    assert _run(convert_hdt_steps(), tmp_path, java=java).returncode == 0
    assert gzip.decompress(output.read_bytes()) == b"kept\n", "A converted file is kept"


@pytest.mark.integration
@pytest.mark.skipif(shutil.which("java") is None, reason="hdt-java needs Java")
def test_hdt_java_writes_every_triple_of_an_hdt_file(tmp_path):
    """Fetches the pinned hdt-java release (19.7 MB) and converts a four-triple HDT."""
    shutil.copy(TINY, tmp_path / "tiny.hdt")
    done = _run(convert_hdt_steps(), tmp_path)
    assert done.returncode == 0, done.stderr
    lines = gzip.decompress((tmp_path / "tiny.hdt.nt.gz").read_bytes()).decode().splitlines()
    assert sorted(lines) == sorted(
        [
            '<urn:a> <urn:p> "x\\ny" .',
            "<urn:a> <urn:q> <urn:b> .",
            '<urn:b> <urn:p> "\\u00E9"@fr .',
            '_:b1 <urn:p> "1"^^<http://www.w3.org/2001/XMLSchema#int> .',
        ]
    )


def test_a_hash_the_registry_gives_is_recorded_and_checked(tmp_path):
    from rdfsolve.models.source_model import SourceModel

    content = b"hdt bytes"
    md5 = hashlib.md5(content).hexdigest()  # noqa: S324 - the hash a publisher gives
    entry = SourceModel.model_validate({**ENTRY, "checksums": {URL: {"md5": md5}}})
    releases = find_metalinks([URL], {}, fetch=lambda url: None, declared=entry.checksums)
    assert releases[URL] == {"version": None, "hashes": {"md5": md5}, "declared": ["md5"]}
    (tmp_path / "rdf").mkdir()
    path = tmp_path / "rdf" / "graph.hdt"
    state = {"last_modified": "Mon, 01 Sep 2026 10:00:00 GMT", "content_length": "9"}
    for body, check in ((content, "match"), (b"changed", "mismatch")):
        path.write_bytes(body)
        write_record(
            tmp_path, [URL], lambda url: state,
            lambda urls, states: find_metalinks(
                urls, states, fetch=lambda url: None, declared=entry.checksums
            ),
        )
        manifest = record_inputs(tmp_path, {URL: path}, [], source="demo")
        assert manifest["downloads"][0]["publisher_check"] == check
        assert manifest["files"][0]["md5"] == hashlib.md5(body).hexdigest()  # noqa: S324
    with pytest.raises(ValueError, match="checksums"):
        SourceModel.model_validate({**ENTRY, "checksums": {URL: {"md5": "abc"}}})
    with pytest.raises(ValueError, match="checksums"):
        SourceModel.model_validate({**ENTRY, "checksums": {URL: {"crc32": md5}}})


def test_the_checksums_of_an_entry_are_not_read_as_downloads(tmp_path):
    """Job 115555 stopped at "'dict' object has no attribute 'rstrip'" when the hashes were a
    download_* field: every download_* field is read as URLs."""
    entry = {**ENTRY, "checksums": {URL: {"md5": MD5}}}
    assert analyse_source(entry).urls == [URL]
    assert list(download_file_names(entry)) == [URL]
    build_qleverfile(entry, tmp_path, 7000, "singularity")
