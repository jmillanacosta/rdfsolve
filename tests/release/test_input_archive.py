"""rdfsolve.release.input_archive: the pinned downloads of a run are kept as a BagIt bag of
hard links (or copies), checked against their pins, and the release names the bag and each
download's place in it."""

import hashlib
import json
from pathlib import Path

import pytest
import yaml
from rdflib import URIRef

from rdfsolve.release.build import build_release_manifest
from rdfsolve.release.input_archive import archive_inputs, verify_archive
from rdfsolve.release.rdf import release_to_rdf

URL = "https://example.org/demo.ttl.gz"


def _run(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Return a run with one source's pins, its work folders and the downloaded file."""
    run, workdirs = tmp_path / "run", tmp_path / "workdirs"
    data = b"<urn:a> <urn:p> <urn:b> .\n"
    downloaded = workdirs / "demo" / "rdf" / "demo.ttl.gz"
    downloaded.parent.mkdir(parents=True)
    downloaded.write_bytes(data)
    converted = workdirs / "demo" / "rdf" / "demo.nt"
    converted.write_bytes(data)
    (run / "demo").mkdir(parents=True)
    (run / "sources.yaml").write_text(
        yaml.safe_dump(
            [{"name": "demo", "download_ttl": [URL], "bioregistry_license": "CC-BY-4.0"}]
        )
    )
    sha = hashlib.sha256(data).hexdigest()
    stat = downloaded.stat()
    pins = {
        "format": "rdfsolve-inputs/1",
        "source": "demo",
        "recorded": "before_index",
        "urls": [URL],
        "downloads": [{"url": URL, "path": "rdf/demo.ttl.gz", "bytes": len(data), "sha256": sha}],
        "files": [
            {
                "path": "rdf/demo.ttl.gz",
                "bytes": len(data),
                "modified_ns": stat.st_mtime_ns,
                "sha256": sha,
                "url": URL,
                "index_input": False,
            },
            {"path": "rdf/demo.nt", "bytes": len(data), "sha256": sha, "index_input": True},
        ],
    }
    (run / "demo" / "demo_local_inputs.json").write_text(json.dumps(pins))
    return run, workdirs, downloaded


def test_the_downloads_are_kept_as_a_bag_and_the_release_names_it(tmp_path):
    run, workdirs, downloaded = _run(tmp_path)
    bag = tmp_path / "bag"
    record = archive_inputs(
        run, workdirs, bag, location="https://archive.example/run/", licenses={"demo": "CC-BY-4.0"}
    )
    kept = bag / "data" / "demo" / "rdf" / "demo.ttl.gz"
    assert kept.samefile(downloaded), "a hard link on the same file system"
    assert not kept.stat().st_mode & 0o222, "read-only: a later download cannot change it"
    assert not (bag / "data" / "demo" / "rdf" / "demo.nt").exists(), "only downloads are kept"
    sha = hashlib.sha256(downloaded.read_bytes()).hexdigest()
    assert (bag / "manifest-sha256.txt").read_text() == f"{sha}  data/demo/rdf/demo.ttl.gz\n"
    assert (bag / "fetch.txt").read_text() == f"{URL} 26 data/demo/rdf/demo.ttl.gz\n"
    assert "BagIt-Version: 1.0" in (bag / "bagit.txt").read_text()
    assert "Payload-Oxum: 26.1" in (bag / "bag-info.txt").read_text()
    assert verify_archive(bag) == []
    assert record["sources"] == {"demo": {"files": 1, "bytes": 26, "license": "CC-BY-4.0"}}
    assert record["placed"] == {"link": 1, "copy": 0}

    manifest = build_release_manifest(run, release_id="test")
    archive = manifest.input_archive
    assert archive is not None and archive.location == "https://archive.example/run/"
    assert (
        archive.manifest_sha256
        == hashlib.sha256((bag / "manifest-sha256.txt").read_bytes()).hexdigest()
    )
    assert archive.record_artifact == next(
        a.artifact_id for a in manifest.artifacts if a.role == "input_archive_record"
    )
    [download] = manifest.datasets[0].input_downloads
    assert download.archive_path == "data/demo/rdf/demo.ttl.gz"
    graph = release_to_rdf(manifest)
    assert URIRef("https://archive.example/run/data/demo/rdf/demo.ttl.gz") in set(graph.objects())

    again = archive_inputs(run, workdirs, bag, copy=True)  # rerun: the same bag, copied
    assert again["manifest_sha256"] == record["manifest_sha256"]


def test_a_file_that_is_not_the_pinned_file_is_refused_and_a_damaged_bag_is_found(tmp_path):
    run, workdirs, downloaded = _run(tmp_path)
    downloaded.write_bytes(b"another release\n")
    with pytest.raises(ValueError, match="not the pinned file"):
        archive_inputs(run, workdirs, tmp_path / "bag")
    assert not (tmp_path / "bag").exists(), "nothing is written"
    assert not (run / "input_archive.json").exists()

    run, workdirs, downloaded = _run(tmp_path / "second")
    bag = tmp_path / "second" / "bag"
    archive_inputs(run, workdirs, bag, copy=True)
    kept = bag / "data" / "demo" / "rdf" / "demo.ttl.gz"
    assert not kept.samefile(downloaded)
    kept.chmod(0o644)
    kept.write_bytes(b"damaged\n")
    assert verify_archive(bag) == [
        "data/demo/rdf/demo.ttl.gz: sha256 differs from manifest-sha256.txt"
    ]
