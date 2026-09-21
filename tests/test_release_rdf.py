from datetime import datetime, timezone
from pathlib import Path

from rdflib import Graph

from rdfsolve.release.model import DatasetReleaseRecord, ReleaseArtifact, ReleaseManifest
from rdfsolve.release.rdf import DCAT, SPDX, release_to_rdf


def test_release_rdf_projects_files_and_checksums_without_internal_status_vocab():
    artifact = ReleaseArtifact(
        artifact_id="sha256:" + "a" * 64,
        path="demo/schema.json",
        sha256="a" * 64,
        byte_size=12,
        media_type="application/json",
        dataset_id="demo",
    )
    manifest = ReleaseManifest(
        release_id="release:test",
        issued=datetime(2026, 9, 18, tzinfo=timezone.utc),
        run_root="run",
        datasets=[
            DatasetReleaseRecord(
                dataset_id="demo",
                snapshot_id="snapshot:demo:x",
                endpoint="https://example.org/sparql",
                artifacts=[artifact.artifact_id],
            )
        ],
        artifacts=[artifact],
    )
    graph = release_to_rdf(manifest)
    serialized = graph.serialize(format="turtle")
    reparsed = Graph().parse(data=serialized, format="turtle")
    assert len(list(reparsed.subjects(None, DCAT.Distribution))) >= 1
    assert len(list(reparsed.objects(None, SPDX.checksumValue))) == 1
    assert "actionStatus" not in serialized
