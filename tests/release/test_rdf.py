"""rdfsolve.release.rdf: a release described as RDF."""

from datetime import datetime, timezone
from rdflib import Graph
from rdfsolve.release.model import (
    DatasetReleaseRecord,
    InputDownloadRecord,
    ReleaseArtifact,
    ReleaseManifest,
)
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


def test_the_pinned_inputs_of_a_snapshot_are_distributions_it_was_derived_from():
    from datetime import UTC

    from rdfsolve.release.rdf import PROV

    item = InputDownloadRecord(
        url="https://example.org/current/demo.ttl.gz",
        final_url="https://mirror.example.org/demo.ttl.gz",
        sha256="c" * 64,
        byte_size=7,
        release_version="2026_03",
        last_modified="Wed, 02 Sep 2026 14:00:00 GMT",
    )
    manifest = ReleaseManifest(
        release_id="release:test",
        issued=datetime(2026, 9, 18, tzinfo=UTC),
        run_root="run",
        datasets=[
            DatasetReleaseRecord(
                dataset_id="demo",
                snapshot_id="https://example.org/snapshot",
                input_downloads=[item, InputDownloadRecord(url="https://example.org/folder/")],
            )
        ],
    )
    graph = release_to_rdf(manifest)
    [distribution] = list(graph.objects(None, PROV.wasDerivedFrom))
    assert (distribution, DCAT.version, None) in graph
    assert str(graph.value(distribution, DCAT.downloadURL)) == item.url
    [value] = list(graph.objects(None, SPDX.checksumValue))
    assert str(value) == "c" * 64
