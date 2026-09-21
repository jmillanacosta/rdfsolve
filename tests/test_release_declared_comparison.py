from pathlib import Path

from click.testing import CliRunner
from rdflib import Graph, Namespace, RDF, SH

from rdfsolve.analysis.release_declared import (
    build_release_declared_comparison,
    write_release_declared_comparison,
)
from rdfsolve.cli import main
from rdfsolve.evidence.declared import DeclaredArtifact, project_declared_evidence
from rdfsolve.evidence.declared_sources import DeclaredArtifactBundle
from rdfsolve.release.build import build_release_manifest, write_release_manifest
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern


def _release(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(
        "sources:\n  demo:\n    endpoint: https://example.org/sparql\n", encoding="utf-8"
    )
    ds_dir = tmp_path / "demo"
    ds_dir.mkdir()
    schema = MinedSchema(
        about=AboutMetadata(dataset_name="demo", endpoint="https://example.org/sparql"),
        patterns=[
            SchemaPattern(
                subject_class="urn:ex:Person",
                property_uri="urn:ex:p",
                object_class="urn:ex:Place",
            )
        ],
    )
    (ds_dir / "demo_schema.json").write_text(
        __import__("json").dumps(schema.to_dict(), indent=2), encoding="utf-8"
    )

    ex = Namespace("urn:ex:")
    graph = Graph()
    graph.add((ex.Shape, RDF.type, SH.NodeShape))
    graph.add((ex.Shape, SH.targetClass, ex.Person))
    graph.add((ex.Shape, SH.property, ex.PS))
    graph.add((ex.PS, SH.path, ex.p))
    graph.add((ex.PS, SH["class"], ex.Place))
    artifact = DeclaredArtifact(
        artifact_id="declared:fixture",
        dataset_id="demo",
        kind="shacl",
        source_graph="urn:graph:shapes",
        retrieved_at="2026-09-21T00:00:00+00:00",
        sha256="0" * 64,
        local_path="declared/shapes.ttl",
        representation="constructed_graph",
        retrieval_method="fixture",
    )
    bundle = DeclaredArtifactBundle(
        dataset_id="demo",
        access_context="remote_endpoint",
        artifacts=[artifact],
        evidence=project_declared_evidence(graph, artifact),
    )
    (ds_dir / "demo_declared_artifacts.json").write_text(
        bundle.model_dump_json(indent=2), encoding="utf-8"
    )
    manifest = build_release_manifest(tmp_path, release_id="release:test")
    write_release_manifest(manifest, tmp_path)
    return manifest


def test_release_declared_comparison_uses_frozen_artifacts_only(tmp_path):
    manifest = _release(tmp_path)
    result = build_release_declared_comparison(manifest, tmp_path)
    assert result.compared_datasets == ["demo"]
    by_dim = {row.dimension: row for row in result.comparisons}
    assert by_dim["property"].relation == "both"
    assert by_dim["class"].relation == "equal"
    json_path, tsv_path = write_release_declared_comparison(result, tmp_path / "analysis")
    assert json_path.exists() and tsv_path.exists()
    assert "urn:ex:Person" in tsv_path.read_text(encoding="utf-8")


def test_release_compare_declared_cli(tmp_path):
    _release(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["release", "compare-declared", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "analysis" / "observed_declared" / "observed_declared.json").exists()
