"""Build a canonical release manifest from one rdfsolve run directory."""

from __future__ import annotations

import hashlib
import json
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from rdfsolve.config import get_base_uri, mint_from_base
from rdfsolve.models.source_model import SourceModel

from .model import (
    DatasetReleaseRecord,
    ExtractionReleaseRecord,
    OntologyReleaseRef,
    OntologyUsageReleaseRecord,
    ReleaseArtifact,
    ReleaseManifest,
)

_SELF_FILES = {
    "release.json",
    "release.ttl",
    "summary.json",
    "summary.tsv",
    "validation_release.json",
}


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return the SHA-256 hex digest of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_id(path: Path, sha256: str) -> str:
    # Content-addressed identity is stable across release-directory relocation.
    return f"sha256:{sha256}"


def _media_type(path: Path) -> str | None:
    suffixes = "".join(path.suffixes).lower()
    overrides = {
        ".ttl": "text/turtle",
        ".trig": "application/trig",
        ".json": "application/json",
        ".jsonld": "application/ld+json",
        ".yaml": "application/yaml",
        ".yml": "application/yaml",
        ".tsv": "text/tab-separated-values",
        ".tsv.gz": "application/gzip",
        ".txt": "text/plain",
    }
    if suffixes in overrides:
        return overrides[suffixes]
    if path.suffix.lower() in overrides:
        return overrides[path.suffix.lower()]
    return mimetypes.guess_type(path.name)[0]


def _role(path: Path) -> str | None:
    name = path.name
    if name == "scientific_checks.json":
        return "scientific_validation_plan"
    if name.endswith("scientific_check_results.json"):
        return "scientific_validation_results"
    for marker, role in (
        ("_schema.json", "canonical_schema"),
        ("_schema.jsonld", "schema_jsonld"),
        ("_report.json", "mining_report"),
        ("_metadata.ttl", "retrieved_metadata"),
        ("_ontology_discovery.json", "ontology_discovery"),
        ("_property_usage.json", "property_usage_evidence"),
        ("_declared_artifacts.json", "declared_artifact_index"),
        ("_ontology_acquisition.json", "ontology_acquisition"),
        ("_ontology.ttl", "generated_ontology_slice"),
        ("_void.ttl", "generated_void"),
        ("_dataset.trig", "retrieved_dataset_description"),
    ):
        if name.endswith(marker):
            return role
    if name == "sources.yaml":
        return "source_registry"
    if name == "environment.txt":
        return "environment"
    if name.endswith(("config.yaml", "config.yml")) or name in {
        "pipeline_config.yaml",
        "pipeline_config.yml",
    }:
        return "pipeline_config"
    if name == "code_commit.txt":
        return "code_commit"
    if name == "identity_overrides.yaml":
        return "identity_overrides"
    if name == "sssom_sources.yaml":
        return "mapping_source_registry"
    if name.startswith("pipeline_results") and name.endswith(".json"):
        return "pipeline_results"
    if path.as_posix().endswith("ontologies/registry.json"):
        return "ontology_registry"
    return None


def _dataset_for_path(
    path: Path, run_root: Path, dataset_ids: set[str] | None = None
) -> str | None:
    rel = path.relative_to(run_root)
    if len(rel.parts) < 2:
        return None
    parent = rel.parts[0]
    if parent.startswith("."):
        return None
    if dataset_ids is not None and parent not in dataset_ids:
        return None
    return parent


def inventory_artifacts(
    run_root: Path, *, dataset_ids: set[str] | None = None
) -> list[ReleaseArtifact]:
    """List every file under a run directory as a content-addressed artifact."""
    rows: list[ReleaseArtifact] = []
    for path in sorted(run_root.rglob("*")):
        if not path.is_file() or path.name in _SELF_FILES:
            continue
        rel = path.relative_to(run_root)
        digest = sha256_file(path)
        rows.append(
            ReleaseArtifact(
                artifact_id=_artifact_id(rel, digest),
                path=rel.as_posix(),
                sha256=digest,
                byte_size=path.stat().st_size,
                media_type=_media_type(path),
                dataset_id=_dataset_for_path(path, run_root, dataset_ids),
                role=_role(path),
            )
        )
    return rows


def _load_sources(run_root: Path) -> dict[str, dict[str, Any]]:
    path = run_root / "sources.yaml"
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if isinstance(raw, dict) and "sources" in raw:
        raw = raw["sources"]
    if isinstance(raw, list):
        return {
            str(item.get("name")): item
            for item in raw
            if isinstance(item, dict) and item.get("name")
        }
    if isinstance(raw, dict):
        out: dict[str, dict[str, Any]] = {}
        for name, item in raw.items():
            if isinstance(item, dict):
                row = dict(item)
                row.setdefault("name", name)
                out[str(name)] = row
        return out
    return {}


def _source_access_files(source: dict[str, Any]) -> dict[str, list[str]]:
    """Preserve configured downloadable/access artifacts without over-classifying them.

    Registry fields such as ``download_owl`` may be ontology material rather
    than a dataset dump.  The release therefore retains the original field name
    and URL(s); downstream summaries can apply an explicit classification policy.
    """
    out: dict[str, list[str]] = {}
    for key, value in source.items():
        if not (key.startswith("download_") or key in {"local_tar_url"}):
            continue
        if not value:
            continue
        values = (
            [value]
            if isinstance(value, str)
            else list(value)
            if isinstance(value, (list, tuple))
            else []
        )
        cleaned = [str(item) for item in values if item]
        if cleaned:
            out[key] = cleaned
    return dict(sorted(out.items()))


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _reports_for_dataset(run_root: Path, dataset_id: str) -> list[tuple[Path, dict[str, Any]]]:
    directory = run_root / dataset_id
    if not directory.is_dir():
        return []
    return [(path, _load_json(path)) for path in sorted(directory.glob("*_report.json"))]


def _report_mode(path: Path) -> str:
    name = path.name
    for mode in ("remote", "local", "grouped"):
        if f"_{mode}_" in name:
            return mode
    return "unknown"


def _combined_completion(states: list[str]) -> str:
    if not states:
        return "unknown"
    if "unfinished" in states:
        return "unfinished"
    if all(state == "unknown" for state in states):
        return "unknown"
    if all(state == "complete" for state in states):
        return "complete"
    if all(state in {"failed", "skipped"} for state in states):
        return "failed" if "failed" in states else "skipped"
    return "partial"


def _completion(report: dict[str, Any]) -> str:
    if "finished_at" in report and not report["finished_at"]:
        return "unfinished"
    state = report.get("completion_state") or report.get("state")
    if isinstance(state, str) and state in {
        "complete",
        "partial",
        "failed",
        "unfinished",
        "skipped",
    }:
        return state
    # Older reports may only contain abort/failure information.
    if report.get("abort_reason"):
        return "failed"
    failures = report.get("query_failures")
    if failures:
        return "partial"
    return "unknown"


def _schema_metadata(path: Path) -> dict[str, Any]:
    raw = _load_json(path)
    schema = raw.get("schema") if isinstance(raw.get("schema"), dict) else raw
    about = (
        schema.get("about")
        if isinstance(schema, dict) and isinstance(schema.get("about"), dict)
        else {}
    )
    return about if isinstance(about, dict) else {}


def _ontology_usages(run_root: Path, dataset_id: str) -> list[OntologyUsageReleaseRecord]:
    directory = run_root / dataset_id
    acquisition_paths = (
        sorted(directory.glob("*_ontology_acquisition.json")) if directory.is_dir() else []
    )
    by_namespace: dict[str, OntologyUsageReleaseRecord] = {}
    if acquisition_paths:
        raw = _load_json(acquisition_paths[0])
        for item in raw.get("candidates", []) if isinstance(raw.get("candidates"), list) else []:
            if not isinstance(item, dict):
                continue
            namespace = str(item.get("namespace", ""))
            by_namespace[namespace] = OntologyUsageReleaseRecord(
                namespace=namespace,
                ontology_id=item.get("ontology_id"),
                identity_basis=str(item.get("identity_basis", "unresolved")),
                observed_class_count=len(item.get("observed_classes") or []),
                observed_property_count=len(item.get("observed_properties") or []),
                graph_evidence_count=len(item.get("graph_evidence") or []),
                reference_source_count=len(item.get("reference_sources") or []),
            )

    usage_paths = sorted(directory.glob("*_ontology_usage.json")) if directory.is_dir() else []
    if usage_paths:
        raw = _load_json(usage_paths[0])
        for usage in raw.get("usages", []) if isinstance(raw.get("usages"), list) else []:
            if not isinstance(usage, dict):
                continue
            namespace = str(usage.get("namespace") or "")
            row = by_namespace.get(namespace) or OntologyUsageReleaseRecord(
                namespace=namespace,
                ontology_id=usage.get("ontology_id"),
                identity_basis="reference_source",
            )
            row.ontology_id = usage.get("ontology_id") or row.ontology_id
            row.ontology_artifact_id = usage.get("ontology_artifact_id")
            row.artifact_relation = usage.get("artifact_relation")
            row.version_match_status = usage.get("version_match_status")
            row.resolved_class_count = len(usage.get("resolved_classes") or [])
            row.unresolved_class_count = len(usage.get("unresolved_classes") or [])
            row.resolved_property_count = len(usage.get("resolved_properties") or [])
            row.unresolved_property_count = len(usage.get("unresolved_properties") or [])
            by_namespace[namespace] = row
    return [by_namespace[key] for key in sorted(by_namespace)]


def _ontology_acquisition_metadata(run_root: Path, dataset_id: str) -> tuple[str | None, int]:
    directory = run_root / dataset_id
    paths = sorted(directory.glob("*_ontology_acquisition.json")) if directory.is_dir() else []
    if not paths:
        return None, 0
    raw = _load_json(paths[0])
    return (
        str(raw.get("mining_context")) if raw.get("mining_context") else None,
        len(raw.get("local_ontology_file_candidates") or []),
    )


def _artifact_refs_by_dataset(artifacts: list[ReleaseArtifact]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for artifact in artifacts:
        if artifact.dataset_id:
            out.setdefault(artifact.dataset_id, []).append(artifact.artifact_id)
    for ids in out.values():
        ids.sort()
    return out


def _snapshot_id(dataset_id: str, about: dict[str, Any], report: dict[str, Any]) -> str:
    """Return the one snapshot identity used throughout rdfsolve.

    Prefer the identity already minted when ``AboutMetadata`` was built.  Older
    runs that predate that field are upgraded deterministically with the same
    content-hash/retrieval-record semantics instead of hashing release artifacts
    and thereby creating a second identity for the same observation.
    """
    existing = about.get("snapshot_id")
    if isinstance(existing, str) and existing:
        return existing
    content_sha256 = about.get("content_sha256")
    if isinstance(content_sha256, str) and content_sha256:
        return mint_from_base(get_base_uri(), "snapshot", dataset_id, "sha256", content_sha256)
    retrieved = (
        about.get("retrieved_at")
        or about.get("generated_at")
        or report.get("started_at")
        or report.get("created_at")
        or "unknown-retrieval"
    )
    return mint_from_base(get_base_uri(), "snapshot", dataset_id, str(retrieved))


def _identity_review(run_root: Path) -> tuple[bool | None, int | None, int | None, str | None]:
    """Evaluate the frozen dataset-identity review inputs of a run.

    The registry-entry count remains available regardless. A canonical dataset
    denominator is returned only when every generated candidate relation has
    been adjudicated by rules or the frozen override file.
    """
    sources_path = run_root / "sources.yaml"
    if not sources_path.exists():
        return None, None, None, "sources.yaml is missing"
    try:
        from rdfsolve.dataset_identity import (
            read_overrides,
            read_registry,
            resolve_identity,
        )

        overrides_path = run_root / "identity_overrides.yaml"
        resolution = resolve_identity(
            read_registry(sources_path),
            read_overrides(overrides_path if overrides_path.exists() else None),
        )
    except Exception as error:
        return None, None, None, f"{type(error).__name__}: {error}"
    return (
        resolution.review_complete,
        len(resolution.candidates),
        resolution.canonical_dataset_count,
        None,
    )


def build_release_manifest(
    run_dir: str | Path,
    *,
    release_id: str | None = None,
    rdfsolve_version: str | None = None,
    issued: datetime | None = None,
    base_uri: str | None = None,
) -> ReleaseManifest:
    """Build the release manifest of one completed run directory."""
    run_root = Path(run_dir).resolve()
    frozen_base_uri = (base_uri or get_base_uri()).rstrip("/") + "/"
    sources = _load_sources(run_root)
    service_records = sorted(
        name
        for name, row in sources.items()
        if SourceModel.model_validate(row).source_role == "service"
    )
    dataset_dirs = {
        path.name
        for path in run_root.iterdir()
        if path.is_dir()
        and not path.name.startswith(".")
        and (
            path.name in sources
            or any(path.glob("*_schema.json"))
            or any(path.glob("*_report.json"))
        )
    }
    dataset_ids = sorted(
        (set(sources) - set(service_records))
        if (run_root / "sources.yaml").exists()
        else dataset_dirs
    )
    artifacts = inventory_artifacts(run_root, dataset_ids=set(dataset_ids))
    artifact_ids = _artifact_refs_by_dataset(artifacts)

    datasets: list[DatasetReleaseRecord] = []
    for dataset_id in dataset_ids:
        source = sources.get(dataset_id, {})
        reports = _reports_for_dataset(run_root, dataset_id)
        report_path, report = reports[0] if len(reports) == 1 else (None, {})
        schema_paths = sorted((run_root / dataset_id).glob("*_schema.json"))
        about = _schema_metadata(schema_paths[0]) if len(schema_paths) == 1 else {}
        extractions: list[ExtractionReleaseRecord] = []
        stems = {path.name.removesuffix("_report.json") for path, _ in reports}
        stems.update(path.name.removesuffix("_schema.json") for path in schema_paths)
        for stem in sorted(stems):
            schema_path = run_root / dataset_id / f"{stem}_schema.json"
            extraction_report = run_root / dataset_id / f"{stem}_report.json"
            raw = _load_json(extraction_report)
            metadata = _schema_metadata(schema_path)
            relative = schema_path.relative_to(run_root).as_posix()
            artifact = next((a for a in artifacts if a.path == relative), None)
            extractions.append(
                ExtractionReleaseRecord(
                    mode=_report_mode(extraction_report),
                    completion_state=_completion(raw),
                    report_path=extraction_report.relative_to(run_root).as_posix()
                    if extraction_report.exists()
                    else None,
                    schema_path=relative if artifact else None,
                    schema_artifact_id=artifact.artifact_id if artifact else None,
                    snapshot_id=_snapshot_id(dataset_id, metadata, raw) if metadata else None,
                    graph_scope=metadata.get("graph_uris") or [],
                    type_context_graph_scope=metadata.get("type_context_graph_uris") or [],
                    ontology_graph_scope=metadata.get("ontology_graph_uris") or [],
                    endpoint=metadata.get("endpoint"),
                    retrieved_at=metadata.get("retrieved_at") or metadata.get("started_at"),
                )
            )
        refs = artifact_ids.get(dataset_id, [])
        endpoint = source.get("endpoint") or None
        access_files = _source_access_files(source)
        downloads = [url for values in access_files.values() for url in values]
        graphs = source.get("graph_uris") or []
        if isinstance(graphs, str):
            graphs = [graphs]
        mode = extractions[0].mode if len(extractions) == 1 else None
        retrieved_at = (
            about.get("retrieved_at") or about.get("mined_at") or report.get("started_at")
        )
        ontology_context, local_ontology_file_count = _ontology_acquisition_metadata(
            run_root, dataset_id
        )
        datasets.append(
            DatasetReleaseRecord(
                dataset_id=dataset_id,
                dataset_kind=SourceModel.model_validate(
                    {"name": dataset_id, **source}
                ).dataset_kind,
                snapshot_id=_snapshot_id(dataset_id, about, report)
                if len(extractions) <= 1
                else None,
                source_version=about.get("source_version"),
                source_version_iri=about.get("source_version_iri"),
                retrieved_at=retrieved_at,
                endpoint=endpoint,
                distributions=[str(item) for item in downloads],
                access_files=access_files,
                graph_scope=[str(item) for item in graphs],
                graph_sources=source.get("graph_sources") or {},
                extraction_mode=mode,
                completion_state=_combined_completion(
                    [item.completion_state for item in extractions]
                ),
                report_path=report_path.relative_to(run_root).as_posix() if report_path else None,
                extractions=extractions,
                artifacts=refs,
                ontology_evidence_context=ontology_context,
                local_ontology_file_candidate_count=local_ontology_file_count,
                ontology_usages=_ontology_usages(run_root, dataset_id),
            )
        )

    code_commit = None
    commit_path = run_root / "code_commit.txt"
    if commit_path.exists():
        code_commit = commit_path.read_text(encoding="utf-8").strip() or None

    source_artifact = next((a.artifact_id for a in artifacts if a.role == "source_registry"), None)
    environment_artifact = next((a.artifact_id for a in artifacts if a.role == "environment"), None)
    pipeline_config_artifacts = sorted(
        a.artifact_id for a in artifacts if a.role == "pipeline_config"
    )
    identity_overrides_artifact = next(
        (a.artifact_id for a in artifacts if a.role == "identity_overrides"), None
    )
    (
        identity_review_complete,
        identity_candidate_count,
        canonical_dataset_count,
        identity_review_error,
    ) = _identity_review(run_root)
    ontology_registry_artifact = next(
        (a.artifact_id for a in artifacts if a.path == "ontologies/registry.json"),
        None,
    )
    if release_id is None:
        stable = {
            "run": run_root.name,
            "code_commit": code_commit,
            "source_registry": source_artifact,
            "identity_overrides": identity_overrides_artifact,
            "identity_review_complete": identity_review_complete,
            "canonical_dataset_count": canonical_dataset_count,
            "datasets": [
                (
                    d.dataset_id,
                    [(e.mode, e.snapshot_id, e.schema_artifact_id) for e in d.extractions],
                )
                for d in datasets
            ],
        }
        release_id = (
            "release:"
            + hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:24]
        )

    return ReleaseManifest(
        release_id=release_id,
        base_uri=frozen_base_uri,
        issued=issued or datetime.now(timezone.utc),
        rdfsolve_version=rdfsolve_version,
        code_commit=code_commit,
        run_root=run_root.name,
        source_registry_artifact=source_artifact,
        environment_artifact=environment_artifact,
        pipeline_config_artifacts=pipeline_config_artifacts,
        identity_overrides_artifact=identity_overrides_artifact,
        identity_review_complete=identity_review_complete,
        identity_candidate_count=identity_candidate_count,
        canonical_dataset_count=canonical_dataset_count,
        identity_review_error=identity_review_error,
        ontology_registry_artifact=ontology_registry_artifact,
        service_records=service_records,
        datasets=datasets,
        artifacts=artifacts,
    )


def write_release_manifest(manifest: ReleaseManifest, run_dir: str | Path) -> Path:
    """Write release.json into the run directory and return its path."""
    path = Path(run_dir) / "release.json"
    path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return path
