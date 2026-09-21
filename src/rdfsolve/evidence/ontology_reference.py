"""Acquire unique reference ontologies and assess empirical KG usage against them.

Reference acquisition is intentionally post-mining.  The same ontology artifact
is downloaded once per run and can then be compared with every dataset that uses
terms from its namespace.  Unless provider version evidence matches the artifact,
this establishes *reference resolvability*, not that the endpoint used that exact
ontology release.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from rdflib import Graph

from rdfsolve.evidence.ontology import (
    OntologyArtifact,
    OntologyUsage,
    assess_ontology_usage,
    observed_terms_from_patterns,
)
from rdfsolve.evidence.ontology_acquisition import OntologyAcquisitionPlan, OntologyUsageCandidate
from rdfsolve.evidence.ontology_artifacts import fetch_and_archive_ontology
from rdfsolve.evidence.ontology_registry import OntologyRegistry


class OntologyReferenceRun(BaseModel):
    """Artifacts, usage files and failures of one ontology reference run."""

    registry_path: str
    acquired_artifacts: list[str] = Field(default_factory=list)
    dataset_usage_files: list[str] = Field(default_factory=list)
    failures: dict[str, str] = Field(default_factory=dict)


def _load_patterns(schema_path: Path) -> list[dict[str, Any]]:
    raw = json.loads(schema_path.read_text(encoding="utf-8"))
    schema = (
        raw.get("schema") if isinstance(raw, dict) and isinstance(raw.get("schema"), dict) else raw
    )
    patterns = schema.get("patterns", []) if isinstance(schema, dict) else []
    return [item for item in patterns if isinstance(item, dict)]


def _version_match(candidate: OntologyUsageCandidate, artifact: OntologyArtifact) -> str:
    provider_values: set[str] = set()
    provider_iris: set[str] = set()
    for graph in candidate.graph_evidence:
        for evidence in graph.version_evidence:
            if evidence.scope not in {"ontology", "graph"}:
                continue
            if evidence.predicate.endswith("versionIRI"):
                provider_iris.add(evidence.value)
            else:
                provider_values.add(evidence.value)
    if not provider_values and not provider_iris:
        return "unknown"
    artifact_values = (
        set(artifact.version_values) | set(artifact.issued_values) | set(artifact.modified_values)
    )
    artifact_iris = set(artifact.version_iris)
    if (provider_iris and provider_iris & artifact_iris) or (
        provider_values and provider_values & artifact_values
    ):
        return "matched"
    # A stated provider version and a stated artifact version that differ is
    # informative, but an unversioned artifact cannot establish a mismatch.
    if (provider_iris and artifact_iris) or (provider_values and artifact_values):
        return "mismatched"
    return "unknown"


def _source_key(candidate: OntologyUsageCandidate) -> tuple[str, str] | None:
    if not candidate.ontology_id or not candidate.reference_sources:
        return None
    source = candidate.reference_sources[0]
    return candidate.ontology_id, source.source_url


def acquire_reference_ontologies(
    output_dir: str | Path,
    *,
    fetcher: Callable[[str], bytes] | None = None,
    max_artifacts: int | None = None,
) -> OntologyReferenceRun:
    """Acquire all unique reference sources described by ontology acquisition plans.

    Dataset schema files are read as serialized JSON so this stage does not import
    optional schema-conversion dependencies.
    """
    root = Path(output_dir)
    ontology_root = root / "ontologies"
    cache = ontology_root / "artifacts"
    registry_path = ontology_root / "registry.json"
    registry = (
        OntologyRegistry.load(registry_path) if registry_path.exists() else OntologyRegistry()
    )

    plan_rows: list[tuple[Path, OntologyAcquisitionPlan]] = []
    by_source: dict[tuple[str, str], list[tuple[Path, OntologyUsageCandidate]]] = defaultdict(list)
    local_file_sources: dict[str, list[tuple[Path, str | None, str]]] = defaultdict(list)
    for path in sorted(root.glob("*/*_ontology_acquisition.json")):
        plan = OntologyAcquisitionPlan.model_validate_json(path.read_text(encoding="utf-8"))
        plan_rows.append((path, plan))
        for candidate in plan.candidates:
            key = _source_key(candidate)
            if key:
                by_source[key].append((path, candidate))
        for source in plan.local_ontology_file_candidates:
            local_file_sources[source.source_url].append(
                (path, source.source_dataset_id, source.source_field)
            )

    failures: dict[str, str] = {}
    graph_by_artifact: dict[str, Graph] = {}
    artifact_by_source: dict[tuple[str, str], OntologyArtifact] = {}
    for idx, ((ontology_id, source_url), _rows) in enumerate(sorted(by_source.items())):
        if max_artifacts is not None and idx >= max_artifacts:
            break
        # Reuse a previously archived artifact from this exact source URL when
        # present; otherwise fetch one immutable content-addressed artifact.
        existing = next(
            (
                (artifact, Path(artifact.local_path))
                for artifact in registry.releases(ontology_id)
                if artifact.source_url == source_url
                and artifact.local_path
                and Path(artifact.local_path).exists()
            ),
            None,
        )
        try:
            if existing is not None:
                from rdfsolve.evidence.ontology_artifacts import parse_ontology_bytes

                artifact, existing_path = existing
                graph = parse_ontology_bytes(existing_path.read_bytes(), source_name=source_url)
            else:
                artifact, graph = fetch_and_archive_ontology(
                    source_url, cache_dir=cache, fetcher=fetcher
                )
                registry.register_artifact(
                    ontology_id,
                    artifact,
                    preferred_iri=artifact.ontology_iris[0] if artifact.ontology_iris else None,
                    prefixes=[ontology_id],
                )
            artifact_by_source[(ontology_id, source_url)] = artifact
            graph_by_artifact[artifact.artifact_id] = graph
        except Exception as error:
            failures[source_url] = str(error)

    # Local-distribution OWL files must be assessed from the exact archived
    # bytes used/prepared by the local run.  They are never silently refetched
    # here, because a later URL response may differ from the mined snapshot.
    local_artifacts: dict[tuple[Path, str], tuple[OntologyArtifact, Graph, str]] = {}
    from rdfsolve.evidence.ontology_artifacts import archive_ontology_file

    for plan_path, plan in plan_rows:
        for source in plan.local_ontology_file_candidates:
            if not source.archived_path:
                continue
            archived = plan_path.parent / source.archived_path
            if not archived.is_file():
                continue
            local_key = (plan_path, source.source_url)
            try:
                artifact, graph = archive_ontology_file(
                    archived,
                    cache_dir=cache,
                    source_url=source.source_url,
                )
                if source.sha256 and artifact.sha256 != source.sha256:
                    raise ValueError(f"Archived local ontology hash changed: {source.source_url}")
                ontology_id = (
                    artifact.ontology_iris[0]
                    if artifact.ontology_iris
                    else f"artifact:{artifact.sha256[:16] if artifact.sha256 else 'unknown'}"
                )
                registry.register_artifact(
                    ontology_id,
                    artifact,
                    preferred_iri=(artifact.ontology_iris[0] if artifact.ontology_iris else None),
                )
                local_artifacts[local_key] = (artifact, graph, ontology_id)
                graph_by_artifact[artifact.artifact_id] = graph
            except Exception as error:
                failures[f"{plan.dataset_id}:{source.source_url}"] = str(error)

    registry.save(registry_path)

    usage_files: list[str] = []
    for plan_path, plan in plan_rows:
        dataset_dir = plan_path.parent
        schema_paths = sorted(dataset_dir.glob("*_schema.json"))
        if not schema_paths:
            continue
        patterns = _load_patterns(schema_paths[0])
        usages: list[OntologyUsage] = []
        for candidate in plan.candidates:
            key = _source_key(candidate)
            if key is None or key not in artifact_by_source:
                continue
            artifact = artifact_by_source[key]
            graph = graph_by_artifact[artifact.artifact_id]
            usage = assess_ontology_usage(
                patterns,
                graph,
                dataset_id=plan.dataset_id,
                ontology_artifact_id=artifact.artifact_id,
                expected_classes=candidate.observed_classes,
                expected_properties=candidate.observed_properties,
            )
            usage.ontology_id = candidate.ontology_id
            usage.namespace = candidate.namespace
            usage.artifact_relation = "reference_release"
            usage.version_match_status = _version_match(candidate, artifact)  # type: ignore[assignment]
            usages.append(usage)

        # OWL-formatted files found in a *local source distribution* are assessed
        # against the empirical terms from that local/grouped mine.  They are not
        # used for remote endpoint runs and are retained only when overlap exists.
        observed = observed_terms_from_patterns(patterns)
        for source in plan.local_ontology_file_candidates:
            row = local_artifacts.get((plan_path, source.source_url))
            if row is None:
                continue
            artifact, graph, ontology_id = row
            usage = assess_ontology_usage(
                patterns,
                graph,
                dataset_id=plan.dataset_id,
                ontology_artifact_id=artifact.artifact_id,
                expected_classes=observed.classes,
                expected_properties=observed.properties,
            )
            if not usage.resolved_classes and not usage.resolved_properties:
                continue
            usage.ontology_id = ontology_id
            usage.artifact_relation = "local_distribution_artifact"
            usage.version_match_status = "unknown"
            usages.append(usage)
        if usages:
            out = dataset_dir / f"{plan.dataset_id}_ontology_usage.json"
            out.write_text(
                json.dumps(
                    {"dataset_id": plan.dataset_id, "usages": [u.model_dump() for u in usages]},
                    indent=2,
                ),
                encoding="utf-8",
            )
            usage_files.append(out.relative_to(root).as_posix())

    return OntologyReferenceRun(
        registry_path=registry_path.relative_to(root).as_posix(),
        acquired_artifacts=sorted(graph_by_artifact),
        dataset_usage_files=usage_files,
        failures=failures,
    )


__all__ = ["OntologyReferenceRun", "acquire_reference_ontologies"]
