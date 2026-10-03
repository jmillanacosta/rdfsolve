"""Harvest explicitly configured provider RDF artifacts without mixing them into observations."""

from __future__ import annotations

import shutil
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field
from rdflib import Graph

from rdfsolve.evidence.declared import (
    DeclaredArtifact,
    DeclaredEvidence,
    archive_graph_artifact,
    project_declared_evidence,
)


class DeclaredArtifactBundle(BaseModel):
    """Declared artifacts and evidence read for one dataset."""

    dataset_id: str
    access_context: Literal["remote_endpoint", "local_distribution", "grouped_local_distribution"]
    artifacts: list[DeclaredArtifact] = Field(default_factory=list)
    evidence: list[DeclaredEvidence] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


def _format_for_path(path: str) -> str:
    suffix = Path(path.split("?", 1)[0]).suffix.lower()
    return {
        ".ttl": "turtle",
        ".trig": "trig",
        ".nt": "nt",
        ".nq": "nquads",
        ".rdf": "xml",
        ".owl": "xml",
        ".jsonld": "json-ld",
        ".json": "json-ld",
    }.get(suffix, "turtle")


def _archive_source_bytes(
    *,
    payload: bytes,
    dataset_id: str,
    source_url: str,
    output_path: Path,
    retrieval_method: str,
) -> tuple[DeclaredArtifact, Graph]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload)
    digest = sha256(payload).hexdigest()
    graph = Graph().parse(data=payload.decode("utf-8"), format=_format_for_path(source_url))
    from datetime import datetime, timezone

    artifact = DeclaredArtifact(
        artifact_id=f"declared:{digest}",
        dataset_id=dataset_id,
        kind="sparql_examples",
        source_url=source_url,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        sha256=digest,
        media_type=None,
        local_path=str(output_path),
        representation="source_bytes",
        parse_status="parsed",
        complete=True,
        retrieval_method=retrieval_method,
    )
    return artifact, graph


def harvest_configured_declared_artifacts(
    *,
    source: Any,
    dataset_id: str,
    output_dir: str | Path,
    access_context: Literal["remote_endpoint", "local_distribution", "grouped_local_distribution"],
    helper: Any | None = None,
    base_dir: str | Path = ".",
    max_download_bytes: int = 20_000_000,
) -> DeclaredArtifactBundle:
    """Archive configured SPARQL-example/SHACL artifacts and project supported evidence.

    Endpoint named graphs are harvested only from the remote endpoint context.
    Dump locations are independent provider artifacts and may be archived from any
    run context.  No harvested statement is inserted into empirical patterns.
    """
    bundle = DeclaredArtifactBundle(dataset_id=dataset_id, access_context=access_context)
    locations = getattr(source, "sparql_examples", None)
    if locations is None:
        return bundle
    root = Path(output_dir) / "declared"
    root.mkdir(parents=True, exist_ok=True)

    if access_context == "remote_endpoint" and helper is not None:
        for index, graph_iri in enumerate(locations.shacl_graph_in_endpoint):
            try:
                graph = helper.construct_graph(
                    f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{graph_iri}> {{ ?s ?p ?o }} }}"
                )
                artifact = archive_graph_artifact(
                    graph=graph,
                    dataset_id=dataset_id,
                    kind="sparql_examples",
                    output_path=root / f"sparql_examples_graph_{index:03d}.ttl",
                    source_url=getattr(source, "endpoint", None) or None,
                    source_graph=graph_iri,
                    retrieval_method="sparql-construct",
                    complete=None,
                )
                bundle.artifacts.append(artifact)
                bundle.evidence.extend(project_declared_evidence(graph, artifact))
            except Exception as exc:
                bundle.errors.append(f"graph {graph_iri}: {type(exc).__name__}: {exc}")

    for index, location in enumerate(locations.shacl_dumps):
        try:
            if location.startswith(("http://", "https://")):
                import requests

                with requests.get(location, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    content = bytearray()
                    for chunk in response.iter_content(65536):
                        content.extend(chunk)
                        if len(content) > max_download_bytes:
                            raise ValueError("Declared RDF artifact exceeds configured size limit")
                payload = bytes(content)
                method = "configured-http-artifact"
            else:
                original = Path(location)
                if not original.is_absolute():
                    original = Path(base_dir) / original
                payload = original.read_bytes()
                method = "configured-local-artifact"
            suffix = Path(location.split("?", 1)[0]).suffix or ".ttl"
            artifact, graph = _archive_source_bytes(
                payload=payload,
                dataset_id=dataset_id,
                source_url=location,
                output_path=root / f"sparql_examples_dump_{index:03d}{suffix}",
                retrieval_method=method,
            )
            bundle.artifacts.append(artifact)
            bundle.evidence.extend(project_declared_evidence(graph, artifact))
        except Exception as exc:
            bundle.errors.append(f"artifact {location}: {type(exc).__name__}: {exc}")

    return bundle


def empirical_graph_scope(source: Any) -> list[str]:
    """Return configured data graphs after removing explicitly known example graphs."""
    graph_uris = list(getattr(source, "graph_uris", None) or [])
    locations = getattr(source, "sparql_examples", None)
    if not graph_uris or locations is None:
        return graph_uris
    excluded = set(locations.shacl_graph_in_endpoint)
    selected = [iri for iri in graph_uris if iri not in excluded]
    if graph_uris and excluded and not selected:
        raise ValueError(
            "All configured graph_uris are declared SPARQL-example/SHACL graphs; "
            "configure the empirical data graph scope explicitly"
        )
    return selected


__all__ = [
    "DeclaredArtifactBundle",
    "empirical_graph_scope",
    "harvest_configured_declared_artifacts",
]
