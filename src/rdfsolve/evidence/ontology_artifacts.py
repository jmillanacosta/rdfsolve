"""Versioned storage helpers for external ontology artifacts.

The complete retrieved ontology bytes are archived before any KG-specific usage
slice is derived. Provider version metadata is retained when declared, while
content hashes and retrieval timestamps identify the rdfsolve observation even
when the provider publishes no version.
"""

from __future__ import annotations

import hashlib
import mimetypes
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from rdflib import OWL, Graph, URIRef

from rdfsolve.evidence.ontology import VERSION_PREDICATES, OntologyArtifact


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _ontology_metadata(
    graph: Graph,
) -> tuple[list[str], list[str], list[str], list[str], list[str], list[str]]:
    from rdflib import RDF

    ontology_iris = sorted(
        str(subject)
        for subject in graph.subjects(RDF.type, OWL.Ontology)
        if isinstance(subject, URIRef)
    )
    ontology_set = set(ontology_iris)
    imports = sorted(
        str(obj)
        for ontology in graph.subjects(RDF.type, OWL.Ontology)
        for obj in graph.objects(ontology, OWL.imports)
        if isinstance(obj, URIRef)
    )
    version_iris: set[str] = set()
    version_values: set[str] = set()
    issued_values: set[str] = set()
    modified_values: set[str] = set()
    version_value_predicates = {
        OWL.versionInfo,
        URIRef("http://www.w3.org/ns/dcat#version"),
        URIRef("http://purl.org/pav/version"),
        URIRef("https://schema.org/version"),
    }
    issued_predicate = URIRef("http://purl.org/dc/terms/issued")
    modified_predicate = URIRef("http://purl.org/dc/terms/modified")
    for predicate in VERSION_PREDICATES:
        for subject, value in graph.subject_objects(predicate):
            if str(subject) not in ontology_set:
                continue
            if predicate == OWL.versionIRI and isinstance(value, URIRef):
                version_iris.add(str(value))
            elif predicate in version_value_predicates:
                version_values.add(str(value))
            elif predicate == issued_predicate:
                issued_values.add(str(value))
            elif predicate == modified_predicate:
                modified_values.add(str(value))
    return (
        ontology_iris,
        sorted(version_iris),
        sorted(version_values),
        sorted(issued_values),
        sorted(modified_values),
        imports,
    )


def parse_ontology_bytes(
    data: bytes, *, source_name: str | None = None, rdf_format: str | None = None
) -> Graph:
    """Parse retrieved ontology bytes without changing or normalizing the artifact."""
    graph = Graph()
    if rdf_format is not None:
        graph.parse(data=data, format=rdf_format)
        return graph
    # RDFLib's format guessing requires a location, so try common RDF ontology
    # serializations deterministically for in-memory content.
    errors: list[str] = []
    guessed = None
    if source_name:
        guessed = {
            ".ttl": "turtle",
            ".trig": "trig",
            ".rdf": "xml",
            ".owl": "xml",
            ".xml": "xml",
            ".nt": "nt",
            ".n3": "n3",
            ".jsonld": "json-ld",
            ".json": "json-ld",
        }.get(Path(source_name).suffix.lower())
    formats = [fmt for fmt in (guessed, "turtle", "xml", "json-ld", "nt") if fmt]
    for fmt in dict.fromkeys(formats):
        candidate = Graph()
        try:
            candidate.parse(data=data, format=fmt)
        except Exception as exc:  # caller receives a compact aggregate error
            errors.append(f"{fmt}: {exc}")
            continue
        return candidate
    raise ValueError("Could not parse ontology artifact: " + " | ".join(errors))


def archive_ontology_bytes(
    data: bytes,
    *,
    cache_dir: str | Path,
    source_url: str | None = None,
    source_graph: str | None = None,
    retrieved_at: str | None = None,
    media_type: str | None = None,
    rdf_format: str | None = None,
    filename_hint: str | None = None,
) -> tuple[OntologyArtifact, Graph]:
    """Archive one complete ontology artifact and return its metadata and graph."""
    digest = _sha256(data)
    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    suffix = Path(filename_hint or source_url or "ontology.ttl").suffix or ".rdf"
    path = cache / f"{digest}{suffix}"
    if not path.exists():
        path.write_bytes(data)
    graph = parse_ontology_bytes(
        data,
        source_name=filename_hint or source_url,
        rdf_format=rdf_format,
    )
    (
        ontology_iris,
        version_iris,
        version_values,
        issued_values,
        modified_values,
        imports,
    ) = _ontology_metadata(graph)
    artifact = OntologyArtifact(
        artifact_id=f"sha256:{digest}",
        ontology_iris=ontology_iris,
        version_iris=version_iris,
        version_values=version_values,
        issued_values=issued_values,
        modified_values=modified_values,
        source_url=source_url,
        source_graph=source_graph,
        retrieved_at=retrieved_at or datetime.now(timezone.utc).isoformat(),
        media_type=media_type,
        sha256=digest,
        local_path=str(path),
        imports=imports,
    )
    return artifact, graph


def archive_ontology_file(
    path: str | Path,
    *,
    cache_dir: str | Path,
    source_url: str | None = None,
    source_graph: str | None = None,
    retrieved_at: str | None = None,
    rdf_format: str | None = None,
) -> tuple[OntologyArtifact, Graph]:
    """Archive a local ontology artifact without treating its filename as a version."""
    source = Path(path)
    media_type, _ = mimetypes.guess_type(source.name)
    return archive_ontology_bytes(
        source.read_bytes(),
        cache_dir=cache_dir,
        source_url=source_url or source.as_uri(),
        source_graph=source_graph,
        retrieved_at=retrieved_at,
        media_type=media_type,
        rdf_format=rdf_format,
        filename_hint=source.name,
    )


def fetch_and_archive_ontology(
    url: str,
    *,
    cache_dir: str | Path,
    fetcher: Callable[[str], bytes] | None = None,
    source_graph: str | None = None,
    rdf_format: str | None = None,
) -> tuple[OntologyArtifact, Graph]:
    """Fetch and archive an ontology; injectable fetcher keeps tests/network policy separate."""
    if fetcher is None:
        import httpx

        response = httpx.get(url, follow_redirects=True, timeout=60)
        response.raise_for_status()
        data = response.content
        media_type = response.headers.get("content-type", "").split(";", 1)[0] or None
    else:
        data = fetcher(url)
        media_type = None
    return archive_ontology_bytes(
        data,
        cache_dir=cache_dir,
        source_url=url,
        source_graph=source_graph,
        media_type=media_type,
        rdf_format=rdf_format,
        filename_hint=url,
    )


__all__ = [
    "archive_ontology_bytes",
    "archive_ontology_file",
    "fetch_and_archive_ontology",
    "parse_ontology_bytes",
]
