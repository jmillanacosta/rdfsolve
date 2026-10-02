"""Ontology artifacts: retrieved files, their versions, a registry of them, and local copies.

The complete retrieved ontology bytes are archived before any KG-specific usage
slice is derived. Provider version metadata is retained when declared, while
content hashes and retrieval timestamps identify the rdfsolve observation even
when the provider publishes no version.
"""

from __future__ import annotations

import hashlib
import mimetypes
import shutil
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, Field
from rdflib import OWL, Graph, URIRef

from rdfsolve.ontology.vocabulary import VERSION_PREDICATES as _VERSION_IRIS

VERSION_PREDICATES = tuple(URIRef(p) for p in _VERSION_IRIS)


class OntologyArtifact(BaseModel):
    """One retrieved ontology artifact or graph snapshot.

    Provider version information is optional.  Reproducibility is provided by
    the retrieval time and content hash even when no provider version exists.
    """

    artifact_id: str
    ontology_iris: list[str] = Field(default_factory=list)
    version_iris: list[str] = Field(default_factory=list)
    version_values: list[str] = Field(default_factory=list)
    issued_values: list[str] = Field(default_factory=list)
    modified_values: list[str] = Field(default_factory=list)
    source_url: str | None = None
    source_graph: str | None = None
    retrieved_at: str | None = None
    media_type: str | None = None
    sha256: str | None = None
    local_path: str | None = None
    imports: list[str] = Field(default_factory=list)


class LocalOntologyFileCandidate(BaseModel):
    """OWL-formatted file discovered in a local source distribution.

    ``download_owl`` is a format/classification result from the downloadable
    source bundle.  It does *not* establish that a remote endpoint exposes the
    file, that the file is an authoritative ontology release, or even that the
    file contains ``owl:Ontology``.  Those claims require inspection of the
    retrieved artifact and empirical overlap with the locally mined dataset.
    """

    source_url: str
    source_field: str = "download_owl"
    source_dataset_id: str | None = None
    archived_path: str | None = None
    sha256: str | None = None
    discovery_basis: Literal["local_distribution_format"] = "local_distribution_format"


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


class OntologyRecord(BaseModel):
    """Stable ontology identity independent of any particular retrieved release."""

    ontology_id: str
    preferred_iri: str | None = None
    prefixes: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    artifact_ids: list[str] = Field(default_factory=list)


class OntologyRegistry(BaseModel):
    """Registry of stable ontology identities and content-addressed artifacts.

    No ``latest`` release is inferred because provider version strings are not
    necessarily sortable and retrieval order is not semantic version order.
    """

    ontologies: dict[str, OntologyRecord] = Field(default_factory=dict)
    artifacts: dict[str, OntologyArtifact] = Field(default_factory=dict)

    def register_artifact(
        self,
        ontology_id: str,
        artifact: OntologyArtifact,
        *,
        preferred_iri: str | None = None,
        prefixes: list[str] | None = None,
        aliases: list[str] | None = None,
    ) -> OntologyRecord:
        """Attach one immutable retrieved artifact to a stable ontology identity."""
        existing = self.artifacts.get(artifact.artifact_id)
        if existing is not None and existing.sha256 != artifact.sha256:
            raise ValueError(f"Artifact id collision: {artifact.artifact_id}")
        self.artifacts[artifact.artifact_id] = artifact
        record = self.ontologies.setdefault(
            ontology_id,
            OntologyRecord(ontology_id=ontology_id, preferred_iri=preferred_iri),
        )
        if preferred_iri and record.preferred_iri and record.preferred_iri != preferred_iri:
            if preferred_iri not in record.aliases:
                record.aliases.append(preferred_iri)
        elif preferred_iri and not record.preferred_iri:
            record.preferred_iri = preferred_iri
        for prefix in prefixes or []:
            if prefix not in record.prefixes:
                record.prefixes.append(prefix)
        for alias in aliases or []:
            if alias not in record.aliases and alias != record.preferred_iri:
                record.aliases.append(alias)
        if artifact.artifact_id not in record.artifact_ids:
            record.artifact_ids.append(artifact.artifact_id)
        record.prefixes.sort()
        record.aliases.sort()
        record.artifact_ids.sort()
        return record

    def save(self, path: str | Path) -> Path:
        """Write the registry as JSON and return its path."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        return target

    @classmethod
    def load(cls, path: str | Path) -> OntologyRegistry:
        """Read a registry written by save."""
        return cls.model_validate_json(Path(path).read_text(encoding="utf-8"))

    def releases(self, ontology_id: str) -> list[OntologyArtifact]:
        """Return all archived artifacts for an ontology without choosing a latest release."""
        record = self.ontologies.get(ontology_id)
        if record is None:
            return []
        return [self.artifacts[artifact_id] for artifact_id in record.artifact_ids]


# Files of a local source distribution (download_owl), archived as distribution evidence: not
# endpoint graphs and not reference releases.


def _file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _url_basename(url: str) -> str | None:
    name = Path(unquote(urlparse(url).path)).name
    return name or None


def archive_local_ontology_files(
    *,
    source_dataset_id: str,
    urls: list[str],
    source_workdir: str | Path,
    dataset_output_dir: str | Path,
) -> list[LocalOntologyFileCandidate]:
    """Archive exact local ``download_owl`` files when they can be identified.

    The local downloader keeps original RDF/XML/OWL files alongside converted
    N-Quads, so the bytes used to prepare the local index can normally be copied
    without another network request.  A candidate is still recorded when the
    exact file cannot be located, but ``archived_path`` and ``sha256`` stay null.
    """
    workdir = Path(source_workdir)
    output_dir = Path(dataset_output_dir)
    archive_dir = output_dir / "declared" / "local-distribution"
    rdf_dir = workdir / "rdf"
    rows: list[LocalOntologyFileCandidate] = []

    # Only exact filename matches are accepted.  Guessing among several .owl
    # files could silently associate the wrong distribution artifact with a URL.
    for url in dict.fromkeys(str(item) for item in urls if item):
        name = _url_basename(url)
        source_path = rdf_dir / name if name else None
        archived_rel: str | None = None
        digest: str | None = None
        if source_path is not None and source_path.is_file():
            archive_dir.mkdir(parents=True, exist_ok=True)
            digest = _file_sha256(source_path)
            target = archive_dir / source_path.name
            if target.exists() and _file_sha256(target) != digest:
                target = archive_dir / f"{digest[:12]}-{source_path.name}"
            if not target.exists():
                shutil.copyfile(source_path, target)
            archived_rel = target.relative_to(output_dir).as_posix()

        rows.append(
            LocalOntologyFileCandidate(
                source_url=url,
                source_field="download_owl",
                source_dataset_id=source_dataset_id,
                archived_path=archived_rel,
                sha256=digest,
            )
        )
    return rows


__all__ = [
    "LocalOntologyFileCandidate",
    "OntologyArtifact",
    "OntologyRecord",
    "OntologyRegistry",
    "archive_local_ontology_files",
    "archive_ontology_bytes",
    "archive_ontology_file",
    "fetch_and_archive_ontology",
    "parse_ontology_bytes",
]
