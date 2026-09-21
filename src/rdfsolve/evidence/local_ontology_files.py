"""Archive OWL-formatted files that were part of a local source distribution.

``download_owl`` is a download-format classification used by the local QLever
pipeline.  These files are archived as source-distribution evidence.  They are
not treated as remote endpoint graphs or external reference ontology releases.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from urllib.parse import unquote, urlparse

from rdfsolve.evidence.ontology_acquisition import LocalOntologyFileCandidate


def _sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
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
            digest = _sha256(source_path)
            target = archive_dir / source_path.name
            if target.exists() and _sha256(target) != digest:
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


__all__ = ["archive_local_ontology_files"]
