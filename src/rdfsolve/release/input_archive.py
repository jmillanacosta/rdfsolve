"""Keep a copy of the pinned inputs of a run as a BagIt bag, and let the release point to it.

The pins (rdfsolve.qlever.downloads.record_inputs, ``<run>/<source>/<source>*_inputs.json``)
tell whether a rebuild had the same bytes; they cannot rebuild once a provider replaces a file
(UniProt's current_release, Expasy, RDF Portal's latest/). This module keeps the downloaded files
themselves (not the converted copies, which the pinned converter rebuilds from them) in a bag:

- BagIt 1.0 (RFC 8493), the packaging format of digital preservation: ``data/<source>/<path>``
  with ``manifest-sha256.txt`` (the pinned SHA-256 of each file), ``fetch.txt`` (the URL each
  file was downloaded from), ``bag-info.txt`` and ``tagmanifest-sha256.txt``. Any BagIt tool
  validates it; the same bag can be deposited elsewhere (Zenodo, an institutional repository)
  as it is, for the sources whose licence allows it.
- A file is placed by a hard link when the bag is on the work folders' file system (no space
  used; the link keeps the bytes when the work folder is removed) and copied otherwise. It is
  made read-only, so that a later download cannot change it in place (wget -c appends).
- A file that is not the pinned file (other size or modification time, and another SHA-256)
  is refused; nothing in the bag is then written for it.
- ``<run>/input_archive.json`` records where the bag is, the SHA-256 of its manifest and, per
  pinned download, its path in the bag; the release reads it (ReleaseManifest.input_archive,
  InputDownloadRecord.archive_path).
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

__all__ = ["ARCHIVE_RECORD", "archive_inputs", "verify_archive"]

ARCHIVE_RECORD = "input_archive.json"
ARCHIVE_FORMAT = "rdfsolve-input-archive/1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _place(source: Path, target: Path, *, copy: bool) -> str:
    """Put *source* at *target* by a hard link, or by a copy; return which."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.samefile(source):
            return "link"
        target.chmod(0o644)
        target.unlink()
    if not copy:
        try:
            os.link(source, target)
            return "link"
        except OSError:
            pass
    shutil.copy2(source, target)
    return "copy"


def _bag_path(name: str) -> str:
    """Return a payload path as BagIt writes it (RFC 8493 2.1.3: CR, LF and % encoded)."""
    return name.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def archive_inputs(
    run_dir: str | Path,
    workdirs: str | Path,
    bag_dir: str | Path,
    *,
    location: str | None = None,
    copy: bool = False,
    licenses: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Put the pinned downloads of every source of a run in a BagIt bag; return the record.

    *workdirs* holds each source's work folder (``<workdirs>/<source>``), where the pinned
    paths lie. *location* is how the release names the bag (a URL or DOI once deposited);
    the default is the bag's path. *licenses* (source to licence) are recorded per source, so
    that it can be seen which sources may be deposited publicly. Raise ValueError, before
    anything is written, when a file is missing or is not the pinned file.
    """
    run = Path(run_dir).resolve()
    bag = Path(bag_dir).resolve()
    payload: list[tuple[Path, str, dict[str, Any], str]] = []
    problems = []
    for pins in sorted(run.glob("*/*_inputs.json")):
        manifest = json.loads(pins.read_text(encoding="utf-8"))
        source = str(manifest.get("source") or pins.parent.name)
        files = {item["path"]: item for item in manifest.get("files") or []}
        for download in manifest.get("downloads") or []:
            pinned = files.get(download.get("path") or "")
            if pinned is None:
                problems.append(f"{source}: {download['url']} has no pinned file")
                continue
            path = Path(workdirs) / source / pinned["path"]
            if not path.is_file():
                problems.append(f"{source}: {pinned['path']} is missing")
                continue
            stat = path.stat()
            same = (stat.st_size, stat.st_mtime_ns) == (
                pinned.get("bytes"),
                pinned.get("modified_ns"),
            )
            if not same and _sha256(path) != pinned["sha256"]:
                problems.append(f"{source}: {pinned['path']} is not the pinned file")
                continue
            payload.append((path, f"data/{source}/{pinned['path']}", pinned, download["url"]))
    if problems:
        raise ValueError("Inputs not archived: " + "; ".join(problems))

    placed: dict[str, int] = {"link": 0, "copy": 0}
    for path, name, item, _ in payload:
        target = bag / name
        how = _place(path, target, copy=copy)
        placed[how] += 1
        if how == "copy" and _sha256(target) != item["sha256"]:
            raise ValueError(f"{name}: the copy is not the pinned file")
        target.chmod(0o444)
    total = sum(int(item["bytes"]) for _, _, item, _ in payload)
    manifest_text = "".join(
        f"{item['sha256']}  {_bag_path(name)}\n" for _, name, item, _ in sorted(payload, key=str)
    )
    fetch_text = "".join(
        f"{url} {item['bytes']} {_bag_path(name)}\n"
        for _, name, item, url in sorted(payload, key=str)
    )
    created = datetime.now(UTC)
    tags = {
        "bagit.txt": "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n",
        "bag-info.txt": (
            f"Bagging-Date: {created.date().isoformat()}\n"
            f"Payload-Oxum: {total}.{len(payload)}\n"
            f"External-Description: Downloaded inputs of the local indexes of the rdfsolve "
            f"run {run.name}, as pinned in its <source>_inputs.json files\n"
            f"External-Identifier: {run.name}\n"
            "Bag-Software-Agent: rdfsolve.release.input_archive\n"
        ),
        "manifest-sha256.txt": manifest_text,
        "fetch.txt": fetch_text,
    }
    for name, text in tags.items():
        (bag / name).write_text(text, encoding="utf-8")
    (bag / "tagmanifest-sha256.txt").write_text(
        "".join(f"{_sha256(bag / name)}  {name}\n" for name in sorted(tags)), encoding="utf-8"
    )
    sources: dict[str, dict[str, Any]] = {}
    for _, name, item, _ in payload:
        source = name.split("/", 2)[1]
        entry = sources.setdefault(source, {"files": 0, "bytes": 0})
        entry["files"] += 1
        entry["bytes"] += int(item["bytes"])
        if licenses and licenses.get(source):
            entry["license"] = licenses[source]
    record = {
        "format": ARCHIVE_FORMAT,
        "packaging": "BagIt 1.0 (RFC 8493)",
        "location": location or str(bag),
        "created": created.isoformat(),
        "manifest_sha256": hashlib.sha256(manifest_text.encode()).hexdigest(),
        "file_count": len(payload),
        "byte_size": total,
        "placed": placed,
        "sources": sources,
        "files": {
            f"{name.split('/', 2)[1]}:{url}": name for _, name, _, url in sorted(payload, key=str)
        },
    }
    (run / ARCHIVE_RECORD).write_text(json.dumps(record, indent=1) + "\n", encoding="utf-8")
    return record


def verify_archive(bag_dir: str | Path) -> list[str]:
    """Return how a bag differs from its manifests (empty when every file is as recorded)."""
    bag = Path(bag_dir)
    problems = []
    for manifest in ("manifest-sha256.txt", "tagmanifest-sha256.txt"):
        for line in (bag / manifest).read_text(encoding="utf-8").splitlines():
            digest, name = line.split("  ", 1)
            path = bag / name.replace("%0A", "\n").replace("%0D", "\r").replace("%25", "%")
            if not path.is_file():
                problems.append(f"{name}: missing")
            elif _sha256(path) != digest:
                problems.append(f"{name}: sha256 differs from {manifest}")
    return problems
