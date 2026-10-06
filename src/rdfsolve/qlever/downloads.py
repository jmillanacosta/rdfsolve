"""Record the downloads of a local source, and see updates on the server.

downloads.json in the folder of a source holds the URLs that were downloaded and what the
server said about each file when it was downloaded (final URL after redirects, Last-Modified,
ETag, Content-Length, a Digest header), and the release that a metalink of the provider names
for it. A marker file is there while a download runs, so that a download that did not end is
seen.

inputs.json beside it pins what an index is built from: the size and SHA-256 of each
downloaded file and of each file given to the index, with the record of its download. The
pipeline carries it into the run directory, so that a release names the exact inputs of each
schema, and a run given the manifests of an earlier run refuses to index other files
(unpinned_inputs).
"""

from __future__ import annotations

import calendar
import email.utils
import hashlib
import json
import re
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

RECORD = "downloads.json"
MARKER = ".download.incomplete"
INPUTS = "inputs.json"
INPUTS_FORMAT = "rdfsolve-inputs/1"
Head = Callable[[str], dict[str, str | None] | None]
Metalinks = Callable[[list[str], dict[str, Any]], dict[str, dict[str, Any]]]

# The fields of a server's answer that tell an update (updated_urls).
COMPARED = ("last_modified", "content_length")
# Metalinks (RFC 5854, and version 3 of metalinker.org) that providers publish beside their
# files, looked for in the folder of each download: UniProt publishes RELEASE.meta4 and
# RELEASE.metalink with the release number and the size and MD5 of each file. A metalink that
# the server names in a Link header (rel=describedby, RFC 6249) is read as well.
METALINK_NAMES = ("RELEASE.meta4", "RELEASE.metalink")
# The hash types of a metalink, by the names of hashlib.
_HASH_NAMES = {
    "md5": "md5",
    "sha-1": "sha1",
    "sha1": "sha1",
    "sha-256": "sha256",
    "sha256": "sha256",
}
_METALINK_LIMIT = 50 * 1024 * 1024
_CHUNK = 4 * 1024 * 1024


def server_state(url: str) -> dict[str, str | None] | None:
    """Return what the server says of a URL, or None when it does not answer.

    Last-Modified and Content-Length tell an update; the final URL after redirects, the ETag,
    a Digest or Repr-Digest (RFC 3230, RFC 9530) and a Link header pin the file.
    """
    import requests

    try:
        answer = requests.head(url, allow_redirects=True, timeout=60)
    except requests.RequestException:
        return None
    if answer.status_code != 200:
        return None
    headers = answer.headers
    state = {
        "last_modified": headers.get("Last-Modified"),
        "content_length": headers.get("Content-Length"),
        "final_url": answer.url if answer.url != url else None,
        "etag": headers.get("ETag"),
        "digest": headers.get("Repr-Digest") or headers.get("Digest"),
        "link": headers.get("Link"),
    }
    return {key: value for key, value in state.items() if value is not None or key in COMPARED}


def read_record(workdir: Path) -> dict[str, Any] | None:
    """Return the record of the downloads of a source, or None when there is none."""
    path = workdir / RECORD
    if not path.is_file():
        return None
    record: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return record


def write_record(
    workdir: Path,
    urls: list[str],
    head: Head = server_state,
    metalinks: Metalinks | None = None,
) -> None:
    """Write the record of a download that ended, and remove the marker of a running one.

    With *metalinks*, the release that the provider's metalinks name for each file is recorded.
    The pin of the inputs of an earlier download (inputs.json) is removed.
    """
    files = {url: head(url) for url in urls}
    record: dict[str, Any] = {
        "urls": list(urls),
        "completed_at": datetime.now(UTC).isoformat(),
        "files": files,
    }
    if metalinks is not None:
        record["releases"] = metalinks(list(urls), files)
    (workdir / RECORD).write_text(json.dumps(record, indent=1), encoding="utf-8")
    (workdir / INPUTS).unlink(missing_ok=True)
    (workdir / MARKER).unlink(missing_ok=True)


def needs_download(workdir: Path, urls: list[str], *, has_inputs: bool) -> bool:
    """Tell whether the files of a source are to be downloaded.

    They are when there is no input file, when an earlier download did not end, and when the
    record lists other URLs than the registry entry. A folder with input files and without a
    record (made before the record existed) is taken as downloaded.
    """
    if not has_inputs or (workdir / MARKER).exists():
        return True
    record = read_record(workdir)
    return record is not None and list(record.get("urls", [])) != list(urls)


def updated_urls(
    urls: list[str], record: dict[str, Any] | None, *, built_at: float, head: Head = server_state
) -> list[str]:
    """Return the URLs whose file the server changed since the download.

    With a record, a file is changed when its Last-Modified or Content-Length is another.
    Without a record, a file is changed when its Last-Modified is after *built_at* (the time of
    the index, seconds since 1970). A URL for which the server gives no state is not counted.
    """
    changed = []
    known = (record or {}).get("files") or {}
    for url in urls:
        state = head(url)
        if state is None:
            continue
        if known.get(url):
            if any(state.get(key) != known[url].get(key) for key in COMPARED):
                changed.append(url)
            continue
        parsed = email.utils.parsedate(state.get("last_modified") or "")
        if parsed is not None and calendar.timegm(parsed) > built_at:
            changed.append(url)
    return changed


# Releases named by metalinks


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_metalink(text: str | bytes) -> dict[str, Any]:
    """Read a metalink (version 3 or 4): the release it names and, per file name, its
    version, size and hashes (hash type in lower case: md5, sha-256, ...).
    """
    from xml.etree import ElementTree

    # The expat of Python 3.12 limits entity expansion; a metalink is read up to 50 MB.
    root = ElementTree.fromstring(text)  # noqa: S314
    version = next((c.text for c in root if _local(c.tag) == "version" and c.text), None)
    files: dict[str, dict[str, Any]] = {}
    for item in root.iter():
        if _local(item.tag) != "file" or not item.get("name"):
            continue
        found: dict[str, Any] = {"hashes": {}}
        for child in item.iter():
            name, value = _local(child.tag), (child.text or "").strip()
            if name == "version" and value:
                found["version"] = value
            elif name == "size" and value.isdigit():
                found["size"] = int(value)
            elif name == "hash" and child.get("type") and value:
                found["hashes"][child.get("type", "").lower()] = value.lower()
        files[item.get("name", "").rsplit("/", 1)[-1]] = found
    return {"version": version, "files": files}


def _described_by(link: str | None) -> list[str]:
    """Return the metalinks that a Link header names (rel=describedby, RFC 6249)."""
    found = []
    for target, params in re.findall(r"<([^>]+)>([^,]*)", link or ""):
        if re.search(r'rel="?describedby"?', params) and "metalink" in params:
            found.append(target)
    return found


def _fetch_text(url: str) -> bytes | None:
    import requests

    try:
        with requests.get(url, timeout=60, stream=True) as answer:
            if answer.status_code != 200:
                return None
            data = b""
            for chunk in answer.iter_content(1024 * 1024):
                data += chunk
                if len(data) > _METALINK_LIMIT:
                    return None
            return data
    except requests.RequestException:
        return None


def _file_name_of(url: str) -> str:
    return unquote(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])


def find_metalinks(
    urls: list[str],
    states: dict[str, Any] | None = None,
    *,
    fetch: Callable[[str], bytes | None] = _fetch_text,
) -> dict[str, dict[str, Any]]:
    """Return, per URL, what a metalink of its provider says of its file.

    The metalinks are those that the server names for the file (Link header) and those
    published in the folder of the file (METALINK_NAMES). The result names the metalink, the
    release version (of the file, else of the metalink), and the size and hashes it gives.
    """
    parsed: dict[str, dict[str, Any] | None] = {}

    def read(location: str) -> dict[str, Any] | None:
        """Return a metalink's files, fetched and parsed once per location."""
        if location not in parsed:
            data = fetch(location)
            try:
                parsed[location] = parse_metalink(data) if data else None
            except Exception:  # A file that is not a metalink names no release.
                parsed[location] = None
        return parsed[location]

    releases: dict[str, dict[str, Any]] = {}
    for url in urls:
        if url.endswith("/"):
            continue
        state = (states or {}).get(url) or {}
        base = state.get("final_url") or url
        candidates = [urljoin(base, target) for target in _described_by(state.get("link"))]
        candidates += [urljoin(url, name) for name in METALINK_NAMES]
        name = _file_name_of(url)
        for location in candidates:
            metalink = read(location)
            if metalink is None or name not in metalink["files"]:
                continue
            entry = metalink["files"][name]
            releases[url] = {
                "metalink": location,
                "version": entry.get("version") or metalink.get("version"),
                **({"size": entry["size"]} if "size" in entry else {}),
                "hashes": entry["hashes"],
            }
            break
    return releases


# Pins of the inputs of an index


def digest_file(path: Path, algorithms: Iterable[str] = ("sha256",)) -> dict[str, str]:
    """Return the hex digests of a file, read once in chunks."""
    digests = {name: hashlib.new(name) for name in algorithms}
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(_CHUNK), b""):
            for digest in digests.values():
                digest.update(chunk)
    return {name: digest.hexdigest() for name, digest in digests.items()}


def read_inputs(workdir: Path) -> dict[str, Any] | None:
    """Return the pin of the inputs of a source's index, or None when there is none."""
    path = workdir / INPUTS
    if not path.is_file():
        return None
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return manifest


def download_paths(workdir: Path, entry: dict[str, Any]) -> dict[str, Path | None]:
    """Return the local file of each download of a registry entry, or None when it is not
    there (a published folder, a file renamed by the download, a name given by the server).
    """
    from rdfsolve.qlever.inputs import graph_input_directory
    from rdfsolve.qlever.utils import download_file_names, graph_download_name

    found: dict[str, Path | None] = {}
    graph_sources = entry.get("graph_sources") or {}
    if graph_sources:
        for graph, fields in graph_sources.items():
            directory = graph_input_directory(workdir, graph) / "rdf"
            for key, urls in fields.items():
                for url in urls:
                    path = directory / graph_download_name(url, key)
                    found[url] = path if path.is_file() else None
        return found
    for url, name in download_file_names(entry).items():
        if url.endswith("/"):
            found[url] = None
            continue
        names = [name] if name else []
        names.append(_file_name_of(url))
        # A Turtle file published under another extension is renamed .ttl by the download.
        names += [n.rsplit(".", 1)[0] + ".ttl" for n in list(names) if "." in n]
        found[url] = next(
            (
                directory / n
                for n in names
                for directory in (workdir / "rdf", workdir)
                if n and (directory / n).is_file()
            ),
            None,
        )
    return found


def record_inputs(
    workdir: Path,
    downloads: dict[str, Path | None],
    index_files: list[Path],
    *,
    source: str | None = None,
    stage: str = "before_index",
) -> dict[str, Any]:
    """Pin the downloads and the index inputs of a source, write inputs.json and return it.

    Each file is named by its path in the work folder, its size and SHA-256; a file whose size
    and modification time are those of the earlier pin is not read again. When a metalink of
    the provider gave hashes for a download, they are checked and the result recorded
    (publisher_check). *stage* tells when the pin was taken: before the index was built from
    the files, or after (a folder indexed before inputs were pinned).
    """
    record = read_record(workdir) or {}
    states = record.get("files") or {}
    releases = record.get("releases") or {}
    earlier = {item["path"]: item for item in (read_inputs(workdir) or {}).get("files", [])}
    url_of = {path.resolve(): url for url, path in downloads.items() if path is not None}
    paths = {path.resolve() for path in index_files} | set(url_of)
    inputs = {path.resolve() for path in index_files}
    root = workdir.resolve()

    def relative(path: Path) -> str:
        """Return a path relative to the work folder when it lies inside it."""
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            return str(path)

    def pin(path: Path) -> dict[str, Any]:
        """Return a file's size and SHA-256."""
        stat = path.stat()
        item: dict[str, Any] = {
            "path": relative(path),
            "bytes": stat.st_size,
            "modified_ns": stat.st_mtime_ns,
        }
        url = url_of.get(path)
        given = (releases.get(url) or {}).get("hashes") or {}
        wanted = {"md5", "sha1"} & {_HASH_NAMES.get(kind) for kind in given}
        before = earlier.get(item["path"])
        if (
            before
            and before.get("bytes") == item["bytes"]
            and before.get("modified_ns") == item["modified_ns"]
            and all(name in before for name in wanted)
        ):
            item.update({k: v for k, v in before.items() if k in {"sha256", "md5", "sha1"}})
        else:
            item.update(digest_file(path, ["sha256", *sorted(wanted)]))
        if url:
            item["url"] = url
        item["index_input"] = path in inputs
        return item

    with ThreadPoolExecutor(max_workers=8) as pool:
        files = sorted(pool.map(pin, sorted(paths)), key=lambda item: item["path"])
    by_path = {item["path"]: item for item in files}
    entries = []
    for url, path in downloads.items():
        local = by_path.get(relative(path.resolve())) if path is not None else None
        entry: dict[str, Any] = {"url": url, "path": local["path"] if local else None}
        if local:
            entry.update(bytes=local["bytes"], sha256=local["sha256"])
        entry.update(states.get(url) or {})
        release = releases.get(url)
        if release:
            entry["release"] = release
            if local:
                entry["publisher_check"] = _publisher_check(local, release)
        entries.append(entry)
    manifest = {
        "format": INPUTS_FORMAT,
        "source": source,
        "recorded_at": datetime.now(UTC).isoformat(),
        "recorded": stage,
        "downloaded_at": record.get("completed_at"),
        "urls": record.get("urls") or list(downloads),
        "release_versions": sorted(
            {str(r["version"]) for r in releases.values() if r.get("version")}
        ),
        "downloads": entries,
        "files": files,
    }
    (workdir / INPUTS).write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    return manifest


def _publisher_check(item: dict[str, Any], release: dict[str, Any]) -> str:
    """Compare a pinned file with the size and hashes its publisher gives: match, mismatch,
    or unchecked (no hash that was computed).
    """
    if "size" in release and release["size"] != item["bytes"]:
        return "mismatch"
    compared = [
        item[_HASH_NAMES[kind]] == value
        for kind, value in (release.get("hashes") or {}).items()
        if _HASH_NAMES.get(kind) in item
    ]
    if not compared:
        return "unchecked"
    return "match" if all(compared) else "mismatch"


def read_pins(pins: Path, source: str) -> dict[str, Any] | None:
    """Return the pinned inputs of a source: from a manifest file, or from a run directory
    (<run>/<source>/<source>*_inputs.json, as the pipeline writes it).
    """
    if pins.is_file():
        manifest: dict[str, Any] = json.loads(pins.read_text(encoding="utf-8"))
        return manifest if manifest.get("source") in {None, source} else None
    found = sorted((pins / source).glob(f"{source}*_inputs.json"))
    if not found:
        return None
    pinned: dict[str, Any] = json.loads(found[0].read_text(encoding="utf-8"))
    return pinned


def unpinned_inputs(current: dict[str, Any], pinned: dict[str, Any]) -> list[str]:
    """Return how the inputs of a source differ from their pins (empty when they are the same).

    Every pinned file must be there with its SHA-256, and no other file is given to the index;
    the URLs of the registry entry must be those that were pinned.
    """
    problems = []
    now = {item["path"]: item for item in current.get("files", [])}
    before = {item["path"]: item for item in pinned.get("files", [])}
    if list(current.get("urls") or []) != list(pinned.get("urls") or []):
        problems.append("the registry entry has other URLs than the pinned run")
    for path, item in sorted(before.items()):
        if path not in now:
            problems.append(f"{path}: missing (pinned sha256 {item['sha256']})")
        elif now[path]["sha256"] != item["sha256"]:
            problems.append(
                f"{path}: sha256 {now[path]['sha256']} is not the pinned {item['sha256']}"
            )
    for path, item in sorted(now.items()):
        if path not in before and item.get("index_input"):
            problems.append(f"{path}: an index input that the pinned run did not have")
    return problems
