"""Record the downloads of a local source, and see updates on the server.

downloads.json in the folder of a source holds the URLs that were downloaded and what the
server said about each file (Last-Modified, Content-Length). A marker file is there while a
download runs, so that a download that did not end is seen. The server is asked only when the
run asks for updates.
"""

from __future__ import annotations

import calendar
import email.utils
import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RECORD = "downloads.json"
MARKER = ".download.incomplete"
Head = Callable[[str], dict[str, str | None] | None]


def server_state(url: str) -> dict[str, str | None] | None:
    """Return Last-Modified and Content-Length of a URL, or None when the server gives none."""
    import requests

    try:
        answer = requests.head(url, allow_redirects=True, timeout=60)
    except requests.RequestException:
        return None
    if answer.status_code != 200:
        return None
    return {
        "last_modified": answer.headers.get("Last-Modified"),
        "content_length": answer.headers.get("Content-Length"),
    }


def read_record(workdir: Path) -> dict[str, Any] | None:
    """Return the record of the downloads of a source, or None when there is none."""
    path = workdir / RECORD
    if not path.is_file():
        return None
    record: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return record


def write_record(workdir: Path, urls: list[str], head: Head = server_state) -> None:
    """Write the record of a download that ended, and remove the marker of a running one."""
    record = {
        "urls": list(urls),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "files": {url: head(url) for url in urls},
    }
    (workdir / RECORD).write_text(json.dumps(record, indent=1), encoding="utf-8")
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
            if state != known[url]:
                changed.append(url)
            continue
        parsed = email.utils.parsedate(state.get("last_modified") or "")
        if parsed is not None and calendar.timegm(parsed) > built_at:
            changed.append(url)
    return changed
