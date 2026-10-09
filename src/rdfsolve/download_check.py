"""Check that the download URLs of a registry exist, before a run downloads them.

Each URL is asked with one HEAD request (a GET of its first byte when the server refuses HEAD),
through the host gate that SPARQL requests use (rdfsolve._host_gate): one request at a time per
host, spaced by an interval, with a server's Retry-After honoured, and shared with other
processes through $RDFSOLVE_HTTP_LOCK_DIR. Hosts are checked in parallel. A URL is:

* ``ok``: the server answers 200 (after redirects) with a size other than 0, or no size;
* ``empty``: the server answers with a size of 0 bytes (an empty file);
* ``missing``: 404 or 410;
* ``http_error``: another status (403, 5xx, ...);
* ``unreachable``: no answer (DNS, connection refused, TLS), or ``timeout``.

A URL ending in / is a published folder; its listing is asked for, not its files.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlparse

__all__ = [
    "BAD",
    "DownloadCheck",
    "check_download",
    "check_downloads",
    "registry_downloads",
    "source_status",
]

#: Statuses that a download cannot be made from.
BAD = ("missing", "empty", "http_error", "unreachable", "timeout")
# Statuses of a HEAD request that some servers give where a GET works.
_HEAD_REFUSED = {400, 403, 405, 501}


@dataclass
class DownloadCheck:
    """What the server said of one download URL."""

    source: str
    field: str
    url: str
    status: str
    http_status: int | None = None
    content_length: int | None = None
    final_url: str | None = None
    method: str = "HEAD"
    error: str = ""


def registry_downloads(entries: Iterable[dict[str, Any]]) -> Iterator[tuple[str, str, str]]:
    """Yield (source, field, url) for every download of every registry entry.

    The download_* fields, those of each graph of graph_sources (field graph:<IRI>/download_*),
    and local_tar_url.
    """
    for entry in entries:
        name = str(entry.get("name"))
        for key, value in entry.items():
            if key.startswith("download_") and value:
                for url in [value] if isinstance(value, str) else value:
                    yield name, key, url
        for graph, fields in (entry.get("graph_sources") or {}).items():
            for key, value in (fields or {}).items():
                for url in [value] if isinstance(value, str) else value or []:
                    yield name, f"graph:{graph}/{key}", url
        if entry.get("local_tar_url"):
            yield name, "local_tar_url", entry["local_tar_url"]


def _length(headers: Any) -> int | None:
    """Return the size a response gives: Content-Length, or the total of Content-Range."""
    total = (headers.get("Content-Range") or "").rpartition("/")[2]
    if total.isdigit():
        return int(total)
    value = headers.get("Content-Length")
    return int(value) if value and value.isdigit() else None


def check_download(
    url: str,
    *,
    session: Any = None,
    timeout: float = 60.0,
    interval: float = 1.0,
    source: str = "",
    field: str = "",
) -> DownloadCheck:
    """Ask the server whether *url* can be downloaded (see the module for the statuses)."""
    import requests

    from rdfsolve._host_gate import HostBusyError, host_request
    from rdfsolve._http_policy import defer_host, retry_after_seconds
    from rdfsolve.sparql_helper import _default_agent

    session = session or requests.Session()
    headers = {"User-Agent": os.environ.get("RDFSOLVE_USER_AGENT") or _default_agent()}
    host = urlparse(url).hostname or url
    check = DownloadCheck(source=source, field=field, url=url, status="unreachable")
    folder = url.endswith("/")
    method, busy = "HEAD", 0
    while True:
        check.method = method
        extra = {} if method == "HEAD" or folder else {"Range": "bytes=0-0"}
        try:
            # The wait for a server's cooldown is bounded: a check must end.
            with (
                host_request(host, timeout=600, interval=interval, cooldown_wait=600),
                session.request(
                    method,
                    url,
                    headers={**headers, **extra},
                    allow_redirects=True,
                    timeout=timeout,
                    stream=True,
                ) as answer,
            ):
                code = answer.status_code
                response_headers = answer.headers
                final = answer.url
        except requests.Timeout as error:
            check.status, check.error = "timeout", str(error)[:300]
            return check
        except (requests.RequestException, HostBusyError) as error:
            check.status, check.error = "unreachable", str(error)[:300]
            return check
        check.http_status = code
        check.final_url = final if final != url else None
        if code in (429, 503) and busy < 2:
            # The server asks for time: the host gate waits it out before the next request.
            busy += 1
            wait = retry_after_seconds(response_headers.get("Retry-After"))
            defer_host(host, min(wait if wait is not None else 30.0, 600.0))
            continue
        if method == "HEAD" and code in _HEAD_REFUSED:
            method = "GET"
            continue
        break
    if code in (404, 410):
        check.status = "missing"
    elif code not in (200, 206):
        check.status = "http_error"
    else:
        check.content_length = None if folder else _length(response_headers)
        check.status = "empty" if check.content_length == 0 else "ok"
    return check


def check_downloads(
    items: Iterable[tuple[str, str, str]],
    *,
    interval: float = 1.0,
    timeout: float = 60.0,
    progress: Any = None,
) -> list[DownloadCheck]:
    """Check each (source, field, url): every URL once, hosts in parallel, one at a time each.

    *progress* is called with each result as it comes. Results are in the order of *items*.
    """
    import requests

    items = list(items)
    by_host: dict[str, list[str]] = defaultdict(list)
    for _, _, url in items:
        host = urlparse(url).hostname or url
        if url not in by_host[host]:
            by_host[host].append(url)
    results: dict[str, DownloadCheck] = {}

    def run(urls: list[str]) -> None:
        """Check the URLs of one host, one at a time, through the host gate."""
        with requests.Session() as session:
            for url in urls:
                results[url] = check_download(
                    url, session=session, timeout=timeout, interval=interval
                )
                if progress is not None:
                    progress(results[url])

    with ThreadPoolExecutor(max_workers=max(1, min(len(by_host), 32))) as pool:
        list(pool.map(run, by_host.values()))
    return [
        DownloadCheck(**{**asdict(results[url]), "source": source, "field": field})
        for source, field, url in items
    ]


def source_status(checks: Iterable[DownloadCheck]) -> dict[str, dict[str, Any]]:
    """Return per source the status that the pipeline reads (--download-status-file).

    A source is "accessible" when every URL is ok; otherwise it takes the status of its first
    failing URL, with the counts of each status.
    """
    grouped: dict[str, list[DownloadCheck]] = defaultdict(list)
    for check in checks:
        grouped[check.source].append(check)
    report = {}
    for name, rows in grouped.items():
        failing = [row for row in rows if row.status in BAD]
        counts: dict[str, int] = defaultdict(int)
        for row in rows:
            counts[row.status] += 1
        report[name] = {
            "status": failing[0].status if failing else "accessible",
            "urls": len(rows),
            "counts": dict(counts),
            "failing": [row.url for row in failing],
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    return report
