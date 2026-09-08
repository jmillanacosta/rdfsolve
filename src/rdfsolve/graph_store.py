"""Read selected Graph Store graphs. Never send graph-management writes."""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit

import requests
from rdflib import Dataset, URIRef

from rdfsolve._host_gate import host_request

logger = logging.getLogger(__name__)
FORMATS = {"text/turtle": "turtle", "application/n-triples": "nt", "application/rdf+xml": "xml"}


@dataclass(frozen=True)
class GraphDownload:
    """One complete HTTP transfer, before RDF validation."""

    graph_uri: str
    path: str
    format: str
    bytes: int
    sha256: str
    etag: str | None
    last_modified: str | None


def download_graphs(
    store_url: str,
    graph_uris: list[str],
    output_dir: str | Path,
    *,
    max_bytes: int = 64 * 1024 * 1024,
    timeout: float = 30,
    total_timeout: float = 300,
) -> list[GraphDownload]:
    """Stream selected graphs to a new directory.

    max_bytes bounds decoded bytes across all graphs, not RAM during parsing.
    No retries or redirects: restart explicitly after a failed acquisition.
    Failed runs retain .part files and never receive a complete manifest.
    """
    parsed = urlsplit(store_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.fragment:
        raise ValueError("graph_store_url must be an HTTP(S) URL without a fragment")
    if not graph_uris or any(not urlsplit(uri).scheme for uri in graph_uris):
        raise ValueError("Graph Store retrieval requires explicit absolute graph_uris")
    if min(max_bytes, timeout, total_timeout) <= 0:
        raise ValueError("Download limits must be positive")
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="graph-store-", dir=root))
    results: list[GraphDownload] = []
    used = 0
    deadline = time.monotonic() + total_timeout
    with requests.Session() as session:
        for index, uri in enumerate(dict.fromkeys(graph_uris)):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Graph Store download deadline reached")
            logger.info("Download Graph Store graph %s", uri)
            with (
                host_request(parsed.hostname or store_url, timeout=remaining),
                session.get(
                    store_url,
                    params={"graph": uri},
                    headers={"Accept": "application/n-triples, text/turtle;q=0.9"},
                    timeout=(min(timeout, remaining), min(timeout, remaining)),
                    stream=True,
                    allow_redirects=False,
                ) as response,
            ):
                if response.status_code in {429, 503}:
                    from rdfsolve._http_policy import defer_host, retry_after_seconds

                    defer_host(
                        parsed.hostname or store_url,
                        retry_after_seconds(response.headers.get("Retry-After")) or 30,
                    )
                response.raise_for_status()
                if response.status_code != 200:
                    raise ValueError(f"Expected HTTP 200, received {response.status_code}")
                media_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
                if media_type not in FORMATS:
                    raise ValueError(f"Expected RDF, received {media_type!r}")
                fmt = FORMATS[media_type]
                suffix = {"turtle": "ttl", "nt": "nt", "xml": "rdf"}[fmt]
                path = run / f"graph-{index}.{suffix}"
                partial = path.with_suffix(path.suffix + ".part")
                digest = hashlib.sha256()
                size = 0
                with partial.open("xb") as stream:
                    for chunk in response.iter_content(64 * 1024):
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Graph Store download deadline reached")
                        used += len(chunk)
                        if used > max_bytes:
                            raise ValueError(
                                f"Graph Store download exceeds {max_bytes} decoded bytes"
                            )
                        stream.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                partial.rename(path)
                results.append(
                    GraphDownload(
                        uri,
                        str(path),
                        fmt,
                        size,
                        digest.hexdigest(),
                        response.headers.get("ETag"),
                        response.headers.get("Last-Modified"),
                    )
                )
    (run / "download.json").write_text(
        json.dumps(
            {
                "state": "downloaded",
                "store_url": store_url,
                "graphs": [asdict(item) for item in results],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return results


def load_downloads(
    downloads: list[GraphDownload], *, endpoint_url: str | None = None, timeout: float = 30
) -> Dataset:
    """Parse complete downloads and retain each source graph name.

    This bounded-file path uses RDFLib memory. Large datasets belong in the
    disk-backed QLever workflow. A download limit is not a RAM limit.
    """
    dataset = Dataset()
    for item in downloads:
        dataset.graph(item.graph_uri).parse(item.path, format=item.format, publicID=item.graph_uri)
    if endpoint_url is not None:
        from rdfsolve.sparql_helper import SparqlHelper

        with SparqlHelper(endpoint_url, timeout=timeout, max_retries=1) as helper:
            for item in downloads:
                query = (
                    "SELECT (COUNT(*) AS ?count) WHERE { GRAPH "
                    + URIRef(item.graph_uri).n3()
                    + " { ?s ?p ?o } }"
                )
                rows = helper.select(query, purpose="graph-store/verify-count")["results"][
                    "bindings"
                ]
                expected = int(rows[0]["count"]["value"])
                actual = len(dataset.graph(item.graph_uri))
                if actual != expected:
                    raise ValueError(
                        f"Graph Store count mismatch for {item.graph_uri}: "
                        f"downloaded {actual}, SPARQL reports {expected}. "
                        "The export may be capped or expose a different view; do not mine it."
                    )
        if downloads:
            (Path(downloads[0].path).parent / "validation.json").write_text(
                json.dumps(
                    {
                        "state": "count_verified",
                        "endpoint": endpoint_url,
                        "triples": sum(len(dataset.graph(i.graph_uri)) for i in downloads),
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
    return dataset
