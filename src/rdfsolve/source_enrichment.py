"""Enrich one registry entry without mining instance data."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from rdfsolve.dataset_identity import read_registry
from rdfsolve.endpoint_health import check_endpoint_health, update_endpoint_status
from rdfsolve.models.source_model import SourceModel


def enrich_source(
    name: str,
    endpoint: str,
    *,
    sources_file: str | Path | None = None,
    metadata_graph_uris: list[str] | None = None,
    discover_void: bool = False,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Return health and metadata, using sources_file as read-only context.

    Failed retrieval keeps existing values and records an error. VoID discovery
    is opt-in; metadata uses the default graph unless a scope is supplied.
    """
    from rdfsolve.metadata import query_metadata
    from rdfsolve.void_source import discover_void_source

    parsed = urlsplit(endpoint)
    if not name.strip() or parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Provide a nonempty name and an HTTP(S) endpoint URL")
    if timeout <= 0 or metadata_graph_uris == []:
        raise ValueError("Use a positive timeout and a nonempty metadata graph scope")
    path = Path(sources_file) if sources_file is not None else None
    entries = read_registry(path) if path is not None else []
    existing = next((item for item in entries if item["name"] == name), None)
    if (
        existing
        and existing.get("endpoint")
        and existing["endpoint"].rstrip("/") != endpoint.rstrip("/")
    ):
        raise ValueError(f"Source {name!r} already has a different endpoint")
    entry: dict[str, Any] = dict(existing or {"name": name, "endpoint": endpoint})
    entry["endpoint"] = endpoint
    entry.setdefault("dataset_metadata", None)
    entry.setdefault("metadata_graph_uris", None)
    if metadata_graph_uris is None:
        metadata_graph_uris = entry.get("metadata_graph_uris")
    health = check_endpoint_health(endpoint, timeout=timeout)
    model = SourceModel.model_validate(dict(entry))
    update_endpoint_status(model, health)
    values = model.model_dump()
    for field in (
        "endpoint_status",
        "endpoint_down",
        "last_checked",
        "last_success",
        "last_error",
        "failure_count",
        "avg_response_time",
    ):
        entry[field] = values[field]
    errors: dict[str, str] = {}
    phases = {"health": health.status, "metadata": "skipped", "void": "skipped"}
    if health.status == "up":
        entry["last_error"] = ""
        try:
            document = query_metadata(endpoint, timeout=timeout, graph_uris=metadata_graph_uris)
            entry["dataset_metadata"] = document.project() or None
            entry["metadata_graph_uris"] = metadata_graph_uris
            phases["metadata"] = "complete"
        except Exception as error:
            errors["metadata"] = str(error)
            phases["metadata"] = "failed"
        if discover_void:
            try:
                void = discover_void_source(
                    endpoint,
                    name,
                    timeout=timeout,
                    graph_uris=metadata_graph_uris,
                )
                void_fields = {
                    "void_graphs": void.graph_uris,
                    "void_schema": [
                        uri for uri in void.graph_uris if void.for_graph(uri).has_patterns
                    ],
                    "void_default_graph": void.default_graph,
                    "has_void": void.has_void,
                    "has_void_partitions": void.has_partitions,
                    "has_void_patterns": void.has_patterns,
                }
                entry.update(void_fields)
                phases["void"] = "complete"
            except Exception as error:
                errors["void"] = str(error)
                phases["void"] = "failed"
    else:
        errors["health"] = health.error_message
    entry["enrichment"] = {
        "state": "failed" if health.status != "up" else "partial" if errors else "complete",
        "checked_at": health.timestamp,
        "phases": phases,
        "errors": errors,
    }
    return entry
