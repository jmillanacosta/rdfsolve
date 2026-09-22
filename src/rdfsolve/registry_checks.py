"""Offline findings for the human-curated source registry."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml

from rdfsolve.dataset_identity import (
    DatasetIdentity,
    read_overrides,
    read_registry,
    resolve_identity,
)
from rdfsolve.models.source_model import SourceModel
from rdfsolve.qlever.utils import FORMAT_REGISTRY

_VOLATILE = {
    "endpoint_status",
    "last_checked",
    "last_success",
    "last_error",
    "failure_count",
    "avg_response_time",
    "endpoint_down",
    "dataset_metadata",
    "enrichment",
    "metadata_graph_uris",
    "void_graphs",
    "void_schema",
    "void_default_graph",
    "has_void",
    "has_void_partitions",
    "has_void_patterns",
}
_DOWNLOAD_ALIASES = {
    "download_nquads": "download_nq",
    "download_rdfxml": "download_rdf",
    "download_tar_gz": "download_tgz",
}


def check_registry(
    path: str | Path,
    overrides_path: str | Path | None = None,
) -> list[dict[str, str]]:
    """Return A/B/D3 findings without requests or writes.

    B7 lists provider members; selected groups and prepared inputs are not checked.
    An error blocks the freeze. A warning requires review, not an automatic edit.
    """
    findings: list[dict[str, str]] = []

    def add(
        check: str,
        names: list[str],
        detail: str,
        severity: Literal["error", "warn", "info"] = "warn",
        endpoint: str = "",
    ) -> None:
        """Append one finding."""
        findings.append(
            {
                "check_id": check,
                "sources": ";".join(sorted(names)),
                "endpoint": endpoint,
                "detail": detail,
                "severity": severity,
            }
        )

    try:
        entries = read_registry(path)
    except (OSError, ValueError, yaml.YAMLError) as error:
        add("A1", [], str(error), "error")
        return findings

    sources: list[SourceModel] = []
    identities: list[DatasetIdentity] = []
    by_endpoint: dict[str, list[SourceModel]] = defaultdict(list)
    by_provider: dict[str, list[str]] = defaultdict(list)
    names = [str(row.get("name", "")) for row in entries]
    for name, count in Counter(names).items():
        if count > 1:
            add("A3", [name], f"Duplicate name in {count} rows", "error")
    known_extra = {f"download_{suffix}" for suffix in FORMAT_REGISTRY} | {"local_tar_url"}
    for index, row in enumerate(entries, 1):
        name = str(row.get("name") or f"row:{index}")
        try:
            source = SourceModel.model_validate(dict(row))
            identity = DatasetIdentity.from_entry(row)
        except (TypeError, ValueError) as error:
            add("A1", [name], str(error), "error")
            continue
        sources.append(source)
        identities.append(identity)
        extra = set(row) - SourceModel.model_fields.keys()
        if extra:
            add(
                "A1",
                [name],
                "Keys outside SourceModel: " + ", ".join(sorted(extra)),
                "warn" if extra - known_extra else "info",
            )
        aliases = [f"{key} -> {value}" for key, value in _DOWNLOAD_ALIASES.items() if key in row]
        if aliases:
            add("A2", [name], "; ".join(aliases), "error")
        if not source.name.strip():
            add("A3", [name], "Empty source name", "error")
        elif not re.fullmatch(r"[a-z0-9]+(?:[._-][a-z0-9]+)*", source.name):
            add(
                "A3",
                [name],
                "Name contains characters outside lowercase provider.dataset convention",
            )
        elif "." not in source.name:
            add("A3", [name], "Unqualified source name; review naming convention", "info")
        if source.endpoint:
            try:
                parts = urlsplit(source.endpoint)
                if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
                    raise ValueError("Expected an HTTP(S) endpoint URL")
                endpoint = parts._replace(
                    scheme=parts.scheme.lower(),
                    netloc=parts.netloc.lower(),
                    path=parts.path.rstrip("/"),
                ).geturl()
            except ValueError as error:
                add("A4", [name], str(error), "error", source.endpoint)
            else:
                by_endpoint[endpoint].append(source)
                if endpoint != source.endpoint:
                    add("A4", [name], f"Normalised spelling: {endpoint}", endpoint=source.endpoint)
        empty = [
            str(key)
            for key, value in row.items()
            if value is None or value == "" or value == [] or value == "unknown"
        ]
        if "graph_uris" not in row:
            empty.append("graph_uris (absent)")
        if empty:
            add("A5", [name], "Empty or unspecified: " + ", ".join(sorted(empty)), "info")
        volatile = _VOLATILE.intersection(row)
        if volatile:
            add("A6", [name], "Observation fields: " + ", ".join(sorted(volatile)), "error")
        if "use_graph" in row:
            add("A6", [name], "Unused use_graph field; graph_uris defines the scope")
        if source.endpoint and identity.distributions:
            add(
                "B6",
                [name],
                "Endpoint and download are separate evidence channels of this entry",
                "info",
                source.endpoint,
            )
        if source.local_provider:
            by_provider[source.local_provider].append(name)

    for endpoint, members in sorted(by_endpoint.items()):
        scopes: dict[tuple[str, ...], list[str]] = defaultdict(list)
        for source in members:
            scopes[tuple(sorted(set(source.graph_uris)))].append(source.name)
        for scope, scoped_names in scopes.items():
            if len(scoped_names) > 1:
                add("B1", scoped_names, f"Identical scope: {list(scope)}", "error", endpoint)
        unscoped = scopes.get((), [])
        if unscoped and len(scopes) > 1:
            add(
                "B2",
                [s.name for s in members],
                "Unscoped and named-graph entries share this endpoint",
                "error",
                endpoint,
            )
        if unscoped and len(members) > 1:
            add("B4", unscoped, "No graph scope on a shared endpoint", endpoint=endpoint)
        for left, right in combinations(members, 2):
            a, b = set(left.graph_uris), set(right.graph_uris)
            if a != b and a & b:
                add(
                    "B3",
                    [left.name, right.name],
                    "Overlapping graphs: " + ", ".join(sorted(a & b)),
                    "error",
                    endpoint,
                )
        settings = ("sparql_engine", "sparql_strategy", "supports_graph", "use_graph", "delay")
        differing = [key for key in settings if len({getattr(s, key) for s in members}) > 1]
        if differing:
            add(
                "B8",
                [s.name for s in members],
                "Inconsistent settings: " + ", ".join(differing),
                endpoint=endpoint,
            )
    for provider, provider_names in sorted(by_provider.items()):
        add(
            "B7",
            provider_names,
            f"Provider {provider}: grouped selection and prepared inputs not checked",
        )

    try:
        if overrides_path is not None and not Path(overrides_path).is_file():
            raise ValueError(f"Overrides file does not exist: {overrides_path}")
        overrides = read_overrides(overrides_path)
        resolution = resolve_identity([s.model_dump() for s in sources], overrides)
    except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError) as error:
        add("D3", [], str(error), "error")
        overrides = []
        resolution = None
    decided = {frozenset((r.left, r.right)) for r in overrides}
    upstream_pairs: set[frozenset[str]] = set()
    for field in ("bioregistry_prefix", "kg_registry_id", "local_name"):
        groups: dict[str, list[str]] = defaultdict(list)
        for identity in identities:
            value = getattr(identity, field)
            if value:
                groups[value].append(identity.dataset_id)
        for value, group_names in sorted(groups.items()):
            pairs = {frozenset(pair) for pair in combinations(group_names, 2)} - decided
            if pairs:
                upstream_pairs.update(pairs)
                add("B5", group_names, f"Shared {field}={value}; {len(pairs)} unreviewed pairs")
    downloads: dict[str, list[str]] = defaultdict(list)
    for identity in identities:
        for url in identity.distributions:
            downloads[url].append(identity.dataset_id)
    for url, download_names in sorted(downloads.items()):
        if len(download_names) > 1:
            add(
                "B6",
                download_names,
                f"Shared download, possibly a multi-dataset archive: {url}",
                "info",
            )
    upstream_open = len(upstream_pairs)
    if resolution is not None:
        for candidate in resolution.candidates:
            add(
                "D3",
                [candidate.left, candidate.right],
                f"Open {candidate.relation}: {candidate.basis}",
            )
        invalid = len(entries) - len(sources)
        canonical_count = (
            resolution.canonical_dataset_count
            if not invalid
            and not upstream_open
            and not any(
                r["check_id"] in {"A1", "A3", "A4"} and r["severity"] == "error" for r in findings
            )
            else None
        )
        add(
            "D3",
            [],
            f"{len(resolution.candidates)} resolver candidates; {upstream_open} unreviewed upstream pairs; {invalid} invalid rows; canonical count={canonical_count}",
            "error" if invalid else "warn" if canonical_count is None else "info",
        )
    return sorted(findings, key=lambda row: (row["check_id"], row["sources"], row["detail"]))
