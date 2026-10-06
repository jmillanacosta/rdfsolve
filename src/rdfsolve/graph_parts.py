"""Per-graph parts of a source that is mined across several named graphs.

A source whose files are mapped to the named graphs of its endpoint (``graph_sources``: UniProt,
PubChem's FTP release), or whose graphs need settings of their own (``graph_settings``), is mined
as a whole. Each of its data graphs also gets a schema of its own, its part, so that the graph
can be compared with the same graph elsewhere: the endpoint's VoID scoped to that graph, or a
registry entry that describes that graph alone.

A part is named, in this order:

1. by ``graph_settings[graph].name``;
2. by the registry entry that is a graph scope of the source for that graph: a dataset entry on
   the same endpoint whose only data graph is that graph (the structural rule of
   rdfsolve.dataset_identity for ``graph_scope_of``), such as uniprot.citations;
3. as ``<source>.<last segment of the graph IRI>``, such as pubchem.ftp.anatomy.

The outputs of a part are written to ``<run>/<source>/graphs/<part>/<part><suffix>_*``, next to
the source's own outputs, and listed in ``<run>/<source>/<source><suffix>_graph_parts.json``.
They are not top-level dataset folders, so that the patterns of a source are not counted twice;
the release links a graph-scope entry to its part (rdfsolve.release.build).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from rdfsolve.models.source_model import SourceModel

__all__ = [
    "GRAPHS_DIR",
    "INDEX_SUFFIX",
    "GraphPart",
    "graph_part_dir",
    "graph_parts",
    "has_graph_parts",
    "write_graph_parts_index",
]

GRAPHS_DIR = "graphs"
INDEX_SUFFIX = "_graph_parts.json"


@dataclass(frozen=True)
class GraphPart:
    """One data graph of a source, the name of its schema and the settings to mine it with."""

    graph: str
    name: str
    registry_entry: str | None
    classes_as_data: bool
    membership_properties: tuple[str, ...] = field(default_factory=tuple)
    # True when the graph's settings differ from the source's: it is mined on its own.
    own_settings: bool = False

    def record(self) -> dict[str, Any]:
        """Return the part as written in the index of graph parts."""
        return {
            "graph": self.graph,
            "name": self.name,
            "registry_entry": self.registry_entry,
            "classes_as_data": self.classes_as_data,
            "membership_properties": list(self.membership_properties),
            "own_settings": self.own_settings,
        }


def has_graph_parts(source: SourceModel) -> bool:
    """Return whether the source's data graphs each get a schema of their own."""
    return len(source.graph_uris) > 1 and bool(source.graph_sources or source.graph_settings)


def _endpoint(url: str) -> str:
    return url.strip().rstrip("/")


def _segment(graph: str) -> str:
    """Return the last segment of a graph IRI as a name part."""
    parts = urlsplit(graph)
    # The path of a URN (urn:x:taxa) is split at its colons.
    segments = [s for s in re.split(r"[/:]", parts.path or "") if s]
    raw = parts.fragment or (segments[-1] if segments else parts.hostname or "")
    return re.sub(r"[^a-z0-9_-]+", "_", raw.lower()).strip("_")


def graph_parts(source: SourceModel, registry: Iterable[SourceModel] = ()) -> list[GraphPart]:
    """Return the parts of the source's data graphs, in the order of its graph_uris.

    Raise ValueError when two graphs get one name, when two registry entries claim one graph,
    or when a derived name is the name of another registry entry; ``graph_settings`` then gives
    the graph its name.
    """
    entries = [entry for entry in registry if entry.name != source.name]
    names = {entry.name for entry in entries}
    endpoint = _endpoint(source.endpoint)
    parts: list[GraphPart] = []
    for graph in source.graph_uris:
        settings = source.graph_settings.get(graph)
        scopes = sorted(
            entry.name
            for entry in entries
            if endpoint
            and entry.source_role == "dataset"
            and entry.graph_uris == [graph]
            and _endpoint(entry.endpoint) == endpoint
        )
        if settings is not None and settings.name:
            name = settings.name
            entry = name if name in scopes else None
        elif len(scopes) > 1:
            raise ValueError(
                f"Registry entries {scopes} are each the graph {graph} of {source.name}; "
                "name its part in graph_settings"
            )
        elif scopes:
            name = entry = scopes[0]
        else:
            segment = _segment(graph)
            if not segment:
                raise ValueError(f"No name for the graph {graph}; name it in graph_settings")
            name, entry = f"{source.name}.{segment}", None
            if name in names:
                raise ValueError(
                    f"{name}, the derived name of the graph {graph}, is another registry "
                    "entry; name the part in graph_settings"
                )
        classes_as_data = source.classes_as_data
        membership = tuple(source.membership_properties)
        if settings is not None:
            if settings.classes_as_data is not None:
                classes_as_data = settings.classes_as_data
            if settings.membership_properties is not None:
                membership = tuple(settings.membership_properties)
        parts.append(
            GraphPart(
                graph=graph,
                name=name,
                registry_entry=entry,
                classes_as_data=classes_as_data,
                membership_properties=membership,
                own_settings=(
                    classes_as_data != source.classes_as_data
                    or membership != tuple(source.membership_properties)
                ),
            )
        )
    seen: dict[str, str] = {}
    for part in parts:
        if part.name in seen:
            raise ValueError(
                f"The graphs {seen[part.name]} and {part.graph} of {source.name} are both "
                f"named {part.name}; name them in graph_settings"
            )
        seen[part.name] = part.graph
    return parts


def graph_part_dir(output_dir: Path, source_name: str, part_name: str) -> Path:
    """Return the folder of a part's outputs in a run."""
    return Path(output_dir) / source_name / GRAPHS_DIR / part_name


def write_graph_parts_index(
    output_dir: Path,
    source_name: str,
    suffix: str,
    mode: str,
    rows: list[dict[str, Any]],
    **extra: Any,
) -> Path:
    """Write ``<source><suffix>_graph_parts.json``: each part, how it was made, its schema."""
    path = Path(output_dir) / source_name / f"{source_name}{suffix}{INDEX_SUFFIX}"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"source": source_name, "mode": mode, **extra, "parts": rows}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path
