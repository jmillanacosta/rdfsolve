"""I/O utilities for loading mapping files."""

import json
from pathlib import Path

from rdfsolve.mapping_models.core import MappingEdge


def load_edges_from_jsonld(path: str | Path) -> list[MappingEdge]:
    """Load MappingEdge objects from JSON-LD file."""
    data = json.loads(Path(path).read_text())
    edges = []
    for e in data.get("@graph", []):
        try:
            edges.append(MappingEdge(**e))
        except Exception:
            continue
    return edges
