"""The default graph of an RDFLib Dataset, by the attribute the installed RDFLib names it."""

from __future__ import annotations

from typing import Any

from rdflib import Dataset, Graph


def default_graph(dataset: Dataset) -> Graph:
    """Return the default graph of *dataset* (Dataset.default_graph; default_context before it)."""
    found: Any = getattr(type(dataset), "default_graph", None)
    graph = dataset.default_graph if found is not None else dataset.default_context
    return graph  # type: ignore[no-any-return]
