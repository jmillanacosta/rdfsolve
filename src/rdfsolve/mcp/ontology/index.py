"""Compatibility re-export — use :mod:`rdfsolve.ontology.index` directly."""

from __future__ import annotations

from rdfsolve.ontology.index import (
    OntologyIndex,
    build_ontology_index,
    load_ontology_index,
    save_ontology_index,
)

__all__ = [
    "OntologyIndex",
    "build_ontology_index",
    "load_ontology_index",
    "save_ontology_index",
]
