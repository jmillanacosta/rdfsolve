"""Semantic overlap utilities for computing dataset similarity based on shared classes and predicates."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__ = [
    "compute_class_overlap",
    "compute_predicate_overlap",
    "jaccard_similarity",
    "pairwise_overlap_matrix",
]


def jaccard_similarity(set_a: set[Any], set_b: set[Any]) -> float:
    """Compute Jaccard similarity: intersection size divided by union size."""
    if not set_a and not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def compute_class_overlap(schema_a: Any, schema_b: Any) -> float:
    """Compute Jaccard similarity on class URIs between two schemas."""
    from rdfsolve.schema_utils import extract_class_set

    return jaccard_similarity(
        extract_class_set(schema_a),
        extract_class_set(schema_b),
    )


def compute_predicate_overlap(schema_a: Any, schema_b: Any) -> float:
    """Compute Jaccard similarity on predicate URIs between two schemas."""
    from rdfsolve.schema_utils import extract_predicate_set

    return jaccard_similarity(
        extract_predicate_set(schema_a),
        extract_predicate_set(schema_b),
    )


def pairwise_overlap_matrix(
    schemas: list[Any],
    extractor: Callable[[Any], set[str]],
) -> Any:
    """Compute pairwise Jaccard overlap matrix for schemas using extractor function.

    Args:
        schemas: List of MinedSchema objects.
        extractor: Function to extract set from schema (extract_class_set, extract_predicate_set, etc).

    Returns:
        DataFrame with Jaccard similarity values.
    """
    import pandas as pd

    # Extract sets and names
    schema_sets = [(s, extractor(s)) for s in schemas]
    names = [s.about.dataset_name or f"schema_{i}" for i, (s, _) in enumerate(schema_sets)]

    # Build matrix
    n = len(schemas)
    matrix = [[0.0] * n for _ in range(n)]

    for i in range(n):
        matrix[i][i] = 1.0  # diagonal is always 1.0
        for j in range(i + 1, n):
            sim = jaccard_similarity(schema_sets[i][1], schema_sets[j][1])
            matrix[i][j] = sim
            matrix[j][i] = sim

    return pd.DataFrame(matrix, index=names, columns=names)
