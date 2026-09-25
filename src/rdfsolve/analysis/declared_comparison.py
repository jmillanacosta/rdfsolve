"""Neutral comparison of empirical patterns with simple declared SHACL evidence.

This module reports coverage/set relationships.  It does not treat either layer
as ground truth and does not interpret RDFS/OWL inferential axioms as validation
constraints.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Literal

from pydantic import BaseModel, Field

from rdfsolve.evidence.declared import DeclaredEvidence
from rdfsolve.schema_models.pattern import SchemaPattern

ComparisonDimension = Literal["property", "class", "datatype", "node_kind"]
SetRelation = Literal[
    "both",
    "observed_only",
    "declared_only",
    "equal",
    "declared_subset_of_observed",
    "observed_subset_of_declared",
    "overlap",
    "disjoint",
    "not_comparable",
]


_SHACL_NODE_KIND_SETS: dict[str, set[str]] = {
    "http://www.w3.org/ns/shacl#IRI": {"IRI"},
    "http://www.w3.org/ns/shacl#Literal": {"Literal"},
    "http://www.w3.org/ns/shacl#BlankNode": {"BlankNode"},
    "http://www.w3.org/ns/shacl#BlankNodeOrIRI": {"BlankNode", "IRI"},
    "http://www.w3.org/ns/shacl#BlankNodeOrLiteral": {"BlankNode", "Literal"},
    "http://www.w3.org/ns/shacl#IRIOrLiteral": {"IRI", "Literal"},
}


def _declared_value_set(item: DeclaredEvidence) -> set[str]:
    """Normalize declared values only where SHACL defines set semantics.

    In particular, sh:nodeKind alternatives such as sh:IRIOrLiteral describe an
    allowed set.  They must not be compared as opaque IRIs with empirical
    node-kind labels.
    """
    if item.declaration_type == "shacl_node_kind":
        return set(_SHACL_NODE_KIND_SETS.get(item.value.value, {item.value.value}))
    return {item.value.value}


class EvidenceComparison(BaseModel):
    """Observed and declared values for one class, property and dimension."""

    dataset_id: str
    subject_class: str
    property_uri: str
    dimension: ComparisonDimension
    relation: SetRelation
    observed_values: list[str] = Field(default_factory=list)
    declared_values: list[str] = Field(default_factory=list)
    observed_count: int = 0
    declared_count: int = 0
    channel: str | None = None


def _relation(observed: set[str], declared: set[str]) -> SetRelation:
    if not observed and not declared:
        return "not_comparable"
    if observed and not declared:
        return "observed_only"
    if declared and not observed:
        return "declared_only"
    if observed == declared:
        return "equal"
    if declared < observed:
        return "declared_subset_of_observed"
    if observed < declared:
        return "observed_subset_of_declared"
    if observed & declared:
        return "overlap"
    return "disjoint"


def compare_observed_with_declared_shacl(
    *,
    dataset_id: str,
    patterns: list[SchemaPattern],
    declared: list[DeclaredEvidence],
) -> list[EvidenceComparison]:
    """Compare empirical class/property profiles with simple SHACL declarations.

    Only declared records with both ``focus_class`` and a simple
    ``property_uri`` participate.  RDFS/OWL domain/range semantics are left to
    the ontology-usage analysis where inference can be handled explicitly.
    """
    observed: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(
        lambda: {"class": set(), "datatype": set(), "node_kind": set()}
    )
    for pattern in patterns:
        if pattern.evidence_source != "mined" or pattern.subject_binding != "type":
            continue
        key = (pattern.subject_class, pattern.property_uri)
        if pattern.object_binding == "term":
            observed[key]["node_kind"].add("IRI")
        elif pattern.object_class == "Literal":
            observed[key]["node_kind"].add("Literal")
            if pattern.datatype:
                observed[key]["datatype"].add(pattern.datatype)
        elif pattern.object_class == "BlankNode":
            observed[key]["node_kind"].add("BlankNode")
        elif pattern.object_class == "Resource":
            observed[key]["node_kind"].add("IRI")
        else:
            observed[key]["class"].add(pattern.object_class)

    declared_by_key: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(
        lambda: {"class": set(), "datatype": set(), "node_kind": set()}
    )
    type_to_dimension = {
        "shacl_class": "class",
        "shacl_datatype": "datatype",
        "shacl_node_kind": "node_kind",
    }
    for item in declared:
        dimension = type_to_dimension.get(item.declaration_type)
        if (
            not item.declaration_type.startswith("shacl_")
            or not item.focus_class
            or not item.property_uri
        ):
            continue
        entry = declared_by_key[(item.focus_class, item.property_uri)]
        if dimension is not None:
            entry[dimension].update(_declared_value_set(item))

    keys = sorted(set(observed) | set(declared_by_key))
    result: list[EvidenceComparison] = []
    for subject_class, property_uri in keys:
        obs = observed.get((subject_class, property_uri))
        dec = declared_by_key.get((subject_class, property_uri))
        result.append(
            EvidenceComparison(
                dataset_id=dataset_id,
                subject_class=subject_class,
                property_uri=property_uri,
                dimension="property",
                relation=("both" if obs and dec else "observed_only" if obs else "declared_only"),
                observed_count=1 if obs else 0,
                declared_count=1 if dec else 0,
            )
        )
        for dimension in ("class", "datatype", "node_kind"):
            observed_values = set(obs[dimension]) if obs else set()
            declared_values = set(dec[dimension]) if dec else set()
            if not observed_values and not declared_values:
                continue
            result.append(
                EvidenceComparison(
                    dataset_id=dataset_id,
                    subject_class=subject_class,
                    property_uri=property_uri,
                    dimension=dimension,
                    relation=(
                        "not_comparable"
                        if dimension == "node_kind" and obs and obs["class"] and not observed_values
                        else _relation(observed_values, declared_values)
                    ),
                    observed_values=sorted(observed_values),
                    declared_values=sorted(declared_values),
                    observed_count=len(observed_values),
                    declared_count=len(declared_values),
                )
            )
    return result


__all__ = ["EvidenceComparison", "compare_observed_with_declared_shacl"]
