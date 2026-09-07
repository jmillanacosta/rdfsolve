"""Describe route bindings for future clients without executing queries."""

from __future__ import annotations

from collections import Counter
from typing import Any

from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.navigation import NavigationPath


def navigation_metadata(route: NavigationPath) -> dict[str, Any]:
    """Keep joins and RDF term kinds explicit; a property path alone loses types."""
    triples = []
    bindings: dict[str, dict[str, Any]] = {
        "focus": {"kind": "resource", "class_iri": route.steps[0].subject_class}
    }
    for index, step in enumerate(route.steps):
        subject = "focus" if index == 0 else f"step_{index}"
        obj = "value" if index == len(route.steps) - 1 else f"step_{index + 1}"
        triples.append({"subject": subject, "predicate_iri": step.property_uri, "object": obj})
        if step.object_class == "Literal":
            constraint = {"kind": "literal", "datatype_iri": step.datatype}
        elif step.object_class in ("Resource", "BlankNode"):
            constraint = {"kind": "iri" if step.object_class == "Resource" else "blank_node"}
        else:
            constraint = {"kind": "resource", "class_iri": step.object_class}
        bindings[obj] = constraint
    predicates = Counter(step.property_uri for step in route.steps)
    classes = Counter([step.subject_class for step in route.steps] + [route.steps[-1].object_class])
    return {
        **route.model_dump(mode="json"),
        "path": route.property_path().model_dump(mode="json"),
        "sparql_path": path_to_sparql(route.property_path()),
        "sparql_path_omits_class_filters": True,
        "triples": triples,
        "bindings": bindings,
        "class_matching": "explicit_rdf_type",
        "result_cardinality": "unknown",
        "execution": "not_implemented",
        "repeated_predicates": sorted(p for p, count in predicates.items() if count > 1),
        "revisited_classes": sorted(c for c, count in classes.items() if count > 1),
    }
