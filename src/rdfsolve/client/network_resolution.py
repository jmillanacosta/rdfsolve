"""Resolve query selections without prescribing a graph topology."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from rdfsolve.client.catalogue import words
from rdfsolve.client.query_fragments import QueryPattern

if TYPE_CHECKING:
    from collections.abc import Sequence

    from rdfsolve.client.api import Client


def resolve_patterns(
    client: Client,
    patterns: Sequence[QueryPattern | dict[str, Any]],
    ontology_fallback: bool,
) -> tuple[list[QueryPattern], list[dict[str, Any]]]:
    """Resolve classes first, then fields within the bound owners."""
    selected = [
        p if isinstance(p, QueryPattern) else QueryPattern.model_validate(p) for p in patterns
    ]
    evidence: list[dict[str, Any]] = []
    owners: dict[str, set[str]] = defaultdict(set)
    for index, pattern in enumerate(selected):
        if len(pattern.bindings) != 1:
            continue
        fragment = client.catalogue.fragments.get(pattern.reference)
        if fragment is None:
            result = client.resolve(
                pattern.reference, kind="class", ontology_fallback=ontology_fallback
            )
            evidence.append(result)
            selected[index] = pattern.model_copy(update={"reference": result["reference"]})
            fragment = client.catalogue.fragments[result["reference"]]
        if fragment.kind == "type" and fragment.iri and not pattern.optional:
            owners[pattern.bindings[0]].add(fragment.iri)
    for index, pattern in enumerate(selected):
        if pattern.reference in client.catalogue.fragments:
            continue
        candidates = []
        owner_scope = {
            owner
            for owner in owners[pattern.bindings[0]]
            if any(cls == owner for cls, _ in client.catalogue.field_refs)
        }
        for ref in client.catalogue.field_refs.values():
            field = client.catalogue.fragments[ref]
            if owner_scope and field.owner not in owner_scope:
                continue
            names = [field.label, field.field_name or "", field.description or ""]
            if (field.path and field.path.iri == pattern.reference) or any(
                words(name) == words(pattern.reference) for name in names if name
            ):
                candidates.append(ref)
        if len(candidates) != 1:
            raise ValueError(f"Unknown or ambiguous field {pattern.reference!r}: {candidates}")
        (ref,) = candidates
        selected[index] = pattern.model_copy(update={"reference": ref})
        evidence.append(
            {
                "method": "contract field",
                "name": pattern.reference,
                "reference": ref,
                "owner": client.catalogue.fragments[ref].owner,
            }
        )
    return selected, evidence
