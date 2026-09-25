"""Resolve query selections without prescribing a graph topology."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from rdfsolve.client.catalogue import same_name
from rdfsolve.client.query_fragments import QueryPattern
from rdfsolve.client.resolution import ResolutionError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from rdfsolve.client.api import Client


def resolve_patterns(
    client: Client,
    patterns: Sequence[QueryPattern | dict[str, Any]],
    external_names: bool,
) -> tuple[list[QueryPattern], list[dict[str, Any]], list[str]]:
    """Resolve classes first, then fields named exactly within the bound classes."""
    selected = [
        p if isinstance(p, QueryPattern) else QueryPattern.model_validate(p) for p in patterns
    ]
    evidence: list[dict[str, Any]] = []
    warnings: list[str] = []
    owners: dict[str, set[str]] = defaultdict(set)
    for index, pattern in enumerate(selected):
        if len(pattern.bindings) != 1:
            continue
        fragment = client.catalogue.fragments.get(pattern.reference)
        if fragment is None:
            result = client.resolve(pattern.reference, kind="class", external_names=external_names)
            if result.status != "resolved" or result.reference is None:
                raise ResolutionError(result)
            evidence.append(result.model_dump(mode="json"))
            warnings.extend(result.warnings)
            selected[index] = pattern.model_copy(update={"reference": result.reference})
            fragment = client.catalogue.fragments[result.reference]
        if fragment.kind == "type" and fragment.iri and not pattern.optional:
            owners[pattern.bindings[0]].add(fragment.iri)
    for index, pattern in enumerate(selected):
        if pattern.reference in client.catalogue.fragments:
            continue
        role, name = pattern.bindings[0], pattern.reference
        matches = [
            ref
            for ref in client.catalogue.field_refs.values()
            for field in [client.catalogue.fragments[ref]]
            if (field.path and field.path.iri == name)
            or any(
                same_name(text, name)
                for text in (field.label, field.field_name, field.description)
                if text
            )
        ]
        bound = [ref for ref in matches if client.catalogue.fragments[ref].owner in owners[role]]
        candidates = bound or matches
        if len(candidates) != 1:
            owned = {r: client.catalogue.fragments[r].owner for r in candidates}
            raise ValueError(f"Field {name!r} for ?{role} is unknown or ambiguous: {owned}")
        (ref,) = candidates
        owner = client.catalogue.fragments[ref].owner
        if owners[role] and not bound:
            warnings.append(
                f"{name!r} is declared for <{owner}>, not for the classes bound to ?{role} "
                f"({', '.join(sorted(owners[role]))}); ?{role} must also have type <{owner}>."
            )
        selected[index] = pattern.model_copy(update={"reference": ref})
        evidence.append(
            {
                "method": "contract field",
                "name": name,
                "reference": ref,
                "owner": owner,
                "owner_scope": "bound class" if bound else "any class",
            }
        )
    return selected, evidence, warnings
