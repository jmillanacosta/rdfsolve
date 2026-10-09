"""Exact relations between the member sets of the classes of a schema, read from the data."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ClassExtensions(BaseModel):
    """Classes with the same members, and the nearest classes that hold all members of another.

    A source that states every type of its entities (each one under several class names with
    the same members) repeats its patterns for each name; a view shows one class for each group. The relations are extensional: they hold
    for the data, and an ontology can declare other relations.
    """

    members: dict[str, int] = Field(
        default_factory=dict, description="Distinct typed subjects of each measured class"
    )
    same_members: list[list[str]] = Field(
        default_factory=list, description="Groups of two or more classes with the same members"
    )
    contained_in: dict[str, list[str]] = Field(
        default_factory=dict,
        description="For a class, the nearest classes that hold all its members and more",
    )
    not_checked: dict[str, str] = Field(
        default_factory=dict, description="Classes that were not measured, with the reason"
    )


def relate(overlaps: dict[str, dict[str, int]]) -> ClassExtensions:
    """Relate classes from the members that each class shares with every other class.

    *overlaps* maps a class to the number of its members that are members of each class,
    itself included (its number of members).
    """
    members = {c: shared[c] for c, shared in overlaps.items() if c in shared}
    same: dict[str, set[str]] = {c: {c} for c in members}
    within: dict[str, set[str]] = {c: set() for c in members}
    for a, shared in overlaps.items():
        for b, n in shared.items():
            if a == b or a not in members or b not in members or n != members[a]:
                continue
            if members[a] == members[b]:
                same[a].add(b)
            else:
                within[a].add(b)
    groups = sorted({tuple(sorted(g)) for g in same.values() if len(g) > 1})
    nearest = {}
    for a, outer in within.items():
        # A container of a container of a is not nearest.
        near = {b for b in outer if not any(b in within[c] for c in outer if c not in same[b])}
        if near:
            nearest[a] = sorted(near)
    return ClassExtensions(
        members=dict(sorted(members.items())),
        same_members=[list(g) for g in groups],
        contained_in=dict(sorted(nearest.items())),
    )
