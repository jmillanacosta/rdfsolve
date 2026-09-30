"""Routes across two datasets, tested on the data.

A route is a path in the source dataset that ends at the class of a link, the link, and a path
in the target dataset that starts at the class that the link reaches. A composition of edges
says only that each part exists; a route is tested by a staged join: the start instances and
their link values are read in the source, the values are looked up in the target together with
the target path (with the spellings and replacements of link verification), and the start
instances that reach the end are counted. When every value is read (two local indexes) a
matched route is confirmed; with a sample it is tested.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdfsolve.mappings.signatures import (
    Link,
    LinkEvidence,
    _local,
    _lookup,
    _read_all,
    _read_target,
)
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

if TYPE_CHECKING:
    from rdfsolve.api import Client

Segment = tuple[SchemaPattern, ...]


@dataclass(frozen=True)
class Route:
    """A path in the source, a link, and a path in the target; a path can be empty."""

    link: Link
    before: Segment = ()
    after: Segment = ()

    @property
    def start_class(self) -> str:
        """Return the class of the start instances."""
        return self.before[0].subject_class if self.before else self.link.source_class

    @property
    def end_class(self) -> str:
        """Return the class that the route reaches."""
        return self.after[-1].object_class if self.after else (self.link.target_class or "")


@dataclass
class RouteEvidence:
    """The start instances that were read for a route, and how many reach its end."""

    route: Route
    starts: int
    matched: int
    complete: bool = False

    @property
    def level(self) -> str:
        """Return confirmed when every start instance was read, else tested."""
        return "confirmed" if self.complete and self.matched else "tested"


@dataclass
class RouteResult:
    """The matched routes of one link, the number of routes tested, and why the test stopped."""

    routes: list[RouteEvidence] = field(default_factory=list)
    tested: int = 0
    stop_reason: str | None = None


def propose_segments(
    link: Link, source: MinedSchema, target: MinedSchema
) -> tuple[list[Segment], list[Segment]]:
    """Return the paths to test before and after a link; the empty path is first in each list.

    Before: each schema step and each tested path of the source that ends at the class of the
    link. After: each schema step and each tested path of the target that starts at the class
    that the link reaches and ends at a class.
    """

    def paths(schema: MinedSchema) -> Iterable[Segment]:
        """Return each schema step and each matched tested path of a schema."""
        for pattern in schema.patterns:
            yield (pattern,)
        if schema.navigation is not None:
            for path in schema.navigation.paths:
                if path.instance_support == "matched":
                    yield tuple(path.steps)

    def distinct(found: Iterable[Segment]) -> list[Segment]:
        """Return the empty path and each path once, in their order."""
        seen: dict[tuple[tuple[str, str, str], ...], Segment] = {(): ()}
        for path in found:
            seen.setdefault(tuple(_triple(step) for step in path), path)
        return list(seen.values())

    befores = (p for p in paths(source) if p[-1].object_class == link.source_class)
    afters = (
        p
        for p in paths(target)
        if p[0].subject_class == link.target_class and p[-1].object_class not in _SENTINEL_OBJECTS
    )
    return distinct(befores), distinct(afters)


def _triple(step: SchemaPattern) -> tuple[str, str, str]:
    """Return the class, property and object class of a step."""
    return step.subject_class, step.property_uri, step.object_class


def _walk(steps: Segment, first: str, name: str) -> tuple[str, str]:
    """Return the pattern of a path from the variable *first*, and its last variable."""
    parts, node = [], first
    for index, step in enumerate(steps, 1):
        reached = f"?{name}{index}"
        parts.append(f"{node} <{step.property_uri}> {reached} .")
        if step.object_class not in _SENTINEL_OBJECTS:
            parts.append(f"{reached} a <{step.object_class}> .")
        node = reached
    return " ".join(parts), node


def _source_pattern(route: Route) -> str:
    """Return the pattern of the start instances ?n0 and their link values ?v."""
    path, last = _walk(route.before, "?n0", "n")
    typed = f"?n0 a <{route.start_class}> . {path} "
    if route.before:
        typed += f"{last} a <{route.link.source_class}> . "
    return f"{typed}{last} <{route.link.property}> ?v ."


def check_routes(
    link: Link,
    befores: Sequence[Segment],
    afters: Sequence[Segment],
    source: Client,
    target: Client,
    *,
    sample: int | None = 500,
    replacements: Mapping[str, str] | None = None,
    read_target: bool = False,
    budget_s: float = 600.0,
    clock: Callable[[], float] = time.monotonic,
) -> RouteResult:
    """Test the routes of one link: each path before it with each path after it.

    With *sample* None every start instance is read, else up to *sample* pairs of a start
    instance and a value. A path after the link that no value reaches, and a path before the
    link without start instances, are not combined further. The link alone is not a route.
    """
    replacements = replacements or {}
    deadline = clock() + budget_s
    result = RouteResult()
    reached: dict[int, dict[str, bool]] = defaultdict(dict)

    def reach(index: int, keys: set[str]) -> set[str]:
        """Return the identifiers that reach the end of the path *index* after the link."""
        known = reached[index]
        missing = sorted(keys - set(known))
        if missing:
            extra, _ = _walk(afters[index], "?x", "m")
            options: dict[str, Any] = {"in_target_class": True, "extra": " " + extra}
            found = (
                _read_target(link, target, missing, replacements, **options)
                if read_target
                else None
            )
            if found is None:
                found = _lookup(link, target, missing, replacements, **options)
            known.update({key: key in found for key in missing})
        return {key for key in keys if known[key]}

    for before in befores:
        if clock() >= deadline:
            result.stop_reason = "budget"
            break
        body = source._scope(_source_pattern(Route(link, before)))
        query = f"SELECT DISTINCT ?n0 ?v WHERE {{ {body} }}"
        if sample is None:
            counted = source._select(f"SELECT (COUNT(*) AS ?n) WHERE {{ {query} }}")
            rows = _read_all(source, query, int(counted[0]["n"]["value"]) if counted else None)
        else:
            rows = source._select(f"{query} LIMIT {sample}")
        values: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            parsed = _local(row["v"]["value"])
            if parsed and parsed[0] == link.identifier_type:
                values[row["n0"]["value"]].add(f"{parsed[0]}:{parsed[1]}")
        keys = set().union(*values.values()) if values else set()
        for index, after in enumerate(afters):
            if not before and not after:
                reach(index, keys)  # the link itself: its identifiers are looked up once
                continue
            if clock() >= deadline:
                result.stop_reason = "budget"
                break
            result.tested += 1
            if not values:
                continue
            found = reach(index, keys)
            matched = sum(1 for held in values.values() if held & found)
            if matched:
                route = Route(link, before, after)
                result.routes.append(RouteEvidence(route, len(values), matched, sample is None))
        if result.stop_reason:
            break
    return result


def federated_query(
    route: Route, link: LinkEvidence, endpoints: Mapping[str, str | None]
) -> str | None:
    """Return a federated query of a route, generated from its record and not executed.

    The values of the link are rewritten to the form of the target as the first example of the
    link shows. None when a dataset has no public endpoint, or the target holds the identifier
    as a literal. Identifiers that were found through a replacement are not rewritten.
    """
    source, target = endpoints.get(route.link.source), endpoints.get(route.link.target)
    forms = [form for form in link.target_forms if form != "literal"]
    if not source or not target or not forms or not link.examples:
        return None
    value, term = link.examples[0]
    shared = 0
    while shared < min(len(value), len(term)) and value[-1 - shared] == term[-1 - shared]:
        shared += 1
    before, last = _walk(route.before, "?n0", "n")
    after, _ = _walk(route.after, "?t" if route.link.kind == "join" else "?x", "m")
    start = f"?n0 a <{route.start_class}> . {before} ".replace("  ", " ")
    if value == term:
        rewrite, held = "", "?t"
    else:
        old, new = value[: len(value) - shared], term[: len(term) - shared]
        rewrite = f'  BIND(IRI(CONCAT("{new}", STRAFTER(STR(?v), "{old}"))) AS ?t)\n'
        held = "?v"
    if route.link.kind == "join":
        reached = f"?t a <{route.link.target_class}> ."
    else:
        reached = f"?x <{route.link.target_property}> ?t . ?x a <{route.link.target_class}> ."
    return (
        "# Generated from a tested route of the registry; not executed.\n"
        "SELECT DISTINCT * WHERE {\n"
        f"  SERVICE <{source}> {{ {start}{last} <{route.link.property}> {held} . }}\n"
        f"{rewrite}"
        f"  SERVICE <{target}> {{ {reached} {after} }}\n"
        "}\n"
    )


def write_routes(
    path: str | Path,
    routes: Iterable[RouteEvidence],
    *,
    links: Iterable[LinkEvidence],
    endpoints: Mapping[str, str | None],
    tested: int,
    stop_reason: str | None,
    failed: Sequence[Mapping[str, Any]],
) -> None:
    """Write the matched routes with the counts of the test (format routes-1)."""
    by_link = {evidence.link: evidence for evidence in links}
    rows = []
    for evidence in routes:
        route, link = evidence.route, evidence.route.link
        rows.append(
            {
                "source": link.source,
                "start_class": route.start_class,
                "before": [list(_triple(step)) for step in route.before],
                "link": {
                    "kind": link.kind,
                    "source_class": link.source_class,
                    "property": link.property,
                    "identifier_type": link.identifier_type,
                    "target_class": link.target_class,
                    "target_property": link.target_property,
                },
                "after": [list(_triple(step)) for step in route.after],
                "target": link.target,
                "end_class": route.end_class,
                "start_instances": evidence.starts,
                "matched_instances": evidence.matched,
                "complete": evidence.complete,
                "evidence": evidence.level,
                "federated_query": federated_query(route, by_link[link], endpoints)
                if link in by_link
                else None,
            }
        )
    record = {
        "format": "routes-1",
        "tested": tested,
        "matched": len(rows),
        "stop_reason": stop_reason,
        "failed": list(failed),
        "routes": rows,
    }
    Path(path).write_text(json.dumps(record, indent=1), encoding="utf-8")


def read_routes(path: str | Path) -> list[RouteEvidence]:
    """Read the routes of a file that write_routes made."""

    def steps(rows: list[list[str]]) -> Segment:
        """Return the steps of a written path."""
        return tuple(
            SchemaPattern(subject_class=s, property_uri=p, object_class=o) for s, p, o in rows
        )

    found = []
    for row in json.loads(Path(path).read_text(encoding="utf-8"))["routes"]:
        held = row["link"]
        link = Link(
            held["kind"],
            row["source"],
            held["source_class"],
            held["property"],
            held["identifier_type"],
            row["target"],
            held["target_class"],
            held["target_property"],
        )
        route = Route(link, steps(row["before"]), steps(row["after"]))
        found.append(
            RouteEvidence(route, row["start_instances"], row["matched_instances"], row["complete"])
        )
    return found
