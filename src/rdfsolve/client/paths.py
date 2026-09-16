"""Find bounded class routes and actual resource connections."""

from __future__ import annotations

import logging
import re
import warnings
from collections import defaultdict, deque
from itertools import pairwise, product
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic import BaseModel
from rdflib import RDF, Literal

from rdfsolve.client.hydration import HydrationLimitError, _iri
from rdfsolve.client.query_fragments import Fragment, linear_steps
from rdfsolve.schema_models.paths import PropertyPath

if TYPE_CHECKING:
    from rdfsolve.client.api import Client

COLUMNS = ["Path", "Step", "From", "Link", "Direction", "To"]
CLASS_BATCH_SIZE = 200


def _budget(max_hops: int, max_paths: int) -> None:
    if not 1 <= max_hops <= 6 or max_paths < 1:
        raise ValueError("Use 1..6 hops and a positive max_paths")


def _label(client: Client, iri: str) -> str:
    labels = [
        item.text.value
        for item in client._schema.enrichment.labels
        if item.term_iri == iri and item.text.language in (None, "en")
    ]
    return min(labels) if labels else re.split(r"[/#]", iri)[-1].replace("_", " ")


def class_paths(
    client: Client,
    source: str,
    target: str,
    *,
    max_hops: int,
    both_directions: bool,
    max_paths: int,
    allow_partial: bool = False,
    allow_repeated_classes: bool = False,
    meaning: str = "",
    via: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Enumerate bounded routes in the supplied empirical schema."""
    _budget(max_hops, max_paths)
    start = client.model(source)
    end = client.model(target)
    first, last = str(getattr(start, "rdf_class_iri", "")), str(getattr(end, "rdf_class_iri", ""))
    if first == last and not allow_repeated_classes:
        raise ValueError("Choose two different classes")
    models = {str(getattr(model, "rdf_class_iri", "")): model for model in client.models.values()}
    edges: dict[str, set[tuple[str, str, bool]]] = defaultdict(set)
    reverse: dict[str, set[str]] = defaultdict(set)
    unsupported = []
    for cls, model in models.items():
        for link in client.links(model).itertuples(index=False):
            target_iri = str(client.model(link.target).rdf_class_iri)
            try:
                steps = linear_steps(link.path)
            except ValueError:
                unsupported.append(
                    f"{client.type_name(model)}.{link.field}: use this field directly for alternatives or repeated paths"
                )
                continue
            nodes = [
                cls,
                *[(cls, link.field, target_iri, i) for i in range(1, len(steps))],
                target_iri,
            ]
            for (left, right), (predicate, backward) in zip(pairwise(nodes), steps, strict=True):
                edges[left].add((predicate, right, backward))
                reverse[right].add(left)
                if both_directions:
                    edges[right].add((predicate, left, not backward))
                    reverse[left].add(right)
    distances = {last: 0}
    pending = deque([last])
    while pending:
        node = pending.popleft()
        for previous in sorted(reverse[node], key=str):
            if previous not in distances:
                distances[previous] = distances[node] + 1
                pending.append(previous)
    required = [str(client.model(value).rdf_class_iri) for value in via]
    observed = {}
    if client._schema.navigation is not None:
        observed = {
            tuple((s.subject_class, s.property_uri, s.object_class, False) for s in p.steps): p
            for p in client._schema.navigation.paths
            if all(s.subject_class in models and s.object_class in models for s in p.steps)
        }

    def compatible(route):
        intermediate = [edge[2] for edge in route[:-1]]
        position = 0
        for cls in required:
            try:
                position = intermediate.index(cls, position) + 1
            except ValueError:
                return False
        return True

    from rdfsolve.client.catalogue import words

    wanted = words(meaning)

    vocabulary = {}

    def terms(iri):
        if iri not in vocabulary:
            vocabulary[iri] = words(_label(client, iri)) if isinstance(iri, str) else set()
        return vocabulary[iri]

    def relevance(route):
        predicates, classes = set(), set()
        for left, predicate, right, _ in route:
            predicates.update(terms(predicate))
            classes.update(terms(left) | terms(right))
        return len(wanted & predicates), len(wanted & classes)

    routes = [
        list(route)
        for route in observed
        if route[0][0] == first
        and route[-1][2] == last
        and len(route) <= max_hops
        and compatible(route)
        and (
            allow_repeated_classes
            or len({route[0][0], *(edge[2] for edge in route)}) == len(route) + 1
        )
    ]
    known = {tuple(route) for route in routes}

    queue = deque([(first, {first}, [])])
    truncated, expansions = False, 0
    expansion_limit = max(1000, max_paths * 100)
    while queue:
        node, seen, route = queue.popleft()
        if len(route) + distances.get(node, max_hops + 1) > max_hops:
            continue
        if node == last and route:
            normalized = [
                (s if s in models else None, p, o if o in models else None, back)
                for s, p, o, back in route
            ]
            if compatible(normalized) and tuple(normalized) not in known:
                routes.append(normalized)
                known.add(tuple(normalized))
            continue
        if len(route) >= max_hops:
            continue
        expansions += 1
        if expansions > expansion_limit or len(queue) > expansion_limit:
            truncated = True
            break
        for predicate, next_node, backward in sorted(edges[node], key=str):
            if allow_repeated_classes or next_node not in seen:
                queue.append(
                    (
                        next_node,
                        seen | {next_node},
                        [*route, (node, predicate, next_node, backward)],
                    )
                )
    if truncated and not allow_partial:
        raise HydrationLimitError("Class path budget exhausted; reduce max_hops or raise max_paths")

    def support(route):
        item = observed.get(tuple(route))
        return item.instance_support if item else "not_checked"

    routes.sort(
        key=lambda route: (
            tuple(-value for value in relevance(route)),
            {"matched": 0, "no_match": 2}.get(support(route), 1),
            len(route),
            str(route),
        )
    )
    truncated |= len(routes) > max_paths
    if truncated and not allow_partial:
        raise HydrationLimitError("Class path budget exhausted; reduce max_hops or raise max_paths")
    routes = routes[:max_paths]
    rows = [
        [
            number,
            step,
            client.type_name(models[s]) if s else "Intermediate resource",
            _label(client, p),
            "←" if backward else "→",
            client.type_name(models[o]) if o else "Intermediate resource",
        ]
        for number, route in enumerate(routes, 1)
        for step, (s, p, o, backward) in enumerate(route, 1)
    ]
    table = pd.DataFrame(rows, columns=COLUMNS)
    table.attrs.update(
        routes=routes,
        fragments=[route_fragment(client, route) for route in routes],
        warnings=list(dict.fromkeys(unsupported)),
        observations=[
            {
                "status": support(route),
                "basis": "mined snapshot" if tuple(route) in observed else "schema only",
                "meaning_matches": relevance(route),
                "via": required,
                "sources": observed[tuple(route)].source_count
                if tuple(route) in observed
                else None,
                "matched_sources": observed[tuple(route)].matched_sources
                if tuple(route) in observed
                else None,
            }
            for route in routes
        ],
        basis="generated field paths",
        max_hops=max_hops,
        truncated=truncated,
        expansions=expansions,
        status="partial" if truncated else "complete",
    )
    return table


def route_fragment(client: Client, route) -> Fragment:
    """Compile a generated-model route, retaining its intermediate class constraints."""
    paths = [PropertyPath(operator="predicate", iri=p) for _, p, _, _ in route]
    paths = [
        PropertyPath(operator="inverse", items=[p]) if edge[3] else p
        for edge, p in zip(route, paths, strict=True)
    ]
    labels = [_label(client, route[0][0]) if route[0][0] else "Resource"]
    labels += [_label(client, o) if o else "Resource" for _, _, o, _ in route]
    description = f"{labels[0]} → {labels[-1]}"
    if len(labels) > 2:
        description += " via " + ", ".join(labels[1:-1])
    return Fragment(
        "path",
        description
        + ": "
        + " / ".join(("inverse " if back else "") + _label(client, p) for _, p, _, back in route),
        path=paths[0] if len(paths) == 1 else PropertyPath(operator="sequence", items=paths),
        steps=route,
        basis="generated model paths",
    )


def connection_query(source: str, target: str | None, hops: int, both_directions: bool) -> str:
    """Build a fixed-length path with bound endpoints and no repeated resources."""
    nodes = [_iri(source), *[f"?n{i}" for i in range(1, hops + 1)]]
    if target is not None:
        nodes[-1] = _iri(target)
    branches = []
    for directions in product((False, True) if both_directions else (False,), repeat=hops):
        body = []
        # Keep each connected triple pattern together, before output bindings.
        for i, backward in enumerate(directions):
            left, right = (i + 1, i) if backward else (i, i + 1)
            body.append(f"{nodes[left]} ?p{i} {nodes[right]} .")
            if target is None:
                body.append(f"FILTER(?p{i} != {_iri(str(RDF.type))})")
        for i in range(1, hops + (target is None)):
            body.append(f"FILTER(!isLiteral({nodes[i]}))")
        for i in range(hops + 1):
            for j in range(i):
                body.append(f"FILTER(!sameTerm({nodes[i]}, {nodes[j]}))")
        body.append(f"BIND({_iri(source)} AS ?n0)")
        if target is not None:
            body.append(f"BIND({_iri(target)} AS ?n{hops})")
        for i, backward in enumerate(directions):
            body.append(f"BIND({'true' if backward else 'false'} AS ?back{i})")
        branches.append("{ " + " ".join(body) + " }")
    return " UNION ".join(branches)


def resource_paths(
    client: Client,
    source: str | BaseModel,
    target: str | BaseModel | None,
    *,
    max_hops: int,
    both_directions: bool,
    max_paths: int | None,
) -> pd.DataFrame:
    """Retrieve bounded simple resource paths, marking a limited default view.

    Each path stays in one selected graph. Reverse steps are included when
    both_directions is true. With a target, no predicates are excluded.
    Without a target, exclude rdf:type and literal leaves.
    Endpoint limits still apply; a successful response is not a completeness proof.
    """
    budget = client.max_rows if max_paths is None else max_paths
    _budget(max_hops, budget)
    first = str(vars(source)["uri"]) if isinstance(source, BaseModel) else source
    last = str(vars(target)["uri"]) if isinstance(target, BaseModel) else target
    _iri(first)
    if last is not None:
        _iri(last)
    if first == last:
        raise ValueError("Choose two different resources")
    routes: list[dict[str, Any]] = []
    truncated = False
    description = (
        f"Find connections from {first}"
        if last is None
        else f"Find connections between {first} and {last}"
    )
    with client.step(description):
        for hops in range(1, max_hops + 1):
            body = connection_query(first, last, hops, both_directions)
            variables = [f"?n{i}" for i in range(hops + 1)]
            variables += [f"?p{i} ?back{i}" for i in range(hops)]
            limit = min(client.max_rows, budget - len(routes)) + 1
            query = (
                f"SELECT DISTINCT {' '.join(variables)} ?_graph WHERE {{ "
                + client._scope(body)
                + f" }} LIMIT {limit}"
            )
            bindings = client._select(query)
            if len(bindings) >= limit:
                if max_paths is not None:
                    raise HydrationLimitError(
                        "Too many connections; reduce max_hops or raise max_paths and max_rows"
                    )
                bindings = bindings[: limit - 1]
                truncated = True
            for binding in bindings:
                routes.append(
                    {"hops": hops, "bindings": binding, "query_id": len(client._records())}
                )
            if truncated:
                break
    if truncated:
        client._steps[-1]["status"] = "partial"
    table = resource_path_table(client, routes, max_hops)
    table.attrs.update(truncated=truncated, status="partial" if truncated else "complete")
    if truncated:
        warnings.warn(
            f"Partial connections view: showing {len(routes)} paths; more connections exist. "
            "Use fewer hops or a specific target to narrow the view.",
            UserWarning,
            stacklevel=3,
        )
    return table


def resource_path_table(
    client: Client, routes: list[dict[str, Any]], max_hops: int
) -> pd.DataFrame:
    """Render retained resource paths and retrieve their class annotations."""
    rows = []
    for number, route in enumerate(routes, 1):
        binding = route["bindings"]
        for i in range(route["hops"]):
            s, p, o = (binding[key]["value"] for key in (f"n{i}", f"p{i}", f"n{i + 1}"))
            backward = binding[f"back{i}"]["value"] in ("true", "1")
            rows.append([number, i + 1, s, _label(client, p), "←" if backward else "→", o])
    table = pd.DataFrame(rows, columns=COLUMNS)
    table.attrs.update(routes=routes, basis="queried resource paths", max_hops=max_hops)
    _add_classes(client, table, routes)
    return table


def _add_classes(client: Client, table: pd.DataFrame, routes: list[dict[str, Any]]) -> None:
    nodes = sorted(
        {
            term["value"]
            for route in routes
            for key, term in route["bindings"].items()
            if key.startswith("n") and term["type"] == "uri"
        }
    )
    safe_nodes = {}
    for node in nodes:
        try:
            safe_nodes[node] = _iri(node)
        except ValueError:
            continue
    unread = set(nodes) - safe_nodes.keys()
    table.attrs["unresolved_resources"] = sorted(unread)
    nodes = sorted(safe_nodes)
    classes: dict[tuple[str, str], set[str]] = defaultdict(set)
    models = {getattr(model, "rdf_class_iri", ""): model for model in client.models.values()}
    batch_size = min(CLASS_BATCH_SIZE, client.max_rows)
    with client.step("Read classes along connections"):
        for start in range(0, len(nodes), batch_size):
            values = " ".join(safe_nodes[node] for node in nodes[start : start + batch_size])
            body = client._scope(f"VALUES ?resource {{ {values} }} ?resource a ?class")
            found = client._select(
                f"SELECT DISTINCT ?resource ?class ?_graph WHERE {{ {body} }} LIMIT {client.max_rows + 1}"
            )
            if len(found) > client.max_rows:
                raise HydrationLimitError("Class lookup exceeded max_rows")
            for row in found:
                resource, cls = row["resource"]["value"], row["class"]["value"]
                graph = row.get("_graph", {}).get("value", "")
                classes[(graph, resource)].add(cls)
    recovered = _read_unaddressable_classes(client, routes, unread, safe_nodes)
    classes.update(recovered)
    missing = unread - {resource for _, resource in recovered}
    if missing:
        logging.getLogger(__name__).warning(
            "Class lookup skipped for %d returned identifiers without a safe absolute IRI; "
            "their links remain in the table",
            len(missing),
        )
    from_classes, to_classes = [], []
    for route in routes:
        binding = route["bindings"]
        graph = binding.get("_graph", {}).get("value", "")
        names = []
        for i in range(route["hops"] + 1):
            node = binding[f"n{i}"]
            types = sorted(classes[(graph, node["value"])])
            names.append(
                " | ".join(
                    client.type_name(models[cls]) if cls in models else _label(client, cls)
                    for cls in types
                )
                or (
                    "Class not read"
                    if node["type"] == "bnode"
                    or (node["value"] in unread and (graph, node["value"]) not in recovered)
                    else "No type returned"
                )
            )
        from_classes.extend(names[:-1])
        to_classes.extend(names[1:])
    table.insert(3, "From class", from_classes)
    table["To class"] = to_classes
    table.attrs["resource_classes"] = [
        {"graph": graph, "resource": resource, "classes": sorted(types)}
        for (graph, resource), types in sorted(classes.items())
    ]


def _read_unaddressable_classes(
    client: Client,
    routes: list[dict[str, Any]],
    unread: set[str],
    safe_nodes: dict[str, str],
) -> dict[tuple[str, str], set[str]]:
    """Read malformed returned IRIs through their observed links, without a guessed base."""
    recovered: dict[tuple[str, str], set[str]] = {}
    attempted = set()
    lookups = []
    for route in routes:
        bindings = route["bindings"]
        graph = bindings.get("_graph", {}).get("value", "")
        nodes = [bindings[f"n{i}"] for i in range(route["hops"] + 1)]
        if any(node["type"] != "uri" for node in nodes):
            continue
        for index, node in enumerate(nodes):
            key = (graph, node["value"])
            if node["value"] not in unread or key in attempted:
                continue
            attempted.add(key)
            terms = [safe_nodes.get(n["value"], f"?n{i}") for i, n in enumerate(nodes)]
            body = []
            for i in range(route["hops"]):
                left, right = (
                    (i + 1, i) if bindings[f"back{i}"]["value"] in ("true", "1") else (i, i + 1)
                )
                body.append(f"{terms[left]} {_iri(bindings[f'p{i}']['value'])} {terms[right]} .")
            for i, term in enumerate(nodes):
                if term["value"] in unread:
                    body.append(f"FILTER(STR(?n{i}) = {Literal(term['value']).n3()})")
            body.append(f"OPTIONAL {{ ?n{index} a ?class }}")
            pattern = " ".join(body)
            scoped = f"GRAPH {_iri(graph)} {{ {pattern} }}" if graph else client._scope(pattern)
            lookups.append((key, scoped))
    with client.step("Read classes through retained links"):
        for start in range(0, len(lookups), CLASS_BATCH_SIZE):
            batch = lookups[start : start + CLASS_BATCH_SIZE]
            union = " UNION ".join(
                f"{{ {pattern} BIND({index} AS ?lookup) }}"
                for index, (_, pattern) in enumerate(batch)
            )
            rows = client._select(
                f"SELECT DISTINCT ?lookup ?class WHERE {{ {union} }} LIMIT {client.max_rows + 1}"
            )
            if len(rows) > client.max_rows:
                raise HydrationLimitError("Class lookup exceeded max_rows")
            for row in rows:
                key = batch[int(row["lookup"]["value"])][0]
                recovered.setdefault(key, set())
                if "class" in row:
                    recovered[key].add(row["class"]["value"])
    return recovered
