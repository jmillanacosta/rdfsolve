"""Find bounded class routes and actual resource connections."""

from __future__ import annotations

import logging
import re
from collections import defaultdict, deque
from itertools import product
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic import BaseModel
from rdflib import RDF, Literal

from rdfsolve.hydration import HydrationLimitError, _iri

if TYPE_CHECKING:
    from rdfsolve.client_api import Client

COLUMNS = ["Path", "Step", "From", "Link", "Direction", "To"]


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
) -> pd.DataFrame:
    """Enumerate simple routes in the supplied schema, not instance matches."""
    _budget(max_hops, max_paths)
    start = client.model(source)
    end = client.model(target)
    first, last = str(getattr(start, "rdf_class_iri", "")), str(getattr(end, "rdf_class_iri", ""))
    if first == last:
        raise ValueError("Choose two different classes")
    models = {str(getattr(model, "rdf_class_iri", "")): model for model in client.models.values()}
    edges: dict[str, set[tuple[str, str, bool]]] = defaultdict(set)
    reverse: dict[str, set[str]] = defaultdict(set)
    for pattern in client._schema.patterns:
        s, p, o = pattern.subject_class, pattern.property_uri, pattern.object_class
        if pattern.count == 0 or s not in models or o not in models:
            continue
        edges[s].add((p, o, False))
        reverse[o].add(s)
        if both_directions:
            edges[o].add((p, s, True))
            reverse[s].add(o)
    distances = {last: 0}
    pending = deque([last])
    while pending:
        node = pending.popleft()
        for previous in sorted(reverse[node]):
            if previous not in distances:
                distances[previous] = distances[node] + 1
                pending.append(previous)
    routes: list[list[tuple[str, str, str, bool]]] = []

    def visit(node: str, seen: set[str], route: list[tuple[str, str, str, bool]]) -> None:
        """Extend a route only where the target is still reachable."""
        if len(route) + distances.get(node, max_hops + 1) > max_hops:
            return
        if node == last:
            routes.append(route)
            if len(routes) > max_paths:
                raise HydrationLimitError(
                    "Too many class paths; reduce max_hops or raise max_paths"
                )
            return
        for predicate, next_node, backward in sorted(edges[node]):
            if next_node not in seen:
                visit(
                    next_node, seen | {next_node}, [*route, (node, predicate, next_node, backward)]
                )

    visit(first, {first}, [])
    routes.sort(key=lambda route: (len(route), route))
    rows = [
        [
            number,
            step,
            client.type_name(models[s]),
            _label(client, p),
            "←" if backward else "→",
            client.type_name(models[o]),
        ]
        for number, route in enumerate(routes, 1)
        for step, (s, p, o, backward) in enumerate(route, 1)
    ]
    table = pd.DataFrame(rows, columns=COLUMNS)
    table.attrs.update(routes=routes, basis="mined class patterns", max_hops=max_hops)
    return table


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
    max_paths: int,
) -> pd.DataFrame:
    """Retrieve all simple resource paths within the bounds, or raise on overflow.

    Each path stays in one selected graph. Reverse steps are included when
    both_directions is true. With a target, no predicates are excluded.
    Without a target, exclude rdf:type and literal leaves.
    Endpoint limits still apply; a successful response is not a completeness proof.
    """
    _budget(max_hops, max_paths)
    first = str(vars(source)["uri"]) if isinstance(source, BaseModel) else source
    last = str(vars(target)["uri"]) if isinstance(target, BaseModel) else target
    _iri(first)
    if last is not None:
        _iri(last)
    if first == last:
        raise ValueError("Choose two different resources")
    routes: list[dict[str, Any]] = []
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
            limit = min(client.max_rows, max_paths - len(routes)) + 1
            query = (
                f"SELECT DISTINCT {' '.join(variables)} ?_graph WHERE {{ "
                + client._scope(body)
                + f" }} LIMIT {limit}"
            )
            bindings = client._select(query)
            if len(bindings) >= limit:
                raise HydrationLimitError(
                    "Too many connections; reduce max_hops or raise max_paths and max_rows"
                )
            for binding in bindings:
                routes.append(
                    {"hops": hops, "bindings": binding, "query_id": len(client._records())}
                )
    return resource_path_table(client, routes, max_hops)


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
    with client.step("Read classes along connections"):
        for start in range(0, len(nodes), client.batch_size):
            values = " ".join(safe_nodes[node] for node in nodes[start : start + client.batch_size])
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
            with client.step("Read classes through retained links"):
                rows = client._select(
                    f"SELECT DISTINCT ?n{index} ?class WHERE {{ {scoped} }} LIMIT {client.max_rows + 1}"
                )
            if len(rows) > client.max_rows:
                raise HydrationLimitError("Class lookup exceeded max_rows")
            if rows:
                recovered[key] = {row["class"]["value"] for row in rows if "class" in row}
    return recovered
