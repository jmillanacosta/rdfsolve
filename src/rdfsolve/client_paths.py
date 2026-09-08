"""Find bounded class routes and actual resource connections."""

from __future__ import annotations

import re
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic import BaseModel

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


def connection_query(source: str, target: str, hops: int, both_directions: bool) -> str:
    """Build a fixed-length path with bound endpoints and no repeated resources."""
    body = [f"VALUES ?n0 {{ {_iri(source)} }} VALUES ?n{hops} {{ {_iri(target)} }}"]
    for i in range(hops):
        forward = f"?n{i} ?p{i} ?n{i + 1} . BIND(false AS ?back{i})"
        if both_directions:
            backward = f"?n{i + 1} ?p{i} ?n{i} . BIND(true AS ?back{i})"
            body.append(f"{{ {{ {forward} }} UNION {{ {backward} }} }}")
        else:
            body.append(forward)
    for i in range(1, hops):
        body.append(f"FILTER(!isLiteral(?n{i}))")
    for i in range(hops + 1):
        for j in range(i):
            body.append(f"FILTER(!sameTerm(?n{i}, ?n{j}))")
    return "\n".join(body)


def resource_paths(
    client: Client,
    source: str | BaseModel,
    target: str | BaseModel,
    *,
    max_hops: int,
    both_directions: bool,
    max_paths: int,
) -> pd.DataFrame:
    """Retrieve all simple resource paths within the bounds, or raise on overflow.

    Each path stays in one selected graph. Reverse steps are included when
    both_directions is true. No predicates are excluded, including rdf:type.
    Endpoint limits still apply; a successful response is not a completeness proof.
    """
    _budget(max_hops, max_paths)
    first = str(vars(source)["uri"]) if isinstance(source, BaseModel) else source
    last = str(vars(target)["uri"]) if isinstance(target, BaseModel) else target
    _iri(first)
    _iri(last)
    if first == last:
        raise ValueError("Choose two different resources")
    routes: list[dict[str, Any]] = []
    with client.step(f"Find connections between {first} and {last}"):
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
    classes: dict[tuple[str, str], set[str]] = defaultdict(set)
    models = {getattr(model, "rdf_class_iri", ""): model for model in client.models.values()}
    with client.step("Read classes along connections"):
        for start in range(0, len(nodes), client.batch_size):
            values = " ".join(_iri(node) for node in nodes[start : start + client.batch_size])
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
                or ("Class not read" if node["type"] == "bnode" else "No type returned")
            )
        from_classes.extend(names[:-1])
        to_classes.extend(names[1:])
    table.insert(3, "From class", from_classes)
    table["To class"] = to_classes
    table.attrs["resource_classes"] = [
        {"graph": graph, "resource": resource, "classes": sorted(types)}
        for (graph, resource), types in sorted(classes.items())
    ]
