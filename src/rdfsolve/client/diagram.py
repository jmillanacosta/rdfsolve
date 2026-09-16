"""Draw selected generated models with their RDF labels and identifiers."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from rdfsolve.client.api import Client


def model_diagram(client: Client, kinds: tuple[str, ...]) -> str:
    """Draw generated model labels and retained field paths without source requests."""
    if not kinds:
        raise ValueError("Choose the classes to show")
    models = list(dict.fromkeys(client.model(kind) for kind in kinds))
    ids = {str(model.rdf_class_iri): f"C{i}" for i, model in enumerate(models)}
    lines = ["flowchart LR"]
    for model in models:
        iri = str(model.rdf_class_iri)
        label = _text(client.type_name(model)) + "<br/>" + _text(iri)
        lines.append(f'{ids[iri]}["{label}"]')
    edges = set()
    for model in models:
        for row in client.links(model).itertuples(index=False):
            if str(row.target) in ids:
                label = _text(client.link_name(model, str(row.field)))
                edges.add(f'{ids[str(model.rdf_class_iri)]} -->|"{label}"| {ids[str(row.target)]}')
    return "```mermaid\n" + "\n".join([*lines, *sorted(edges)]) + "\n```"


def path_diagram(
    client: Client,
    paths: pd.DataFrame,
    *,
    path: int | None = None,
    instances: bool = True,
) -> str:
    """Draw only the retained path steps selected in the table."""
    routes = paths.attrs.get("routes")
    if not isinstance(routes, list):
        raise ValueError("Use a table returned by paths_between or connections")
    if path is not None:
        if type(path) is not int or path not in paths["Path"].values:
            raise ValueError("Choose a Path number from the table")
        paths = paths[paths["Path"] == path]
    classes = {
        (item["graph"], item["resource"]): item["classes"]
        for item in paths.attrs.get("resource_classes", [])
    }
    unresolved = set(paths.attrs.get("unresolved_resources", []))
    nodes: dict[str, tuple[str, str]] = {}
    edges: set[tuple[str, str, str]] = set()
    for row in paths.to_dict(orient="records"):
        route = routes[int(row["Path"]) - 1]
        step = int(row["Step"]) - 1
        if isinstance(route, dict):
            binding = route["bindings"]
            s, o = (binding[key]["value"] for key in (f"n{step}", f"n{step + 1}"))
            backward = binding[f"back{step}"]["value"] in ("true", "1")
            labels = (str(row["From class"]), str(row["To class"]))
            keys = [
                f"{route['query_id']}:{binding[f'n{i}']['value']}"
                if binding[f"n{i}"]["type"] == "bnode" or binding[f"n{i}"]["value"] in unresolved
                else binding[f"n{i}"]["value"]
                for i in (step, step + 1)
            ]
            if not instances:
                graph = binding.get("_graph", {}).get("value", "")
                types = [" | ".join(classes.get((graph, iri), [])) for iri in (s, o)]
                keys = [
                    "classes:" + value if value else key
                    for value, key in zip(types, keys, strict=True)
                ]
                s, o = [value or iri for value, iri in zip(types, (s, o), strict=True)]
        else:
            s, _predicate, o, backward = route[step]
            labels = tuple(
                client.type_name(client.model(c)) if c else "Intermediate resource" for c in (s, o)
            )
            keys = [s or f"path:{row['Path']}:{step}", o or f"path:{row['Path']}:{step + 1}"]
            s, o = s or "", o or ""
        for key, iri, label in zip(keys, (s, o), labels, strict=True):
            if key not in nodes:
                nodes[key] = (f"N{len(nodes)}", f"{_text(label)}<br/>{_text(iri)}")
        source, target = keys[::-1] if backward else keys
        edges.add((nodes[source][0], str(row["Link"]), nodes[target][0]))
    lines = ["flowchart LR"]
    lines.extend(f'{name}["{label}"]' for name, label in nodes.values())
    lines.extend(f'{s} -->|"{_text(label)}"| {o}' for s, label, o in sorted(edges))
    notice = "Partial view: more connections exist.\n\n" if paths.attrs.get("truncated") else ""
    return notice + "```mermaid\n" + "\n".join(lines) + "\n```"


def connection_diagram(table: pd.DataFrame, *, instances: bool = False) -> str:
    """Draw only observed routes in a returned connection table or its selected rows.

    Class mode groups identical observed routes within each graph. Route groups
    preserve instance associations and source-query provenance.
    """
    retained = table.attrs.get("connections")
    if not isinstance(retained, dict) or "Connection" not in table:
        raise ValueError("Use read_result(..., output='connections') to retain path evidence")
    if table.empty:
        return "No observed connections in this result."
    groups: dict[str, list[dict[str, Any]]] = {}
    for identifier in dict.fromkeys(table["Connection"]):
        if identifier not in retained:
            raise ValueError("A selected connection has no retained path evidence")
        match = retained[identifier]
        key = (
            identifier
            if instances
            else json.dumps(
                [match["graph"], [node["type"] for node in match["nodes"]], match["links"]],
                sort_keys=True,
            )
        )
        groups.setdefault(key, []).append(match)
    labels = table.attrs.get("class_labels", {})
    lines = ["flowchart LR"]
    for number, matches in enumerate(groups.values()):
        match = matches[0]
        queries = ", ".join(str(value) for value in sorted({item["query_id"] for item in matches}))
        title = f"{len(matches)} observed match(es); queries: {queries}; graph: {match['graph'] or 'default'}"
        lines.append(f'subgraph R{number}["{_text(title)}"]')
        for index, node in enumerate(match["nodes"]):
            cls = labels.get(node["type"], node["type"])
            parts = [cls, node["type"]]
            if instances:
                parts = [node.get("label") or node["value"], node["value"], cls]
            label = "<br/>".join(_text(part) for part in parts)
            lines.append(f'N{number}_{index}["{label}"]')
        for index, link in enumerate(match["links"]):
            left, right = (index + 1, index) if link["inverse"] else (index, index + 1)
            lines.append(f'N{number}_{left} -->|"{_text(link["predicate"])}"| N{number}_{right}')
        lines.append("end")
    notice = "Observed routes with source-query provenance.\n\n"
    if table.attrs.get("coverage", {}).get("status") == "partial":
        notice += "Retrieval was partial.\n\n"
    return notice + "```mermaid\n" + "\n".join(lines) + "\n```"


def _text(value: str) -> str:
    return re.sub(r"[^\w .:/-]", lambda match: f"#{ord(match[0])};", value)
