"""Draw selected generated models with their RDF labels and identifiers."""

from __future__ import annotations

import json
import re
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from rdfsolve.client_api import Client


def model_diagram(client: Client, kinds: tuple[str, ...]) -> str:
    """Use pydantic-2-mermaid for class boxes and mined RDF for directed links.

    Return fenced Mermaid text for notebook Markdown. Do not contact an endpoint
    or a rendering service. Hide fields, methods, and self-links.
    """
    if not kinds:
        raise ValueError("Choose the classes to show")
    models = list(dict.fromkeys(client.model(kind) for kind in kinds))
    try:
        mermaid = import_module("pydantic_mermaid")
    except ModuleNotFoundError as error:
        if error.name != "pydantic_mermaid":
            raise
        raise ImportError(
            "Install rdfsolve[notebooks] in the notebook kernel's Python environment"
        ) from error
    except ImportError as error:
        raise ImportError(
            "Cannot load pydantic-2-mermaid. If pydantic-mermaid is also installed, "
            "uninstall it and reinstall pydantic-2-mermaid: both packages write "
            "to the same module. Original error: " + str(error)
        ) from error
    module = ModuleType("SelectedModels")
    ids = {model: f"C{i}" for i, model in enumerate(models)}
    for model, name in ids.items():
        setattr(module, name, model)
    generator = mermaid.MermaidGenerator(module)
    for box in generator.graph.class_dict.values():
        box.properties = []
    chart = str(generator.generate_chart(relations=mermaid.Relations(0)))
    # Sort the library's boxes because its class allow-list is a set.
    blocks = chart.removeprefix("```mermaid\n").removesuffix("```").strip().split("\n\n")
    lines = ["classDiagram", "direction LR"]
    labels = {}
    for model, name in ids.items():
        label = _text(client.type_name(model))
        iri = _text(str(getattr(model, "rdf_class_iri", "")))
        labels[name] = f"`{label}<br/>{iri}`"
    # Quarto's older Mermaid supports quoted names, but not class display aliases.
    lines.extend(
        re.sub(r"\bC\d+\b", lambda match: labels[match[0]], block)
        for block in sorted(blocks)
        if block.strip().startswith("class ")
    )
    edges = set()
    for model, source in ids.items():
        for row in client.links(model).itertuples(index=False):
            target = client.model(str(row.target))
            if target in ids and target is not model:
                label = _text(client.link_name(model, str(row.field)))
                edges.add(f"{labels[source]} --> {labels[ids[target]]} : {label}")
    lines.extend(sorted(edges))
    return "```mermaid\n" + "\n".join(lines) + "\n```"


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
            labels = (client.type_name(client.model(s)), client.type_name(client.model(o)))
            keys = [s, o]
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

    Class mode groups identical observed routes within a graph. Keep routes separate
    to avoid implying joins between different instances. This does not establish
    which statements in an agent's prose each route supports. No requests are sent.
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
    notice = "Observed routes retained in this result; not sentence-level citations.\n\n"
    if table.attrs.get("coverage", {}).get("status") == "partial":
        notice += "Retrieval was partial.\n\n"
    return notice + "```mermaid\n" + "\n".join(lines) + "\n```"


def _text(value: str) -> str:
    return re.sub(r"[^\w .:/-]", lambda match: f"#{ord(match[0])};", value)
