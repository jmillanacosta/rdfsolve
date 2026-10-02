"""Draw selected generated models with their RDF labels and identifiers."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from rdfsolve._uri import curie_from_prefixes
from rdfsolve.client.hydration import class_iri

if TYPE_CHECKING:
    import pandas as pd

    from rdfsolve.client.api import Client


def model_diagram(
    client: Client,
    kinds: tuple[str, ...],
    *,
    fenced: bool = True,
    namespaces: Iterable[str] = (),
    iris: str = "curie",
    merge: bool = True,
) -> str:
    """Draw generated models and their links without source requests.

    namespaces keeps the classes in these namespaces (IRIs or prefixes of the schema).
    iris shows the class IRI as "curie", "full" or "none". merge draws one edge per pair of
    classes, with the names of all links between them.
    """
    if iris not in {"curie", "full", "none"}:
        raise ValueError('Use iris="curie", "full" or "none"')
    prefixes = client.schema.get_prefixes()
    wanted = [prefixes.get(n, n) for n in namespaces]
    models = [
        model
        for model in (
            list(dict.fromkeys(client.model(kind) for kind in kinds))
            if kinds
            else list(client.models.values())
        )
        if not wanted or class_iri(model).startswith(tuple(wanted))
    ]
    ids = {class_iri(model): f"C{i}" for i, model in enumerate(models)}
    lines = ["flowchart LR"]
    for model in models:
        iri = class_iri(model)
        found = curie_from_prefixes(iri, prefixes) if iris == "curie" else None
        shown = {"full": iri, "curie": found[0] if found else iri, "none": ""}[iris]
        lines.append(_node(ids[iri], client.type_name(model), shown))
    edges: dict[tuple[str, str], list[str]] = {}
    for model in models:
        for row in client.links(model).itertuples(index=False):
            target = class_iri(client.models[str(row.target)])
            if target in ids:
                label = _md(client.link_name(model, str(row.field)))
                pair = edges.setdefault((ids[class_iri(model)], ids[target]), [])
                if label not in pair:
                    pair.append(label)
    drawn = sorted(
        f'{a} -->|"`{chr(10).join(sorted(labels))}`"| {b}'
        if merge
        else "\n".join(f'{a} -->|"`{label}`"| {b}' for label in sorted(labels))
        for (a, b), labels in edges.items()
    )
    body = "\n".join([*lines, *drawn, *_STYLE[: 1 + bool(drawn)]])
    return "```mermaid\n" + body + "\n```" if fenced else body


def path_diagram(
    client: Client,
    paths: pd.DataFrame,
    *,
    path: int | None = None,
    instances: bool = True,
    fenced: bool = True,
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
    prefixes = client.schema.get_prefixes()
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
            labels = (
                client.type_name(client.model(s)) if s else "Intermediate resource",
                client.type_name(client.model(o)) if o else "Intermediate resource",
            )
            keys = [s or f"path:{row['Path']}:{step}", o or f"path:{row['Path']}:{step + 1}"]
            s, o = s or "", o or ""
        for key, iri, label in zip(keys, (s, o), labels, strict=True):
            if key not in nodes:
                shown = ", ".join(_short(_curie(part, prefixes)) for part in iri.split(" | "))
                nodes[key] = (f"N{len(nodes)}", _node("", _specific(client, label), shown))
        source, target = keys[::-1] if backward else keys
        edges.add((nodes[source][0], str(row["Link"]), nodes[target][0]))
    lines = ["flowchart LR"]
    lines.extend(name + label for name, label in nodes.values())
    lines.extend(f'{s} -->|"{_text(label)}"| {o}' for s, label, o in sorted(edges))
    notice = "Partial view: more connections exist.\n\n" if paths.attrs.get("truncated") else ""
    body = "\n".join([*lines, *_STYLE[: 1 + bool(edges)]])
    if not fenced:
        return body + ("\n%% " + notice.strip() if notice else "")
    return notice + "```mermaid\n" + body + "\n```"


def link_diagram(
    client: Client, kind: str, links: Iterable[str], *, top: int = 4, fenced: bool = True
) -> str:
    """Draw one record type, the links of it that are named, and what each link reaches.

    A link is a field name or label ("source", "Is part of"); "^Is part of" is a link that
    points to the record type. The reached record types are those the mined schema counts for
    the link, in the record type's own namespace (WikiPathways' drawing classes are left out),
    with the number of statements; the *top* most counted per link, the others as one node.
    """
    from collections import defaultdict

    from rdfsolve.conversion import _link

    focus = class_iri(client.model(kind))
    parts = re.split(r"(?<=[#/])", focus)
    namespace = "".join(parts[:-1]) if len(parts) > 1 else ""
    prefixes = client.schema.get_prefixes()

    def name(iri: str) -> str:
        """Return the record type name of a class, else the class's short form."""
        if iri in ("Literal", "Resource", "BlankNode"):
            return {"Literal": "text or value", "Resource": "IRI", "BlankNode": "blank node"}[iri]
        try:
            return client.type_name(client.model(iri))
        except ValueError:
            return _curie(iri, prefixes)

    nodes = {focus: "C0"}
    lines = [_node("C0", name(focus), _curie(focus, prefixes))]
    edges = []
    for link in links:
        inverse = link.startswith("^")
        prop = _link(client, link.lstrip("^"), kind)
        counts: dict[str, int] = defaultdict(int)
        for pattern in client.schema.patterns:
            if pattern.property_uri != prop:
                continue
            here, there = (
                (pattern.object_class, pattern.subject_class)
                if inverse
                else (pattern.subject_class, pattern.object_class)
            )
            if here != focus or (there.startswith("http") and not there.startswith(namespace)):
                continue
            counts[there] += pattern.count or 0
        ranked = sorted(counts.items(), key=lambda item: -item[1])
        if len(ranked) > top:
            rest = ranked[top:]
            other = f"C{len(nodes)}"
            nodes[f"{link} other"] = other
            lines.append(
                _node(other, f"{len(rest)} other types", ", ".join(name(t) for t, _ in rest[:4]))
            )
            lines.append(f"style {other} fill:#f2f2f2,stroke:#9a9a9a,stroke-dasharray:3 3")
            label = _md(f"{link.lstrip('^')} ({sum(c for _, c in rest):,})")
            edges.append(
                f'{other} -.->|"`{label}`"| C0' if inverse else f'C0 -.->|"`{label}`"| {other}'
            )
            ranked = ranked[:top]
        for there, count in ranked:
            if there not in nodes:
                nodes[there] = f"C{len(nodes)}"
                lines.append(
                    _node(
                        nodes[there],
                        name(there),
                        _curie(there, prefixes) if there.startswith("http") else "",
                    )
                )
            label = _md(f"{link.lstrip('^')} ({count:,})")
            a, b = (nodes[there], nodes[focus]) if inverse else (nodes[focus], nodes[there])
            edges.append(f'{a} -->|"`{label}`"| {b}')
    body = "\n".join(
        [
            "flowchart LR",
            *lines,
            *edges,
            "classDef default fill:#eef4fb,stroke:#3b6ea8,stroke-width:1.5px,color:#1d2b3a",
            "style C0 fill:#fdf1d6,stroke:#b07a12,stroke-width:2px",
            "linkStyle default stroke:#7a8aa0,stroke-width:1.5px",
        ]
    )
    return f"```mermaid\n{body}\n```" if fenced else body


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


_STYLE = (
    "classDef default fill:#eef4fb,stroke:#3b6ea8,stroke-width:1.5px,color:#1d2b3a",
    "linkStyle default stroke:#7a8aa0,stroke-width:1.5px",
)


def _node(name: str, title: str, detail: str = "") -> str:
    """Draw a rounded node with the title in bold and the identifier on a second line."""
    text = f"**{_md(title)}**" + (f"\n{_md(detail)}" if detail else "")
    return f'{name}("`{text}`")'


def _md(value: str) -> str:
    """Escape text for a Mermaid markdown string."""
    return re.sub(r"[`\"*_<>]", lambda match: f"#{ord(match[0])};", value)


def _specific(client: Client, names: str) -> str:
    """Of the classes of a resource, name the one with the fewest instances (the most specific)."""
    counts = client.schema.about.class_entity_counts or {}

    def size(name: str) -> float:
        """Return the number of instances of a class (unknown: infinite)."""
        try:
            return counts.get(class_iri(client.model(name)), float("inf"))
        except ValueError:
            return float("inf")

    return min(names.split(" | "), key=size)


def _short(shown: str) -> str:
    """Keep the last two segments of a long IRI that has no registered prefix."""
    if not shown.startswith(("http://", "https://")) or len(shown) <= 48:
        return shown
    return "…/" + "/".join(shown.rstrip("/").split("/")[-2:])


def _curie(iri: str, prefixes: dict[str, str]) -> str:
    """Show an IRI as a CURIE of the schema prefixes, or of its registered namespace."""
    if not iri.startswith(("http://", "https://", "urn:")):
        return iri
    found = curie_from_prefixes(iri, prefixes)
    if found:
        return found[0]
    from rdfsolve.identifiers import curie

    return curie(iri)


def _text(value: str) -> str:
    return re.sub(r"[^\w .:/-]", lambda match: f"#{ord(match[0])};", value)
