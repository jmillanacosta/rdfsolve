"""Draw selected generated models with their RDF labels and identifiers."""

from __future__ import annotations

import re
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
    lines.extend(sorted(block for block in blocks if block.strip().startswith("class ")))
    for model, name in ids.items():
        label = _text(client.type_name(model))
        iri = _text(str(getattr(model, "rdf_class_iri", "")))
        lines.append(f'class {name}["{label}<br/>{iri}"]')
    edges = set()
    for model, source in ids.items():
        for row in client.links(model).itertuples(index=False):
            target = client.model(str(row.target))
            if target in ids and target is not model:
                label = _text(client.link_name(model, str(row.field)))
                edges.add(f"{source} --> {ids[target]} : {label}")
    lines.extend(sorted(edges))
    return "```mermaid\n" + "\n".join(lines) + "\n```"


def _text(value: str) -> str:
    return re.sub(r"[^\w .:/-]", lambda match: f"#{ord(match[0])};", value)
