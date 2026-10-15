"""Export RDF-config models from canonical patterns and observed examples."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from hashlib import sha256
from typing import TYPE_CHECKING

from rdflib import XSD, Graph, Literal

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern


def _name(text: str) -> str:
    """Use a label or IRI local part as an identifier."""
    words = re.findall(r"[A-Za-z0-9]+", text)
    name = "".join(word[0].upper() + word[1:] for word in words) or "Term"
    return "Term" + name if name[0].isdigit() else name


def to_rdfconfig(
    schema: MinedSchema,
    *,
    endpoint_url: str | None = None,
    endpoint_name: str | None = None,
    graph_uri: str | None = None,
) -> dict[str, str]:
    """Export model, prefix, and endpoint YAML.

    Missing examples stay absent. Cardinality is zero-or-more because
    mined patterns do not establish required fields or upper bounds.
    Bare blank nodes need a nested model, which these patterns do not
    supply; reject them instead of emitting an empty, unusable branch.
    Definitions and release identity use YAML comments, not new RDF-config keys.
    The subjects without a type (untyped patterns) are one subject, UntypedSubject, without an
    ``a`` (rdf:type) line: they have no class.
    """
    if schema.shapes is not None or schema.navigation is not None:
        import logging

        logging.getLogger(__name__).warning(
            "RDF-config does not retain source SHACL profiles or composed navigation. Keep canonical JSON."
        )
    if any(p.object_class == "BlankNode" for p in schema.patterns):
        raise ValueError(
            "RDF-config needs nested models for blank nodes. "
            "Use canonical JSON or SHACL for these patterns."
        )

    labels: dict[str, str] = {}
    for p in schema.patterns:
        for iri, label in (
            (p.subject_class, None if p.untyped_subject else p.subject_label),
            (p.property_uri, p.property_label),
            (p.object_class, p.object_label),
        ):
            if label:
                labels.setdefault(iri, label)
    for annotation in schema.enrichment.labels:
        labels.setdefault(annotation.term_iri, annotation.text.value)

    names: dict[str, str] = {}
    used: set[str] = set()
    for iri in sorted(set(schema.get_classes()) | set(schema.get_properties())):
        local = iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1].rsplit(":", 1)[-1]
        base = _name(labels.get(iri, local))
        candidate = base
        index = 0
        while candidate in used:
            index += 1
            suffix = sha256(f"{iri}:{index}".encode()).hexdigest()[:8]
            candidate = f"{base}{suffix}"
        used.add(candidate)
        names[iri] = candidate

    prefixes = {"xsd": str(XSD)}
    graph = Graph(bind_namespaces="none")
    for prefix, namespace in prefixes.items():
        graph.bind(prefix, namespace)

    groups: dict[str, dict[str, list[SchemaPattern]]] = defaultdict(lambda: defaultdict(list))
    untyped: dict[str, list[SchemaPattern]] = defaultdict(list)
    for pattern in schema.patterns:
        if pattern.untyped_subject:
            untyped[pattern.property_uri].append(pattern)
        else:
            groups[pattern.subject_class][pattern.property_uri].append(pattern)

    def property_lines(
        owner: str, by_property: dict[str, list[SchemaPattern]], untyped_owner: bool
    ) -> list[str]:
        """Write the properties of one subject, with their value classes and examples."""
        out = []
        for property_iri, patterns in sorted(by_property.items()):
            description = schema.enrichment.description(property_iri)
            if description:
                out.append("  # " + json.dumps(description, ensure_ascii=False))
            out.append(f"  - {json.dumps(f'<{property_iri}>*')}:")
            values: list[str | None] = []
            for pattern in patterns:
                if pattern.object_class not in ("Resource", "Literal"):
                    values.append(names[pattern.object_class])
            for example in schema.enrichment.examples:
                if (
                    example.untyped_subject != untyped_owner
                    or (not untyped_owner and example.subject_class != owner)
                    or example.property_uri != property_iri
                ):
                    continue
                term = example.value
                if term.kind == "uri":
                    values.append(f"<{term.value}>")
                elif term.kind == "literal":
                    literal = term.to_rdf()
                    if not term.datatype and not term.language:
                        literal = Literal(term.value, datatype=XSD.string, normalize=False)
                    values.append(literal.n3(namespace_manager=graph.namespace_manager))
            # RDF-config permits an object name without an example.
            if not values:
                values.append(None)
            for index, value in enumerate(dict.fromkeys(values), 1):
                variable = f"{subject_names[owner]}_{names[property_iri]}_{index}".lower()
                out.append(f"    - {variable}: {json.dumps(value, ensure_ascii=False)}")
        return out

    subject_names = dict(names)

    lines = ["# Observed patterns; examples are convenience samples."]
    if schema.about.schema_version:
        lines.append("# Source version: " + json.dumps(schema.about.schema_version))
    for class_iri in schema.get_classes():
        description = schema.enrichment.description(class_iri)
        if description:
            lines.append("# " + json.dumps(description, ensure_ascii=False))
        examples = [
            f"<{term.value}>"
            for term in schema.enrichment.class_examples.get(class_iri, [])
            if term.kind == "uri"
        ]
        subject = " ".join([names[class_iri], *dict.fromkeys(examples)])
        lines.extend([f"- {json.dumps(subject)}:", f"  - a: {json.dumps(f'<{class_iri}>')}"])
        lines.extend(property_lines(class_iri, groups[class_iri], False))
    if untyped:
        name = "UntypedSubject"
        while name in used:
            name += "_"
        used.add(name)
        subject_names[""] = name
        examples = [
            f"<{e.subject.value}>"
            for e in schema.enrichment.examples
            if e.untyped_subject and e.subject.kind == "uri"
        ][:1]
        lines.append(
            "# Subjects without rdf:type (rdfsolve untyped patterns): not a class, no a: line."
        )
        lines.append(f"- {json.dumps(' '.join([name, *examples]))}:")
        lines.extend(property_lines("", untyped, True))

    endpoint_url = endpoint_url or schema.about.endpoint
    endpoint: dict[str, list[object]] = {}
    if endpoint_url:
        settings: list[object] = [endpoint_url]
        graph_uris = [graph_uri] if graph_uri else schema.about.graph_uris
        if graph_uris:
            settings.append({"graph": graph_uris})
        endpoint[endpoint_name or "endpoint"] = settings

    # JSON is valid YAML and keeps endpoint names and IRIs safely quoted.
    return {
        "model": "\n".join(lines) + "\n",
        "prefix": json.dumps({p: f"<{iri}>" for p, iri in sorted(prefixes.items())}, indent=2)
        + "\n",
        "endpoint": json.dumps(endpoint, indent=2) + "\n" if endpoint else "",
    }
