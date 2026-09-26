"""Show the schema of one source as short text lines for a language model."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from difflib import get_close_matches
from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.enrichment import RdfTerm
    from rdfsolve.schema_models.pattern import SchemaPattern

# Added when the schema does not use the prefix or the namespace.
COMMON_PREFIXES = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "schema": "http://schema.org/",
    "void": "http://rdfs.org/ns/void#",
    "pav": "http://purl.org/pav/",
    "prov": "http://www.w3.org/ns/prov#",
    "dcat": "http://www.w3.org/ns/dcat#",
}
_LOCAL = re.compile(r"[\w-](?:[\w.-]*[\w-])?")
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
# A route is a list of steps: (from class, property, to class, forward, count).
Step = tuple[str, str, str, bool, int]


@lru_cache(maxsize=1024)
def registered_namespace(prefix: str) -> str | None:
    """Give the RDF namespace that Bioregistry records for a prefix, if any."""
    import bioregistry

    resource = bioregistry.get_resource(prefix)
    return resource.get_rdf_uri_prefix() if resource else None


def _key(text: str) -> str:
    """Remove case, spaces and punctuation from a name."""
    return re.sub(r"[\W_]+", "", text).casefold()


def _local(iri: str) -> str:
    """Return the part of an IRI after the last slash or hash."""
    return re.split(r"[#/]", iri.rstrip("/#"))[-1]


class SchemaView:
    """The classes, properties, counts and example values of one mined schema."""

    def __init__(self, schema: MinedSchema) -> None:
        """Index the patterns, labels and examples of the schema."""
        self.schema = schema
        self.prefixes = dict(schema.get_prefixes())
        for prefix, namespace in COMMON_PREFIXES.items():
            if prefix not in self.prefixes and namespace not in self.prefixes.values():
                self.prefixes[prefix] = namespace
        self._namespaces = sorted(
            ((namespace, prefix) for prefix, namespace in self.prefixes.items()),
            key=lambda item: (-len(item[0]), item[1]),
        )
        self.counts = dict(schema.about.class_entity_counts or {})
        self.classes = set(schema.get_classes())
        self.properties = set(schema.get_properties())
        self.labels: dict[str, str] = {}
        self.outgoing: dict[str, list[SchemaPattern]] = defaultdict(list)
        self.incoming: dict[str, list[SchemaPattern]] = defaultdict(list)
        self.uses: dict[str, list[SchemaPattern]] = defaultdict(list)
        for pattern in schema.patterns:
            for iri, label in (
                (pattern.subject_class, pattern.subject_label),
                (pattern.property_uri, pattern.property_label),
                (pattern.object_class, pattern.object_label),
            ):
                if label and "://" in iri and _key(label) != _key(_local(iri)):
                    self.labels.setdefault(iri, label)
            self.outgoing[pattern.subject_class].append(pattern)
            self.uses[pattern.property_uri].append(pattern)
            if pattern.object_class in self.classes:
                self.incoming[pattern.object_class].append(pattern)
        self.examples: dict[tuple[str, str], RdfTerm] = {}
        if schema.enrichment is not None:
            for note in schema.enrichment.labels:
                if _key(note.text.value) != _key(_local(note.term_iri)):
                    self.labels[note.term_iri] = note.text.value
            for example in schema.enrichment.examples:
                key = (example.subject_class, example.property_uri)
                self.examples.setdefault(key, example.value)

    def curie(self, iri: str) -> str:
        """Write an IRI as a CURIE when a prefix is known, else in angle brackets."""
        for namespace, prefix in self._namespaces:
            if iri.startswith(namespace) and _LOCAL.fullmatch(iri[len(namespace) :]):
                return f"{prefix}:{iri[len(namespace) :]}"
        return f"<{iri}>"

    def named(self, iri: str) -> str:
        """Write an IRI as a CURIE, with its label when the label adds information."""
        label = self.labels.get(iri)
        return (
            f"{self.curie(iri)} {json.dumps(label, ensure_ascii=False)}"
            if label
            else self.curie(iri)
        )

    def term(self, term: RdfTerm) -> str:
        """Write one example value in SPARQL form, shortened."""
        if term.kind == "uri":
            return self.curie(term.value)
        if term.kind != "literal":
            return "[blank node]"
        value = " ".join(term.value.split())
        text = json.dumps(value if len(value) <= 60 else value[:60] + "…", ensure_ascii=False)
        if term.language:
            return f"{text}@{term.language}"
        if term.datatype and not term.datatype.endswith("#string"):
            return f"{text}^^{self.curie(term.datatype)}"
        return text

    def expand(self, text: str) -> str | None:
        """Read a CURIE, an IRI, or an IRI in angle brackets."""
        text = text.strip()
        if text.startswith("<") and text.endswith(">"):
            return text[1:-1]
        if re.match(r"[a-z][a-z0-9+.-]*://", text, re.IGNORECASE):
            return text
        prefix, colon, local = text.partition(":")
        if colon and prefix in self.prefixes:
            return self.prefixes[prefix] + local
        return None

    def match_classes(self, text: str) -> list[str]:
        """Find the classes that a CURIE, an IRI, a label or a local name gives."""
        iri = self.expand(text)
        if iri is not None:
            return [iri] if iri in self.classes else []
        key = _key(text)
        return sorted(
            c
            for c in self.classes
            if key in {_key(self.labels.get(c, "")), _key(_local(c)), _key(self.curie(c))}
        )

    def similar(self, text: str, terms: Iterable[str]) -> list[str]:
        """Return up to three schema terms with a name near the text."""
        names = {}
        for iri in terms:
            names[self.curie(iri)] = iri
            if iri in self.labels:
                names[self.labels[iri]] = iri
        close = get_close_matches(text, list(names), n=6, cutoff=0.6)
        return list(dict.fromkeys(self.curie(names[name]) for name in close))[:3]

    def overview(self, limit: int = 40) -> str:
        """Describe the source, its largest classes and its prefixes."""
        about = self.schema.about
        lines = [
            f"Source: {about.dataset_name or 'RDF data'}"
            + (f" ({about.description})" if about.description else "")
        ]
        classes = sorted(self.classes, key=lambda c: (-self.counts.get(c, 0), self.curie(c)))
        lines.append(f"Classes ({len(classes)}), with number of instances:")
        lines += [
            f"  {self.named(c)}" + (f" {self.counts[c]}" if c in self.counts else "")
            for c in classes[:limit]
        ]
        if len(classes) > limit:
            lines.append(f"  … {len(classes) - limit} more. Use schema with search words.")
        own = self.schema.get_prefixes()
        lines.append("Prefixes: " + " ".join(f"{p}: <{ns}>" for p, ns in sorted(own.items())))
        return "\n".join(lines)

    def _value(self, pattern: SchemaPattern) -> str:
        """Write the value type of a pattern, with its count."""
        if pattern.object_class == "Literal":
            kind = "literal" + (f" {self.curie(pattern.datatype)}" if pattern.datatype else "")
        elif pattern.object_class == "Resource":
            kind = "IRI with no class"
        elif "://" in pattern.object_class:
            kind = self.named(pattern.object_class)
        else:
            kind = pattern.object_class.lower()
        return kind + (f" [{pattern.count}]" if pattern.count is not None else "")

    def card(self, iri: str, limit: int = 30) -> str:
        """Describe one class: its properties, value types, counts and links to it."""
        count = self.counts.get(iri)
        lines = [self.named(iri) + (f", instances: {count}" if count is not None else "")]
        groups: dict[str, list[SchemaPattern]] = defaultdict(list)
        for pattern in self.outgoing.get(iri, []):
            if pattern.property_uri != RDF_TYPE:
                groups[pattern.property_uri].append(pattern)
        ranked = sorted(groups.items(), key=lambda item: -max(p.count or 0 for p in item[1]))
        if ranked:
            lines.append(f"  Properties (?x a {self.curie(iri)} ; property value):")
        for prop, patterns in ranked[:limit]:
            patterns.sort(key=lambda p: -(p.count or 0))
            example = self.examples.get((iri, prop))
            lines.append(
                f"    {self.named(prop)} → "
                + ", ".join(self._value(p) for p in patterns[:4])
                + (f"  e.g. {self.term(example)}" if example else "")
            )
        if len(ranked) > limit:
            lines.append(f"    … {len(ranked) - limit} more properties.")
        links = sorted(self.incoming.get(iri, []), key=lambda p: -(p.count or 0))
        if links:
            lines.append(f"  Links to it (?y property ?x, with ?x a {self.curie(iri)}):")
        lines += [
            f"    {self.curie(p.subject_class)} {self.curie(p.property_uri)}"
            + (f" [{p.count}]" if p.count is not None else "")
            for p in links[:limit]
        ]
        return "\n".join(lines)

    def search(self, words: Sequence[str], limit: int = 12) -> str:
        """Find classes and properties whose names contain the words."""
        queries = [text.casefold().split() for text in words if text.strip()]

        def score(iri: str) -> int:
            """Count the queries that all have their words in the names of a term."""
            names = " ".join([self.labels.get(iri, ""), self.curie(iri), iri]).casefold()
            return sum(all(word in names for word in query) for query in queries)

        def ranked(terms: Iterable[str], weight: dict[str, int]) -> list[str]:
            """Order matching terms by score and size."""
            scored = [(score(iri), weight.get(iri, 0), iri) for iri in terms]
            return [iri for s, _, iri in sorted(scored, key=lambda i: (-i[0], -i[1])) if s][:limit]

        sizes = {p: sum(x.count or 0 for x in patterns) for p, patterns in self.uses.items()}
        classes = ranked(self.classes, self.counts)
        properties = ranked(self.properties - {RDF_TYPE}, sizes)
        lines = ["Classes:"] if classes else ["No class name matches."]
        lines += [
            f"  {self.named(c)}" + (f" {self.counts[c]}" if c in self.counts else "")
            for c in classes
        ]
        lines += ["Properties (subject class → value):"] if properties else []
        for prop in properties:
            uses = sorted(self.uses[prop], key=lambda p: -(p.count or 0))[:3]
            lines.append(
                f"  {self.named(prop)}: "
                + "; ".join(f"{self.curie(p.subject_class)} → {self._value(p)}" for p in uses)
            )
        if not properties:
            lines.append("No property name matches.")
        return "\n".join(lines)

    def routes(
        self, source: str, target: str, max_hops: int = 3, limit: int = 8
    ) -> list[list[list[Step]]]:
        """Find the shortest chains of properties from one class to another.

        Routes with the same properties and other classes on the middle nodes are
        given as one group.
        """
        edges: dict[str, list[Step]] = defaultdict(list)
        for pattern in self.schema.patterns:
            if pattern.object_class in self.classes and pattern.property_uri != RDF_TYPE:
                a, b, count = pattern.subject_class, pattern.object_class, pattern.count or 0
                edges[a].append((a, pattern.property_uri, b, True, count))
                if a != b:
                    edges[b].append((b, pattern.property_uri, a, False, count))
        for steps in edges.values():
            steps.sort(key=lambda step: -step[4])
        groups: dict[tuple[tuple[str, bool], ...], list[list[Step]]] = {}
        frontier: list[list[Step]] = [[]]
        for _ in range(max_hops):
            following = []
            for route in frontier:
                here = route[-1][2] if route else source
                seen = {source, *(step[2] for step in route)}
                for step in edges.get(here, []):
                    if step[2] == target:
                        chain = tuple((s[1], s[3]) for s in (*route, step))
                        groups.setdefault(chain, []).append([*route, step])
                    elif step[2] not in seen:
                        following.append([*route, step])
            if len(groups) >= limit:
                break
            frontier = following[:5000]
        return list(groups.values())[:limit]

    def route_text(self, group: list[list[Step]]) -> str:
        """Write a group of routes as SPARQL triple patterns from ?source to ?target."""
        route = group[0]
        names = ["?source", *(f"?n{i}" for i in range(1, len(route))), "?target"]
        triples = []
        for i, (_, prop, _, forward, _) in enumerate(route):
            a, b = names[i], names[i + 1]
            triples.append(
                f"{a} {self.curie(prop)} {b} ." if forward else f"{b} {self.curie(prop)} {a} ."
            )
        middle = ", ".join(
            f"{names[i + 1]} a " + " or ".join(dict.fromkeys(self.curie(r[i][2]) for r in group))
            for i in range(len(route) - 1)
        )
        counts = ", ".join(str(step[4]) for step in route)
        note = f"{middle}; counts {counts}" if middle else f"count {counts}"
        return " ".join(triples) + f"  ({note})"
