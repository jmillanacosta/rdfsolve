"""A SHACL shapes graph as a target model: its shapes' classes are kinds, its property shapes
relations between them.

A node shape's target class (sh:targetClass, or the shape itself when it is a class) is a kind,
named by its label (rdfs:label, of the shape or the class) or its local name. A property shape
(sh:path an IRI) is a relation from the shape's kind to the kinds it allows (sh:class, sh:node,
alone or in sh:or); one whose values are literals (sh:datatype, sh:nodeKind sh:Literal) relates no
two kinds, and one on a naming property names its node. sh:in lists are value lists. Classes
related by rdfs:subClassOf in the graph give the hierarchy. Shapes marked sh:deactivated still
describe the model (a mined schema's shapes are observations, not constraints), so they count.
Paths other than one IRI are not relations; they are counted in skipped.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from rdfsolve.target_model import KindInfo, RelationInfo, TargetModel, ValueInfo

__all__ = ["Shacl"]

SH = "http://www.w3.org/ns/shacl#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
LITERAL = "literal"  # the range of a relation whose values are literals (not a kind)
NAMING = (
    RDFS + "label",
    "http://schema.org/name",
    "https://schema.org/name",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
    "http://purl.org/dc/terms/title",
)
DEFINITIONS = (
    RDFS + "comment",
    SH + "description",
    "http://www.w3.org/2004/02/skos/core#definition",
    "http://purl.obolibrary.org/obo/IAO_0000115",
)


class Shacl(TargetModel):
    """The kinds and relations a SHACL shapes graph describes."""

    def __init__(self, graph: Any, name: str = "", version: str = "") -> None:
        """Read the shapes of an rdflib graph."""
        from rdflib import URIRef

        self.graph = graph
        self.name, self.version = name or "shapes", version
        g = graph

        def sh(local: str) -> Any:
            """Return a SHACL term."""
            return URIRef(SH + local)

        def text(node: Any, predicates: tuple[str, ...]) -> str:
            """Return the first literal of these predicates."""
            for p in predicates:
                for value in g.objects(node, URIRef(p)):
                    return str(value)
            return ""

        def word_name(iri: str, *labelled: Any) -> str:
            """Return the label of the first node that has one, else the IRI's local name in words."""
            from rdfsolve.naming import label

            for node in labelled:
                found = text(node, (RDFS + "label", SH + "name"))
                if found:  # a label in words is kept; one written as an identifier is split
                    return found if " " in found.strip() else label(found)
            return label(iri.rstrip("/#").rsplit("#", 1)[-1].rsplit("/", 1)[-1])

        # Kinds: each shape's target classes (or the shape, when it is a class itself)
        shapes: dict[Any, list[str]] = {}
        for shape in set(g.subjects(URIRef(RDF_TYPE), sh("NodeShape"))) | set(
            g.subjects(sh("targetClass"), None)
        ):
            targets = [str(c) for c in g.objects(shape, sh("targetClass"))]
            if not targets and (shape, URIRef(RDF_TYPE), URIRef(RDFS + "Class")) in g:
                targets = [str(shape)]
            if targets:
                shapes[shape] = targets
        names: dict[str, str] = {}
        definitions: dict[str, str] = {}
        for shape, classes in shapes.items():
            for c in classes:
                names.setdefault(c, word_name(c, URIRef(c), shape))
                definitions.setdefault(c, text(URIRef(c), DEFINITIONS) or text(shape, DEFINITIONS))
        node_classes = {str(s): cs for s, cs in shapes.items()}  # sh:node targets

        def allowed(node: Any) -> tuple[list[str], bool]:
            """Return the classes a property (or an sh:or member) allows, and whether it is literal."""
            from rdflib.collection import Collection

            classes = [str(c) for c in g.objects(node, sh("class"))]
            classes += [
                c for n in g.objects(node, sh("node")) for c in node_classes.get(str(n), [])
            ]
            literal = (
                any(True for _ in g.objects(node, sh("datatype")))
                or (node, sh("nodeKind"), sh("Literal")) in g
            )
            for alternatives in g.objects(node, sh("or")):
                for member in Collection(g, alternatives):
                    more, lit = allowed(member)
                    classes += more
                    literal = literal or lit
            return classes, literal

        # Relations: one per property IRI, with the kind pairs its shapes allow
        pairs: dict[str, set[tuple[str, str]]] = defaultdict(set)
        literal_of: dict[str, bool] = {}
        rel_names: dict[str, str] = {}
        rel_definitions: dict[str, str] = {}
        values: dict[str, list[str]] = {}
        self.skipped = 0
        for shape, classes in shapes.items():
            for prop in g.objects(shape, sh("property")):
                path = next(iter(g.objects(prop, sh("path"))), None)
                if path is None or not isinstance(path, URIRef):
                    self.skipped += 1
                    continue
                iri = str(path)
                rel_names.setdefault(iri, word_name(iri, path, prop).lower())
                rel_definitions.setdefault(iri, text(prop, DEFINITIONS) or text(path, DEFINITIONS))
                reached, literal = allowed(prop)
                literal_of[iri] = literal_of.get(iri, True) and literal and not reached
                for c in classes:
                    for r in reached:  # a class allowed but given no shape of its own is a kind too
                        names.setdefault(r, word_name(r, URIRef(r)))
                        pairs[iri].add((c, r))
                for listed in g.objects(prop, sh("in")):
                    from rdflib.collection import Collection

                    values[iri] = [
                        str(v).rsplit("#", 1)[-1].rsplit("/", 1)[-1] for v in Collection(g, listed)
                    ]
        self._names = names
        parents = {
            c: tuple(
                names[str(p)]
                for p in g.objects(URIRef(c), URIRef(RDFS + "subClassOf"))
                if str(p) in names
            )
            for c in names
        }
        self._kinds = [
            KindInfo(names[c], c, parents=parents[c], definition=definitions.get(c, ""))
            for c in sorted(names, key=lambda c: names[c])
        ]
        self._relations = []
        for iri in sorted(rel_names, key=lambda i: rel_names[i]):
            between = tuple(sorted({(names[a], names[b]) for a, b in pairs.get(iri, ())}))
            starts, ends = {a for a, _ in between}, {b for _, b in between}
            enumeration = rel_names[iri] if iri in values else None
            self._relations.append(
                RelationInfo(
                    rel_names[iri],
                    iri,
                    definition=rel_definitions.get(iri, ""),
                    domain=next(iter(starts)) if len(starts) == 1 else None,
                    range=enumeration
                    or (
                        LITERAL
                        if literal_of.get(iri)
                        else (next(iter(ends)) if len(ends) == 1 else None)
                    ),
                    pairs=between,
                    enumeration=enumeration,
                )
            )
        self._values = [ValueInfo(v, rel_names[iri]) for iri, vs in values.items() for v in vs]
        self._naming = next((r.name for r in self._relations if r.iri in NAMING), None)
        caps = {"iris", "directed"}
        if any(k.parents for k in self._kinds):
            caps.add("hierarchy")
        if any(k.definition for k in self._kinds) or any(r.definition for r in self._relations):
            caps.add("definitions")
        self.capabilities = frozenset(caps)
        namespaces = Counter(
            c.rsplit("#", 1)[0] + "#" if "#" in c else c.rsplit("/", 1)[0] + "/" for c in names
        )
        self.base = namespaces.most_common(1)[0][0] if namespaces else ""
        self.prefix = next(
            (
                str(p)
                for p, ns in g.namespace_manager.namespaces()
                if str(ns) == self.base and str(p)
            ),
            "shapes",
        )

    @classmethod
    def read(cls, path: str | Path, name: str | None = None, format: str | None = None) -> Shacl:
        """Read a SHACL shapes file (any RDF format rdflib reads; the format from the extension)."""
        from rdflib import Graph

        graph = Graph().parse(str(path), format=format)
        return cls(graph, name or Path(path).name.split(".")[0])

    def kinds(self) -> list[KindInfo]:
        """Return the kinds: the shapes' classes."""
        return list(self._kinds)

    def relations(self) -> list[RelationInfo]:
        """Return the relations: one per property path, between the kinds its shapes allow."""
        return list(self._relations)

    def values(self) -> list[ValueInfo]:
        """Return the values of the sh:in lists."""
        return list(self._values)

    def kind_ancestors(self, name: str) -> list[str]:
        """Return a kind's ancestors along rdfs:subClassOf, nearest first."""
        by_name = {k.name: k for k in self._kinds}
        out: list[str] = []
        todo = list(by_name[name].parents) if name in by_name else []
        while todo:
            parent = todo.pop(0)
            if parent not in out:
                out.append(parent)
                todo += list(by_name[parent].parents) if parent in by_name else []
        return out

    def naming_relation(self) -> str | None:
        """Return the relation on a naming property (rdfs:label, schema:name, …), when one has a shape."""
        return self._naming
