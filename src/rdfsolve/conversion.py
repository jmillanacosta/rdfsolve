"""Conversion profiles: rules that turn a source's RDF into a target property-graph schema.

A rule is a path rule. For each instance of a focus class (WikiPathways' wp:Catalysis), it
links the nodes that a subject path reaches (wp:source: the enzyme) to the nodes that an object
path reaches (wp:target: the reaction), or to a constant (a Biolink category), with the target's
predicate (biolink:catalyzes). Paths are SHACL property paths, written in SPARQL path syntax:
sequences and inverses reach further (``<wp#target>/<wp#target>`` from a catalysis is the
reaction's product). Class filters keep the ends of one class, and *pairs* links each pair of
the nodes one path reaches (the proteins of a complex).

A rule is written as SHACL (a node shape with a sh:TripleRule, SHACL Advanced Features) and
compiled to a SPARQL CONSTRUCT that runs on one endpoint: no federation. A profile is the rules
for one source and one target schema version, with the provenance of both (property_graph.
provenance). Joins across sources are not rules: they go through the SSSOM resolution.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyoxigraph as ox
    from rdflib import Graph
    from rdflib.term import Node

    from rdfsolve.client.api import Client
    from rdfsolve.schema_models.paths import PropertyPath

__all__ = ["Profile", "Rule"]


def _path(text: str) -> PropertyPath:
    """Return a property path from SPARQL path text, or from a plain IRI."""
    from rdfsolve.schema_models.paths import PropertyPath

    text = text.strip()
    if text.startswith(("http://", "https://", "urn:")):
        text = f"<{text}>"
    return PropertyPath.from_sparql(text)


@dataclass(frozen=True)
class Rule:
    """For each instance of *focus*, link what *subject* reaches to what *object* reaches.

    *subject* and *object* are paths from the focus node (SPARQL path syntax, or an IRI); None
    is the focus node itself. *value* is a constant object instead of a path (a category).
    *subject_class* and *object_class* keep the ends of these classes. *pairs* links each pair
    of the nodes that *subject* reaches (once per pair), with *object* unused. *unless_class*
    skips focus nodes of this class (a wp:GeneProduct rule skips the ones that are also
    wp:Protein, which a more specific rule takes).
    """

    focus: str
    predicate: str
    subject: str | None = None
    object: str | None = None
    value: str | None = None
    subject_class: str | None = None
    object_class: str | None = None
    pairs: bool = False
    name: str | None = None
    unless_class: str | None = None

    def to_construct(self, scope: str = "") -> str:
        """Return the SPARQL CONSTRUCT of the rule; *scope* is a pattern on ?x (the focus)."""
        from rdfsolve.schema_models.exporters.paths import path_to_sparql

        lines = [f"?x a <{self.focus}> ."]
        if self.unless_class:
            lines.append(f"FILTER NOT EXISTS {{ ?x a <{self.unless_class}> }}")
        if scope:
            lines.append(scope.strip())
        if self.subject is None:
            subject = "?x"
        else:
            subject = "?s"
            lines.append(f"?x {path_to_sparql(_path(self.subject))} ?s .")
        if self.pairs:
            if self.subject is None:
                raise ValueError("A rule of pairs needs a subject path")
            obj = "?o"
            lines.append(f"?x {path_to_sparql(_path(self.subject))} ?o .")
            lines.append("FILTER(STR(?s) < STR(?o))")
            object_class = self.subject_class
        elif self.value is not None:
            obj = f"<{self.value}>"
            object_class = None
        elif self.object is None:
            obj = "?x"
            object_class = None
        else:
            obj = "?o"
            lines.append(f"?x {path_to_sparql(_path(self.object))} ?o .")
            object_class = self.object_class
        if self.subject_class and subject != "?x":
            lines.append(f"?s a <{self.subject_class}> .")
        if object_class and obj == "?o":
            lines.append(f"?o a <{object_class}> .")
        body = "\n  ".join(lines)
        return f"CONSTRUCT {{ {subject} <{self.predicate}> {obj} }}\nWHERE {{\n  {body}\n}}"

    def add_shacl(self, graph: Graph, shape: Node) -> None:
        """Add the rule to *shape* as a sh:TripleRule (subject and object node expressions)."""
        from rdflib import RDF, SH, BNode, URIRef

        from rdfsolve.schema_models.exporters.paths import path_to_rdf

        def nodes(path: str | None, cls: str | None) -> Node:
            """Return the node expression of the nodes a path reaches, filtered by a class."""
            if path is None:
                return SH.this
            expression = BNode()
            graph.add((expression, SH.path, path_to_rdf(_path(path), graph)))
            if not cls:
                return expression
            filtered, condition = BNode(), BNode()
            graph.add((condition, SH["class"], URIRef(cls)))
            graph.add((filtered, SH.filterShape, condition))
            graph.add((filtered, SH.nodes, expression))
            return filtered

        rule = BNode()
        graph.add((shape, SH.rule, rule))
        graph.add((rule, RDF.type, SH.TripleRule))
        if self.unless_class:
            condition, negated = BNode(), BNode()
            graph.add((negated, SH["class"], URIRef(self.unless_class)))
            graph.add((condition, SH["not"], negated))
            graph.add((rule, SH.condition, condition))
        graph.add((rule, SH.subject, nodes(self.subject, self.subject_class)))
        graph.add((rule, SH.predicate, URIRef(self.predicate)))
        if self.pairs:
            graph.add((rule, SH.object, nodes(self.subject, self.subject_class)))
        elif self.value is not None:
            graph.add((rule, SH.object, URIRef(self.value)))
        else:
            graph.add((rule, SH.object, nodes(self.object, self.object_class)))


@dataclass
class Profile:
    """The rules that turn one source's RDF into one target schema (at a version)."""

    name: str
    rules: list[Rule]
    target: str | None = None
    target_version: str | None = None
    provenance: Mapping[str, str | None] = field(default_factory=dict)

    def to_shacl(self) -> str:
        """Return the profile as SHACL: a node shape per focus class with its rules, in Turtle.

        The provenance (source release, target schema and version, the tool) is written as
        comments and as triples on the profile (prov:wasDerivedFrom, pav:createdWith,
        dcterms:conformsTo), as Fold.to_shacl does.
        """
        from rdflib import RDF, SH, BNode, Graph, Literal, Namespace, URIRef

        from rdfsolve.config import mint

        prov, pav = Namespace("http://www.w3.org/ns/prov#"), Namespace("http://purl.org/pav/")
        dcterms = Namespace("http://purl.org/dc/terms/")
        graph = Graph()
        for prefix, namespace in (("sh", SH), ("prov", prov), ("pav", pav), ("dcterms", dcterms)):
            graph.bind(prefix, namespace)
        info = {
            k: v
            for k, v in {
                **self.provenance,
                "target": self.target,
                "target version": self.target_version,
            }.items()
            if v
        }
        profile = URIRef(mint("profile", self.name))
        graph.add((profile, RDF.type, URIRef("http://www.w3.org/ns/shacl#ShapesGraph")))
        if info.get("source description"):
            graph.add((profile, prov.wasDerivedFrom, URIRef(str(info["source description"]))))
        if info.get("generated with"):
            graph.add((profile, pav.createdWith, Literal(info["generated with"])))
        if info.get("target"):
            target = BNode()
            graph.add((profile, dcterms.conformsTo, target))
            graph.add((target, dcterms.title, Literal(info["target"])))
            if info.get("target version"):
                graph.add((target, pav.version, Literal(info["target version"])))
        shapes: dict[str, URIRef] = {}
        for rule in self.rules:
            if rule.focus not in shapes:
                shape = URIRef(
                    mint("profile", self.name, rule.focus.rsplit("#", 1)[-1].rsplit("/", 1)[-1])
                )
                shapes[rule.focus] = shape
                graph.add((shape, RDF.type, SH.NodeShape))
                graph.add((shape, SH.targetClass, URIRef(rule.focus)))
                graph.add((shape, dcterms.isPartOf, profile))
            rule.add_shacl(graph, shapes[rule.focus])
        comments = "".join(f"# {k}: {v}\n" for k, v in info.items())
        return comments + graph.serialize(format="turtle")

    def constructs(self, scope: str = "") -> list[str]:
        """Return the CONSTRUCT of each rule, with *scope* (a pattern on ?x, the focus)."""
        return [rule.to_construct(scope) for rule in self.rules]

    def run(self, client: Client, scope: str = "") -> ox.Dataset:
        """Run each rule's CONSTRUCT on *client*'s endpoint (recorded in its session); return
        all the statements they give.
        """
        import pyoxigraph as ox

        out = ox.Dataset()
        for query in self.constructs(scope):
            for quad in client.construct(query):
                out.add(quad)
        return out

    def counts(self, statements: Iterable[ox.Quad]) -> dict[str, int]:
        """Return how many statements each predicate of the profile has in *statements*."""
        from collections import Counter

        wanted = {rule.predicate for rule in self.rules}
        found = Counter(q.predicate.value for q in statements if q.predicate.value in wanted)
        return {p: found.get(p, 0) for p in sorted(wanted)}


def rules_of(folds: Sequence[Any]) -> list[Rule]:
    """Return the path rules of property-graph folds (Fold): one-step paths, or pairs."""
    out = []
    for fold in folds:
        if fold.members is not None:
            out.append(
                Rule(
                    fold.cls,
                    fold.relation(),
                    subject=fold.members,
                    subject_class=fold.among,
                    pairs=True,
                    name=fold.name,
                )
            )
        else:
            out.append(
                Rule(
                    fold.cls,
                    fold.relation(),
                    subject=fold.source,
                    object=fold.target,
                    name=fold.name,
                )
            )
    return out
