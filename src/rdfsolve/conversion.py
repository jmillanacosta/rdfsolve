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

import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyoxigraph as ox
    from rdflib import Graph
    from rdflib.term import Node

    from rdfsolve.client.api import Client
    from rdfsolve.schema_models.paths import PropertyPath

__all__ = ["Biolink", "Profile", "Query", "Rule", "to_kgx", "within", "write_query"]


def _expand(text: str, client: Client | None = None) -> str:
    """Return the IRI of "a" (rdf:type), of a CURIE or of an IRI: the prefixes of the client's
    mined schema first, then the Biolink Model's own prefix, then Bioregistry.
    """
    import bioregistry

    if text == "a":
        return _TYPE
    if text.startswith(("http://", "https://", "urn:")):
        return text
    prefix, sep, local = text.partition(":")
    if not sep:
        raise ValueError(f"{text!r}: write an IRI, a CURIE or 'a'")
    known = client.schema.get_prefixes() if client is not None else {}
    base = known.get(prefix) or (Biolink.BASE if prefix == "biolink" else None)
    base = base or bioregistry.get_uri_prefix(prefix)
    if not base:
        raise ValueError(f"{text!r}: unknown prefix {prefix!r}")
    return base + local


def _link(client: Client, name: str, focus: str) -> str:
    """Return the property IRI of a link by field name, else by label: of the focus's record
    type first, else the one property that the record types name so.
    """

    def key(text: str) -> str:
        """Return a name compared without case, spaces or underscores."""
        return re.sub(r"[\\s_]+", "", text).lower()

    def found(table: Any) -> set[str]:
        """Return the properties a field table names so: by field name, else by label."""
        for column in ("field", "label"):
            hits = {
                str(r.property)
                for r in table.itertuples()
                if key(str(getattr(r, column))) == key(name)
            }
            if hits:
                return hits
        return set()

    own = found(client.fields(focus))
    if len(own) == 1:
        return next(iter(own))
    anywhere: set[str] = set()
    for model in client.models.values():
        anywhere |= found(client.fields(model))
    if len(anywhere) != 1:
        raise ValueError(
            f"Link {name!r}: {'no' if not anywhere else 'several'} properties {sorted(anywhere)}"
        )
    return next(iter(anywhere))


def _literal(text: str) -> str:
    """Return a plain literal in SPARQL syntax."""
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


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
    of the nodes that *subject* reaches (once per pair), with *object* unused. *unless_classes*
    skips focus nodes of any of these classes (those a more specific rule takes).
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
    unless_classes: tuple[str, ...] = ()
    literal: bool = False  # *value* is a literal (a Biolink qualifier value), not an IRI
    # The names of the ends in a query written from the rule (write_query): ?enzyme, ?reaction.
    subject_as: str | None = field(default=None, compare=False)
    object_as: str | None = field(default=None, compare=False)
    # The rest of the query's pattern: (path from the focus, class or None) that must exist for
    # the statement to be made, as in a CONSTRUCT whose WHERE must match as a whole.
    requires: tuple[tuple[str, str | None], ...] = field(default=(), compare=False)

    @classmethod
    def between(
        cls,
        client: Client,
        focus: str,
        predicate: str,
        *,
        subject: str | None = None,
        object: str | None = None,
        value: str | None = None,
        subject_kind: str | None = None,
        object_kind: str | None = None,
        pairs: bool = False,
        subject_as: str | None = None,
        object_as: str | None = None,
        unless_kinds: Sequence[str] = (),
        literal: bool = False,
        name: str | None = None,
    ) -> Rule:
        """Return a rule written with the names the client uses.

        *focus* and the kinds are record types ("Catalysis"); *subject* and *object* are links
        of the focus by field name or label ("source", "Is part of"), a path of them
        ("target/source"), or a link that points to the focus ("^Is part of"); None is the
        focus itself. *predicate* and *value* are the target's terms: IRIs, CURIEs
        ("biolink:catalyzes") or "a" for rdf:type.
        """
        from rdfsolve.client.hydration import class_iri

        def kind(name: str | None, links: Sequence[str] = (), to: str | None = None) -> str | None:
            """Return the class IRI of a record type; a shared name is decided by the links the
            rule uses from it (towards the focus).
            """
            if not name:
                return None
            if ":" in name and not name.startswith(("http://", "https://")):
                name = _expand(name, client)
            return class_iri(client.model(name, links=links, to=to))

        def end(path_text: str | None) -> list[str]:
            """Return the link that reaches an end, seen from the end (to the focus)."""
            if not path_text:
                return []
            last = path_text.split("/")[-1]
            return [last.lstrip("^")] if last.startswith("^") else ["^" + last]

        def path(text: str | None) -> str | None:
            """Return a path of links as SPARQL path text, each step resolved by the client."""
            if text is None:
                return None
            steps = []
            for step in text.split("/"):
                inverse = step.startswith("^")
                steps.append(
                    ("^" if inverse else "") + f"<{_link(client, step.lstrip('^'), focus_iri)}>"
                )
            return "/".join(steps)

        first = [p.split("/")[0] for p in (subject, object) if p]
        focus_iri = kind(focus, first) or focus
        return cls(
            focus_iri,
            _expand(predicate, client),
            subject=path(subject),
            object=path(object),
            value=value if literal or value is None else _expand(value, client),
            subject_class=kind(
                subject_kind, end(subject), focus_iri if subject and "/" not in subject else None
            ),
            object_class=kind(
                object_kind, end(object), focus_iri if object and "/" not in object else None
            ),
            pairs=pairs,
            name=name,
            unless_classes=tuple(c for k in unless_kinds if (c := kind(k, first))),
            literal=literal,
            subject_as=subject_as,
            object_as=object_as,
        )

    def to_construct(self, scope: str = "") -> str:
        """Return the SPARQL CONSTRUCT of the rule; *scope* is a pattern on ?x (the focus)."""
        from rdfsolve.schema_models.exporters.paths import path_to_sparql

        lines = [f"?x a <{self.focus}> ."]
        for unless in self.unless_classes:
            lines.append(f"FILTER NOT EXISTS {{ ?x a <{unless}> }}")
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
            obj = _literal(self.value) if self.literal else f"<{self.value}>"
            object_class = None
        elif self.object is None:
            obj = "?x"
            object_class = None
        else:
            obj = "?o"
            lines.append(f"?x {path_to_sparql(_path(self.object))} ?o .")
            object_class = self.object_class
        for i, (path, cls) in enumerate(self.requires):
            typed = f" ?r{i} a <{cls}> ." if cls else ""
            lines.append(f"FILTER EXISTS {{ ?x {path_to_sparql(_path(path))} ?r{i} .{typed} }}")
        if self.subject_class and subject != "?x":
            lines.append(f"?s a <{self.subject_class}> .")
        if object_class and obj == "?o":
            lines.append(f"?o a <{object_class}> .")
        body = "\n  ".join(lines)
        return f"CONSTRUCT {{ {subject} <{self.predicate}> {obj} }}\nWHERE {{\n  {body}\n}}"

    def add_shacl(self, graph: Graph, shape: Node) -> Node:
        """Add the rule to *shape* as a sh:TripleRule (subject and object node expressions);
        return the rule's node.
        """
        from rdflib import RDF, SH, BNode, Literal, URIRef

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
        for path, cls in self.requires:
            condition, prop = BNode(), BNode()
            graph.add((rule, SH.condition, condition))
            graph.add((condition, SH.property, prop))
            graph.add((prop, SH.path, path_to_rdf(_path(path), graph)))
            if cls:
                qualified = BNode()
                graph.add((qualified, SH["class"], URIRef(cls)))
                graph.add((prop, SH.qualifiedValueShape, qualified))
                graph.add((prop, SH.qualifiedMinCount, Literal(1)))
            else:
                graph.add((prop, SH.minCount, Literal(1)))
        for unless in self.unless_classes:
            condition, negated = BNode(), BNode()
            graph.add((negated, SH["class"], URIRef(unless)))
            graph.add((condition, SH["not"], negated))
            graph.add((rule, SH.condition, condition))
        graph.add((rule, SH.subject, nodes(self.subject, self.subject_class)))
        graph.add((rule, SH.predicate, URIRef(self.predicate)))
        if self.pairs:
            graph.add((rule, SH.object, nodes(self.subject, self.subject_class)))
        elif self.value is not None:
            graph.add(
                (rule, SH.object, Literal(self.value) if self.literal else URIRef(self.value))
            )
        else:
            graph.add((rule, SH.object, nodes(self.object, self.object_class)))
        return rule


@dataclass
class Profile:
    """The rules that turn one source's RDF into one target schema (at a version)."""

    name: str
    rules: list[Rule]
    target: str | None = None
    target_version: str | None = None
    provenance: Mapping[str, str | None] = field(default_factory=dict)
    queries: list[Query] = field(default_factory=list)
    dataset: str | None = None  # the mined source whose shapes the rules attach to

    @classmethod
    def from_queries(
        cls,
        paths: Iterable[str | Path],
        *,
        name: str | None = None,
        dataset: str | None = None,
        provenance: Mapping[str, str | None] | None = None,
        client: Client | None = None,
    ) -> Profile:
        """Return the profile of query files (Query): the rules of each, the target and its
        version from their headers (``# target: biolink 4.4.5``); the source's release and
        endpoint from *client* (its mined schema). A query outside the plain subset is kept and
        run whole.
        """
        queries = [Query.read(p) for p in sorted(map(str, paths))]
        targets = {q.header.get("target", "") for q in queries}
        if len(targets) > 1:
            raise ValueError(f"The queries convert to different targets: {sorted(targets)}")
        target, _, version = next(iter(targets), "").partition(" ")
        sources = {q.header.get("source") for q in queries} - {None}
        dataset = dataset or (next(iter(sources)) if len(sources) == 1 else None)
        rules = [rule for q in queries for rule in q.rules()]
        if provenance is None and client is not None:
            from rdfsolve.property_graph import provenance as described

            provenance = described(client.schema)
        return cls(
            name or f"{dataset or 'local'}-{target or 'target'}",
            rules,
            target or None,
            version or None,
            dict(provenance or {}),
            queries,
            dataset,
        )

    def whole(self) -> list[Query]:
        """Return the queries outside the plain subset, which are run as written."""
        return [q for q in self.queries if q.problem]

    def to_sssom(self) -> Any:
        """Return what the rules map, as an SSSOM mapping set derived from the queries.

        - ``?x a C`` to ``?x a K``: C to K.
        - A one-step link from the focus to a target predicate: the property shape (the link in
          the context of the focus class) to the predicate (``biolink:subject`` and
          ``biolink:object`` of an association too).
        - A focus that stands for an edge (neither end of it, one step to each): its class to
          the predicate, and its two property shapes to ``biolink:subject`` and
          ``biolink:object``.

        The predicate is skos:broadMatch (every instance of the subject is one of the object:
        what the query states) unless the query's header says ``# match: exact`` or
        ``close``. Longer paths and pairs map no single term: they are in the SHACL only. Each
        row's curation rule is the query.
        """
        import bioregistry
        from curies import Converter
        from sssom import Mapping
        from sssom.util import MappingSetDataFrame

        from rdfsolve.config import get_base_uri, mint
        from rdfsolve.mappings.sssom import create_sssom_mappings
        from rdfsolve.schema_models.exporters.shacl import shape_iri

        dataset = self.dataset or "local"
        by_name = {q.name: q for q in self.queries}
        found: dict[tuple[str, str], Rule] = {}

        def step(path: str | None) -> str | None:
            """Return the property IRI of a one-step forward path (``<p>``), else None."""
            hit = re.fullmatch(r"<([^<>]+)>", path or "")
            return hit.group(1) if hit else None

        for rule in self.rules:
            if rule.pairs:
                continue
            if rule.predicate == _TYPE and rule.subject is None and rule.value and not rule.literal:
                found.setdefault((rule.focus, rule.value), rule)
            elif rule.subject is None and (link := step(rule.object)):
                found.setdefault((shape_iri(dataset, rule.focus, link), rule.predicate), rule)
            elif (start := step(rule.subject)) and (end := step(rule.object)):
                base = rule.predicate.rsplit("/", 1)[0] + "/"
                found.setdefault((rule.focus, rule.predicate), rule)
                found.setdefault((shape_iri(dataset, rule.focus, start), base + "subject"), rule)
                found.setdefault((shape_iri(dataset, rule.focus, end), base + "object"), rule)
        prefix_map = {
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "semapv": "https://w3id.org/semapv/vocab/",
            "shape": mint("dataset", dataset) + "/shapes/",
            "rdfsolve": get_base_uri(),
        }
        for iri in sorted({i for k in found for i in k}):
            if any(iri.startswith(v) for v in prefix_map.values()):
                continue
            base = iri.rsplit("#", 1)[0] + "#" if "#" in iri else iri.rsplit("/", 1)[0] + "/"
            # Bioregistry's prefix where it knows the namespace (biolink), else the last segment.
            known = bioregistry.curie_from_iri(iri)
            name = known.partition(":")[0] if known and iri.startswith(base) else None
            name = name or base.rstrip("/#").rsplit("/", 1)[-1].lower().replace(".", "_") or "ns"
            prefix_map.setdefault(name, base)
        converter = Converter.from_prefix_map(prefix_map)
        rows = []
        for (subject, obj), rule in found.items():
            query = by_name.get(rule.name or "")
            match = (query.header.get("match", "broad") if query else "broad").lower()
            rows.append(
                Mapping(
                    subject_id=converter.compress(subject, passthrough=True),
                    predicate_id=f"skos:{match}Match",
                    object_id=converter.compress(obj, passthrough=True),
                    mapping_justification="semapv:ManualMappingCuration",
                    curation_rule=[mint("conversion", dataset, rule.name or "rule")],
                    curation_rule_text=[query.header.get("title", "")] if query else None,
                )
            )
        msdf: MappingSetDataFrame = create_sssom_mappings(
            rows, mint("mappings", self.name), converter=converter
        )
        return msdf

    def rebuilds(self, client: Client, scope: str = "") -> list[dict[str, Any]]:
        """Run each query as written and its compiled rules on *client* (with *scope*, a pattern
        on ?x, the focus); return whether they give the same statements. A pair's two
        directions count as one statement.
        """
        rows: list[dict[str, Any]] = []
        for query in self.queries:
            if query.problem:
                rows.append({"query": query.name, "same": None, "note": query.problem})
                continue

            symmetric = {r.predicate for r in query.rules() if r.pairs}
            written = _statements(
                client.construct(query.scoped(scope) if scope else query.text), symmetric
            )
            compiled: set[tuple[str, str, str]] = set()
            for rule in query.rules():
                compiled |= _statements(client.construct(rule.to_construct(scope)), symmetric)
            rows.append(
                {
                    "query": query.name,
                    "as written": len(written),
                    "compiled": len(compiled),
                    "same": written == compiled,
                }
            )
        return rows

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
                from rdfsolve.schema_models.exporters.shacl import shape_iri

                # The mined node shape of the focus class, when the source is known: the rules
                # hang from the shapes the source was described with.
                shape = URIRef(
                    shape_iri(self.dataset, rule.focus)
                    if self.dataset
                    else mint(
                        "profile", self.name, rule.focus.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
                    )
                )
                shapes[rule.focus] = shape
                graph.add((shape, RDF.type, SH.NodeShape))
                graph.add((shape, SH.targetClass, URIRef(rule.focus)))
                graph.add((shape, dcterms.isPartOf, profile))
            node = rule.add_shacl(graph, shapes[rule.focus])
            if rule.name and rule.name in {q.name for q in self.queries}:
                graph.add(
                    (
                        node,
                        prov.wasDerivedFrom,
                        URIRef(mint("conversion", self.dataset or "local", rule.name)),
                    )
                )
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
        for query in [*self.constructs(scope), *(q.text for q in self.whole())]:
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


# -- conversions written as SPARQL ------------------------------------------------------------

_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


@dataclass
class Query:
    """A conversion written as one SPARQL CONSTRUCT, with a header of ``# key: value`` lines.

    The WHERE is the source pattern and the template the target's statements. The header
    (title, description, source, target, endpoint) follows the SIB sparql-examples convention.
    A query in the plain subset (triple patterns, rdf:type, property paths, ``FILTER(?a != ?b)``
    for pairs, ``FILTER NOT EXISTS { ?x a C }``) is turned into rules (:meth:`rules`); any
    other query is run whole, and :attr:`problem` says why.
    """

    path: Path
    text: str
    header: dict[str, str]
    problem: str | None = None

    @classmethod
    def read(cls, path: str | Path) -> Query:
        """Read a query file."""
        path = Path(path)
        text = path.read_text()
        header = {}
        for line in text.splitlines():
            if not line.startswith("#"):
                break
            key, _, value = line[1:].partition(":")
            if value:
                header[key.strip()] = value.strip()
        return cls(path, text, header)

    @property
    def name(self) -> str:
        """Return the file name without its extension."""
        return self.path.stem

    def _parsed(self) -> Any:
        from rdflib.plugins.sparql import prepareQuery

        return prepareQuery(self.text).algebra

    def executable(self) -> str:
        """Return the query as a SHACL SPARQL executable in Turtle (the sparql-examples form)."""
        from rdflib import RDF, RDFS, SH, Graph, Literal, Namespace, URIRef

        from rdfsolve.config import mint

        schema, dcterms = Namespace("https://schema.org/"), Namespace("http://purl.org/dc/terms/")
        graph = Graph()
        graph.bind("sh", SH)
        graph.bind("schema", schema)
        graph.bind("dcterms", dcterms)
        node = URIRef(mint("conversion", self.header.get("source", "local"), self.name))
        graph.add((node, RDF.type, SH.SPARQLExecutable))
        graph.add((node, RDF.type, SH.SPARQLConstructExecutable))
        if self.header.get("title"):
            graph.add((node, RDFS.label, Literal(self.header["title"], lang="en")))
        if self.header.get("description"):
            graph.add((node, RDFS.comment, Literal(self.header["description"], lang="en")))
        if self.header.get("endpoint"):
            graph.add((node, schema.target, URIRef(self.header["endpoint"])))
        if self.header.get("target"):
            graph.add((node, dcterms.conformsTo, Literal(self.header["target"])))
        body = "\n".join(line for line in self.text.splitlines() if not line.startswith("#"))
        graph.add((node, SH.construct, Literal(body.strip())))
        return graph.serialize(format="turtle")

    def _where(self) -> str:
        """Return the text of the query from its WHERE on."""
        found = re.search(r"\bWHERE\b", self.text, re.IGNORECASE)
        return self.text[found.start() :] if found else self.text

    def _pattern(self) -> tuple[list[tuple[Any, Any, Any]], list[Any]]:
        """Return the triple patterns and filters of the WHERE, or raise ValueError."""
        triples: list[tuple[Any, Any, Any]] = []
        filters: list[Any] = []

        def walk(node: Any) -> None:
            """Collect the triples and filters of a plain pattern."""
            kind = getattr(node, "name", "")
            if kind in ("Project", "Distinct"):
                walk(node.p)
            elif kind == "Filter":
                filters.append(node.expr)
                walk(node.p)
            elif kind == "Join":
                walk(node.p1)
                walk(node.p2)
            elif kind == "BGP":
                triples.extend(node.triples)
            else:
                raise ValueError(f"{kind or type(node).__name__} is outside the plain subset")

        walk(self._parsed().p)
        return triples, filters

    def rules(self) -> list[Rule]:
        """Return the rules of the query; [] (with :attr:`problem`) outside the plain subset."""
        from rdflib import Literal, URIRef, Variable

        try:
            triples, filters = self._pattern()
        except ValueError as error:
            self.problem = str(error)
            return []
        types: dict[Any, list[str]] = defaultdict(list)
        links: list[tuple[Any, str, Any]] = []
        for s, p, o in triples:
            if str(p) == _TYPE and isinstance(o, URIRef):
                types[s].append(str(o))
            elif isinstance(s, Variable) and isinstance(o, Variable):
                links.append((s, p.n3() if not isinstance(p, URIRef) else f"<{p}>", o))
            else:
                self.problem = f"{s.n3()} {p.n3()} {o.n3()}: a constant end is outside the subset"
                return []
        pairs: set[frozenset[Any]] = set()
        unless: dict[Any, list[str]] = {}
        for expr in _conjuncts(filters):
            kind = getattr(expr, "name", "")
            if kind == "RelationalExpression" and expr.op == "!=":
                pairs.add(frozenset((expr.expr, expr.other)))
            elif kind == "Builtin_NOTEXISTS":
                inner = _triples_of(expr["graph"])
                if len(inner) == 1 and str(inner[0][1]) == _TYPE:
                    unless.setdefault(inner[0][0], []).append(str(inner[0][2]))
                    continue
                self.problem = "FILTER NOT EXISTS other than one rdf:type is outside the subset"
                return []
            else:
                self.problem = f"FILTER {kind} is outside the subset"
                return []
        if any(len(found) > 1 for found in types.values()):
            self.problem = "a variable with several classes is outside the subset"
            return []
        variables = {v for s, _, o in links for v in (s, o)} | set(types)
        focus = _focus(
            [v for v in types if not any(v in pair for pair in pairs)],
            links,
            variables,
            self._where(),
        )
        if focus is None:
            self.problem = "no typed variable reaches every other one"
            return []
        paths = _paths(focus, links)
        cls = types[focus][0]

        def class_of(v: Any) -> str | None:
            """Return the class a variable other than the focus is kept to."""
            return types[v][0] if v != focus and types.get(v) else None

        rules = []
        for s, p, o in self._parsed().template:
            subject = None if s == focus else paths[s]
            ends = {s, o} | {v for pair in pairs if s in pair or o in pair for v in pair}
            requires = tuple(
                (paths[v], types[v][0] if types.get(v) else None)
                for v in sorted(variables, key=str)
                if v != focus and v not in ends
            )
            common: dict[str, Any] = {
                "focus": cls,
                "predicate": str(p),
                "subject": subject,
                "subject_class": class_of(s),
                "name": self.name,
                "unless_classes": tuple(unless.get(focus, [])),
                "requires": requires,
            }
            if frozenset((s, o)) in pairs:
                rules.append(Rule(**common, pairs=True))
            elif isinstance(o, Variable):
                rules.append(
                    Rule(
                        **common, object=None if o == focus else paths[o], object_class=class_of(o)
                    )
                )
            else:
                rules.append(Rule(**common, value=str(o), literal=isinstance(o, Literal)))
        return rules

    def focus_variable(self) -> str | None:
        """Return the name of the variable the rules hang from (the typed root)."""
        from rdflib import URIRef, Variable

        try:
            triples, filters = self._pattern()
        except ValueError:
            return None
        types = {s for s, p, o in triples if str(p) == _TYPE and isinstance(o, URIRef)}
        links = [
            (s, "", o) for s, p, o in triples if isinstance(s, Variable) and isinstance(o, Variable)
        ]
        pairs = {
            v
            for e in filters
            if getattr(e, "name", "") == "RelationalExpression"
            for v in (e.expr, e.other)
        }
        variables = {v for s, _, o in links for v in (s, o)} | types
        found = _focus([v for v in types if v not in pairs], links, variables, self._where())
        return str(found) if found is not None else None

    def scoped(self, scope: str) -> str:
        """Return the query with *scope* (a pattern on ?x) on its focus variable."""
        focus = self.focus_variable()
        if focus is None:
            return self.text
        found = re.search(r"\bWHERE\s*\{", self.text, re.IGNORECASE)
        if found is None:
            return self.text
        where = re.sub(r"\?x\b", "?" + focus, scope)
        return f"{self.text[: found.end()]}\n  {where}{self.text[found.end() :]}"

    def check(self, schema: Any) -> list[dict[str, Any]]:
        """Return each triple pattern of the WHERE against the mined *schema*: the property shape
        it uses and how many statements the schema counted, or that the schema has none.
        """
        from rdflib import URIRef

        from rdfsolve.schema_models.exporters.shacl import shape_iri

        triples, _ = self._pattern()
        types = {s: str(o) for s, p, o in triples if str(p) == _TYPE and isinstance(o, URIRef)}
        counted: dict[tuple[str, str], int] = defaultdict(int)
        for pattern in schema.patterns:
            counted[(pattern.subject_class, pattern.property_uri)] += pattern.count or 0
        classes = {pattern.subject_class for pattern in schema.patterns}
        dataset = schema.about.dataset_name or "local"
        rows: list[dict[str, Any]] = []
        for s, p, o in triples:
            if str(p) == _TYPE:
                rows.append(
                    {
                        "pattern": f"?{s} a <{o}>",
                        "shape": shape_iri(dataset, str(o)) if str(o) in classes else None,
                        "statements": None,
                        "found": str(o) in classes,
                    }
                )
                continue
            # A one-step inverse link (?pathway ^isPartOf ?entity) is the link of the other end.
            if (
                getattr(p, "arg", None) is not None
                and isinstance(p.arg, URIRef)
                and type(p).__name__ == "InvPath"
            ):
                s, p, o = o, p.arg, s
            cls = types.get(s)
            if cls is None or not isinstance(p, URIRef):
                rows.append(
                    {
                        "pattern": f"?{s} {p.n3()} ?{o}",
                        "shape": None,
                        "statements": None,
                        "found": None,
                    }
                )
                continue
            count = counted.get((cls, str(p)))
            near = [
                q
                for c, q in counted
                if c == cls
                and q.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
                == str(p).rsplit("#", 1)[-1].rsplit("/", 1)[-1]
                and q != str(p)
            ]
            rows.append(
                {
                    "pattern": f"?{s} <{p}> ?{o}",
                    "shape": shape_iri(dataset, cls, str(p)) if count is not None else None,
                    "statements": count,
                    "found": count is not None,
                    **({"did you mean": near} if count is None and near else {}),
                }
            )
        return rows


def _conjuncts(filters: Iterable[Any]) -> list[Any]:
    """Return the filters with AND-joined ones taken apart (two FILTER NOT EXISTS are one
    ConditionalAndExpression once parsed).
    """
    out = []
    for expr in filters:
        if getattr(expr, "name", "") == "ConditionalAndExpression":
            others = expr["other"] if "other" in expr else []  # noqa: SIM401  (rdflib overrides get)
            out += _conjuncts([expr["expr"], *others])
        else:
            out.append(expr)
    return out


def _triples_of(pattern: Any) -> list[Any]:
    """Return the triple patterns of a parsed group (a BGP, or a group of parts)."""
    if pattern is None:
        return []
    # rdflib's parsed nodes are dictionaries that answer any attribute: read the keys.
    if "triples" in pattern:
        return list(pattern["triples"])
    parts = pattern["part"] if "part" in pattern else []  # noqa: SIM401  (rdflib overrides get)
    return [t for part in parts for t in _triples_of(part)]


def _statements(dataset: Iterable[Any], symmetric: set[str]) -> set[tuple[str, str, str]]:
    """Return the statements of *dataset*; one of a *symmetric* predicate counts both ways."""
    out = set()
    for quad in dataset:
        s, p, o = str(quad.subject), quad.predicate.value, str(quad.object)
        out.add((min(s, o), p, max(s, o)) if p in symmetric else (s, p, o))
    return out


def _focus(
    candidates: list[Any], links: list[tuple[Any, str, Any]], variables: set[Any], where: str
) -> Any:
    """Return the first candidate written in *where* (the WHERE text) that reaches every
    variable: the query's author chooses the node the rules hang from by writing it first.
    """

    def reach(start: Any) -> set[Any]:
        """Return the variables reachable from *start*, either way along links."""
        seen, todo = {start}, [start]
        while todo:
            v = todo.pop()
            for s, _, o in links:
                for a, b in ((s, o), (o, s)):
                    if a == v and b not in seen:
                        seen.add(b)
                        todo.append(b)
        return seen

    def written(v: Any) -> int:
        """Return where a variable is first written, or the end."""
        found = re.search(rf"[?$]{re.escape(str(v))}\b", where)
        return found.start() if found else len(where)

    ranked = sorted(candidates, key=written)
    return next((v for v in ranked if reach(v) >= variables), None)


def _paths(focus: Any, links: list[tuple[Any, str, Any]]) -> dict[Any, str]:
    """Return the SPARQL path from *focus* to each variable (forward steps first)."""
    paths: dict[Any, str] = {focus: ""}
    todo = [focus]
    while todo:
        v = todo.pop(0)
        for s, p, o in links:
            inverse = (
                f"^{p}" if re.fullmatch(r"\^?<[^<>]+>", p) and not p.startswith("^") else f"^({p})"
            )
            for a, b, step in ((s, o, p), (o, s, inverse)):
                if a == v and b not in paths:
                    paths[b] = f"{paths[v]}/{step}" if paths[v] else step
                    todo.append(b)
    return paths


@dataclass
class Biolink:
    """The Biolink Model at one version, read from its LinkML YAML: classes and slots."""

    version: str
    classes: dict[str, dict[str, Any]]
    slots: dict[str, dict[str, Any]]

    BASE = "https://w3id.org/biolink/vocab/"

    @classmethod
    def read(cls, path: str | Path) -> Biolink:
        """Read biolink-model.yaml (a pinned copy)."""
        import yaml

        data = yaml.safe_load(Path(path).read_text())
        return cls(str(data.get("version", "")), data.get("classes", {}), data.get("slots", {}))

    def class_iri(self, name: str) -> str:
        """Return the IRI of a class ("small molecule": biolink:SmallMolecule)."""
        return self.BASE + "".join(w[:1].upper() + w[1:] for w in name.split(" "))

    def slot_iri(self, name: str) -> str:
        """Return the IRI of a slot ("has input": biolink:has_input)."""
        return self.BASE + name.replace(" ", "_")

    def ancestors(self, name: str) -> list[str]:
        """Return a class's ancestors along is_a, nearest first."""
        out, current = [], self.classes.get(name, {}).get("is_a")
        while current and current not in out:
            out.append(current)
            current = self.classes.get(current, {}).get("is_a")
        return out

    def name_of(self, term: str) -> str:
        """Return the model's name of a term given as a CURIE, an IRI or a name."""
        local = term.rsplit(":", 1)[-1].rsplit("/", 1)[-1]
        for name in (*self.classes, *self.slots):
            if (
                name == local
                or self.class_iri(name).endswith("/" + local)
                or self.slot_iri(name).endswith("/" + local)
            ):
                return name
        raise ValueError(f"{term!r} is not a class or slot of Biolink {self.version}")

    def diagram(self, *names: str, terms: Sequence[str] = (), fenced: bool = True) -> str:
        """Draw the part of the model that *terms* use (Mermaid): a class with its parents (is_a),
        its mixins and the identifier prefixes it takes; a slot as an edge from its domain to
        its range, with the slot it specializes.
        """
        from rdfsolve.client.diagram import _md, _node

        nodes: dict[str, str] = {}
        lines, edges = [], []

        def node(name: str, detail: str = "", style: str = "") -> str:
            """Return the node of a class, drawn once."""
            if name not in nodes:
                nodes[name] = f"B{len(nodes)}"
                lines.append(_node(nodes[name], name, detail))
                if style:
                    lines.append(f"style {nodes[name]} {style}")
            return nodes[name]

        for term in [*names, *terms]:
            name = self.name_of(term)
            if name in self.classes:
                c = self.classes[name]
                prefixes = ", ".join((c.get("id_prefixes") or [])[:6])
                child = node(
                    name,
                    f"ids: {prefixes}" if prefixes else "",
                    "fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px",
                )
                current = name
                for parent in self.ancestors(name)[:2]:
                    edges.append(f'{nodes[current]} -->|"is a"| {node(parent)}')
                    current = parent
                for mixin in c.get("mixins") or []:
                    edges.append(
                        f'{child} -.->|"mixin"| {node(mixin, "mixin", "fill:#f2f2f2,stroke:#9a9a9a,stroke-dasharray:3 3")}'
                    )
            else:
                slot = self.slots[name]
                domain = node(slot.get("domain") or "any class")
                range_ = node(slot.get("range") or "any class")
                parent = f" (is a {slot['is_a']})" if slot.get("is_a") else ""
                edges.append(f'{domain} ==>|"{_md(name + parent)}"| {range_}')
        body = "\n".join(
            [
                "flowchart LR",
                *lines,
                *edges,
                "classDef default fill:#eef4fb,stroke:#3b6ea8,stroke-width:1.5px,color:#1d2b3a",
            ]
        )
        return f"```mermaid\n{body}\n```" if fenced else body

    def curie(self, iri: str) -> str | None:
        """Return an identifier as a CURIE with the prefix the model writes (its spelling of the
        prefix, as its classes list them in id_prefixes), compared as
        Bioregistry prefixes. None when the model takes no prefix for it.
        """
        import bioregistry

        from rdfsolve.identifiers import parse

        found = parse(iri)
        if found is None:
            return None
        if not hasattr(self, "_prefixes"):
            self._prefixes = {
                (bioregistry.normalize_prefix(p) or p.lower()): p
                for c in self.classes.values()
                for p in c.get("id_prefixes") or []
            }
        prefix = self._prefixes.get(found.prefix)
        return f"{prefix}:{found.local}" if prefix else None

    def hierarchy(self) -> dict[str, set[str]]:
        """Return the ancestors (IRIs) of each class IRI, for the most specific category."""
        return {
            self.class_iri(n): {self.class_iri(a) for a in self.ancestors(n)} for n in self.classes
        }

    def categories(self, prefix: str) -> list[str]:
        """Return the categories (class IRIs, not mixins) where an identifier prefix belongs:
        those that list it (compared as Bioregistry prefixes: UniProtKB is uniprot) while none
        of their ancestors does. Biolink repeats a prefix on narrower classes (UniProtKB on
        Protein and on ProteinIsoform); the prefix belongs to the broadest (Protein).
        """
        import bioregistry

        wanted = bioregistry.normalize_prefix(prefix) or prefix.lower()
        found = [
            name
            for name, c in self.classes.items()
            if not c.get("mixin")
            and any(
                (bioregistry.normalize_prefix(p) or p.lower()) == wanted
                for p in c.get("id_prefixes") or []
            )
        ]
        listed = set(found)
        return sorted(self.class_iri(n) for n in found if not listed & set(self.ancestors(n)))


def categorize(
    graph: Any, biolink: Biolink, pairs: Iterable[tuple[str, str]] = ()
) -> dict[str, Any]:
    """Give each node of *graph* (PropertyGraph) without a Biolink category the one its
    identifiers allow: the categories Biolink lists for the prefix of each of its identifiers,
    and of the identifiers decided to name the same entity (*pairs*, Resolution.pairs), shared
    by all of them. An Ensembl id (Gene or Protein in Biolink) drawn for a protein that the
    decision names with a UniProt id is a Protein. The category is a kind, not a statement: it is
    not given back as RDF. Where the prefixes allow none or several, the node is listed.
    """
    from rdfsolve.identifiers import parse
    from rdfsolve.property_graph import _UNSTATED

    targets: dict[str, set[str]] = defaultdict(set)
    for a, b in pairs:
        targets[a].add(b)
    given: Counter[str] = Counter()
    left: dict[str, list[str]] = {}
    for node in graph.nodes.values():
        if any(label.startswith(biolink.BASE) for label in node.labels):
            continue
        iris = set(node.members or [node.id])
        iris |= {t for i in list(iris) for t in targets.get(i, ())}
        prefixes = {found.prefix for i in iris if (found := parse(i))}
        if not prefixes:
            continue
        allowed = [set(biolink.categories(p)) for p in prefixes]
        common = set.intersection(*allowed) if allowed else set()
        if len(common) == 1:
            category = next(iter(common))
            # Each stated class keeps the IRI that stated it; the category states nothing.
            node.label_origins = (node.label_origins or [node.id] * len(node.labels)) + [_UNSTATED]
            node.labels.append(category)
            given[category] += 1
        else:
            left[node.id] = sorted(prefixes)
    graph._name = None  # names are made again with the new labels
    return {
        "given": dict(given.most_common()),
        "left": left,
        "basis": f"Biolink {biolink.version} id_prefixes of the node's identifiers and of those decided to name it",
    }


def identity_kept(source: Any, target: Any) -> dict[str, Any]:
    """Return whether the converted graph *target* groups identifiers into nodes as *source*
    does (each a PropertyGraph): a conversion must not merge or split entities.
    """

    def groups(graph: Any) -> dict[str, frozenset[str]]:
        """Return each identifier's group of identifiers."""
        from rdfsolve.identifiers import parse

        out = {}
        for node in graph.nodes.values():
            keys = frozenset(
                found.curie for i in (node.members or [node.id]) if (found := parse(i))
            )
            for key in keys:
                out[key] = keys
        return out

    a, b = groups(source), groups(target)
    shared = set(a) & set(b)
    # The target may hold fewer identifiers of an entity (only those the queries name).
    split = sorted(k for k in shared if not b[k] <= a[k])
    return {
        "passed": not split,
        "identifiers compared": len(shared),
        "merged differently": split[:10],
    }


def write_query(
    rules: Sequence[Rule],
    *,
    title: str,
    description: str = "",
    source: Any = "",
    target: Any = "",
    endpoint: str = "",
    prefixes: Mapping[str, str] | None = None,
    focus_as: str | None = None,
) -> str:
    """Return the rules of one focus as a query file: a ``# key: value`` header and one plain
    CONSTRUCT, which Query reads back into the same rules.

    *source* is a client (its name and its mined prefixes) or a name; *target* a Biolink model
    or a name with its version. The focus is ``?<focus_as>``, else ``?<its class>``; an end is ``?<subject_as>`` or ``?<object_as>``, else named
    after the last link of its path. *prefixes* abbreviate IRIs (those of the source's schema
    and of the target).
    """
    if len({r.focus for r in rules}) != 1:
        raise ValueError("A query has one focus: write one query per focus class")
    known = dict(prefixes or {})
    release = made_with = ""
    if hasattr(source, "schema"):  # a client: its name, release and mined prefixes
        from rdfsolve.property_graph import provenance

        described = provenance(source.schema)
        release = str(described.get("source release") or "")
        made_with = str(described.get("generated with") or "")
        known = {**source.schema.get_prefixes(), **known}
        source = source.schema.about.dataset_name or ""
    if isinstance(target, Biolink):
        known.setdefault("biolink", Biolink.BASE)
        target = f"biolink {target.version}"

    def term(iri: str) -> str:
        """Return an IRI as a prefixed name where a prefix fits it, else <iri>."""
        best = max(
            ((p, ns) for p, ns in known.items() if iri.startswith(ns)),
            key=lambda x: len(x[1]),
            default=None,
        )
        if best and re.fullmatch(r"[A-Za-z_][\w.-]*", iri[len(best[1]) :] or "-"):
            used.add(best[0])
            return f"{best[0]}:{iri[len(best[1]) :]}"
        return f"<{iri}>"

    def short(iri: str) -> str:
        """Return the local name of an IRI."""
        return re.split(r"[/#]", iri.rstrip("/#>"))[-1]

    def path_text(path: str) -> str:
        """Return a path with its IRIs abbreviated."""
        return re.sub(r"<([^<>]+)>", lambda m: term(m.group(1)), path)

    used: set[str] = set()
    focus = rules[0].focus
    root = "?" + re.sub(r"\W+", "_", focus_as or short(focus)).lower()
    names: dict[str, str] = {}
    taken = {root}

    def var(path: str, wanted: str | None) -> str:
        """Return the variable at the end of a path, one per path."""
        if path in names:
            return names[path]
        base = "?" + re.sub(r"\W+", "_", wanted or short(path.split("/")[-1].lstrip("^"))).lower()
        name, n = base, 2
        while name in taken:
            name, n = f"{base}{n}", n + 1
        taken.add(name)
        names[path] = name
        return name

    where = [f"{root} a {term(focus)} ."]
    template, filters, later = [], [], []
    classes: dict[str, str] = {}
    for rule in rules:
        subject = root if rule.subject is None else var(rule.subject, rule.subject_as)
        if rule.subject is not None and rule.subject_class:
            classes[subject] = rule.subject_class
        if rule.pairs and rule.subject is not None:
            other = var(
                rule.subject + " other", rule.object_as or (rule.subject_as or "other") + "_other"
            )
            later.append(f"{root} {path_text(rule.subject)} {other} .")
            if rule.subject_class:
                classes[other] = rule.subject_class
            filters.append(f"FILTER({subject} != {other})")
            obj = other
        elif rule.value is not None:
            obj = _literal(rule.value) if rule.literal else term(rule.value)
        elif rule.object is None:
            obj = root
        else:
            obj = var(rule.object, rule.object_as)
            if rule.object_class:
                classes[obj] = rule.object_class
        for path, cls in rule.requires:
            needed = var(path, None)
            if cls:
                classes[needed] = cls
        predicate = "a" if rule.predicate == _TYPE else term(rule.predicate)
        template.append(f"  {subject} {predicate} {obj} .")
        for unless in rule.unless_classes:
            filters.append(f"FILTER NOT EXISTS {{ {root} a {term(unless)} }}")
    for path, name in names.items():
        if not path.endswith(" other"):
            where.append(f"{root} {path_text(path)} {name} .")
    where += later
    where += [f"{v} a {term(c)} ." for v, c in classes.items()]
    where += sorted(set(filters))
    header = {
        "title": title,
        "description": description,
        "source": source,
        "source release": release,
        "target": target,
        "endpoint": endpoint,
        "generated with": made_with,
    }
    lines = [f"# {k}: {v}" for k, v in header.items() if v]
    lines += [f"PREFIX {p}: <{known[p]}>" for p in sorted(used)]
    body = "\n".join(dict.fromkeys(template))
    lines += [
        "",
        "CONSTRUCT {",
        body,
        "}",
        "WHERE {",
        *(f"  {w}" for w in dict.fromkeys(where)),
        "}",
        "",
    ]
    return "\n".join(lines)


def within(client: Client, records: Any, via: str) -> str:
    """Return the scope "the focus is linked by *via* to these records, or is one of them"
    (Profile.run and rebuilds): ``within(wp, pathway, via="Is part of")`` is what is drawn in
    the pathway, and the pathway itself. *via* is a link name of the client.
    """
    iris = [str(vars(record)["uri"]) for record in records.records]
    kinds = {client.type_name(type(record)) for record in records.records}
    if not iris or len(kinds) != 1:
        raise ValueError("Give records of one record type")
    prop = _link(client, via, next(iter(kinds)))
    values = " ".join(f"<{iri}>" for iri in iris)
    return f"{{ ?x <{prop}> ?within . VALUES ?within {{ {values} }} }} UNION {{ VALUES ?x {{ {values} }} }}"


_ASSOCIATION_SLOTS = ("subject", "predicate", "object")


def to_kgx(
    graph: Any, biolink: Biolink, folder: str | Path, knowledge_source: str
) -> tuple[Path, Path]:
    """Write a property graph of Biolink statements as KGX TSV (nodes.tsv, edges.tsv); return
    their paths.

    Node ids are CURIEs: with the prefixes Biolink writes (Biolink.curie), else Bioregistry's,
    else a prefix named for the IRI's namespace; prefixes.json beside the files expands those. A node's
    category is its Biolink classes (biolink:NamedThing when it has none). Each edge of a
    Biolink predicate is a row; a node that is a biolink:Association (an inhibition: subject,
    predicate, object and qualifiers) is one row too. Edges get the provenance KGX requires
    (*knowledge_source*, an infores CURIE; knowledge_level and agent_type).
    """
    import csv

    base = biolink.BASE
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    import json

    from rdfsolve._uri import curie_from_prefixes, prefix_map
    from rdfsolve.identifiers import parse

    # A CURIE for every node (KGX requires one): the prefix Biolink writes, else Bioregistry's,
    # else a prefix named for the IRI's namespace; the prefixes used are written beside the files.
    used: dict[str, str] = {}
    unregistered = sorted(
        {
            n
            for n in graph.nodes
            if not biolink.curie(n) and parse(n) is None and not n.startswith("_:")
        }
    )
    minted = prefix_map(unregistered, {})

    def short(iri: str) -> str:
        """Return a node id as a CURIE."""
        if found := biolink.curie(iri):
            return found
        if (registered := parse(iri)) is not None:
            used[registered.prefix] = iri[: len(iri) - len(registered.local)]
            return registered.curie
        hit = curie_from_prefixes(iri, minted)
        if hit:
            used[hit[1]] = hit[2]
            return hit[0]
        return iri

    def term(iri: str) -> str:
        """Return a Biolink term as biolink:<local>."""
        return "biolink:" + iri[len(base) :] if iri.startswith(base) else iri

    associations = {n for n, node in graph.nodes.items() if base + "Association" in node.labels}
    parts: dict[str, dict[str, str]] = defaultdict(dict)
    rows = []
    for edge in graph.edges:
        slot = edge.type[len(base) :] if edge.type.startswith(base) else None
        if edge.source in associations and slot in _ASSOCIATION_SLOTS:
            parts[edge.source][slot] = edge.target
        elif slot:
            rows.append(
                {
                    "subject": short(edge.source),
                    "predicate": term(edge.type),
                    "object": short(edge.target),
                }
            )
    for nid in sorted(associations):
        node, found = graph.nodes[nid], parts[nid]
        predicate = found.get("predicate") or next(
            (v.lexical for v in node.properties.get(base + "predicate", [])), None
        )
        if not (found.get("subject") and found.get("object") and predicate):
            continue
        row = {
            "subject": short(found["subject"]),
            "predicate": term(predicate),
            "object": short(found["object"]),
        }
        for key, values in node.properties.items():
            if key.startswith(base) and key.endswith("_qualifier") and values:
                row[key[len(base) :]] = values[0].lexical
        rows.append(row)
    nodes_path, edges_path = folder / "nodes.tsv", folder / "edges.tsv"
    with nodes_path.open("w", newline="") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow(["id", "category", "name"])
        for nid, node in sorted(graph.nodes.items()):
            if nid in associations:
                continue
            categories = [term(c) for c in node.labels if c.startswith(base)] or [
                "biolink:NamedThing"
            ]
            names = [v.lexical for v in node.properties.get(base + "name", [])]
            out.writerow([short(nid), "|".join(categories), names[0] if names else ""])
    qualifiers = sorted({k for row in rows for k in row} - {"subject", "predicate", "object"})
    with edges_path.open("w", newline="") as f:
        out = csv.writer(f, delimiter="\t")
        header = [
            "subject",
            "predicate",
            "object",
            *qualifiers,
            "knowledge_level",
            "agent_type",
            "primary_knowledge_source",
        ]
        out.writerow(header)
        for row in rows:
            out.writerow(
                [
                    row["subject"],
                    row["predicate"],
                    row["object"],
                    *(row.get(q, "") for q in qualifiers),
                    "knowledge_assertion",
                    "manual_agent",
                    knowledge_source,
                ]
            )
    (folder / "prefixes.json").write_text(json.dumps(dict(sorted(used.items())), indent=1))
    return nodes_path, edges_path
