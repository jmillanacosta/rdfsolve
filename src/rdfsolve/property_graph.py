"""Property graphs from RDF: nodes and edges with typed properties, checked against the RDF.

An RDF graph becomes a property graph by rules that can be undone:

- a subject (IRI or blank node) is a node, identified by its IRI or by ``_:`` and its blank-node
  label; its ``rdf:type`` values are its labels;
- a literal value is a node property; its lexical form, datatype and language are kept;
- a link to another resource is an edge, whose type is the predicate;
- a :class:`Fold` turns the instances of a class (a catalysis, an axiom) into edges between the
  two resources each instance links, with the instance's other values as edge properties. An
  instance without exactly one source and one target, or that something else links to, stays a
  node.

Two choices have defaults that can be set:

- ``types``: how a literal becomes a native value (:data:`DEFAULT_TYPES`: integers, floats and
  decimals, booleans, dates). A :class:`Conversion` is used only when formatting the native value
  gives back the lexical form; otherwise the lexical form is kept, with its datatype beside it.
- ``names``: how labels, edge types and keys are named: ``"local"`` (the local name, or the
  CURIE where local names clash; the default), ``"curie"``, ``"label"`` (the labels of the mined
  schema), ``"iri"``, a function of the IRI, or a mapping of IRIs to names over the default.

An :class:`Identity` merges the IRIs of one registered identifier into one node (listing its
IRIs) and decides each mapping between identifiers: an attribute when both are known to be of
one kind and the link is one to one, an edge otherwise; the report lists the undecided ones.

The gates of :meth:`PropertyGraph.report` check an export: :meth:`PropertyGraph.to_oxigraph` gives
back the input (nothing is lost silently), names are unique, literals keep their types, keys that
the schema measured as single-valued have one value, and each fold is applied only where it
holds. Writers for networkx, GraphML, Neo4j (``neo4j-admin import``) and JSON check what they
write where a reader is at hand.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from itertools import combinations
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyoxigraph as ox

from rdfsolve._uri import curie_from_prefixes, prefix_map
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema

REFERENCE = "@id"  # the datatype of a property value that refers to a resource
_XSD = "http://www.w3.org/2001/XMLSchema#"
_STRING = _XSD + "string"
_LANG_STRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
# An Oxigraph dataset or store, quads or triples; an RDFLib graph is converted once.
Source = ox.Dataset | ox.Store | Iterable[ox.Quad | ox.Triple] | Any


@dataclass(frozen=True)
class Conversion:
    """Parse a lexical form into a native value, and format it back; *neo4j* names its type."""

    parse: Callable[[str], Any]
    format: Callable[[Any], str] = str
    neo4j: str = "string"


_INTEGER = Conversion(int, str, "long")
_FLOAT = Conversion(float, repr, "double")
DEFAULT_TYPES: dict[str, Conversion | None] = {
    **{
        _XSD + name: _INTEGER
        for name in (
            "integer",
            "int",
            "long",
            "short",
            "byte",
            "nonNegativeInteger",
            "positiveInteger",
            "nonPositiveInteger",
            "negativeInteger",
            "unsignedLong",
            "unsignedInt",
            "unsignedShort",
            "unsignedByte",
        )
    },
    _XSD + "double": _FLOAT,
    _XSD + "float": _FLOAT,
    _XSD + "decimal": _FLOAT,
    _XSD + "boolean": Conversion(
        lambda text: {"true": True, "false": False}[text],
        lambda v: "true" if v else "false",
        "boolean",
    ),
    _XSD + "date": Conversion(date.fromisoformat, date.isoformat, "date"),
}
NAME_STRATEGIES = ("local", "curie", "label", "iri")


@dataclass(frozen=True)
class Value:
    """A property value: the lexical form with its datatype and language, or a reference."""

    lexical: str
    datatype: str | None = None
    lang: str | None = None

    @classmethod
    def of(cls, term: ox.Literal | ox.NamedNode | ox.BlankNode) -> Value:
        """Return the value of an RDF literal (a plain string has no datatype), or a reference."""
        if isinstance(term, ox.Literal):
            datatype = term.datatype.value
            plain = datatype in (_STRING, _LANG_STRING)
            return cls(term.value, None if plain else datatype, term.language)
        return cls(_ox_id(term), REFERENCE)

    def ox_term(self) -> ox.Literal | ox.NamedNode | ox.BlankNode:
        """Return the Oxigraph term of the value."""
        if self.datatype == REFERENCE:
            return _ox_node(self.lexical)
        if self.lang:
            return ox.Literal(self.lexical, language=self.lang)
        datatype = ox.NamedNode(self.datatype) if self.datatype else None
        return ox.Literal(self.lexical, datatype=datatype)


Properties = dict[str, list[Value]]


@dataclass
class PGNode:
    """A node: its id (IRI, or ``_:`` and a blank-node label), class IRIs and properties.

    A node that several IRIs of one identifier were merged into (see :class:`Identity`) lists
    them in ``members``; ``origins`` gives, for each key, the IRI that stated each value, and
    ``label_origins`` the IRI that stated each label, so that the statements can be given back.
    """

    id: str
    labels: list[str] = field(default_factory=list)
    properties: Properties = field(default_factory=dict)
    members: list[str] = field(default_factory=list)
    origins: dict[str, list[str]] = field(default_factory=dict)
    label_origins: list[str] = field(default_factory=list)


@dataclass
class PGEdge:
    """An edge: its type (a predicate IRI, or the fold that made it) and properties.

    ``via`` is the id of the folded node, which :meth:`PropertyGraph.to_oxigraph` recreates.
    """

    source: str
    type: str
    target: str
    properties: Properties = field(default_factory=dict)
    via: str | None = None
    fold: int | None = None
    source_iri: str | None = None  # the IRI written in the RDF, when a merge changed the end
    target_iri: str | None = None


@dataclass(frozen=True)
class Fold:
    """Turn each instance of *cls* into an edge from its *source* value to its *target* value.

    IRIs or CURIEs of the schema. *name* names the edges (default: named as the class).
    *evidence* is what the mined schema measured, when the fold was suggested.
    """

    cls: str
    source: str
    target: str
    name: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict, compare=False, hash=False)


SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
EXACT_MATCH = "http://www.w3.org/2004/02/skos/core#exactMatch"


@dataclass(frozen=True)
class Identity:
    """Which nodes name one entity, and how a mapping between two identifiers is shown.

    - Nodes whose IRIs are one registered identifier (rdfsolve.identifiers.parse: the same
      prefix and local identifier, as identifiers.org/uniprot/Q13510 and
      purl.uniprot.org/uniprot/Q13510) are merged into one node that lists its IRIs.
    - A link to an identifier that has no data of its own becomes a reference property of the
      node (an attribute) when both identifiers are known to be of the same kind and the link
      is one to one; otherwise it stays an edge. A link between two kinds (a gene to its
      proteins) is a relation, not a sameness.
    - *kinds* maps a registered prefix to the classes that the source issuing it gives its
      identifiers (Client.issued_kinds). A prefix without kinds is unknown: its links stay
      edges and the report lists them as undecided, with what would decide them.
    - *mappings* are the predicates that can state that two identifiers are one entity (by
      default the declared identities: owl:sameAs, skos:exactMatch and Bio2RDF cross-references);
      other links (part of a pathway) are never judged.
    - *exact* merges two different identifiers of one kind that are linked one to one both ways
      and both have data (a WikiPathways metabolite and its ChEBI class), into one node listing
      both; without it they stay linked by an edge.
    - *labels*: a merged node has the classes of each of its sources (wp:Metabolite from
      WikiPathways, owl:Class from ChEBI), which would split one kind of entity into two node
      types. With "role" (the default) it keeps the classes its sources give it other than the
      issuer kinds of *kinds*: the issuer's classes decide identity, the data's own classes are
      its node type, so a metabolite is a Metabolite whether it was merged or not. "majority"
      keeps the classes of the source that most nodes share (fragile: WP4726 has 70 ChEBI
      classes and 67 metabolites); a sequence of class IRIs keeps the first class it holds;
      "all" keeps every class. The other classes become its ``type`` property (references),
      so the RDF is given back.
    """

    merge: bool = True
    kinds: Mapping[str, Iterable[str]] = field(default_factory=dict, compare=False, hash=False)
    attributes: bool = True
    mappings: Iterable[str] | None = None
    exact: bool = False
    labels: str | Sequence[str] = "role"


def _ox_id(term: ox.NamedNode | ox.BlankNode) -> str:
    return f"_:{term.value}" if isinstance(term, ox.BlankNode) else term.value


def _ox_node(node_id: str) -> ox.NamedNode | ox.BlankNode:
    return ox.BlankNode(node_id[2:]) if node_id.startswith("_:") else ox.NamedNode(node_id)


def _quads(source: Source) -> ox.Dataset:
    """Return the statements of an RDFLib graph, an Oxigraph dataset or store, or quads."""
    if isinstance(source, ox.Store):
        return ox.Dataset(source.quads_for_pattern(None, None, None, None))
    if isinstance(source, ox.Dataset):
        return source
    if hasattr(source, "namespace_manager"):  # an RDFLib graph: converted once
        from rdfsolve.local_rdf import to_oxigraph

        return to_oxigraph(cast(Any, source))
    return ox.Dataset(
        q if isinstance(q, ox.Quad) else ox.Quad(q.subject, q.predicate, q.object) for q in source
    )


def _local(iri: str) -> str:
    return re.split(r"[#/:]", iri.rstrip("#/"))[-1] or iri


Names = str | Callable[[str], str] | Mapping[str, str]


class PropertyGraph:
    """A property graph made from RDF, with the gates that check it against that RDF."""

    def __init__(
        self,
        nodes: dict[str, PGNode],
        edges: list[PGEdge],
        *,
        source: ox.Dataset,
        prefixes: Mapping[str, str] | None = None,
        folds: Iterable[Fold] = (),
        fold_counts: list[dict[str, int]] | None = None,
        single: Iterable[str] = (),
        labels: Mapping[str, str] | None = None,
        types: Mapping[str, Conversion | None] | None = None,
        native: bool = True,
        names: Names = "local",
    ) -> None:
        """Hold a built graph; use :meth:`from_rdf` to build one."""
        self.nodes = nodes
        self.edges = edges
        self.source = source
        self.prefixes = dict(prefixes or {})
        self.folds = list(folds)
        self.fold_counts = fold_counts or [{} for _ in self.folds]
        self.single = set(single)
        self.labels = dict(labels or {})
        self.types = {**DEFAULT_TYPES, **(types or {})} if native else {}
        if isinstance(names, str) and names not in NAME_STRATEGIES:
            raise ValueError(f"Use names= one of {NAME_STRATEGIES}, a function, or a mapping")
        self.naming = names
        self.checks: dict[str, Any] = {}
        self.named_graphs: list[str] = []
        sizes: Counter[str] = Counter()
        for item in _items(nodes.values(), edges):
            for key, values in item.properties.items():
                sizes[key] = max(sizes[key], len(values))
        # A key is a list when the schema measured several values or the data has several.
        self.lists = {
            k for k, n in sizes.items() if n > 1 or (self.single and k not in self.single)
        }
        self._name: dict[str, str] | None = None  # IRI (or fold:<n>) -> name
        self._groups: dict[str, list[str]] = {}
        self.identity: dict[str, Any] | None = None  # the decisions of an Identity

    # -- building ---------------------------------------------------------------------------

    @classmethod
    def from_rdf(
        cls,
        graph: Source,
        *,
        schema: MinedSchema | None = None,
        folds: Iterable[Fold] = (),
        types: Mapping[str, Conversion | None] | None = None,
        native: bool = True,
        names: Names = "local",
        prefixes: Mapping[str, str] | None = None,
        identity: Identity | None = None,
        as_attributes: Iterable[str] | None = None,
    ) -> PropertyGraph:
        """Build a property graph from an RDF graph, with the folds applied where they hold.

        *identity* merges the nodes of one identifier and decides how mappings are shown
        (:class:`Identity`); the report records every decision and what stays undecided.
        *as_attributes* are predicates whose IRI values are node attributes (references), not
        edges: by default the class and property hierarchy (rdfs:subClassOf, subPropertyOf), so
        a ChEBI class lists its superclasses; a blank-node value (an OWL restriction) stays an
        edge.

        *types* overrides :data:`DEFAULT_TYPES` per datatype IRI (None keeps the lexical form);
        ``native=False`` keeps every literal as written. *names*: see the module documentation.
        *prefixes* name namespaces for CURIEs (and folds), over those of the schema.
        """
        given = dict(prefixes or {})
        prefixes = {**(dict(schema.get_prefixes()) if schema is not None else {}), **given}
        if hasattr(graph, "namespaces"):
            prefixes.update({p: str(ns) for p, ns in graph.namespaces() if p and p not in prefixes})
        quads = _quads(graph)
        from rdfsolve.ontology.vocabulary import HIERARCHY_PREDICATES

        attribute_predicates = set(HIERARCHY_PREDICATES if as_attributes is None else as_attributes)
        nodes: dict[str, PGNode] = {}
        edges: list[PGEdge] = []
        named: set[str] = set()

        def node(term: ox.NamedNode | ox.BlankNode) -> PGNode:
            key = _ox_id(term)
            if key not in nodes:
                nodes[key] = PGNode(key)
            return nodes[key]

        for quad in sorted(quads, key=str):
            s, p, o = quad.subject, quad.predicate.value, quad.object
            if not isinstance(quad.graph_name, ox.DefaultGraph):
                named.add(str(quad.graph_name))
            if not isinstance(s, (ox.NamedNode, ox.BlankNode)):
                raise ValueError(f"Quoted triples are not supported as subjects: {quad}")
            subject = node(s)
            if p == _TYPE and isinstance(o, ox.NamedNode):
                subject.labels.append(o.value)
            elif isinstance(o, ox.Literal) or (
                p in attribute_predicates and isinstance(o, ox.NamedNode)
            ):
                subject.properties.setdefault(p, []).append(Value.of(o))
            elif isinstance(o, (ox.NamedNode, ox.BlankNode)):
                node(o)
                edges.append(PGEdge(subject.id, p, _ox_id(o)))
            else:
                raise ValueError(f"Quoted triples are not supported as objects: {quad}")
        expanded = [_expand(f, prefixes) for f in folds]
        counts = [_apply_fold(i, f, nodes, edges) for i, f in enumerate(expanded)]
        decisions = _apply_identity(identity, nodes, edges) if identity is not None else None
        built = cls(
            nodes,
            edges,
            source=quads,
            prefixes=prefixes,
            folds=expanded,
            fold_counts=counts,
            single=_single_valued(schema) if schema is not None else (),
            labels=_schema_labels(schema) if schema is not None else {},
            types=types,
            native=native,
            names=names,
        )
        built.named_graphs = sorted(named)
        built.identity = decisions
        return built

    # -- values -----------------------------------------------------------------------------

    def native(self, value: Value) -> Any:
        """Return the native value when its conversion formats back to the lexical form."""
        conversion = self.types.get(value.datatype or "")
        if conversion is None:
            return value.lexical
        try:
            parsed = conversion.parse(value.lexical)
            return parsed if conversion.format(parsed) == value.lexical else value.lexical
        except (ValueError, KeyError, TypeError):
            return value.lexical

    def _typed(self, value: Value) -> bool:
        """Return whether the native value stands for the literal without a datatype beside it."""
        if value.datatype in (None, _STRING):
            return True
        return value.datatype != REFERENCE and not isinstance(self.native(value), str)

    # -- names ------------------------------------------------------------------------------

    def names(self) -> dict[str, dict[str, str]]:
        """Return the names of labels, edge types and keys, each mapped to its IRI.

        A folded edge type maps to ``fold:<n>``, the fold that made it.
        """
        namer = self._namer()
        return {g: {namer[i]: i for i in iris} for g, iris in self._groups.items()}

    def duplicate_names(self) -> dict[str, list[str]]:
        """Return the names that more than one IRI has, within labels, types or keys."""
        namer = self._namer()
        out: dict[str, list[str]] = {}
        for iris in self._groups.values():
            used = Counter(namer[i] for i in iris)
            for iri in iris:
                if used[namer[iri]] > 1:
                    out.setdefault(namer[iri], []).append(iri)
        return out

    def _namer(self) -> dict[str, str]:
        if self._name is None:
            self._name = self._make_names()
        return self._name

    def _make_names(self) -> dict[str, str]:
        labels = {label for n in self.nodes.values() for label in n.labels}
        types = {e.type for e in self.edges if e.fold is None}
        keys = {k for item in _items(self.nodes.values(), self.edges) for k in item.properties}
        iris = labels | types | keys | {f.cls for f in self.folds}
        known = prefix_map(iris, self.prefixes)
        curie = {}
        for iri in iris:
            found = curie_from_prefixes(iri, known)
            curie[iri] = found[0] if found else iri
        strategy = self.naming
        overrides = dict(strategy) if isinstance(strategy, Mapping) else {}
        if callable(strategy):
            first = {iri: strategy(iri) for iri in iris}
        elif strategy == "curie":
            first = curie
        elif strategy == "iri":
            first = {iri: iri for iri in iris}
        elif strategy == "label":
            first = {iri: self.labels.get(iri) or _local(iri) for iri in iris}
        else:
            first = {iri: _local(iri) for iri in iris}
        if not callable(strategy) and strategy not in ("curie", "iri"):
            # Names that clash fall back to the CURIE (never for a function or the IRI).
            used = Counter(first.values())
            first = {iri: n if used[n] == 1 else curie[iri] for iri, n in first.items()}
        name = {**first, **{iri: n for iri, n in overrides.items() if iri in iris}}
        folds = {f"fold:{i}": f.name or name[f.cls] for i, f in enumerate(self.folds)}
        self._groups = {
            "labels": sorted(labels),
            "types": [*sorted(types), *folds],
            "keys": sorted(keys),
        }
        return {**name, **folds}

    def _edge_type(self, edge: PGEdge, namer: Mapping[str, str]) -> str:
        return namer[f"fold:{edge.fold}" if edge.fold is not None else edge.type]

    # -- RDF --------------------------------------------------------------------------------

    def to_oxigraph(self) -> ox.Dataset:
        """Return the statements the property graph stands for, as an Oxigraph dataset."""
        out = ox.Dataset()
        type_ = ox.NamedNode(_TYPE)

        def add(
            subject: ox.NamedNode | ox.BlankNode,
            properties: Properties,
            origins: Mapping[str, list[str]] | None = None,
        ) -> None:
            for key, values in properties.items():
                predicate = ox.NamedNode(key)
                stated = (origins or {}).get(key)
                for i, value in enumerate(values):
                    who = _ox_node(stated[i]) if stated else subject
                    out.add(ox.Quad(who, predicate, value.ox_term()))

        for node in self.nodes.values():
            subject = _ox_node(node.id)
            for i, label in enumerate(node.labels):
                who = _ox_node(node.label_origins[i]) if node.label_origins else subject
                out.add(ox.Quad(who, type_, ox.NamedNode(label)))
            add(subject, node.properties, node.origins)
        for edge in self.edges:
            source = _ox_node(edge.source_iri or edge.source)
            target = _ox_node(edge.target_iri or edge.target)
            if edge.fold is None:
                out.add(ox.Quad(source, ox.NamedNode(edge.type), target))
                continue
            fold, via = self.folds[edge.fold], _ox_node(edge.via or "")
            out.add(ox.Quad(via, type_, ox.NamedNode(fold.cls)))
            out.add(ox.Quad(via, ox.NamedNode(fold.source), source))
            out.add(ox.Quad(via, ox.NamedNode(fold.target), target))
            add(via, edge.properties)
        return out

    # -- gates ------------------------------------------------------------------------------

    def report(self) -> dict[str, Any]:
        """Check the gates: lossless, identity, names, literal types, multiplicity, folds."""
        missing, extra = _difference(self.source, self.to_oxigraph())
        duplicates = self.duplicate_names()
        kinds: dict[str, set[tuple[str | None, bool]]] = defaultdict(set)
        multi: Counter[str] = Counter()
        native = total = 0
        for item in _items(self.nodes.values(), self.edges):
            for key, values in item.properties.items():
                kinds[key].update((v.datatype, v.lang is not None) for v in values)
                if key in self.single and len(values) > 1:
                    multi[key] += 1
                for v in values:
                    total += 1
                    native += v.datatype not in (None, _STRING, REFERENCE) and self._typed(v)
        namer = self._namer()
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "lossless": {
                "passed": not missing and not extra and not self.named_graphs,
                "missing": len(missing),
                "extra": len(extra),
                "examples": sorted(missing)[:3],
                "named_graphs": self.named_graphs,
            },
            "identity": {
                "passed": True,
                "blank_nodes": sum(i.startswith("_:") for i in self.nodes),
                "basis": "IRI, or _: and the blank-node label"
                if self.identity is None
                else "registered identifier (rdfsolve.identifiers), else IRI",
                **(self.identity or {}),
            },
            "names": {
                "passed": not duplicates,
                "strategy": self.naming
                if isinstance(self.naming, str)
                else type(self.naming).__name__,
                "names": len(self._namer()),
                "duplicates": duplicates,
            },
            "literal_types": {
                "passed": True,
                "typed_values": native,
                "values": total,
                "mixed_keys": sorted(namer[k] for k, found in kinds.items() if len(found) > 1),
            },
            "multiplicity": {
                "passed": None if not self.single else not multi,
                "single_valued_keys": len(self.single),
                "violations": {namer[k]: n for k, n in multi.items()},
            },
            "folds": [
                {"fold": self._fold_label(i), **counts} for i, counts in enumerate(self.fold_counts)
            ],
            "checks": self.checks,
        }

    def _fold_label(self, index: int) -> str:
        fold = self.folds[index]
        return f"{_local(fold.cls)}: {_local(fold.source)} -> {_local(fold.target)}"

    # -- networkx and GraphML ---------------------------------------------------------------

    def _plain(self, properties: Properties) -> dict[str, Any]:
        """Return properties as native values: one value, or a list for a multi-valued key.

        A value without a native form keeps its lexical form, with ``<key>__datatype`` beside
        it; ``<key>__lang`` keeps languages.
        """
        namer = self._namer()
        out: dict[str, Any] = {}
        for key, values in properties.items():
            name = namer[key]
            # A value that two IRIs of a merged node both state is shown once.
            values = list(dict.fromkeys(values))
            native = [self.native(v) for v in values]
            out[name] = native if key in self.lists else native[0]
            if not all(self._typed(v) for v in values):
                kinds = [v.datatype or _STRING for v in values]
                out[f"{name}__datatype"] = kinds if key in self.lists else kinds[0]
            if any(v.lang for v in values):
                langs = [v.lang or "" for v in values]
                out[f"{name}__lang"] = langs if key in self.lists else langs[0]
        return out

    def to_networkx(self) -> Any:
        """Return a ``networkx.MultiDiGraph``: node and edge attributes are native values.

        Nodes carry ``labels``; edges are keyed by their type and carry ``type`` (and ``via``,
        the folded node).
        """
        import networkx as nx

        namer = self._namer()
        graph: Any = nx.MultiDiGraph()
        for node in self.nodes.values():
            labels = list(dict.fromkeys(namer[label] for label in node.labels))
            ids = {"ids": list(node.members)} if node.members else {}
            graph.add_node(node.id, labels=labels, **ids, **self._plain(node.properties))
        for edge in self.edges:
            kind = self._edge_type(edge, namer)
            extra = {"via": edge.via} if edge.via else {}
            attrs = {"type": kind, **extra, **self._plain(edge.properties)}
            graph.add_edge(edge.source, edge.target, key=kind, **attrs)
        return graph

    def to_graphml(self, path: str | Path) -> Path:
        """Write GraphML (lists as JSON text, dates as ISO text), read it back and compare."""
        import networkx as nx

        flat = _flatten(self.to_networkx())
        path = Path(path)
        nx.write_graphml(flat, path)
        back = nx.read_graphml(path, force_multigraph=True)
        same = (
            back.number_of_nodes() == flat.number_of_nodes()
            and back.number_of_edges() == flat.number_of_edges()
            and all(dict(back.nodes[n]) == dict(flat.nodes[n]) for n in flat.nodes)
        )
        self.checks["graphml"] = {"passed": same, "path": str(path)}
        if not same:
            raise ValueError(f"GraphML read back differs from what was written: {path}")
        return path

    # -- JSON -------------------------------------------------------------------------------

    def to_json(self) -> dict[str, Any]:
        """Return nodes, edges, names and the report: lossless, for user interfaces and tools.

        Each property value has ``value`` (native where JSON has the type) and, when it has
        one, ``datatype`` and ``lang``.
        """
        namer = self._namer()

        def value(v: Value) -> dict[str, Any]:
            native = self.native(v)
            plain = native if isinstance(native, (str, int, float, bool)) else v.lexical
            out: dict[str, Any] = {"value": plain}
            if v.datatype:
                out["datatype"] = v.datatype
            if v.lang:
                out["lang"] = v.lang
            return out

        def props(properties: Properties) -> dict[str, list[dict[str, Any]]]:
            return {namer[k]: [value(v) for v in values] for k, values in properties.items()}

        return {
            "nodes": [
                {
                    "id": n.id,
                    "labels": list(dict.fromkeys(namer[label] for label in n.labels)),
                    "properties": props(n.properties),
                    **({"ids": list(n.members)} if n.members else {}),
                }
                for n in self.nodes.values()
            ],
            "edges": [
                {
                    "source": e.source,
                    "type": self._edge_type(e, namer),
                    "target": e.target,
                    "properties": props(e.properties),
                    **({"via": e.via} if e.via else {}),
                }
                for e in self.edges
            ],
            "names": self.names(),
            "report": self.report(),
        }

    # -- Neo4j ------------------------------------------------------------------------------

    def to_neo4j(self, folder: str | Path) -> Path:
        """Write ``neo4j-admin database import full`` files, the name map and the command.

        nodes.csv and relationships.csv have typed headers (``key:long``, ``key:date[]``) from
        the conversions; a key whose values are not all of one type is a string. import.sh runs
        the import with an array delimiter that no value contains.
        """
        folder = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        namer = self._namer()
        node_rows = [
            {
                "id:ID": n.id,
                ":LABEL": list(dict.fromkeys(namer[label] for label in n.labels)),
                **({"ids": list(n.members)} if n.members else {}),
                **self._plain(n.properties),
            }
            for n in self.nodes.values()
        ]
        edge_rows = [
            {
                ":START_ID": e.source,
                ":END_ID": e.target,
                ":TYPE": self._edge_type(e, namer),
                **({"via": e.via} if e.via else {}),
                **self._plain(e.properties),
            }
            for e in self.edges
        ]
        texts = [
            str(v)
            for row in node_rows + edge_rows
            for x in row.values()
            for v in (x if isinstance(x, list) else [x])
        ]
        delimiter = next(d for d in (";", "|", "\u001f") if not any(d in t for t in texts))
        _write_neo4j_csv(folder / "nodes.csv", node_rows, delimiter, fixed=("id:ID", ":LABEL"))
        fixed = (":START_ID", ":END_ID", ":TYPE")
        _write_neo4j_csv(folder / "relationships.csv", edge_rows, delimiter, fixed=fixed)
        (folder / "names.json").write_text(json.dumps(self.names(), indent=2))
        (folder / "report.json").write_text(json.dumps(self.report(), indent=2, default=str))
        code = "U+001F" if delimiter == "\u001f" else delimiter
        (folder / "import.sh").write_text(
            "#!/bin/sh\n# Run where neo4j-admin is installed; DATABASE defaults to neo4j.\n"
            'neo4j-admin database import full "${DATABASE:-neo4j}" --overwrite-destination=true '
            f"--array-delimiter='{code}' --nodes=nodes.csv --relationships=relationships.csv\n"
        )
        self.checks["neo4j"] = {"path": str(folder), "array_delimiter": code}
        return folder


# -- helpers ----------------------------------------------------------------------------------


def _items(nodes: Iterable[PGNode], edges: Iterable[PGEdge]) -> list[PGNode | PGEdge]:
    return [*nodes, *edges]


def _difference(source: ox.Dataset, built: ox.Dataset) -> tuple[set[str], set[str]]:
    """Return the statements missing from and added to *built*, blank nodes canonicalized.

    Graph names are not part of a property graph: statements are compared in the default graph,
    and the named graphs of the source are listed in the report.
    """
    canonical = []
    for dataset in (source, built):
        copy = ox.Dataset(ox.Quad(q.subject, q.predicate, q.object) for q in dataset)
        copy.canonicalize(ox.CanonicalizationAlgorithm.RDFC_1_0)
        canonical.append({str(q) for q in copy})
    return canonical[0] - canonical[1], canonical[1] - canonical[0]


def _expand(fold: Fold, prefixes: Mapping[str, str]) -> Fold:
    def iri(text: str) -> str:
        prefix, _, local = text.partition(":")
        if text.startswith(("http://", "https://", "urn:")) or prefix not in prefixes:
            return text
        return prefixes[prefix] + local

    return Fold(iri(fold.cls), iri(fold.source), iri(fold.target), fold.name, fold.evidence)


def _apply_fold(
    index: int, fold: Fold, nodes: dict[str, PGNode], edges: list[PGEdge]
) -> dict[str, int]:
    """Fold the instances where it holds; count applied instances and kept ones by reason."""
    outgoing: dict[str, list[PGEdge]] = defaultdict(list)
    incoming: Counter[str] = Counter()
    for edge in edges:
        if edge.fold is None:
            outgoing[edge.source].append(edge)
            incoming[edge.target] += 1
    counts: Counter[str] = Counter()
    folded: set[int] = set()
    for node in [n for n in nodes.values() if fold.cls in n.labels]:
        out = outgoing[node.id]
        sources = [e for e in out if e.type == fold.source]
        targets = [e for e in out if e.type == fold.target]
        if incoming[node.id]:
            counts["kept: linked to"] += 1
            continue
        if len(sources) != 1 or len(targets) != 1:
            counts["kept: not one source and one target"] += 1
            continue
        properties: Properties = {k: list(v) for k, v in node.properties.items()}
        for edge in out:
            if edge is not sources[0] and edge is not targets[0]:
                properties.setdefault(edge.type, []).append(Value(edge.target, REFERENCE))
        for label in node.labels:
            if label != fold.cls:
                properties.setdefault(_TYPE, []).append(Value(label, REFERENCE))
        edges.append(
            PGEdge(
                sources[0].target,
                f"fold:{index}",
                targets[0].target,
                properties,
                via=node.id,
                fold=index,
            )
        )
        folded.update(id(e) for e in out)
        del nodes[node.id]
        counts["applied"] += 1
    edges[:] = [e for e in edges if id(e) not in folded]
    return dict(counts)


def _merge_nodes(nodes: dict[str, PGNode], members: Iterable[str], keep: str) -> None:
    """Merge *members* into the node *keep*, keeping the IRI that stated each label and value."""
    node = nodes[keep]
    labels: list[str] = []
    label_origins: list[str] = []
    properties: Properties = {}
    origins: dict[str, list[str]] = {}
    iris: list[str] = []
    for member in [keep, *sorted(set(members) - {keep})]:
        part = nodes[member]
        labels += part.labels
        label_origins += part.label_origins or [member] * len(part.labels)
        for k, values in part.properties.items():
            properties.setdefault(k, []).extend(values)
            origins.setdefault(k, []).extend(part.origins.get(k) or [member] * len(values))
        iris += part.members or [member]
        if member != keep:
            del nodes[member]
    node.labels, node.label_origins = labels, label_origins
    node.properties, node.origins = properties, origins
    node.members = sorted(set(iris))


def _keep(nodes: dict[str, PGNode], members: Iterable[str], preferred: str | None) -> str:
    """Return the member that a merge keeps: most data, then the preferred IRI, then by IRI."""

    def weight(member: str) -> tuple[int, bool, str]:
        node = nodes[member]
        data = len(node.labels) + sum(len(v) for v in node.properties.values())
        return (data, member == preferred, member)

    return max(members, key=weight)


def _retarget(nodes: dict[str, PGNode], edges: list[PGEdge], canonical: Mapping[str, str]) -> int:
    """Point edges at merged nodes; a statement within one merged node becomes its property."""
    internal = 0
    kept: list[PGEdge] = []
    for edge in edges:
        source = canonical.get(edge.source, edge.source)
        target = canonical.get(edge.target, edge.target)
        if source != edge.source and edge.source_iri is None:
            edge.source_iri = edge.source
        if target != edge.target and edge.target_iri is None:
            edge.target_iri = edge.target
        edge.source, edge.target = source, target
        written_source = edge.source_iri or source
        written_target = edge.target_iri or target
        if edge.fold is None and source == target and written_source != written_target:
            _add_value(nodes[source], edge.type, Value(written_target, REFERENCE), written_source)
            internal += 1
            continue
        kept.append(edge)
    edges[:] = kept
    return internal


def _apply_identity(
    identity: Identity, nodes: dict[str, PGNode], edges: list[PGEdge]
) -> dict[str, Any]:
    """Merge the nodes of one identifier and decide each mapping; return the decisions."""
    from rdfsolve.identifiers import parse
    from rdfsolve.mappings.declared import is_declared_property

    parsed = {nid: parse(nid) for nid in nodes if not nid.startswith("_:")}
    groups: dict[str, list[str]] = defaultdict(list)
    if identity.merge:
        for nid, found in parsed.items():
            if found is not None:
                groups[found.curie].append(nid)
    canonical: dict[str, str] = {}
    merged: list[dict[str, Any]] = []
    for key, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        keep = _keep(nodes, members, cast(Any, parsed[members[0]]).iri())
        _merge_nodes(nodes, members, keep)
        canonical.update(dict.fromkeys(members, keep))
        merged.append({"identifier": key, "node": keep, "iris": sorted(members)})
    internal = _retarget(nodes, edges, canonical)

    chosen = set(identity.mappings) if identity.mappings is not None else None
    issued = {prefix: set(classes) for prefix, classes in identity.kinds.items()}
    incoming: Counter[str] = Counter(
        target for _, target in {(e.source, e.target) for e in edges if e.fold is None}
    )
    has_out = {e.source for e in edges}
    per_link: Counter[tuple[str, str]] = Counter(
        (e.source, e.type) for e in edges if e.fold is None
    )
    decisions: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    removed: set[int] = set()
    same: list[tuple[str, str]] = []
    for edge in edges:
        if edge.fold is not None:
            continue
        if not (edge.type in chosen if chosen is not None else is_declared_property(edge.type)):
            continue  # not a predicate that can state that two identifiers are one entity
        to = parsed.get(edge.target_iri or edge.target)
        if to is None or edge.source == edge.target:
            continue  # not a link to an identifier, or a statement of a node about itself
        start = parsed.get(edge.source_iri or edge.source)
        kinds_from = issued.get(start.prefix) if start else None
        kinds_to = issued.get(to.prefix)
        target_node = nodes.get(edge.target)
        bare = (
            target_node is not None
            and not target_node.labels
            and not target_node.properties
            and edge.target not in has_out
        )
        one_to_one = per_link[(edge.source, edge.type)] == 1 and incoming[edge.target] == 1
        if start is None:
            decision = "edge: undecided, the source is not a registered identifier"
        elif kinds_from is None or kinds_to is None:
            unknown = sorted(
                {p for p, k in ((start.prefix, kinds_from), (to.prefix, kinds_to)) if k is None}
            )
            decision = f"edge: undecided, kind unknown for {', '.join(unknown)}"
        elif not kinds_from & kinds_to:
            decision = "edge: a relation between two kinds"
        elif start.prefix == to.prefix and start.curie != to.curie:
            decision = "edge: two identifiers of one namespace are two entities"
        elif not one_to_one:
            decision = "edge: same kind, not one to one"
        elif bare and identity.attributes:
            decision = "attribute: same kind, one to one"
            _add_value(
                nodes[edge.source],
                edge.type,
                Value(edge.target_iri or edge.target, REFERENCE),
                edge.source_iri or edge.source,
            )
            del nodes[edge.target]
            removed.add(id(edge))
        elif identity.exact:
            decision = "merged: same kind, one to one (exact)"
            same.append((edge.source, edge.target))
        else:
            decision = "edge: same kind, one to one; merged with Identity(exact=True)"
        group = (edge.type, start.prefix if start else "-", to.prefix, decision)
        row = decisions.setdefault(
            group,
            {
                "predicate": edge.type,
                "from": group[1],
                "to": group[2],
                "decision": decision,
                "links": 0,
                "example": [edge.source_iri or edge.source, edge.target_iri or edge.target],
            },
        )
        row["links"] += 1
    edges[:] = [e for e in edges if id(e) not in removed]
    exact = 0
    refused = 0
    if same:
        root: dict[str, str] = {}

        def find(n: str) -> str:
            while root.get(n, n) != n:
                n = root[n]
            return n

        for left, right in same:
            root[find(left)] = find(right)
        clusters: dict[str, list[str]] = defaultdict(list)
        for n in {n for pair in same for n in pair}:
            clusters[find(n)].append(n)
        found_in: dict[str, str] = {}
        for members in clusters.values():
            iris = [iri for m in members for iri in (nodes[m].members or [m])]
            ids = {found.curie: found.prefix for iri in iris if (found := parse(iri))}
            if len(set(ids.values())) < len(ids):
                refused += 1  # a merge never joins two identifiers of one namespace
                continue
            keep = _keep(nodes, members, None)
            _merge_nodes(nodes, members, keep)
            found_in.update(dict.fromkeys(members, keep))
            exact += 1
        internal += _retarget(nodes, edges, found_in)
    issuer_classes = {c for classes in issued.values() for c in classes}
    relabelled = _reconcile_labels(nodes, identity.labels, issuer_classes)
    rows = sorted(decisions.values(), key=lambda r: (-r["links"], r["predicate"]))
    undecided = [r for r in rows if "undecided" in r["decision"]]
    for row in undecided:
        missing = [p for p in (row["from"], row["to"]) if p != "-" and p not in issued]
        row["decide_with"] = (
            "Identity(kinds={" + ", ".join(repr(p) + ": [...]" for p in missing) + "})"
        )
    return {
        "merged": len(merged),
        "merged_iris": sum(len(m["iris"]) for m in merged),
        "merged_examples": merged[:5],
        "merged_exact": exact,
        "exact_refused": refused,
        "statements_within_merged_nodes": internal,
        "kinds": {prefix: sorted(classes) for prefix, classes in sorted(issued.items())},
        "mappings": rows,
        "undecided": len(undecided),
        "labels": relabelled,
    }


def _reconcile_labels(
    nodes: dict[str, PGNode], policy: str | Sequence[str], issuer_classes: Iterable[str] = ()
) -> dict[str, Any]:
    """Give each merged node the classes of one of its sources; the others become ``type``."""
    if policy == "all":
        return {"policy": "all", "nodes": 0}
    graph_labels = [set(n.labels) for n in nodes.values()]
    moved = 0
    kept: Counter[str] = Counter()
    ties = 0
    for node in nodes.values():
        if not node.members or not node.label_origins:
            continue
        by_source: dict[str, set[str]] = defaultdict(set)
        for label, origin in zip(node.labels, node.label_origins, strict=True):
            by_source[origin].add(label)
        candidates = {frozenset(found) for found in by_source.values() if found}
        if len(candidates) < 2:
            continue
        if policy == "role":
            roles = [c for c in candidates if not c & set(issuer_classes)]
            if len(roles) != 1:
                ties += 1
                continue
            chosen = roles[0]
        elif isinstance(policy, str):
            support = {c: sum(c <= labels for labels in graph_labels) for c in candidates}
            best = max(support.values())
            top = [c for c in candidates if support[c] == best]
            if len(top) > 1:
                ties += 1
                continue
            chosen = top[0]
        else:
            preferred = next((c for cls in policy for c in candidates if cls in c), None)
            if preferred is None:
                continue
            chosen = preferred
        labels, origins = [], []
        for label, origin in zip(node.labels, node.label_origins, strict=True):
            if label in chosen:
                labels.append(label)
                origins.append(origin)
            else:
                _add_value(node, _TYPE, Value(label, REFERENCE), origin)
                moved += 1
        node.labels, node.label_origins = labels, origins
        kept[" ".join(sorted(chosen))] += 1
    return {
        "policy": policy if isinstance(policy, str) else list(policy),
        "nodes": sum(kept.values()),
        "kept_classes": dict(kept),
        "classes_moved_to_type": moved,
        "ties_kept_all": ties,
        "basis": "the data's own classes; issuer kinds go to type" if policy == "role" else "",
    }


def _add_value(node: PGNode, key: str, value: Value, written: str) -> None:
    """Add a value to a node, with the IRI that stated it when the node is a merge."""
    node.properties.setdefault(key, []).append(value)
    if node.members:
        stated = node.origins.setdefault(key, [node.id] * (len(node.properties[key]) - 1))
        stated.append(written)


def _single_valued(schema: MinedSchema) -> set[str]:
    """Return the predicates measured with one value per subject in every pattern."""
    seen: dict[str, bool] = {}
    for pattern in schema.patterns:
        if pattern.count is None or pattern.distinct_subjects is None:
            seen[pattern.property_uri] = False
            continue
        one = pattern.count == pattern.distinct_subjects
        seen[pattern.property_uri] = seen.get(pattern.property_uri, True) and one
    return {p for p, one in seen.items() if one}


def _schema_labels(schema: MinedSchema) -> dict[str, str]:
    """Return the labels of the classes and properties of the mined schema."""
    labels: dict[str, str] = {}
    for p in schema.patterns:
        for iri, label in (
            (p.subject_class, p.subject_label),
            (p.property_uri, p.property_label),
            (p.object_class, p.object_label),
        ):
            if label and iri not in labels:
                labels[iri] = label
    return labels


def suggest_folds(schema: MinedSchema) -> list[Fold]:
    """Propose folds from the mined schema: classes with two links that every instance has once.

    A link qualifies when, for its class, every pattern counts one value per subject, every
    instance has it (when the class size is known), and it does not point to a container (many
    instances sharing few objects, such as a pathway). Each pair of qualifying links is proposed,
    with what was measured as evidence; a person or a model chooses.
    """
    sizes = schema.about.class_entity_counts or {}
    links: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for p in schema.patterns:
        if p.object_class in _SENTINEL_OBJECTS or p.property_uri == _TYPE:
            continue
        entry = links[p.subject_class].setdefault(
            p.property_uri, {"functional": True, "subjects": 0, "objects": 0, "classes": []}
        )
        entry["functional"] &= p.count is not None and p.count == p.distinct_subjects
        entry["subjects"] = max(entry["subjects"], p.distinct_subjects or 0)
        entry["objects"] = max(entry["objects"], p.distinct_objects or 0)
        entry["classes"].append(p.object_class)
    folds: list[Fold] = []
    for cls, by_property in sorted(links.items()):
        size = sizes.get(cls)
        good = sorted(
            prop
            for prop, e in by_property.items()
            if e["functional"]
            and (size is None or e["subjects"] == size)
            and not e["objects"] * 2 < e["subjects"]
        )
        for source, target in combinations(good, 2):
            evidence = {
                "instances": size,
                "source": by_property[source],
                "target": by_property[target],
                "other_links": sorted(set(by_property) - {source, target}),
            }
            folds.append(Fold(cls, source, target, evidence=evidence))
    return folds


def _flatten(graph: Any) -> Any:
    """Return a copy for GraphML: lists as JSON text, other non-scalar values as text."""
    flat = graph.copy()

    def scalar(value: Any) -> Any:
        if isinstance(value, list):
            return json.dumps(
                [v if isinstance(v, (str, int, float, bool)) else str(v) for v in value]
            )
        return value if isinstance(value, (str, int, float, bool)) else str(value)

    for _, attrs in flat.nodes(data=True):
        attrs.update({k: scalar(v) for k, v in attrs.items()})
    for *_, attrs in flat.edges(keys=True, data=True):
        attrs.update({k: scalar(v) for k, v in attrs.items()})
    return flat


def _neo4j_type(values: list[Any]) -> str:
    kinds = {type(v) for v in values}
    if kinds == {bool}:
        return "boolean"
    if kinds == {int}:
        return "long"
    if kinds and kinds <= {int, float}:
        return "double"
    if kinds == {date}:
        return "date"
    return "string"


def _write_neo4j_csv(
    path: Path, rows: list[dict[str, Any]], delimiter: str, *, fixed: tuple[str, ...]
) -> None:
    columns: dict[str, list[Any]] = defaultdict(list)
    arrays: set[str] = set()
    for row in rows:
        for key, value in row.items():
            if isinstance(value, list):
                arrays.add(key)
                columns[key].extend(value)
            else:
                columns[key].append(value)
    keys = [k for k in fixed if k in columns] + sorted(k for k in columns if k not in fixed)
    header = {}
    for key in keys:
        if key in fixed:
            header[key] = key
        else:
            kind = _neo4j_type(columns[key])
            header[key] = f"{key}:{kind}[]" if key in arrays else f"{key}:{kind}"
    strings = {k for k in keys if header[k].split(":")[-1].startswith("string")}

    def cell(key: str, value: Any) -> str:
        items = value if isinstance(value, list) else [value]
        if key in strings:
            return delimiter.join(str(v) for v in items)
        return delimiter.join(
            ("true" if v else "false") if isinstance(v, bool) else str(v) for v in items
        )

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([header[k] for k in keys])
        for row in rows:
            writer.writerow([cell(k, row[k]) if k in row else "" for k in keys])
