"""Property graphs from RDF: nodes and edges with typed properties, checked against the RDF.

An RDF graph becomes a property graph by rules that can be undone:

- a subject (IRI or blank node) is a node, identified by its IRI or by ``_:`` and its blank-node
  label; its ``rdf:type`` values are its labels;
- a literal value is a node property; its lexical form, datatype and language are kept;
- a link to another resource is an edge, whose type is the predicate, when the RDF describes that
  resource (it has a class or a value); a resource the RDF only cites (a UniProt cross-reference
  to InterPro, a superclass) is a reference value of the node that cites it, not a node;
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
On a merged node the record of the source that issues the identifier speaks for it: where it
states a key (the name of ChEBI 15377 is "water"), the values of the other sources are kept
beside it under the key and the source (``label_wikipathways``: "2 H2O", "oxidized thioredoxin").

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
# A key that holds the values a source states beside those of the issuer: "<predicate> @<source>".
_BESIDE = " @"


def _predicate(key: str) -> str:
    """Return the predicate IRI of a property key."""
    return key.partition(_BESIDE)[0]


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
    them in ``members`` (and those of another kind in ``apart``); ``origins`` gives, for each key, the IRI that stated each value, and
    ``label_origins`` the IRI that stated each label, so that the statements can be given back.
    """

    id: str
    labels: list[str] = field(default_factory=list)
    properties: Properties = field(default_factory=dict)
    members: list[str] = field(default_factory=list)
    origins: dict[str, list[str]] = field(default_factory=dict)
    label_origins: list[str] = field(default_factory=list)
    # Identifiers of another kind that a source drew the node with (WikiPathways draws an
    # enzyme with its Ensembl gene id): listed in members for the round trip, shown apart.
    apart: list[str] = field(default_factory=list)


@dataclass
class PGEdge:
    """An edge: its type (a predicate IRI, or the fold that made it) and properties.

    ``via`` is the id of the folded node, which :meth:`PropertyGraph.to_oxigraph` recreates.
    A *derived* edge (two proteins of one complex) states nothing of its own: it is kept out
    of the RDF that the graph gives back.
    """

    source: str
    type: str
    target: str
    properties: Properties = field(default_factory=dict)
    via: str | None = None
    fold: int | None = None
    source_iri: str | None = None  # the IRI written in the RDF, when a merge changed the end
    target_iri: str | None = None
    derived: bool = False  # made by a fold of pairs; not a statement, so not given back
    # Values a fold attached (the enzyme of a catalysis), not given back as RDF; and the
    # nodes it absorbed to attach them (the catalysis), which are given back.
    attached: Properties = field(default_factory=dict)
    absorbed: list[PGNode] = field(default_factory=list)
    # Further statements this edge stands for, as (source, target) IRIs written in the RDF:
    # two drawings of one entity, merged, each linked to the same node: one edge, two statements.
    also: list[tuple[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class Fold:
    """Turn each instance of *cls* into an edge from its *source* value to its *target* value.

    IRIs or CURIEs of the schema. *name* names the edges (default: named as the class).
    *evidence* is what the mined schema measured, when the fold was suggested. With *each*, an
    instance with several sources or targets (a conversion of two substrates into two products)
    gives an edge for each source and target, all keeping the instance (via).

    A fold of pairs (:meth:`pairs`) instead links each pair of the instance's *members* (the
    proteins of a complex) with a derived edge, and keeps the instance: it is an entity with
    data of its own. Each fold is written as a SPARQL CONSTRUCT over one endpoint
    (:meth:`to_construct`) and as a SHACL rule (:meth:`to_shacl`), so the operation can be
    logged and run again.
    """

    cls: str
    source: str
    target: str
    name: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict, compare=False, hash=False)
    members: str | None = None
    among: str | None = None
    predicate: str | None = None
    dataset: str | None = None
    each: bool = False
    attach_to: str | None = None

    @classmethod
    def attach(
        cls,
        of: str,
        value: str,
        to: str,
        *,
        name: str | None = None,
        target_class: str | None = None,
    ) -> Fold:
        """Return a fold that puts the *value* of each instance of *of* on the edges of a folded
        node: a wp:Catalysis's enzyme (wp:source) on the edges its reaction (wp:target) was
        folded into, as ``catalyzed_by``.

        The instance is absorbed into those edges (given back as RDF); an instance whose
        target was not folded stays a node. *target_class* (wp:Conversion) is the class of the
        folded targets, which the fold's CONSTRUCT keeps.
        """
        return cls(
            of,
            value,
            to,
            name or f"{_local(value)}_of_{_local(of)}",
            attach_to=to,
            among=target_class,
        )

    @classmethod
    def pairs(
        cls,
        of: str,
        members: str,
        *,
        among: str | None = None,
        name: str | None = None,
        predicate: str | None = None,
    ) -> Fold:
        """Return a fold that links each pair of members of each instance of *of*.

        *members* is the property from the instance to its members (wp:participants of a
        Complex); *among* keeps the members of this class (wp:Protein); *predicate* is the
        target schema's IRI of the relation (biolink:in_complex_with).
        """
        return cls(
            of,
            members,
            members,
            name or f"same_{_local(of)}",
            members=members,
            among=among,
            predicate=predicate,
        )

    def relation(self) -> str:
        """Return the IRI of the relation the fold's edges stand for.

        The target schema's IRI when the fold has one (*predicate*); otherwise one of rdfsolve,
        under the dataset: https://w3id.org/rdfsolve/pg/schemas/<dataset>/<label>.
        """
        from rdfsolve.config import mint

        if self.predicate:
            return self.predicate
        return mint("pg", "schemas", self.dataset or "local", self.name or _local(self.cls))

    def to_construct(self, focus: Iterable[str] = ()) -> str:
        """Return the SPARQL CONSTRUCT that gives the fold's edges from one endpoint.

        *focus* limits it to these instances (VALUES); a fold of pairs gives each pair once.
        """
        values = " ".join(f"<{iri}>" for iri in focus)
        scope = f"VALUES ?x {{ {values} }}\n  " if values else ""
        if self.attach_to is not None:
            return (
                f"CONSTRUCT {{ ?t <{self.relation()}> ?s }}\nWHERE {{\n  {scope}"
                f"?x a <{self.cls}> ; <{self.source}> ?s ; <{self.attach_to}> ?t ."
                + (f"\n  ?t a <{self.among}> ." if self.among else "")
                + "\n}"
            )
        if self.members is None:
            return (
                f"CONSTRUCT {{ ?s <{self.relation()}> ?t }}\nWHERE {{\n  {scope}"
                f"?x a <{self.cls}> ; <{self.source}> ?s ; <{self.target}> ?t .\n}}"
            )
        kind = f"\n  ?a a <{self.among}> . ?b a <{self.among}> ." if self.among else ""
        return (
            f"CONSTRUCT {{ ?a <{self.relation()}> ?b }}\nWHERE {{\n  {scope}"
            f"?x a <{self.cls}> ; <{self.members}> ?a, ?b .{kind}\n"
            "  FILTER(STR(?a) < STR(?b))\n}"
        )

    def to_shacl(self, provenance: Mapping[str, str | None] | None = None) -> str:
        """Return the fold as a SHACL node shape with a sh:TripleRule (SHACL-AF), in Turtle.

        The shape targets the class; the rule's subject and object are node expressions on the
        paths (filtered by the class of the members, for a fold of pairs). *provenance*
        (:func:`provenance`) says which release of the source and which version of the target
        schema the rule is for, and what made it: as comments, and as triples on the shape
        (prov:wasDerivedFrom, pav:createdWith, dcterms:conformsTo).
        """
        from rdfsolve.config import mint

        def nodes(path: str) -> str:
            """Return the node expression of the values of *path*, filtered by *among*."""
            if self.members is not None and self.among:
                return (
                    f"[ sh:filterShape [ sh:class <{self.among}> ] ; "
                    f"sh:nodes [ sh:path <{path}> ] ]"
                )
            return f"[ sh:path <{path}> ]"

        info = {k: v for k, v in (provenance or {}).items() if v}
        comments = "".join(f"# {k}: {v}\n" for k, v in info.items())
        about = ""
        if info.get("source description"):
            about += f"  prov:wasDerivedFrom <{info['source description']}> ;\n"
        if info.get("generated with"):
            about += f'  pav:createdWith "{info["generated with"]}" ;\n'
        if info.get("target"):
            version = (
                f' ; pav:version "{info["target version"]}"' if info.get("target version") else ""
            )
            about += f'  dcterms:conformsTo [ dcterms:title "{info["target"]}"{version} ] ;\n'
        return (
            comments
            + "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
            + "@prefix prov: <http://www.w3.org/ns/prov#> .\n"
            + "@prefix pav: <http://purl.org/pav/> .\n"
            + "@prefix dcterms: <http://purl.org/dc/terms/> .\n"
            f"<{mint('fold', self.name or _local(self.cls))}> a sh:NodeShape ;\n"
            + about
            + f"  sh:targetClass <{self.cls}> ;\n"
            "  sh:rule [ a sh:TripleRule ;\n"
            f"    sh:subject {nodes(self.source)} ;\n"
            f"    sh:predicate <{self.relation()}> ;\n"
            f"    sh:object {nodes(self.target)} ] .\n"
        )


def provenance(
    schema: MinedSchema, *, target: str | None = None, target_version: str | None = None
) -> dict[str, str | None]:
    """Return what a conversion rule is for and what made it, from the mined schema's metadata.

    The source release is the one the metadata miner read (VoID, version or issued date); when
    the source states none, the snapshot the schema was mined from says when, and the release
    is written as unknown. *target* and *target_version* name the property-graph schema the
    rule converts to (Biolink Model, 4.4.5).
    """
    from importlib.metadata import PackageNotFoundError, version

    about = schema.about
    try:
        made = f"rdfsolve {version('rdfsolve')}"
    except PackageNotFoundError:
        made = about.generated_by
    release = about.source_version or about.source_issued or about.source_modified
    snapshot = about.retrieved_at or about.generated_at
    return {
        "source": about.title or about.dataset_name,
        "source release": str(release)
        if release
        else f"not stated by the source; schema mined from a snapshot of {snapshot}",
        "source endpoint": about.endpoint,
        "source description": about.void_uri,
        "target": target,
        "target version": target_version,
        "generated with": made,
    }


class UpstreamWarning(UserWarning):
    """Something the graph shows to be wrong in a source (see PropertyGraph.findings)."""


@dataclass
class Finding:
    """A disagreement that points upstream: what the source states, what the issuer states.

    *shape* (SHACL) and *query* (SPARQL SELECT on the source's endpoint) find every node of
    the same pattern in the source, when the node sits in a fold (an enzyme as the source of
    a catalysis); otherwise they are None and the finding stands for the node alone.
    """

    kind: str
    node: str
    source: str
    stated: list[str]
    issuer: list[str]
    pattern: str
    shape: str | None = None
    query: str | None = None

    def __str__(self) -> str:
        """Say the finding in one sentence, with where to look for the others."""
        where = " Every such node in the source: Finding.query (SPARQL) or .shape (SHACL)."
        return (
            f"{self.source} names {self.node} {self.stated!r}; its issuer names it "
            f"{self.issuer!r} ({self.pattern}).{where if self.query else ''}"
        )


RARE = 0.05  # a place in a pattern that a source uses for fewer of its values than this is rare

# Properties that give a name or a synonym of a term (the issuer's names to compare with).
NAME_PROPERTIES = (
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
    "http://www.w3.org/2004/02/skos/core#altLabel",
    "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym",
    "http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym",
    "http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym",
    "http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym",
    "http://purl.uniprot.org/core/mnemonic",
)


def _normal(name: str) -> str:
    """Return a name for comparison: compatibility form, lower case, no leading count, no
    punctuation ("2 H₂O" and "h2o" agree).
    """
    import unicodedata

    text = unicodedata.normalize("NFKC", name).casefold().strip()
    text = re.sub(r"^\d+\s+", "", text)
    return re.sub(r"[^0-9a-z]+", "", text)


def _agrees(name: str, names: Iterable[str]) -> bool:
    """Return whether a name agrees with one of *names* (equal, or one holds the other)."""
    mine = _normal(name)
    for other in names:
        theirs = _normal(other)
        if (
            mine
            and theirs
            and (
                mine == theirs
                or (min(len(mine), len(theirs)) >= 4 and (mine in theirs or theirs in mine))
            )
        ):
            return True
    return False


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
    - *same* are pairs of IRIs decided to name one entity (Decision.pairs: the claims that a
      decision accepted); they are merged, unless their kinds are known and differ.
      *variants* are pairs of identifiers of one namespace that name one entity in another
      form (tautomers in ChEBI); only these may share a node with each other.
    - *labels*: a merged node has the classes of each of its sources (wp:Metabolite from
      WikiPathways, owl:Class from ChEBI), which would split one kind of entity into two node
      types. With "issuer" (the default) a node whose identifier's issuer gives a kind of
      entity has that kind as its node type, however each source drew it, and the sources'
      classes become its ``type``; where the issuer's class names no kind (a generic class
      such as owl:Class) it is "role". With "role" it
      keeps the classes its sources give it other than the issuer kinds of *kinds*: the
      issuer's classes decide identity, the data's own classes are its node type, so a
      metabolite is a Metabolite whether it was merged or not. "majority"
      keeps the classes of the source that most nodes share (fragile: WP4726 has 70 ChEBI
      classes and 67 metabolites); a sequence of class IRIs keeps the first class it holds;
      "all" keeps every class. The other classes become its ``type`` property (references),
      so the RDF is given back.
    - A decided pair of two known kinds (an Ensembl gene id and a UniProt accession, once
      *kinds* says Ensembl ids name genes) is one node only when the record of one kind is at
      hand and the other is an identifier a source drew it with: the node is the issuer's
      (the protein) and the other identifier is kept apart, as an attribute named by its
      prefix (``ensembl``), not as one of its ids. *unfold* names prefixes whose identifiers
      kept apart become nodes again, with what their source states about them (the gene, its
      NCBI Gene id, and the link to the protein); their classes are their kinds.
      A merge across a prefix of unknown kind is made and reported, with what would decide it.
    """

    merge: bool = True
    kinds: Mapping[str, Iterable[str]] = field(default_factory=dict, compare=False, hash=False)
    attributes: bool = True
    mappings: Iterable[str] | None = None
    exact: bool = False
    labels: str | Sequence[str] = "issuer"
    same: Iterable[tuple[str, str]] = field(default=(), compare=False, hash=False)
    variants: Iterable[tuple[str, str]] = field(default=(), compare=False, hash=False)
    unfold: Iterable[str] = field(default=(), compare=False, hash=False)

    @classmethod
    def of(cls, *clients: Any, decision: Any = None, **options: Any) -> Identity:
        """Return an Identity with the kinds that *clients* issue and the pairs a decision
        accepted (rdfsolve.mappings.claims.Decision), merged as one entity each.
        """
        kinds: dict[str, list[str]] = {}
        for client in clients:
            kinds.update(client.issued_kinds())
        if decision is not None:
            options.setdefault("same", decision.pairs())
            options.setdefault("variants", decision.variants)
        return cls(kinds={**kinds, **options.pop("kinds", {})}, **options)


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
        self.cited: dict[str, Any] = {}  # resources the RDF only cites, kept as values
        self.specific: dict[str, Any] = {}  # superclasses moved from labels to the type property
        self.attached_pairs: dict[int, set[tuple[str, str]]] = {}  # attach folds: (target, value)
        # How often the sources use each (class, property, class): what is rare in a source.
        self.patterns: Counter[tuple[str, str, str]] = Counter()
        sizes: Counter[str] = Counter()
        for item in _items(nodes.values(), edges):
            for key, values in item.properties.items():
                sizes[key] = max(sizes[key], len(values))
        for edge in edges:
            for key, values in edge.attached.items():
                sizes[key] = max(sizes[key], len(values), 2)  # several enzymes are common
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
        schema: MinedSchema | Sequence[MinedSchema] | None = None,
        folds: Iterable[Fold] = (),
        types: Mapping[str, Conversion | None] | None = None,
        native: bool = True,
        names: Names = "local",
        prefixes: Mapping[str, str] | None = None,
        identity: Identity | None = None,
        as_attributes: Iterable[str] | None = None,
        hierarchy: Mapping[str, Iterable[str]] | None = None,
    ) -> PropertyGraph:
        """Build a property graph from an RDF graph, with the folds applied where they hold.

        *identity* merges the nodes of one identifier and decides how mappings are shown
        (:class:`Identity`); the report records every decision and what stays undecided.
        *as_attributes* are predicates whose IRI values are node attributes (references), not
        edges: by default the class and property hierarchy (rdfs:subClassOf, subPropertyOf), so
        a ChEBI class lists its superclasses; a blank-node value (an OWL restriction) stays an
        edge. Whatever the predicate, a resource the RDF only cites is a reference value.
        *schema* is the mined schema, or the schemas of each source of the RDF.

        *hierarchy* gives the superclasses of classes (Client.superclasses): a node's labels are
        then its most specific stated classes (wp:Protein, not also wp:GeneProduct and
        wp:DataNode), whatever superclasses a record happens to state; the others go to its
        ``type`` property, so the RDF is given back.

        *types* overrides :data:`DEFAULT_TYPES` per datatype IRI (None keeps the lexical form);
        ``native=False`` keeps every literal as written. *names*: see the module documentation.
        *prefixes* name namespaces for CURIEs (and folds), over those of the schema.
        """
        given = dict(prefixes or {})
        schemas: list[MinedSchema] = (
            [] if schema is None else list(schema) if isinstance(schema, Sequence) else [schema]
        )
        prefixes = {}
        for each in reversed(schemas):
            prefixes.update(dict(each.get_prefixes()))
        prefixes.update(given)
        if hasattr(graph, "namespaces"):
            prefixes.update({p: str(ns) for p, ns in graph.namespaces() if p and p not in prefixes})
        quads = _quads(graph)
        from rdfsolve.ontology.vocabulary import HIERARCHY_PREDICATES

        attribute_predicates = set(HIERARCHY_PREDICATES if as_attributes is None else as_attributes)
        nodes: dict[str, PGNode] = {}
        edges: list[PGEdge] = []
        named: set[str] = set()

        def node(term: ox.NamedNode | ox.BlankNode) -> PGNode:
            """Return the node of a term, made on first use."""
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
            elif (
                isinstance(o, ox.Literal)
                or (p in attribute_predicates and isinstance(o, ox.NamedNode))
                or o == s  # a statement about itself (a ChEBI id mapped to itself) is a value
            ):
                subject.properties.setdefault(p, []).append(Value.of(o))
            elif isinstance(o, (ox.NamedNode, ox.BlankNode)):
                node(o)
                edges.append(PGEdge(subject.id, p, _ox_id(o)))
            else:
                raise ValueError(f"Quoted triples are not supported as objects: {quad}")
        expanded = [_expand(f, prefixes) for f in folds]
        absorbable = _absorbable(expanded, nodes, edges)
        counts: list[dict[str, int]] = [{} for _ in expanded]
        attached_pairs: dict[int, set[tuple[str, str]]] = {}
        for i, fold in enumerate(expanded):
            if fold.attach_to is None:
                counts[i] = _apply_fold(i, fold, nodes, edges, ignore=absorbable)
        for i, fold in enumerate(expanded):
            if fold.attach_to is not None:
                counts[i], attached_pairs[i] = _apply_attach(i, fold, nodes, edges)
        decisions = _apply_identity(identity, nodes, edges) if identity is not None else None
        cited = _cited_as_values(nodes, edges)
        specific = _most_specific(nodes, hierarchy) if hierarchy else {}
        built = cls(
            nodes,
            edges,
            source=quads,
            prefixes=prefixes,
            folds=expanded,
            fold_counts=counts,
            single=set().union(*(_single_valued(each) for each in schemas)),
            labels={k: v for each in reversed(schemas) for k, v in _schema_labels(each).items()},
            types=types,
            native=native,
            names=names,
        )
        built.named_graphs = sorted(named)
        built.identity = decisions
        built.cited = cited
        built.specific = specific
        built.attached_pairs = attached_pairs
        _attached_to_nodes(nodes, edges)
        for each in schemas:
            for pattern in each.patterns:
                key = (pattern.subject_class, pattern.property_uri, pattern.object_class)
                built.patterns[key] += pattern.count or 0
        return built

    @classmethod
    def from_results(cls, *results: Any, identity: Any = None, **options: Any) -> PropertyGraph:
        """Build a property graph from result sets of one or more clients.

        The records of each client are exported together (with the links between them) and
        named with each client's schema. *folds* are by default those that the schemas suggest
        and the records bear out (:func:`suggested_folds`); "suggested" in a list of folds
        stands for them (``folds=["suggested", Fold.pairs(...)]``). *hierarchy* is by default
        read from each client's endpoint (Client.superclasses); None leaves labels as stated. *identity* is an :class:`Identity`, or a decision
        (rdfsolve.mappings.claims.Decision), read with the kinds these clients issue; the
        options of an Identity (``kinds``, ``unfold``, ``labels``) can be given here too.
        """
        from dataclasses import fields

        settings = {f.name for f in fields(Identity)}
        chosen = {k: options.pop(k) for k in list(options) if k in settings}
        clients: list[Any] = []
        for found in results:
            if not any(found.client is c for c in clients):
                clients.append(found.client)
        records = ox.Dataset()
        for client in clients:
            for quad in client.to_oxigraph(*[r for r in results if r.client is client]):
                records.add(quad)
        folds = options.get("folds", "suggested")
        given = [folds] if isinstance(folds, str) else list(folds)
        if "suggested" in given:
            found = suggested_folds([c.schema for c in clients], records)
            given = [f for g in given for f in (found if g == "suggested" else [g])]
        # A fold without a dataset takes the one whose schema has the folded class.
        from dataclasses import replace

        def source_of(fold: Fold) -> str | None:
            """Return the dataset name of the client whose schema has the fold's class."""
            for client in clients:
                if any(p.subject_class == fold.cls for p in client.schema.patterns):
                    return str(client.schema.about.dataset_name or "") or None
            return None

        options["folds"] = [
            f if not isinstance(f, Fold) or f.dataset else replace(f, dataset=source_of(f))
            for f in given
        ]
        if identity is not None and not isinstance(identity, Identity):
            identity = Identity.of(*clients, decision=identity, **chosen)
        elif chosen:
            raise ValueError(f"Give {sorted(chosen)} to the Identity, or a decision as identity")
        options.setdefault("schema", [c.schema for c in clients])
        if options.get("hierarchy", "sources") == "sources":
            stated = {q.object.value for q in records if q.predicate.value == _TYPE}
            hierarchy: dict[str, set[str]] = defaultdict(set)
            for client in clients:
                for found, above in client.superclasses(sorted(stated)).items():
                    hierarchy[found] |= above
            options["hierarchy"] = dict(hierarchy)
        return cls.from_rdf(records, identity=identity, **options)

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
        keys |= {k for edge in self.edges for k in edge.attached}
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
            # Names that clash fall back to the CURIE (never for a function or the IRI). Labels,
            # edge types and keys are separate in a property graph: a clash is within one of them
            # (an edge type wp:source and a key dc:source are both "source").
            clashing = set()
            for group in (labels, types | {f.cls for f in self.folds}, keys):
                used = Counter(first[i] for i in group)
                clashing |= {i for i in group if used[first[i]] > 1}
            first = {iri: curie[iri] if iri in clashing else n for iri, n in first.items()}
        name = {**first, **{iri: n for iri, n in overrides.items() if iri in iris}}
        for key in keys:
            base, _, source = key.partition(_BESIDE)
            if source:
                name[key] = f"{name.get(base) or _local(base)}_{source}"
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
            """Add the statements of properties, each from the IRI that stated it."""
            for key, values in properties.items():
                predicate = ox.NamedNode(_predicate(key))
                stated = (origins or {}).get(key)
                for i, value in enumerate(values):
                    who = _ox_node(stated[i]) if stated else subject
                    out.add(ox.Quad(who, predicate, value.ox_term()))

        for node in self.nodes.values():
            subject = _ox_node(node.id)
            for i, label in enumerate(node.labels):
                if node.label_origins and node.label_origins[i] == _UNSTATED:
                    continue  # a kind, not a statement
                who = _ox_node(node.label_origins[i]) if node.label_origins else subject
                out.add(ox.Quad(who, type_, ox.NamedNode(label)))
            add(subject, node.properties, node.origins)
        for edge in self.edges:
            if edge.derived:
                continue  # made by a fold of pairs: no statement of its own
            source = _ox_node(edge.source_iri or edge.source)
            target = _ox_node(edge.target_iri or edge.target)
            if edge.fold is None:
                out.add(ox.Quad(source, ox.NamedNode(edge.type), target))
                for start, end in edge.also:
                    out.add(ox.Quad(_ox_node(start), ox.NamedNode(edge.type), _ox_node(end)))
                continue
            for node in edge.absorbed:
                for label in node.labels:
                    out.add(ox.Quad(_ox_node(node.id), type_, ox.NamedNode(label)))
                add(_ox_node(node.id), node.properties)
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
        node_types = Counter(
            " + ".join(sorted({namer[label] for label in n.labels})) or "(no class)"
            for n in self.nodes.values()
        )
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "node_types": dict(node_types.most_common()),
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
            "cited": self.cited,
            "most_specific_labels": self.specific,
            "checks": self.checks,
        }

    def _fold_label(self, index: int) -> str:
        fold = self.folds[index]
        if fold.members is not None:
            among = f" ({_local(fold.among)})" if fold.among else ""
            return f"{_local(fold.cls)}: pairs of {_local(fold.members)}{among}"
        return f"{_local(fold.cls)}: {_local(fold.source)} -> {_local(fold.target)}"

    def findings(self, *, warn: bool = True) -> list[Finding]:
        """Return the disagreements between a source's names and the issuer's, node by node.

        On a merged node, each name the source gives (``label`` beside the issuer's, see
        Identity) is compared with the issuer's names and synonyms; a name that agrees with none
        is a finding: an enzyme that WikiPathways draws as a metabolite with a ChEBI id is named
        "Esterase" by WikiPathways and "arecoline hydrobromide" by ChEBI. A name equal to one of
        the node's own identifiers agrees (a gene symbol, hgnc.symbol:HMGCR). When the node is
        an end of a folded edge in a place the source rarely uses for its class (the mined
        schema: 31 metabolites against 3,484 gene products as source of a catalysis; under
        :data:`RARE`), the finding carries the SHACL shape and the SPARQL query that find every
        node in that place with that class in the source. With *warn*, each finding is also issued as an UpstreamWarning.
        """
        import warnings

        found: list[Finding] = []
        for node in self.nodes.values():
            for key in [k for k in node.properties if _BESIDE in k]:
                predicate, _, source = key.partition(_BESIDE)
                if predicate not in NAME_PROPERTIES:
                    continue
                from rdfsolve.identifiers import parse

                names = [v.lexical for k in NAME_PROPERTIES for v in node.properties.get(k, [])]
                # A name that is one of the node's own identifiers agrees (hgnc.symbol:HMGCR).
                names += [
                    read.local
                    for values in node.properties.values()
                    for v in values
                    if v.datatype == REFERENCE and (read := parse(v.lexical))
                ]
                stated = [v.lexical for v in node.properties[key]]
                wrong = [n for n in stated if not _agrees(n, names)]
                if not wrong or not names:
                    continue
                origins = node.origins.get(key) or []
                member = origins[0] if origins else node.id
                classes = [
                    label
                    for label, origin in zip(node.labels, node.label_origins or [], strict=False)
                    if origin == member
                ] or list(node.labels)
                finding = Finding(
                    "name",
                    node.id,
                    source,
                    wrong,
                    [v.lexical for v in node.properties.get(predicate, [])] or names[:3],
                    f"{', '.join(_local(c) for c in classes)} in {source}",
                )
                places = [
                    (
                        self.folds[edge.fold],
                        self.folds[edge.fold].source
                        if edge.source == node.id
                        else self.folds[edge.fold].target,
                    )
                    for edge in self.edges
                    if edge.fold is not None
                    and not edge.derived
                    and node.id in (edge.source, edge.target)
                ]
                # A node a fold attached to edges (the enzyme of a catalysis) is in that place.
                relations = {f.relation(): f for f in self.folds if f.attach_to is not None}
                places += [
                    (relations[key], relations[key].source)
                    for edge in self.edges
                    for key, values in edge.attached.items()
                    if key in relations and any(v.lexical == node.id for v in values)
                ]
                for fold, link in places:
                    finding.pattern = (
                        f"{_local(classes[0])} as {_local(link)} of a {_local(fold.cls)}"
                    )
                    # Only a place the source rarely uses is worth a query for all such nodes.
                    used = sum(
                        n for (c, p, _), n in self.patterns.items() if (c, p) == (fold.cls, link)
                    )
                    here = self.patterns.get((fold.cls, link, classes[0]), 0)
                    if used and here / used >= RARE:
                        break
                    finding.pattern += (
                        f", rare in {source}: {here} of {used}" if used else ", how rare unknown"
                    )
                    finding.query = (
                        f"SELECT DISTINCT ?instance ?node ?name WHERE {{\n"
                        f"  ?instance a <{fold.cls}> ; <{link}> ?node .\n"
                        f"  ?node a <{classes[0]}> .\n"
                        f"  OPTIONAL {{ ?node <http://www.w3.org/2000/01/rdf-schema#label> ?name }}\n}}"
                    )
                    finding.shape = (
                        "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
                        f"[] a sh:NodeShape ; sh:targetClass <{fold.cls}> ;\n"
                        f"  sh:property [ sh:path <{link}> ; sh:not [ sh:class <{classes[0]}> ] ;\n"
                        f'    sh:message "a {_local(classes[0])} as {_local(link)} of a '
                        f'{_local(fold.cls)}" ] .\n'
                    )
                    break
                found.append(finding)
                if warn:
                    warnings.warn(str(finding), UpstreamWarning, stacklevel=2)
        return found

    def fold_edges(self, index: int) -> set[tuple[str, str]]:
        """Return the ends of the edges of fold *index*, as the RDF wrote them."""
        if self.folds[index].attach_to is not None:
            return set(self.attached_pairs.get(index, set()))
        return {
            (edge.source_iri or edge.source, edge.target_iri or edge.target)
            for edge in self.edges
            if edge.fold == index
        }

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

    def to_networkx(self, edge_types: Iterable[str] | None = None) -> Any:
        """Return a ``networkx.MultiDiGraph``: node and edge attributes are native values.

        With *edge_types* (edge type names: "catalyzes"), only those edges and their nodes.
        Each node also carries ``category`` (its node types joined by " + ", or "no category")
        and ``title`` (its name: a name or label property, else the end of its IRI), for
        drawing.

        Nodes carry ``labels`` (and ``ids``); edges carry ``type`` (and ``via``, the folded node)
        and are keyed by both: two folded nodes give two edges between the same ends. A property named as one of these (the other classes of
        a folded catalysis, rdf:type) is kept as ``rdf_<name>``.
        """
        import networkx as nx

        namer = self._namer()
        wanted = set(edge_types) if edge_types is not None else None
        edges = [e for e in self.edges if wanted is None or self._edge_type(e, namer) in wanted]
        kept = {end for e in edges for end in (e.source, e.target)} if wanted is not None else None
        graph: Any = nx.MultiDiGraph()
        for node in self.nodes.values():
            if kept is not None and node.id not in kept:
                continue
            labels = list(dict.fromkeys(namer[label] for label in node.labels))
            graph.add_node(
                node.id,
                labels=labels,
                category=" + ".join(labels) or "no category",
                title=_title(node),
                **_ids(node),
                **_aside(self._plain(node.properties)),
            )
        for edge in edges:
            kind = self._edge_type(edge, namer)
            extra: dict[str, Any] = {"via": edge.via} if edge.via else {}
            if edge.derived:
                extra["derived"] = True
            attrs = {
                "type": kind,
                **extra,
                **_aside(self._plain(edge.properties)),
                **_aside(self._plain(edge.attached)),
            }
            key = f"{kind} {edge.via}" if edge.via else kind
            graph.add_edge(edge.source, edge.target, key=key, **attrs)
        return graph

    def to_pg_schema(self, name: str = "graph") -> str:
        """Return the graph's types in PG-Schema (Angles et al., SIGMOD 2023), as S3PG writes it.

        A node type for each combination of labels, with its properties (OPTIONAL when some
        nodes of the type lack it; ARRAY when a key holds several values); an edge type for
        each edge type, once, with the node types it joins (a union when several). The graph
        type is LOOSE: it describes the
        graph that was built, it does not close it.
        """
        namer = self._namer()

        def type_name(labels: Iterable[str]) -> str:
            """Return the PG-Schema type name of a combination of labels."""
            words = sorted({re.sub(r"\W+", "", namer[label]) for label in labels}) or ["Node"]
            return words[0][0].lower() + "".join(words)[1:] + "Type"

        def content(values: list[Any], key: str) -> str:
            """Return the PG-Schema content type of the values of one key."""
            kind = {"long": "INT64", "double": "FLOAT64", "boolean": "BOOL", "date": "DATE"}
            found = kind.get(_neo4j_type(values), "STRING")
            return f"{found} ARRAY" if key in self.lists else found

        def properties(items: list[PGNode] | list[PGEdge]) -> str:
            """Return the properties of a type, with OPTIONAL for keys some items lack."""
            values: dict[str, list[Any]] = defaultdict(list)
            present: Counter[str] = Counter()
            for item in items:
                for key, found in item.properties.items():
                    values[key] += [self.native(v) for v in found]
                    present[key] += 1
            parts = []
            for key in sorted(values, key=lambda k: namer[k]):
                optional = "" if present[key] == len(items) else "OPTIONAL "
                word = re.sub(r"\W+", "_", namer[key])
                parts.append(f"{optional}{word} {content(values[key], key)}")
            return " { " + ", ".join(parts) + " }" if parts else ""

        by_type: dict[str, list[PGNode]] = defaultdict(list)
        node_type: dict[str, str] = {}
        labels_of: dict[str, list[str]] = {}
        for node in self.nodes.values():
            found = type_name(node.labels)
            by_type[found].append(node)
            node_type[node.id] = found
            labels_of[found] = sorted({namer[label] for label in node.labels})
        lines = [
            f"  ({t}: {' & '.join(labels_of[t]) or 'Node'}{properties(items)})"
            for t, items in sorted(by_type.items())
        ]
        by_edge: dict[str, list[PGEdge]] = defaultdict(list)
        for edge in self.edges:
            by_edge[self._edge_type(edge, namer)].append(edge)
        for kind, items in sorted(by_edge.items()):
            sources = " | ".join(
                f":{t}" for t in sorted({node_type.get(e.source, "nodeType") for e in items})
            )
            targets = " | ".join(
                f":{t}" for t in sorted({node_type.get(e.target, "nodeType") for e in items})
            )
            word = re.sub(r"\W+", "", kind)
            word = word.lower() if word.isupper() else word[0].lower() + word[1:]
            lines.append(f"  ({sources})-[{word}Type: {kind}{properties(items)}]->({targets})")
        return f"CREATE GRAPH TYPE {name}Type LOOSE {{\n" + ",\n".join(lines) + "\n}\n"

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
            """Return a value as JSON: native where JSON has the type."""
            native = self.native(v)
            plain = native if isinstance(native, (str, int, float, bool)) else v.lexical
            out: dict[str, Any] = {"value": plain}
            if v.datatype:
                out["datatype"] = v.datatype
            if v.lang:
                out["lang"] = v.lang
            return out

        def props(properties: Properties) -> dict[str, list[dict[str, Any]]]:
            """Return properties as JSON, by name."""
            return {namer[k]: [value(v) for v in values] for k, values in properties.items()}

        return {
            "nodes": [
                {
                    "id": n.id,
                    "labels": list(dict.fromkeys(namer[label] for label in n.labels)),
                    "properties": props(n.properties),
                    **_ids(n),
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
                    **({"derived": True} if e.derived else {}),
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
                **_ids(n),
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
                **({"derived": True} if e.derived else {}),
                **self._plain(e.properties),
                **self._plain(e.attached),
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


_RESERVED = ("type", "via", "labels", "ids", "derived", "category", "title")


def _ids(node: PGNode) -> dict[str, Any]:
    """Return the ids of a merged node, and the identifiers of another kind by prefix."""
    from rdfsolve.identifiers import parse

    if not node.members:
        return {}
    out: dict[str, Any] = {"ids": [m for m in node.members if m not in node.apart]}
    for iri in node.apart:
        read = parse(iri)
        key = read.prefix if read else "apart"
        out[key] = [*out[key], iri] if key in out else iri
    return out


_TITLE_PROPERTIES = ("https://w3id.org/biolink/vocab/name", *NAME_PROPERTIES)


def _title(node: PGNode) -> str:
    """Return a node's name for drawing: a name or label property, else the end of its IRI."""
    for key in _TITLE_PROPERTIES:
        for value in node.properties.get(key, []):
            return str(value.lexical)
    return re.split(r"[/#]", node.id.rstrip("/"))[-1]


def _aside(properties: dict[str, Any]) -> dict[str, Any]:
    """Return properties with those named as a reserved attribute renamed ``rdf_<name>``."""
    return {
        f"rdf_{k}" if k.partition("__")[0] in _RESERVED else k: v for k, v in properties.items()
    }


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
        """Return the IRI of a CURIE, or the text."""
        prefix, _, local = text.partition(":")
        if text.startswith(("http://", "https://", "urn:")) or prefix not in prefixes:
            return text
        return prefixes[prefix] + local

    return Fold(
        iri(fold.cls),
        iri(fold.source),
        iri(fold.target),
        fold.name,
        fold.evidence,
        iri(fold.members) if fold.members else None,
        iri(fold.among) if fold.among else None,
        iri(fold.predicate) if fold.predicate else None,
        fold.dataset,
        fold.each,
        iri(fold.attach_to) if fold.attach_to else None,
    )


def _absorbable(folds: list[Fold], nodes: dict[str, PGNode], edges: list[PGEdge]) -> set[str]:
    """Return the instances that an attach fold will absorb: one target, a value, no links in."""
    linked = {e.target for e in edges}
    out: set[str] = set()
    for fold in folds:
        if fold.attach_to is None:
            continue
        for node in nodes.values():
            if fold.cls not in node.labels or node.id in linked:
                continue
            mine = [e for e in edges if e.source == node.id]
            if sum(e.type == fold.attach_to for e in mine) == 1 and any(
                e.type == fold.source for e in mine
            ):
                out.add(node.id)
    return out


def _apply_attach(
    index: int, fold: Fold, nodes: dict[str, PGNode], edges: list[PGEdge]
) -> tuple[dict[str, int], set[tuple[str, str]]]:
    """Put each instance's values on the edges its target was folded into; absorb it there."""
    by_via: dict[str, list[PGEdge]] = defaultdict(list)
    for edge in edges:
        if edge.fold is not None and edge.via:
            by_via[edge.via].append(edge)
    counts: Counter[str] = Counter()
    pairs: set[tuple[str, str]] = set()
    gone: set[int] = set()
    for node in [n for n in nodes.values() if fold.cls in n.labels]:
        out = [e for e in edges if e.source == node.id and e.fold is None]
        targets = [e for e in out if e.type == fold.attach_to]
        values = [e for e in out if e.type == fold.source]
        folded = by_via.get(targets[0].target, []) if len(targets) == 1 else []
        if not values or not folded:
            counts["kept: its target was not folded"] += 1
            continue
        absorbed = PGNode(
            node.id, list(node.labels), {k: list(v) for k, v in node.properties.items()}
        )
        for edge in out:
            absorbed.properties.setdefault(edge.type, []).append(Value(edge.target, REFERENCE))
        for edge in folded:
            edge.attached.setdefault(fold.relation(), []).extend(
                Value(v.target, REFERENCE) for v in values
            )
            edge.absorbed.append(absorbed)
        pairs |= {(targets[0].target, v.target) for v in values}
        gone.update(id(e) for e in out)
        del nodes[node.id]
        counts["applied"] += 1
    edges[:] = [e for e in edges if id(e) not in gone]
    return dict(counts), pairs


def _attached_to_nodes(nodes: dict[str, PGNode], edges: list[PGEdge]) -> None:
    """Point attached values at the nodes they became (a merged enzyme's UniProt IRI)."""
    node_of = {member: n.id for n in nodes.values() for member in (n.members or [n.id])}
    for edge in edges:
        for key, values in edge.attached.items():
            edge.attached[key] = list(
                dict.fromkeys(Value(node_of.get(v.lexical, v.lexical), REFERENCE) for v in values)
            )


def _apply_pairs(
    index: int, fold: Fold, nodes: dict[str, PGNode], edges: list[PGEdge]
) -> dict[str, int]:
    """Link each pair of members of each instance with a derived edge; keep the instance."""
    members: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        if edge.fold is None and edge.type == fold.members:
            member = nodes.get(edge.target)
            if member is not None and (fold.among is None or fold.among in member.labels):
                members[edge.source].add(edge.target)
    counts: Counter[str] = Counter()
    for node in [n for n in nodes.values() if fold.cls in n.labels]:
        found = sorted(members.get(node.id, ()))
        if len(found) < 2:
            counts["kept: fewer than two members"] += 1
            continue
        for a, b in combinations(found, 2):
            edges.append(PGEdge(a, f"fold:{index}", b, via=node.id, fold=index, derived=True))
            counts["pairs"] += 1
        counts["applied"] += 1
    return dict(counts)


def _apply_fold(
    index: int,
    fold: Fold,
    nodes: dict[str, PGNode],
    edges: list[PGEdge],
    ignore: set[str] = frozenset(),  # type: ignore[assignment]
) -> dict[str, int]:
    """Fold the instances where it holds; count applied instances and kept ones by reason.

    Links from *ignore* (instances an attach fold will absorb, such as the catalysis of a
    reaction) do not keep an instance from folding.
    """
    if fold.members is not None:
        return _apply_pairs(index, fold, nodes, edges)
    outgoing: dict[str, list[PGEdge]] = defaultdict(list)
    incoming: Counter[str] = Counter()
    for edge in edges:
        if edge.derived or edge.source in ignore:
            continue
        incoming[edge.target] += 1  # also an edge an earlier fold made (a catalysis of it)
        if edge.fold is None:
            outgoing[edge.source].append(edge)
    counts: Counter[str] = Counter()
    folded: set[int] = set()
    for node in [n for n in nodes.values() if fold.cls in n.labels]:
        out = outgoing[node.id]
        sources = [e for e in out if e.type == fold.source]
        targets = [e for e in out if e.type == fold.target]
        if incoming[node.id]:
            counts["kept: linked to"] += 1
            continue
        if not sources or not targets or (not fold.each and (len(sources) > 1 or len(targets) > 1)):
            counts["kept: not one source and one target"] += 1
            continue
        properties: Properties = {k: list(v) for k, v in node.properties.items()}
        for edge in out:
            if edge not in sources and edge not in targets:
                properties.setdefault(edge.type, []).append(Value(edge.target, REFERENCE))
        for label in node.labels:
            if label != fold.cls:
                properties.setdefault(_TYPE, []).append(Value(label, REFERENCE))
        for start in sources:
            for end in targets:
                edges.append(
                    PGEdge(
                        start.target,
                        f"fold:{index}",
                        end.target,
                        {k: list(v) for k, v in properties.items()},
                        via=node.id,
                        fold=index,
                    )
                )
        folded.update(id(e) for e in out)
        del nodes[node.id]
        counts["applied"] += 1
    edges[:] = [e for e in edges if id(e) not in folded]
    return dict(counts)


def _most_specific(
    nodes: dict[str, PGNode], hierarchy: Mapping[str, Iterable[str]]
) -> dict[str, Any]:
    """Keep each node's most specific stated classes as labels; move the others to ``type``."""
    above = {cls: set(found) for cls, found in hierarchy.items()}
    moved: Counter[str] = Counter()
    for node in nodes.values():
        stated = set(node.labels)
        general = {c for c in stated if any(c in above.get(other, ()) for other in stated)}
        if not general:
            continue
        origins = node.label_origins or [node.id] * len(node.labels)
        labels, kept_origins = [], []
        for label, origin in zip(node.labels, origins, strict=True):
            if label in general:
                _add_value(node, _TYPE, Value(label, REFERENCE), origin)
                moved[label] += 1
            else:
                labels.append(label)
                kept_origins.append(origin)
        node.labels = labels
        node.label_origins = kept_origins if node.label_origins else []
    return {
        "moved_to_type": dict(moved.most_common()),
        "basis": "the source's own class hierarchy (rdfs:subClassOf)",
    }


def _cited_as_values(nodes: dict[str, PGNode], edges: list[PGEdge]) -> dict[str, Any]:
    """Make each resource that the RDF only cites a reference value of the node citing it.

    A resource is described when it has a class, a value, or a link of its own; one that is
    only the object of links (an InterPro entry that a UniProt record cites) is not a node.
    The value keeps the IRI as written, so the statement is given back.
    """
    starts = {e.source for e in edges}
    folded = {end for e in edges if e.fold is not None for end in (e.source, e.target)}
    bare = {
        nid
        for nid, node in nodes.items()
        if not node.labels and not node.properties and nid not in starts and nid not in folded
    }
    by_predicate: Counter[str] = Counter()
    kept: list[PGEdge] = []
    for edge in edges:
        if edge.fold is None and edge.target in bare:
            statements = [(edge.source_iri or edge.source, edge.target_iri or edge.target)]
            for written, cited in [*statements, *edge.also]:
                _add_value(nodes[edge.source], edge.type, Value(cited, REFERENCE), written)
                by_predicate[edge.type] += 1
            continue
        kept.append(edge)
    edges[:] = kept
    removed = sorted(bare)
    for nid in removed:
        del nodes[nid]
    return {
        "resources": len(removed),
        "links": sum(by_predicate.values()),
        "by_predicate": dict(by_predicate.most_common()),
        "basis": "cited, not described: no class, value or link of its own",
    }


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
        """Return the rank of a member: data, then preferred, then IRI."""
        node = nodes[member]
        data = len(node.labels) + sum(len(v) for v in node.properties.values())
        return (data, member == preferred, member)

    return max(members, key=weight)


def _collapse(edges: list[PGEdge]) -> int:
    """Make edges that merges made the same (ends, type, properties) one edge; return how many
    were folded in. The edge keeps each statement's IRIs as written (``also``).
    """
    first: dict[tuple[str, str, str, str], PGEdge] = {}
    kept: list[PGEdge] = []
    for edge in edges:
        if edge.fold is not None or edge.derived or edge.via:
            kept.append(edge)
            continue
        key = (edge.source, edge.type, edge.target, repr(sorted(edge.properties.items())))
        if key not in first:
            first[key] = edge
            kept.append(edge)
            continue
        same = first[key]
        written = (edge.source_iri or edge.source, edge.target_iri or edge.target)
        if written != (same.source_iri or same.source, same.target_iri or same.target):
            same.also.append(written)
    folded_in = len(edges) - len(kept)
    edges[:] = kept
    return folded_in


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
        if edge.derived and source == target:
            continue  # two members merged into one entity: no pair
        written_source = edge.source_iri or source
        written_target = edge.target_iri or target
        if edge.fold is None and source == target:
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
    # A decided pair names identifiers as a source wrote them (identifiers.org/chebi/...); its
    # node is the node of that identifier, whatever IRI the node has.
    by_identifier: dict[str, str] = {}
    for nid, node in nodes.items():
        for iri in node.members or [nid]:
            if (found := parse(iri)) is not None:
                by_identifier.setdefault(found.curie, nid)

    def node_of(iri: str) -> str | None:
        """Return the node of an IRI, or of its identifier."""
        if iri in nodes:
            return iri
        found = parse(iri)
        return by_identifier.get(found.curie) if found else canonical.get(iri)

    for left, right in identity.same:
        a, b = node_of(left), node_of(right)
        if a is None or b is None or a == b:
            continue
        same.append((a, b))  # two kinds: decided per cluster, by the records at hand
    allowed = {
        frozenset((found_a.curie, found_b.curie))
        for a, b in identity.variants
        if (found_a := parse(a)) and (found_b := parse(b))
    }
    exact = 0
    refused = 0
    apart = 0
    if same:
        root: dict[str, str] = {}

        def find(n: str) -> str:
            """Return the root of a node."""
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
            by_prefix: dict[str, list[str]] = defaultdict(list)
            for key, prefix in ids.items():
                by_prefix[prefix].append(key)
            if any(not _joined(keys, allowed) for keys in by_prefix.values() if len(keys) > 1):
                refused += 1  # never two identifiers of one namespace, but declared variants
                continue
            # The issuer's record names the node (UniProt's IRI for a protein that WikiPathways
            # draws with an Ensembl gene id), else the member with most data.
            issuers = [
                m
                for m in members
                if (read := parse(m)) and set(nodes[m].labels) & issued.get(read.prefix, set())
            ]
            kinds_known = {frozenset(issued[p]) for p in by_prefix if p in issued}
            issuer_kinds = {frozenset(issued[cast(Any, parse(m)).prefix]) for m in issuers}
            if _disjoint(kinds_known) and len(issuer_kinds) != 1:
                refused += 1  # two kinds, and not one record with an identifier of the other
                continue
            keep = min(issuers) if issuers else _keep(nodes, members, None)
            _merge_nodes(nodes, members, keep)
            found_in.update(dict.fromkeys(members, keep))
            if _disjoint(kinds_known):
                kind = next(iter(issuer_kinds))
                nodes[keep].apart = sorted(
                    iri
                    for iri in nodes[keep].members
                    if (read := parse(iri))
                    and read.prefix in issued
                    and not issued[read.prefix] & kind
                )
                apart += len(nodes[keep].apart)
            exact += 1
        internal += _retarget(nodes, edges, found_in)
    beside = _issuer_speaks(nodes, issued)
    relabelled = _reconcile_labels(nodes, identity.labels, issued)
    unfolded = _unfold(nodes, edges, set(identity.unfold), issued)
    collapsed = _collapse(edges)
    # A node that joins several namespaces, one of which no source here gives a kind.
    unknown_kinds: Counter[str] = Counter()
    for node in nodes.values():
        spaces = {read.prefix for iri in node.members if (read := parse(iri))}
        if len(spaces) > 1:
            unknown_kinds.update(p for p in spaces if p not in issued)
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
        "issuer_values": beside,
        "kept_apart": apart,
        "edges_made_one": collapsed,
        "unfolded": unfolded,
        "merged_with_unknown_kind": {
            "identifiers": dict(unknown_kinds.most_common()),
            "decide_with": "Identity(kinds={"
            + ", ".join(repr(p) + ": [...]" for p in sorted(unknown_kinds))
            + "})"
            if unknown_kinds
            else "",
            "basis": "merged as decided; no source here says what these identifiers name",
        },
    }


_UNSTATED = ""  # the origin of a class that no statement gives (the kind of an unfolded node)


def _disjoint(kinds: set[frozenset[str]]) -> bool:
    """Return whether two of the kinds share no class."""
    return any(not a & b for a, b in combinations(kinds, 2))


def _unfold(
    nodes: dict[str, PGNode],
    edges: list[PGEdge],
    prefixes: set[str],
    issued: Mapping[str, set[str]],
) -> dict[str, Any]:
    """Make the identifiers kept apart of *prefixes* nodes again, with what is stated of them.

    The values their IRI states move to the new node; a link from it to the node it was kept
    apart from (BridgeDb's gene to protein link) becomes an edge; the classes the IRI states
    stay with the node it was drawn as (their role), and the new node has its kind.
    """
    from rdfsolve.identifiers import parse

    made = 0
    for node in list(nodes.values()):
        for iri in list(node.apart):
            read = parse(iri)
            if read is None or read.prefix not in prefixes:
                continue
            kinds = sorted(issued.get(read.prefix, ()))
            other = PGNode(iri, labels=kinds, label_origins=[_UNSTATED] * len(kinds))
            for key in list(node.properties):
                stated = node.origins.get(key) or [node.id] * len(node.properties[key])
                kept: list[Value] = []
                kept_origins: list[str] = []
                for value, origin in zip(node.properties[key], stated, strict=True):
                    if origin != iri:
                        kept.append(value)
                        kept_origins.append(origin)
                    elif (
                        value.datatype == REFERENCE
                        and value.lexical in node.members
                        and value.lexical != iri  # the gene naming itself stays its value
                    ):
                        target = None if value.lexical == node.id else value.lexical
                        edges.append(PGEdge(iri, _predicate(key), node.id, target_iri=target))
                    else:
                        other.properties.setdefault(_predicate(key), []).append(value)
                if kept:
                    node.properties[key], node.origins[key] = kept, kept_origins
                else:
                    node.properties.pop(key)
                    node.origins.pop(key, None)
            for edge in edges:
                if edge.fold is None and edge.source == node.id and edge.source_iri == iri:
                    edge.source, edge.source_iri = iri, None
            node.apart.remove(iri)
            node.members.remove(iri)
            nodes[iri] = other
            made += 1
    return {"nodes": made, "prefixes": sorted(prefixes)}


def _issuer_speaks(nodes: dict[str, PGNode], issued: Mapping[str, set[str]]) -> dict[str, Any]:
    """On a merged node, let the issuer's record speak for each key it states.

    The issuer's record is the member whose classes are those its source gives the identifiers
    it issues (the ChEBI class of chebi:15377). Where it states a key, the values of the other
    members move beside it, to the key and their source (WikiPathways' labels of water: "2
    H2O", "oxidized thioredoxin"), so the node has the issuer's name and nothing is lost.
    """
    from rdfsolve.identifiers import parse

    source_of = {c: prefix for prefix, classes in issued.items() for c in classes}
    moved: Counter[str] = Counter()
    for node in nodes.values():
        if not node.members or not node.label_origins:
            continue
        classes: dict[str, set[str]] = defaultdict(set)
        for label, origin in zip(node.labels, node.label_origins, strict=True):
            classes[origin].add(label)
        issuers = {
            member
            for member, found in classes.items()
            if (read := parse(member)) and found & issued.get(read.prefix, set())
        }
        if not issuers:
            continue
        for key in list(node.properties):
            stated = node.origins.get(key) or [node.id] * len(node.properties[key])
            if not any(origin in issuers for origin in stated):
                continue
            keep: list[Value] = []
            keep_origins: list[str] = []
            for value, origin in zip(node.properties[key], stated, strict=True):
                if origin in issuers:
                    keep.append(value)
                    keep_origins.append(origin)
                    continue
                source = next(
                    (source_of[c] for c in sorted(classes.get(origin, ())) if c in source_of),
                    "other",
                )
                aside = f"{key}{_BESIDE}{source}"
                node.properties.setdefault(aside, []).append(value)
                node.origins.setdefault(aside, []).append(origin)
                moved[aside] += 1
            node.properties[key], node.origins[key] = keep, keep_origins
    return {
        "values_beside_the_issuer": dict(moved.most_common()),
        "basis": "the record of the source that issues the identifier names the node",
    }


# Classes that say an identifier names a class or a concept, not what kind of entity it is.
_GENERIC_CLASSES = frozenset(
    {
        "http://www.w3.org/2002/07/owl#Class",
        "http://www.w3.org/2002/07/owl#NamedIndividual",
        "http://www.w3.org/2002/07/owl#Thing",
        "http://www.w3.org/2000/01/rdf-schema#Class",
        "http://www.w3.org/2000/01/rdf-schema#Resource",
        "http://www.w3.org/2004/02/skos/core#Concept",
    }
)


def _reconcile_labels(
    nodes: dict[str, PGNode],
    policy: str | Sequence[str],
    issued: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    """Give each merged node the classes of one of its sources; the others become ``type``."""
    if policy == "all":
        return {"policy": "all", "nodes": 0}
    graph_labels = [set(n.labels) for n in nodes.values()]
    moved = 0
    kept: Counter[str] = Counter()
    ties = by_issuer = 0
    for node in nodes.values():
        if not node.members or not node.label_origins:
            continue
        by_source: dict[str, set[str]] = defaultdict(set)
        for label, origin in zip(node.labels, node.label_origins, strict=True):
            by_source[origin].add(label)
        candidates = {frozenset(found) for found in by_source.values() if found}
        if len(candidates) < 2:
            continue
        if policy in ("issuer", "role"):
            # The issuer kinds of this node's own identifiers (CAS and ChEBI for a metabolite),
            # not those of every source (WikiPathways issues its DataNode).
            from rdfsolve.identifiers import parse

            own = {found.prefix for iri in node.members if (found := parse(iri))}
            issuer_classes = {c for prefix in own for c in (issued or {}).get(prefix, ())}
            named = {c for c in candidates if c & issuer_classes and not c <= _GENERIC_CLASSES}
            roles = [c for c in candidates if not c & issuer_classes]
            if policy == "issuer" and len(named) == 1:
                # The issuer's kind of entity, however a source drew it.
                chosen = frozenset(next(iter(named)) & issuer_classes)
                by_issuer += 1
            elif len(roles) != 1:
                ties += 1
                continue
            else:
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
        "by_issuer": by_issuer,
        "basis": {
            "issuer": "the issuer's kind of entity where it names one (not owl:Class), else the"
            " data's own classes; the other classes go to type",
            "role": "the data's own classes; issuer kinds go to type",
        }.get(policy if isinstance(policy, str) else "", ""),
    }


def _joined(keys: list[str], pairs: set[frozenset[str]]) -> bool:
    """Return whether *pairs* join every key into one group."""
    root = {k: k for k in keys}

    def find(k: str) -> str:
        """Return the root of a key."""
        while root[k] != k:
            k = root[k]
        return k

    for pair in pairs:
        a, b = tuple(pair)
        if a in root and b in root:
            root[find(a)] = find(b)
    return len({find(k) for k in keys}) == 1


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


def suggested_folds(schemas: Iterable[MinedSchema], records: Source) -> list[Fold]:
    """Return the folds that hold in *records*: classes whose instances link two things once.

    A link qualifies for a class when every instance in the records has one value of it, an
    IRI, and it does not point to a container (most instances sharing one value, such as the
    pathway). A class with exactly two qualifying links is folded; the direction is the one a
    schema suggests (:func:`suggest_folds`), else the order of the link IRIs (give a Fold to
    set it). A class with other counts is not folded, nor one whose instances a more
    specific class already folds (Catalysis, not its superclass Interaction).
    """
    quads = _quads(records)
    instances: dict[str, set[str]] = defaultdict(set)
    values: dict[tuple[str, str], list[str]] = defaultdict(list)
    links: dict[str, set[str]] = defaultdict(set)
    for quad in quads:
        if quad.predicate.value == _TYPE:
            instances[quad.object.value].add(quad.subject.value)
        elif not isinstance(quad.object, ox.Literal):
            values[(quad.subject.value, quad.predicate.value)].append(quad.object.value)
            links[quad.subject.value].add(quad.predicate.value)
    directed = {(f.cls, f.source, f.target) for schema in schemas for f in suggest_folds(schema)}

    def holds(members: set[str], link: str) -> bool:
        """Return whether every member has one value of the link, not shared by most."""
        found = [values.get((m, link), []) for m in members]
        if any(len(v) != 1 for v in found):
            return False
        return len(members) < 3 or len({v[0] for v in found}) * 2 >= len(members)

    chosen: list[Fold] = []
    covered: set[str] = set()
    for cls, members in sorted(instances.items(), key=lambda item: (len(item[1]), item[0])):
        if members & covered:
            continue
        candidates = set().union(*(links[m] for m in members))
        good = sorted(link for link in candidates if holds(members, link))
        if len(good) != 2:
            continue
        covered |= members
        a, b = good
        source, target = (b, a) if (cls, b, a) in directed else (a, b)
        chosen.append(Fold(cls, source, target, evidence={"instances": len(members)}))
    return chosen


def _flatten(graph: Any) -> Any:
    """Return a copy for GraphML: lists as JSON text, other non-scalar values as text."""
    flat = graph.copy()

    def scalar(value: Any) -> Any:
        """Return a value that GraphML can hold."""
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
        """Return the CSV cell of a value."""
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
