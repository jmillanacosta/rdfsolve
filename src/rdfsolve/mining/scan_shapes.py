"""Exact observed SHACL shapes of the data in a row store (scan mining).

One active sh:NodeShape per class, with one sh:PropertyShape per property that the class's
instances use. Every constraint is the extreme of what the rows show, so the shapes validate
the data they were read from with no violation:

- the instances of a class are its SHACL instances in the data, the nodes with
  ``rdf:type/rdfs:subClassOf*`` the class (SHACL 1.0, 2.1.3.1), which sh:targetClass selects;
- sh:minCount and sh:maxCount are the least and the most values of the property among all
  instances (an instance without the property has 0 values; a minimum of 0 is not written);
- the values are described by sh:or of: sh:class for each class of a typed object (its
  rdf:type), sh:nodeKind sh:IRI or sh:BlankNode for objects without a class, sh:datatype for
  each datatype of the literals (the source datatype, after the census restore of QLever's
  numeric types), with sh:languageIn for language-tagged strings; a single option is written
  without sh:or; sh:nodeKind gives the mix of node kinds;
- a datatype with ill-typed literals (which sh:datatype rejects, as a SHACL engine checks the
  lexical form) is given as sh:nodeKind sh:Literal, and the property shape says how many;
- with *closed*, sh:closed true and sh:ignoredProperties (rdf:type): every property of an
  instance has a property shape.

The counts are annotations, not constraints, written with VoID terms on partitions of their
own (``observed-class-…``): the instances of the class (void:entities); for each property its
triples, the instances that have it (void:distinctSubjects), its distinct objects and, with
void-ext, the distinct IRI, blank-node and literal objects. Each shape points to its partition
(dcterms:source) and to the dataset IRI (schema:isBasedOn), which carries the release when the
base IRI is versioned. The number of instances with each number of values (the histogram) is
in the property shape's sh:description and in the profile (JSON).

These shapes are kept apart from the deactivated value-type templates (exporters/shacl.py) and
from the constraints that a source declares: their IRIs are ``…/shapes/observed-…``.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from hashlib import md5
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from rdfsolve.mining.scan import RDF_LANG, RDF_TYPE, XSD_STRING

if TYPE_CHECKING:
    import polars as pl
    from rdflib import Graph

    from rdfsolve.mining.scan import RowStore, StoreView

__all__ = ["ClassProfile", "PropertyProfile", "class_property_profiles", "observed_shapes"]

RDFS_SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
XSD = "http://www.w3.org/2001/XMLSchema#"
# Lexical forms that rdflib (and so pySHACL) accepts for these datatypes; other forms of XSD
# datatypes are checked with rdflib one distinct form at a time.
_SAFE = {
    XSD + "integer": r"^[+-]?[0-9]+$",
    XSD + "decimal": r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$",
    XSD + "double": r"^([+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?|-?INF|NaN)$",
    XSD + "float": r"^([+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?|-?INF|NaN)$",
    XSD + "boolean": r"^(true|false|1|0)$",
}
# Datatypes whose every lexical form is valid.
_ANY_FORM = frozenset({XSD_STRING, RDF_LANG, XSD + "anyURI"})
_KINDS = {"iri": "IRI", "bnode": "BlankNode", "literal": "Literal"}


class PropertyProfile(BaseModel):
    """What the instances of one class do with one property."""

    property_uri: str
    instances: int = Field(description="SHACL instances of the class")
    subjects: int = Field(description="Instances with at least one value")
    triples: int
    min_count: int
    max_count: int
    values_per_instance: dict[int, int] = Field(
        description="Instances by number of values (0 included)"
    )
    node_kinds: dict[str, int] = Field(description="Values by node kind")
    distinct_objects: int
    distinct_objects_by_kind: dict[str, int]
    object_classes: dict[str, int] = Field(
        default_factory=dict, description="Values by rdf:type class (a value may have several)"
    )
    unclassed: dict[str, int] = Field(
        default_factory=dict, description="IRI and blank-node values without an IRI class"
    )
    datatypes: dict[str, int] = Field(default_factory=dict, description="Literals by datatype")
    datatype_options: dict[str, list[str]] = Field(
        default_factory=dict,
        description="A datatype QLever reports for several source datatypes of the property",
    )
    ill_typed: dict[str, int] = Field(default_factory=dict)
    languages: dict[str, int] = Field(default_factory=dict)


class ClassProfile(BaseModel):
    """The instances of one class and their properties."""

    class_iri: str
    instances: int
    properties: list[PropertyProfile] = Field(default_factory=list)


def _bare(expr: pl.Expr) -> pl.Expr:
    return expr.str.strip_prefix("<").str.strip_suffix(">")


def _instances(store: RowStore | StoreView, classes: Sequence[str] | None) -> pl.LazyFrame:
    """Return the SHACL instances (s, c): rdf:type, then rdfs:subClassOf* between IRIs."""
    import polars as pl

    predicates = store.predicates
    if RDF_TYPE not in predicates:
        return pl.LazyFrame(schema={"s": pl.String, "c": pl.String})
    typed = (
        store.rows(RDF_TYPE)
        .filter(pl.col("kind") == "iri")
        .select("s", c=_bare(pl.col("o")))
        .unique()
    )
    parents: dict[str, set[str]] = defaultdict(set)
    if RDFS_SUBCLASS in predicates:
        edges = (
            store.rows(RDFS_SUBCLASS)
            .filter(pl.col("s").str.starts_with("<") & (pl.col("kind") == "iri"))
            .select(a=_bare(pl.col("s")), b=_bare(pl.col("o")))
            .unique()
            .collect()
        )
        for a, b in edges.iter_rows():
            parents[a].add(b)
    if not parents:
        found = typed
    else:
        direct = typed.select("c").unique().collect()["c"].to_list()
        pairs = []
        for cls in direct:  # rdfs:subClassOf*: the class and each ancestor
            seen, todo = {cls}, [cls]
            while todo:
                for parent in parents.get(todo.pop(), ()):
                    if parent not in seen:
                        seen.add(parent)
                        todo.append(parent)
            pairs += [(cls, ancestor) for ancestor in seen]
        closure = pl.LazyFrame(pairs, schema={"c": pl.String, "a": pl.String}, orient="row")
        found = typed.join(closure, on="c").select("s", c="a").unique()
    if classes is not None:
        found = found.filter(pl.col("c").is_in(list(classes)))
    return found


def _restore(census: dict[str, dict[str, int]] | None, predicate: str) -> dict[str, list[str]]:
    """Return the source datatypes of each datatype that QLever reports, for one property."""
    from rdfsolve.qlever.datatypes import REPORTED

    found: dict[str, list[str]] = {}
    for reported, group in REPORTED.items():
        source = sorted(dt for dt in (census or {}).get(predicate, {}) if dt in group)
        if source:
            found[reported] = source
    return found


def _lexical() -> pl.Expr:
    """Return the lexical form of a literal term (TSV writes numbers bare)."""
    import polars as pl

    o = pl.col("o")
    quoted = o.str.extract(r'(?s)^"(.*)"(?:\^\^<[^>]*>|@[A-Za-z0-9-]+)?$', 1)
    return pl.when(o.str.starts_with('"')).then(quoted).otherwise(o)


def _ill_typed(pairs: Iterable[tuple[str, str]]) -> set[tuple[str, str]]:
    """Return the (datatype, lexical form) pairs that rdflib does not accept."""
    from rdflib import Literal, URIRef

    logger = logging.getLogger("rdflib.term")
    level = logger.level
    logger.setLevel(logging.CRITICAL)  # rdflib logs each form it cannot convert
    try:
        return {(dt, lex) for dt, lex in pairs if Literal(lex, datatype=URIRef(dt)).ill_typed}
    finally:
        logger.setLevel(level)


def class_property_profiles(
    store: RowStore | StoreView,
    *,
    classes: Sequence[str] | None = None,
    census: dict[str, dict[str, int]] | None = None,
) -> list[ClassProfile]:
    """Profile the properties of the SHACL instances of each class, one predicate at a time.

    *classes* limits the classes (by default every class with an instance); *census* is the
    literal-datatype census of the index (qlever.datatypes.read_census), which gives back the
    source datatypes of the numbers that QLever folds into xsd:int and xsd:double. Without it,
    a store read from QLever keeps the folded datatypes.
    """
    import polars as pl

    instances = _instances(store, classes)
    sizes = dict(instances.group_by("c").agg(n=pl.len()).collect().iter_rows())
    object_types = (
        store.rows(RDF_TYPE).filter(pl.col("kind") == "iri").select("s", oc=_bare(pl.col("o")))
        if RDF_TYPE in store.predicates
        else pl.LazyFrame(schema={"s": pl.String, "oc": pl.String})
    ).rename({"s": "o"})
    profiles: dict[str, list[PropertyProfile]] = defaultdict(list)
    for predicate in sorted(store.predicates):
        if predicate == RDF_TYPE:
            continue
        options = _restore(census, predicate)
        unique = {k: v[0] for k, v in options.items() if len(v) == 1}
        rows = store.rows(predicate).with_columns(
            d=pl.col("d").replace(unique) if unique else pl.col("d")
        )
        edges = rows.join(instances, on="s")
        literal = pl.col("kind") == "literal"
        checked = (
            rows.filter(literal & ~pl.col("d").is_in([*_ANY_FORM, *options]))
            .filter(pl.col("d").str.starts_with(XSD))
            .select("d", lex=_lexical())
            .unique()
        )
        frames = pl.collect_all(
            [
                edges.group_by("c", "s").agg(n=pl.len()).group_by("c", "n").agg(k=pl.len()),
                edges.group_by("c", "kind").agg(n=pl.len(), distinct=pl.col("oid").n_unique()),
                edges.group_by("c").agg(n=pl.len(), distinct=pl.col("oid").n_unique()),
                edges.filter(~literal)
                .join(object_types, on="o")
                .group_by("c", "oc")
                .agg(n=pl.len()),
                edges.filter(~literal)
                .join(object_types, on="o", how="anti")
                .group_by("c", "kind")
                .agg(n=pl.len()),
                edges.filter(literal)
                .with_columns(
                    lang=pl.when(pl.col("d") == RDF_LANG).then(
                        pl.col("o").str.extract(r"@([A-Za-z0-9-]+)$", 1)
                    )
                )
                .group_by("c", "d", "lang")
                .agg(n=pl.len()),
                checked,
            ],
            engine="streaming",
        )
        counts, kinds, totals, typed, unclassed, literals, forms = frames
        bad: set[tuple[str, str]] = set()
        for dt, pattern in _SAFE.items():
            subset = forms.filter((pl.col("d") == dt) & ~pl.col("lex").str.contains(pattern))
            bad |= _ill_typed(subset.iter_rows())
        bad |= _ill_typed(forms.filter(~pl.col("d").is_in(list(_SAFE))).iter_rows())
        ill: dict[tuple[str, str], int] = {}
        if bad:
            frame = pl.LazyFrame(
                sorted(bad), schema={"d": pl.String, "lex": pl.String}, orient="row"
            )
            for c, d, n in (
                edges.filter(literal)
                .with_columns(lex=_lexical())
                .join(frame, on=["d", "lex"])
                .group_by("c", "d")
                .agg(n=pl.len())
                .collect()
                .iter_rows()
            ):
                ill[c, d] = n
        per: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "hist": {},
                "kinds": {},
                "distinct": {},
                "oc": {},
                "un": {},
                "dt": {},
                "lang": {},
            }
        )
        for c, n, k in counts.iter_rows():
            per[c]["hist"][n] = k
        for c, kind, n, distinct in kinds.iter_rows():
            per[c]["kinds"][_KINDS[kind]] = n
            per[c]["distinct"][_KINDS[kind]] = distinct
        for c, n, distinct in totals.iter_rows():
            per[c]["triples"], per[c]["all"] = n, distinct
        for c, oc, n in typed.iter_rows():
            per[c]["oc"][oc] = n
        for c, kind, n in unclassed.iter_rows():
            per[c]["un"][_KINDS[kind]] = n
        for c, d, lang, n in literals.iter_rows():
            per[c]["dt"][d] = per[c]["dt"].get(d, 0) + n
            if lang:
                per[c]["lang"][lang] = per[c]["lang"].get(lang, 0) + n
        for c, found in per.items():
            hist = dict(sorted(found["hist"].items()))
            subjects = sum(hist.values())
            if sizes[c] > subjects:
                hist = {0: sizes[c] - subjects, **hist}
            profiles[c].append(
                PropertyProfile(
                    property_uri=predicate,
                    instances=sizes[c],
                    subjects=subjects,
                    triples=found["triples"],
                    min_count=min(hist),
                    max_count=max(hist),
                    values_per_instance=hist,
                    node_kinds=found["kinds"],
                    distinct_objects=found["all"],
                    distinct_objects_by_kind=found["distinct"],
                    object_classes=dict(sorted(found["oc"].items())),
                    unclassed=found["un"],
                    datatypes=dict(sorted(found["dt"].items())),
                    datatype_options={
                        k: v for k, v in options.items() if k in found["dt"] and len(v) > 1
                    },
                    ill_typed={d: n for (cc, d), n in ill.items() if cc == c},
                    languages=dict(sorted(found["lang"].items())),
                )
            )
    return [
        ClassProfile(class_iri=c, instances=n, properties=profiles.get(c, []))
        for c, n in sorted(sizes.items())
    ]


def _short(iri: str) -> str:
    return md5(iri.encode(), usedforsecurity=False).hexdigest()[:8]


_NODE_KIND = {
    frozenset({"IRI"}): "IRI",
    frozenset({"BlankNode"}): "BlankNode",
    frozenset({"Literal"}): "Literal",
    frozenset({"IRI", "BlankNode"}): "BlankNodeOrIRI",
    frozenset({"IRI", "Literal"}): "IRIOrLiteral",
    frozenset({"BlankNode", "Literal"}): "BlankNodeOrLiteral",
}


def _options(profile: PropertyProfile) -> list[list[tuple[str, Any]]]:
    """Return the value options of a property, each a list of (constraint, value)."""
    found: list[list[tuple[str, Any]]] = [[("class", c)] for c in profile.object_classes]
    if profile.unclassed.get("IRI"):
        found.append([("nodeKind", "IRI")])
    if profile.unclassed.get("BlankNode"):
        found.append([("nodeKind", "BlankNode")])
    loose = False
    for datatype in profile.datatypes:
        if datatype in profile.ill_typed:
            loose = True
        elif datatype == RDF_LANG:
            found.append([("datatype", RDF_LANG), ("languageIn", sorted(profile.languages))])
        else:
            found += [[("datatype", d)] for d in profile.datatype_options.get(datatype, [datatype])]
    if loose:
        found.append([("nodeKind", "Literal")])
    return found


def observed_shapes(
    profiles: Sequence[ClassProfile],
    *,
    dataset_name: str,
    dataset_iri: str | None = None,
    graph_uris: Sequence[str] | None = None,
    closed: bool = False,
) -> Graph:
    """Write the active observed shapes of *profiles* and their count partitions as RDF.

    *graph_uris* names the graph scope the profiles were read from (a store view), which the
    shapes' descriptions state; *closed* adds sh:closed true with sh:ignoredProperties
    (rdf:type).
    """
    from rdflib import BNode, Graph, Literal, Namespace, URIRef
    from rdflib.collection import Collection
    from rdflib.namespace import DCTERMS, RDF
    from rdflib.namespace import XSD as XSDNS
    from rdflib.term import Node

    from rdfsolve.config import mint

    sh = Namespace("http://www.w3.org/ns/shacl#")
    void = Namespace("http://rdfs.org/ns/void#")
    void_ext = Namespace("http://ldf.fi/void-ext#")
    schema_org = Namespace("https://schema.org/")
    dataset = dataset_iri or mint("dataset", dataset_name or "unnamed")
    shapes, partitions = f"{dataset}/shapes/observed-", f"{dataset}/partition/observed-"
    g = Graph()
    for prefix, namespace in (
        ("sh", sh),
        ("void", void),
        ("void-ext", void_ext),
        ("schema", schema_org),
    ):
        g.bind(prefix, namespace)

    def number(n: int) -> Literal:
        """Return *n* as an xsd:integer literal."""
        return Literal(n, datatype=XSDNS.integer)

    def constrain(node: Any, constraints: list[tuple[str, Any]]) -> None:
        """Add the SHACL *constraints* to the shape *node*."""
        for name, value in constraints:
            if name == "nodeKind":
                g.add((node, sh.nodeKind, sh[value]))
            elif name == "languageIn":
                items = BNode()
                Collection(g, items, [Literal(v) for v in value])
                g.add((node, sh.languageIn, items))
            else:
                g.add((node, sh[name], URIRef(value)))

    scope = (
        "the RDF merge of the graphs " + ", ".join(sorted(graph_uris))
        if graph_uris
        else "the default graph"
    )
    for cls in profiles:
        node = URIRef(f"{shapes}ns-{_short(cls.class_iri)}")
        partition = URIRef(f"{partitions}class-{_short(cls.class_iri)}")
        g.add((node, RDF.type, sh.NodeShape))
        g.add((node, sh.targetClass, URIRef(cls.class_iri)))
        g.add((node, DCTERMS.source, partition))
        g.add((node, schema_org.isBasedOn, URIRef(dataset)))
        g.add(
            (
                node,
                sh.description,
                Literal(
                    f"Observed in {scope}: the least and most values of each property among the "
                    f"{cls.instances} instances (rdf:type/rdfs:subClassOf*) of the class, and "
                    "every kind, class and datatype of the values. Not a constraint the source "
                    "declares."
                ),
            )
        )
        if closed:
            g.add((node, sh.closed, Literal(True)))
            ignored = BNode()
            Collection(g, ignored, [RDF.type])
            g.add((node, sh.ignoredProperties, ignored))
        g.add((partition, RDF.type, void.Dataset))
        g.add((partition, void["class"], URIRef(cls.class_iri)))
        g.add((partition, void.entities, number(cls.instances)))
        for prop in cls.properties:
            shape = URIRef(f"{shapes}ps-{_short(cls.class_iri)}-{_short(prop.property_uri)}")
            part = URIRef(
                f"{partitions}class-{_short(cls.class_iri)}-prop-{_short(prop.property_uri)}"
            )
            g.add((node, sh.property, shape))
            g.add((shape, RDF.type, sh.PropertyShape))
            g.add((shape, sh.path, URIRef(prop.property_uri)))
            g.add((shape, DCTERMS.source, part))
            if prop.min_count:
                g.add((shape, sh.minCount, number(prop.min_count)))
            g.add((shape, sh.maxCount, number(prop.max_count)))
            kind = _NODE_KIND.get(frozenset(k for k, n in prop.node_kinds.items() if n))
            if kind:
                g.add((shape, sh.nodeKind, sh[kind]))
            options = _options(prop)
            if len(options) == 1:
                constrain(shape, options[0])
            elif options:
                members: list[Node] = []
                for option in options:
                    member = BNode()
                    constrain(member, option)
                    members.append(member)
                head = BNode()
                Collection(g, head, members)
                g.add((shape, sh["or"], head))
            histogram = ", ".join(f"{n}: {k}" for n, k in prop.values_per_instance.items())
            text = f"Instances by number of values: {histogram}."
            if prop.ill_typed:
                text += (
                    " Ill-typed literals (any literal accepted): "
                    + ", ".join(f"{d} {n}" for d, n in sorted(prop.ill_typed.items()))
                    + "."
                )
            g.add((shape, sh.description, Literal(text)))
            g.add((partition, void.propertyPartition, part))
            g.add((part, RDF.type, void.Dataset))
            g.add((part, void.property, URIRef(prop.property_uri)))
            g.add((part, void.triples, number(prop.triples)))
            g.add((part, void.distinctSubjects, number(prop.subjects)))
            g.add((part, void.distinctObjects, number(prop.distinct_objects)))
            for name, key in (
                ("distinctIRIReferenceObjects", "IRI"),
                ("distinctBlankNodeObjects", "BlankNode"),
                ("distinctLiterals", "Literal"),
            ):
                if key in prop.distinct_objects_by_kind:
                    g.add((part, void_ext[name], number(prop.distinct_objects_by_kind[key])))
    return g
