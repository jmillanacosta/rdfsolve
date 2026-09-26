"""Read the domain and range declarations of published vocabularies as declared schema rows.

The vocabularies are read into an Oxigraph dataset. Files, RDFLib graphs, Oxigraph data and
Turtle text can be given, alone or in a list.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TypeAlias

import pyoxigraph as ox
from rdflib import OWL, RDF, RDFS, XSD, Graph

from rdfsolve.local_rdf import to_oxigraph
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.collections import CollectionProfile
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.schema_models.pattern import SchemaPattern

VocabularySource: TypeAlias = "str | Path | Graph | ox.Store | ox.Dataset"
SCHEMA = "https://schema.org/"
DCAM = "http://purl.org/dc/dcam/"
# Every class is an owl:Thing and an rdfs:Resource. Properties with these domains apply to all.
UNIVERSAL = (str(OWL.Thing), str(RDFS.Resource))
DOMAINS = (
    str(RDFS.domain),
    SCHEMA + "domainIncludes",
    "http://schema.org/domainIncludes",
    DCAM + "domainIncludes",
)
RANGES = (
    str(RDFS.range),
    SCHEMA + "rangeIncludes",
    "http://schema.org/rangeIncludes",
    DCAM + "rangeIncludes",
)
DESCRIBED = (str(RDFS.label), str(RDFS.comment), str(RDFS.isDefinedBy))
# Types that define a term. A file that gives these types and descriptions to terms of a
# namespace gives the prefix of that namespace first.
DEFINES = {
    str(RDFS.Class),
    str(OWL.Class),
    str(RDF.Property),
    str(OWL.ObjectProperty),
    str(OWL.DatatypeProperty),
    str(OWL.AnnotationProperty),
}
# A property without a declared range can have any value that its kind of property allows.
UNDECLARED_RANGE = {
    str(OWL.DatatypeProperty): (str(RDFS.Literal),),
    str(OWL.ObjectProperty): (str(RDFS.Resource),),
    str(RDF.Property): (str(RDFS.Literal), str(RDFS.Resource)),
}

# schema.org data types as RDF literal datatypes (https://schema.org/docs/datamodel.html).
# Date allows the partial ISO 8601 forms that schema.org accepts.
DATATYPES: dict[str, tuple[str, ...]] = {
    "Text": (str(XSD.string), str(RDF.langString)),
    "Date": (str(XSD.date), str(XSD.gYearMonth), str(XSD.gYear)),
    "DateTime": (str(XSD.dateTime),),
    "Time": (str(XSD.time),),
    "Number": (str(XSD.decimal),),
    "Float": (str(XSD.double),),
    "Integer": (str(XSD.integer),),
    "Boolean": (str(XSD.boolean),),
}


class Vocabulary:
    """Declarations in one or more vocabularies, read with Oxigraph."""

    def __init__(self, sources: VocabularySource | Sequence[VocabularySource]) -> None:
        """Read files by extension, RDFLib graphs, Oxigraph data and Turtle text."""
        quads: list[ox.Quad] = []
        owners: dict[str, str] = {}  # namespace -> prefix of the file that defines its terms
        others: dict[str, str] = {}  # namespace -> first prefix that another file declares
        items: list[VocabularySource] = (
            list(sources) if isinstance(sources, (list, tuple)) else [sources]  # type: ignore[list-item]
        )
        for source in items:
            found, declared = _read(source)
            quads.extend(found)
            described = {_iri(q.subject) for q in found if q.predicate.value in DESCRIBED}
            defined = {
                _iri(q.subject)
                for q in found
                if q.predicate.value == str(RDF.type)
                and _iri(q.object) in DEFINES
                and _iri(q.subject) in described
            } - {None}
            for prefix, namespace in declared.items():
                own = any(term and term.startswith(namespace) for term in defined)
                (owners if own else others).setdefault(namespace, prefix)
        self.dataset = ox.Dataset(quads)
        # One prefix for each namespace. The prefix of the defining vocabulary is used first.
        self.prefixes = {prefix: ns for ns, prefix in {**others, **owners}.items()}

    def objects(self, subject: str, predicate: str) -> list[str]:
        """Return the IRIs that are values of *predicate* for *subject*."""
        return [
            q.object.value
            for q in self.dataset.quads_for_subject(ox.NamedNode(subject))
            if q.predicate.value == predicate and isinstance(q.object, ox.NamedNode)
        ]

    def subjects(self, predicate: str, value: str) -> list[str]:
        """Return the IRIs that have *value* for *predicate*."""
        return [
            q.subject.value
            for q in self.dataset.quads_for_object(ox.NamedNode(value))
            if q.predicate.value == predicate and isinstance(q.subject, ox.NamedNode)
        ]

    def has(self, subject: str, predicate: str | None = None, value: str | None = None) -> bool:
        """Return True when a statement about *subject* matches."""
        return any(
            predicate in (None, q.predicate.value)
            and (value is None or q.object == ox.NamedNode(value))
            for q in self.dataset.quads_for_subject(ox.NamedNode(subject))
        )

    def lineage(self, cls: str) -> list[str]:
        """Return the class and its ancestors, the nearest ancestors first."""
        found, level = [cls], [cls]
        while level:
            level = [
                parent
                for child in level
                for parent in self.objects(child, str(RDFS.subClassOf))
                if parent not in found
            ]
            found.extend(dict.fromkeys(level))
        return found


def _iri(term: object) -> str | None:
    """Return the IRI of a named node, or None for other terms."""
    return term.value if isinstance(term, ox.NamedNode) else None


def _read(source: VocabularySource) -> tuple[list[ox.Quad], dict[str, str]]:
    """Return the statements of one source and the prefixes that it declares."""
    if isinstance(source, (ox.Store, ox.Dataset)):
        return list(source), {}
    if isinstance(source, Graph):
        return list(to_oxigraph(source)), {p: str(ns) for p, ns in source.namespaces() if p}
    parser = (
        ox.parse(path=source)
        if isinstance(source, Path)
        else ox.parse(source, format=ox.RdfFormat.TURTLE)
    )
    quads = list(parser)
    prefixes = {p: ns for p, ns in parser.prefixes.items() if p}
    if (
        isinstance(source, Path)
        and ox.RdfFormat.from_extension(source.suffix[1:]) == ox.RdfFormat.RDF_XML
    ):
        # The RDF/XML parser gives no prefixes. The XML namespace declarations give them.
        events = ET.iterparse(source, events=["start-ns"])  # noqa: S314 - local vocabulary files
        prefixes.update((p, ns) for _, (p, ns) in events if p)
    return quads, prefixes


def vocabulary_to_minedschema(
    vocabulary: VocabularySource | Sequence[VocabularySource], classes: Iterable[str]
) -> MinedSchema:
    """Declare, for each class, every property whose domain is the class or an ancestor.

    Properties without a domain apply to every class. Without a range, a property can have any
    value that its kind allows. Ranges that are classes become object rows. XSD datatypes and
    schema.org data types become literal rows. rdfs:Literal allows any literal. owl:Thing and
    rdfs:Resource allow any IRI or blank node. URL becomes an IRI row. An rdf:List range
    declares list values whose members have the other ranges. The rows are declarations, not
    observations (evidence_source "vocabulary").
    """
    vocab = Vocabulary(vocabulary)
    requested = [str(c) for c in classes]
    unknown = [c for c in requested if not vocab.has(c)]
    if unknown:
        raise ValueError(f"Classes not declared in the vocabulary: {unknown}")
    # A property is described by a range, a label, a comment or a defining vocabulary.
    # Vocabularies also give a type to properties that they only mention: schema.org lists
    # its external equivalents in this way.
    declared = {
        prop
        for kind in UNDECLARED_RANGE
        for prop in vocab.subjects(str(RDF.type), kind)
        if any(vocab.has(prop, p) for p in (*RANGES, *DESCRIBED))
    }
    # RDFS: a property without a declared domain can describe any resource.
    anywhere = {prop for prop in declared if not any(vocab.has(prop, d) for d in DOMAINS)}
    patterns: list[SchemaPattern] = []
    lists: list[CollectionProfile] = []
    for cls in requested:
        lineage = {*vocab.lineage(cls), *UNIVERSAL}
        properties = {
            prop
            for domain in DOMAINS
            for ancestor in lineage
            for prop in vocab.subjects(domain, ancestor)
        } | anywhere
        for prop in sorted(properties):
            ranges = _ranges(vocab, prop)
            rows = [
                row
                for target in sorted(ranges - {str(RDF.List)})
                for row in _objects(vocab, target)
            ]
            patterns.extend(
                SchemaPattern(
                    subject_class=cls,
                    property_uri=prop,
                    object_class=object_class,
                    datatype=datatype,
                    evidence_source="vocabulary",
                )
                for object_class, datatype in rows
            )
            if str(RDF.List) in ranges:
                lists.append(_list_profile(cls, prop, rows))
    schema = MinedSchema(
        patterns=patterns,
        about=AboutMetadata.build(),
        class_hierarchy=_hierarchy(vocab, [*requested, *_ranges_modelled(patterns, requested)]),
        collections=lists or None,
    )
    used = {
        iri
        for p in patterns
        for iri in (p.subject_class, p.property_uri, p.object_class, p.datatype)
        if iri
    }
    # The prefixes that the vocabularies declare, for the namespaces that the rows use.
    schema.prefixes = {
        prefix: namespace
        for prefix, namespace in vocab.prefixes.items()
        if any(iri.startswith(namespace) for iri in used)
    }
    schema.about.pattern_count = len(patterns)
    schema.about.class_count = len(schema.get_classes())
    schema.about.property_count = len(schema.get_properties())
    schema.enrichment = SchemaEnrichment.from_oxigraph(
        vocab.dataset, schema.get_classes(), schema.get_properties()
    )
    return schema


def _list_profile(cls: str, prop: str, rows: list[tuple[str, str | None]]) -> CollectionProfile:
    """Declare that values can be RDF lists whose members have the other ranges."""
    classes = sorted({o for o, _ in rows if o not in _SENTINEL_OBJECTS})
    kinds = {"IRI", "BlankNode"} if classes or ("Resource", None) in rows else set()
    datatypes = sorted({d for o, d in rows if o == "Literal" and d})
    return CollectionProfile(
        subject_class=cls,
        property_uri=prop,
        member_types=classes,
        member_kinds=sorted(kinds | ({"Literal"} if datatypes else set())),
        member_datatypes=datatypes,
        evidence_source="vocabulary",
    )


def _ranges(vocab: Vocabulary, prop: str) -> set[str]:
    """Return the declared ranges, or the values that the kind of property allows."""
    ranges = {r for rng in RANGES for r in vocab.objects(prop, rng)}
    if ranges:
        return ranges
    kinds = [kind for kind in UNDECLARED_RANGE if vocab.has(prop, str(RDF.type), kind)]
    return set(UNDECLARED_RANGE[kinds[0]]) if kinds else set()


def _objects(vocab: Vocabulary, target: str) -> list[tuple[str, str | None]]:
    """Return the RDF forms of the values of one declared range."""
    if target.startswith(str(XSD)) or target == str(RDF.langString):
        return [("Literal", target)]
    if target in UNIVERSAL:
        return [("Resource", None), ("BlankNode", None)]
    if target == str(RDFS.Literal):
        # Any literal. Plain text is written as text. Explicit typed literals are accepted.
        return [("Literal", datatype) for datatype in (*DATATYPES["Text"], str(RDFS.Literal))]
    for ancestor in vocab.lineage(target):
        if not ancestor.startswith((SCHEMA, "http://schema.org/")):
            continue
        name = ancestor.rsplit("/", 1)[-1]
        if name == "URL":
            return [("Resource", None)]
        if name in DATATYPES:  # Integer is found before its parent Number.
            return [("Literal", datatype) for datatype in DATATYPES[name]]
    return [(target, None)]


def _ranges_modelled(patterns: list[SchemaPattern], requested: list[str]) -> list[str]:
    """Return the classes that are only ranges. They also get models and a place in the hierarchy."""
    ranges = {p.object_class for p in patterns if p.object_class not in _SENTINEL_OBJECTS}
    return sorted(ranges - set(requested))


def _hierarchy(vocab: Vocabulary, classes: list[str]) -> dict[str, list[str]]:
    """Return the nearest modelled ancestors of each modelled class."""
    modelled = set(classes)
    ancestors = {cls: set(vocab.lineage(cls)[1:]) for cls in classes}
    hierarchy: dict[str, list[str]] = {}
    for cls in classes:
        candidates = ancestors[cls] & modelled
        nearest = {c for c in candidates if not any(c in ancestors[o] for o in candidates)}
        if nearest:
            hierarchy[cls] = sorted(nearest)
    return hierarchy
