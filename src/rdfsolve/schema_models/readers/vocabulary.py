"""Read a published vocabulary's domain and range declarations as declared schema rows."""

from __future__ import annotations

from collections.abc import Iterable

from rdflib import OWL, RDF, RDFS, XSD, Graph, Namespace, URIRef

from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.schema_models.pattern import SchemaPattern

SCHEMA = Namespace("https://schema.org/")
DCAM = Namespace("http://purl.org/dc/dcam/")
# Every class is an owl:Thing and an rdfs:Resource: properties with these domains apply to all.
UNIVERSAL = (OWL.Thing, RDFS.Resource)
DOMAINS = (
    RDFS.domain,
    SCHEMA.domainIncludes,
    URIRef("http://schema.org/domainIncludes"),
    DCAM.domainIncludes,
)
RANGES = (
    RDFS.range,
    SCHEMA.rangeIncludes,
    URIRef("http://schema.org/rangeIncludes"),
    DCAM.rangeIncludes,
)
DESCRIBED = (RDFS.label, RDFS.comment, RDFS.isDefinedBy)
# A property without a declared range may take any value its kind of property allows.
UNDECLARED_RANGE = {
    OWL.DatatypeProperty: (RDFS.Literal,),
    OWL.ObjectProperty: (RDFS.Resource,),
    RDF.Property: (RDFS.Literal, RDFS.Resource),
}

# schema.org data types as RDF literal datatypes (https://schema.org/docs/datamodel.html).
# Date allows the partial ISO 8601 forms schema.org accepts.
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


def vocabulary_to_minedschema(vocabulary: str | Graph, classes: Iterable[str]) -> MinedSchema:
    """Declare, for each class, every property whose domain is the class or an ancestor.

    Properties without a domain apply to every class; without a range they take any value
    their kind allows. Ranges that are classes become object rows, XSD datatypes and schema.org
    data types become literal rows, rdfs:Literal admits any literal, owl:Thing and
    rdfs:Resource admit any IRI or blank node, and URL becomes an IRI row. The rows are declarations, not
    observations (evidence_source "vocabulary").
    """
    graph = (
        vocabulary
        if isinstance(vocabulary, Graph)
        else Graph().parse(data=vocabulary, format="turtle")
    )
    requested = [URIRef(c) for c in classes]
    unknown = [str(c) for c in requested if not any(graph.triples((c, None, None)))]
    if unknown:
        raise ValueError(f"Classes not declared in the vocabulary: {unknown}")
    # Defined here: a range, a label, a comment or a defining vocabulary. Vocabularies also
    # type properties they only mention (schema.org lists its external equivalents).
    declared = {
        prop
        for kind in UNDECLARED_RANGE
        for prop in graph.subjects(RDF.type, kind)
        if isinstance(prop, URIRef) and any(graph.value(prop, p) for p in (*RANGES, *DESCRIBED))
    }
    # RDFS: a property without a declared domain may describe any resource.
    anywhere = {prop for prop in declared if not any(graph.value(prop, d) for d in DOMAINS)}
    patterns: list[SchemaPattern] = []
    for cls in requested:
        lineage = {cls, *graph.transitive_objects(cls, RDFS.subClassOf), *UNIVERSAL}
        properties = {
            prop
            for domain in DOMAINS
            for ancestor in lineage
            for prop in graph.subjects(domain, ancestor)
            if isinstance(prop, URIRef)
        } | anywhere
        for prop in sorted(properties):
            for target in sorted(_ranges(graph, prop)):
                for object_class, datatype in _objects(graph, target):
                    patterns.append(
                        SchemaPattern(
                            subject_class=str(cls),
                            property_uri=str(prop),
                            object_class=object_class,
                            datatype=datatype,
                            evidence_source="vocabulary",
                        )
                    )
    schema = MinedSchema(
        patterns=patterns,
        about=AboutMetadata.build(),
        class_hierarchy=_hierarchy(graph, requested),
    )
    schema.about.pattern_count = len(patterns)
    schema.about.class_count = len(schema.get_classes())
    schema.about.property_count = len(schema.get_properties())
    schema.enrichment = SchemaEnrichment.from_rdf_graph(
        graph, schema.get_classes(), schema.get_properties()
    )
    return schema


def _ranges(graph: Graph, prop: URIRef) -> set[URIRef]:
    """Return declared ranges, or what the property's kind allows when none is declared."""
    ranges = {r for rng in RANGES for r in graph.objects(prop, rng) if isinstance(r, URIRef)}
    if ranges:
        return ranges
    kinds = [kind for kind in UNDECLARED_RANGE if (prop, RDF.type, kind) in graph]
    return set(UNDECLARED_RANGE[kinds[0]]) if kinds else set()


def _objects(graph: Graph, target: URIRef) -> list[tuple[str, str | None]]:
    """How values of one declared range are written in RDF."""
    if str(target).startswith(str(XSD)) or target == RDF.langString:
        return [("Literal", str(target))]
    if target in UNIVERSAL:
        return [("Resource", None), ("BlankNode", None)]
    if target == RDFS.Literal:
        # Any literal: plain text is written as text, explicit typed literals are accepted.
        return [("Literal", datatype) for datatype in (*DATATYPES["Text"], str(RDFS.Literal))]
    lineage = {target, *graph.transitive_objects(target, RDFS.subClassOf)}
    names = {
        str(t).rsplit("/", 1)[-1]
        for t in lineage
        if str(t).startswith(("https://schema.org/", "http://schema.org/"))
    }
    if "URL" in names:
        return [("Resource", None)]
    for name, datatypes in DATATYPES.items():
        if name in names:
            return [("Literal", datatype) for datatype in datatypes]
    return [(str(target), None)]


def _hierarchy(graph: Graph, classes: list[URIRef]) -> dict[str, list[str]]:
    """Nearest requested ancestors of each requested class."""
    requested = set(classes)
    ancestors: dict[URIRef, set[URIRef]] = {
        cls: {
            a
            for a in graph.transitive_objects(cls, RDFS.subClassOf)
            if isinstance(a, URIRef) and a != cls
        }
        for cls in classes
    }
    hierarchy: dict[str, list[str]] = {}
    for cls in classes:
        candidates = ancestors[cls] & requested
        nearest = {c for c in candidates if not any(c in ancestors[o] for o in candidates)}
        if nearest:
            hierarchy[str(cls)] = sorted(str(c) for c in nearest)
    return hierarchy
