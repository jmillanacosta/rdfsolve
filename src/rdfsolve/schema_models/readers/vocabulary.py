"""Read a published vocabulary's domain and range declarations as declared schema rows."""

from __future__ import annotations

from collections.abc import Iterable

from rdflib import RDF, RDFS, XSD, Graph, Namespace, URIRef

from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.schema_models.pattern import SchemaPattern

SCHEMA = Namespace("https://schema.org/")
# Every class is an owl:Thing and an rdfs:Resource: properties with these domains apply to all.
UNIVERSAL = (URIRef("http://www.w3.org/2002/07/owl#Thing"), RDFS.Resource)
DOMAINS = (RDFS.domain, SCHEMA.domainIncludes, URIRef("http://schema.org/domainIncludes"))
RANGES = (RDFS.range, SCHEMA.rangeIncludes, URIRef("http://schema.org/rangeIncludes"))

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

    Ranges that are classes become object rows, XSD datatypes and schema.org data types
    become literal rows, and URL becomes an IRI row. The rows are declarations, not
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
    patterns: list[SchemaPattern] = []
    for cls in requested:
        lineage = {cls, *graph.transitive_objects(cls, RDFS.subClassOf), *UNIVERSAL}
        properties = {
            prop
            for domain in DOMAINS
            for ancestor in lineage
            for prop in graph.subjects(domain, ancestor)
            if isinstance(prop, URIRef)
        }
        for prop in sorted(properties):
            for target in sorted(
                {r for rng in RANGES for r in graph.objects(prop, rng) if isinstance(r, URIRef)}
            ):
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


def _objects(graph: Graph, target: URIRef) -> list[tuple[str, str | None]]:
    """How values of one declared range are written in RDF."""
    if str(target).startswith(str(XSD)) or target == RDF.langString:
        return [("Literal", str(target))]
    if target == RDFS.Literal:
        return [("Literal", datatype) for datatype in DATATYPES["Text"]]
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
