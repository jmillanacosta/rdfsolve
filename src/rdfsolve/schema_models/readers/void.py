"""VoID to MinedSchema conversion functions."""

from __future__ import annotations

import logging

from rdflib import RDF, Graph, Namespace, URIRef
from rdflib.query import ResultRow

from rdfsolve.local_rdf import LocalBackend, LocalRdf
from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

VOID = Namespace("http://rdfs.org/ns/void#")
VOID_EXT = Namespace("http://ldf.fi/void-ext#")


SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
# The links from a VoID dataset to the parts that describe it.
_PARTS = (VOID.classPartition, VOID.propertyPartition, VOID.subset, VOID_EXT.datatypePartition)


def void_datasets_of_graphs(g: Graph, graph_names: list[str]) -> list[URIRef]:
    """Return the VoID datasets that a service description gives for the named graphs."""
    return sorted(
        {
            dataset
            for name in graph_names
            for named in g.subjects(SD.name, URIRef(name))
            for dataset in g.objects(named, SD.graph)
            if isinstance(dataset, URIRef)
        }
    )


def scope_void_graph(g: Graph, datasets: list[URIRef]) -> Graph:
    """Return the part of a VoID description that describes DATASETS.

    A description of a whole endpoint (void-generator: IDSM describes 39 graphs and their union)
    holds every graph's partitions. From each dataset its partitions, subsets and linksets are
    followed; the end of a linkset that another dataset describes contributes its class only.
    """
    scoped = Graph()
    for prefix, namespace in g.namespaces():
        scoped.bind(prefix, namespace)
    seen: set[object] = set()
    stack: list[object] = list(datasets)
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        for triple in g.triples((node, None, None)):  # type: ignore[arg-type]
            scoped.add(triple)
        for part in _PARTS:
            stack.extend(g.objects(node, part))  # type: ignore[arg-type]
    for linkset in list(scoped.subjects(VOID.linkPredicate, None)):
        for end in (VOID.subjectsTarget, VOID.objectsTarget):
            for target in list(scoped.objects(linkset, end)):
                if target not in seen:
                    for cls in g.objects(target, VOID["class"]):
                        scoped.add((target, VOID["class"], cls))
    return scoped


def void_to_minedschema(void_ttl: str, *, local_backend: LocalBackend = "oxigraph") -> MinedSchema:
    """Parse VoID Turtle into MinedSchema.

    Args:
        void_ttl: VoID document in Turtle format

    Returns:
        MinedSchema with patterns reconstructed from VoID

    Example:
        >>> void_ttl = Path("dataset_void.ttl").read_text()
        >>> schema = void_to_minedschema(void_ttl)
        >>> print(len(schema.patterns))
        435
    """
    g = Graph()
    g.parse(data=void_ttl, format="turtle")
    return void_graph_to_minedschema(g, local_backend=local_backend)


def void_graph_to_minedschema(
    g: Graph,
    *,
    endpoint: str | None = None,
    report_untyped: bool = True,
    local_backend: LocalBackend = "oxigraph",
) -> MinedSchema:
    """Read VoID RDF without treating metadata predicates as patterns."""
    patterns = _extract_patterns_from_void(g, local_backend=local_backend)
    if report_untyped:
        warn_untyped_partitions(g, patterns)
    about = _extract_metadata_from_void(g, endpoint=endpoint)
    about.pattern_count = len(patterns)
    for partition in set(g.objects(None, VOID.classPartition)) | set(
        g.subjects(VOID["class"], None)
    ):
        class_iri = g.value(partition, VOID["class"])
        count = optional_count(g.value(partition, VOID.entities))
        if class_iri is not None and count is not None:
            about.class_entity_counts[str(class_iri)] = count

    from rdfsolve.schema_models.enrichment import SchemaEnrichment
    from rdfsolve.schema_models.metadata import MetadataDocument, RetainedMetadata

    schema = MinedSchema(
        patterns=patterns,
        about=about,
        prefixes={prefix: str(namespace) for prefix, namespace in g.namespaces()},
        source_metadata=RetainedMetadata.from_document(
            MetadataDocument(graph=g, endpoint=endpoint, scope="retained VoID RDF")
        ),
    )
    schema.enrichment = SchemaEnrichment.from_rdf_graph(
        g, schema.get_classes(), schema.get_properties()
    )
    return schema


def _extract_patterns_from_void(
    g: Graph, *, local_backend: LocalBackend = "oxigraph"
) -> list[SchemaPattern]:
    """Extract SchemaPattern list from VoID graph."""
    from rdflib.namespace import RDFS

    patterns = []
    engine = LocalRdf(g, backend=local_backend)

    # Extract all rdfs:label triples for URIs
    labels: dict[str, str] = {}
    for s, _, o in g.triples((None, RDFS.label, None)):
        labels[str(s)] = str(o)

    # Query nested class partitions with property partitions
    query = """
    PREFIX void: <http://rdfs.org/ns/void#>
    PREFIX void-ext: <http://ldf.fi/void-ext#>

    SELECT ?subjectClass ?property ?objectClass ?datatype ?count
    WHERE {
        # Top-level class partition
        ?dataset void:classPartition ?cp .
        ?cp void:class ?subjectClass .

        # Property partition within class partition
        ?cp void:propertyPartition ?pp .
        ?pp void:property ?property .

        # Keep each object partition and its own count in a separate row.
        {
            ?pp void:classPartition ?objCp .
            ?objCp void:class ?objectClass .
            OPTIONAL { ?objCp void:triples ?count }
        } UNION {
            ?pp void-ext:datatypePartition ?dp .
            ?dp void-ext:datatype ?datatype .
            OPTIONAL { ?dp void:triples ?count }
        }
    }
    """

    for row in engine.query(query):
        if not isinstance(row, ResultRow):
            raise TypeError("Expected a SELECT result row")
        subject_class = str(row.subjectClass)
        property_uri = str(row.property)

        # Get count safely
        count_val = row.get("count")
        count = optional_count(count_val)

        if row.get("objectClass"):
            # Typed object pattern
            object_class = str(row.objectClass)
            patterns.append(
                SchemaPattern(
                    subject_class=subject_class,
                    property_uri=property_uri,
                    object_class=object_class,
                    count=count,
                    evidence_source="void",
                    subject_label=labels.get(subject_class),
                    property_label=labels.get(property_uri),
                    object_label=labels.get(object_class),
                )
            )
        elif row.get("datatype"):
            # Literal pattern with datatype
            patterns.append(
                SchemaPattern(
                    subject_class=subject_class,
                    property_uri=property_uri,
                    object_class="Literal",
                    datatype=str(row.datatype),
                    count=count,
                    evidence_source="void",
                    subject_label=labels.get(subject_class),
                    property_label=labels.get(property_uri),
                )
            )

    # Linksets give the class at each end of a link (void-generator, as published by IDSM, UniProt,
    # Rhea, Bgee, SwissLipids). Read with the nested partitions: a description with datatype
    # partitions states its links between classes only as linksets. A link that a nested
    # partition already states is not read twice.
    linkset_query = """
    PREFIX void: <http://rdfs.org/ns/void#>

    SELECT ?subjectClass ?predicate ?objectClass ?count ?subjects ?objects
    WHERE {
        ?ls a void:Linkset .
        ?ls void:subjectsTarget ?st .
        ?st void:class ?subjectClass .
        ?ls void:linkPredicate ?predicate .
        ?ls void:objectsTarget ?ot .
        ?ot void:class ?objectClass .
        OPTIONAL { ?ls void:triples ?count }
        OPTIONAL { ?ls void:distinctSubjects ?subjects }
        OPTIONAL { ?ls void:distinctObjects ?objects }
    }
    """
    nested = {(p.subject_class, p.property_uri, p.object_class) for p in patterns}
    linked: dict[tuple[str, str, str], SchemaPattern] = {}
    for row in engine.query(linkset_query):
        if not isinstance(row, ResultRow):
            raise TypeError("Expected a SELECT result row")
        key = (str(row.subjectClass), str(row.predicate), str(row.objectClass))
        # A linkset of rdf:type states that members of a class are typed with classes
        # (owl:Class): membership, which the class partitions state, not a link.
        if key in nested or key[1] == str(RDF.type):
            continue
        count = optional_count(row.get("count"))
        previous = linked.get(key)
        if previous is not None:
            # The same links described twice, with their ends typed in the graph and in the union
            # of graphs (IDSM): the larger description holds the other.
            if count is not None and (previous.count is None or count > previous.count):
                previous.count = count
                previous.distinct_subjects = optional_count(row.get("subjects"))
                previous.distinct_objects = optional_count(row.get("objects"))
            continue
        subject_class, property_uri, object_class = key
        linked[key] = SchemaPattern(
            subject_class=subject_class,
            property_uri=property_uri,
            object_class=object_class,
            count=count,
            distinct_subjects=optional_count(row.get("subjects")),
            distinct_objects=optional_count(row.get("objects")),
            evidence_source="void",
            subject_label=labels.get(subject_class),
            property_label=labels.get(property_uri),
            object_label=labels.get(object_class),
        )
    patterns.extend(linked.values())

    return patterns


def _extract_metadata_from_void(g: Graph, *, endpoint: str | None = None) -> AboutMetadata:
    """Project one dataset without guessing across independent descriptions."""
    from rdflib.namespace import DCTERMS, FOAF, OWL
    from rdflib.term import Node

    from rdfsolve.schema_models.metadata import MetadataDocument

    metadata = MetadataDocument(graph=g, endpoint=endpoint).project()
    subject = metadata.pop("metadata_subject_iri", None)
    blank_subject = metadata.pop("metadata_subject_blank_node", None)
    metadata.pop("metadata_identity_basis", None)
    if subject is None and blank_subject is None:
        return AboutMetadata.build()
    from rdflib import BNode

    dataset_uri = URIRef(subject) if subject is not None else BNode(blank_subject)

    def unique(predicate: URIRef) -> Node | None:
        """Return a value only when the dataset states exactly one."""
        values = set(g.objects(dataset_uri, predicate))
        return next(iter(values)) if len(values) == 1 else None

    documents = set(g.subjects(FOAF.primaryTopic, dataset_uri))
    document = next(iter(documents)) if len(documents) == 1 else None
    schema_version = g.value(document, OWL.versionInfo) if document is not None else None
    generated_at = g.value(document, DCTERMS.created) if document is not None else None
    return AboutMetadata.build(
        **metadata,
        endpoint=str(unique(VOID.sparqlEndpoint))
        if unique(VOID.sparqlEndpoint) is not None
        else None,
        dataset_name=metadata.get("title"),
        class_count=optional_count(unique(VOID.classes)) or 0,
        property_count=optional_count(unique(VOID.properties)) or 0,
        triple_count_estimate=optional_count(unique(VOID.triples)),
        distinct_subject_count=optional_count(unique(VOID.distinctSubjects)),
        distinct_object_count=optional_count(unique(VOID.distinctObjects)),
        schema_version=str(schema_version) if schema_version is not None else None,
        finished_at=str(generated_at) if generated_at is not None else None,
    )


def warn_untyped_partitions(g: Graph, patterns: list[SchemaPattern]) -> None:
    """Report partitions still missing object kinds after all readers finish."""
    represented = {(p.subject_class, p.property_uri) for p in patterns}
    ambiguous = []
    for cp in g.objects(None, VOID.classPartition):
        subject_node = g.value(cp, VOID["class"])
        if subject_node is None:
            continue
        for pp in g.objects(cp, VOID.propertyPartition):
            predicate = g.value(pp, VOID.property)
            if predicate is not None and (str(subject_node), str(predicate)) not in represented:
                ambiguous.append((str(subject_node), str(predicate)))
    if ambiguous:
        logging.getLogger(__name__).warning(
            f"VoID omits object kinds for {len(ambiguous)} property partitions; "
            f"these are not converted to patterns. Examples: {ambiguous[:3]}. "
            "Use the canonical schema JSON to preserve all object kinds.",
        )
