"""VoID to MinedSchema conversion functions."""

from __future__ import annotations

from rdflib import Graph, Namespace, URIRef
from rdflib.query import ResultRow

from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.core import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.void_model import VoidDataset

VOID = Namespace("http://rdfs.org/ns/void#")
VOID_EXT = Namespace("http://ldf.fi/void-ext#")


def minedschema_to_void(schema: MinedSchema, base_url: str = "https://example.org") -> VoidDataset:
    """Read structural VoID fields from the canonical RDF exporter."""
    from rdflib.namespace import FOAF

    graph = schema.to_void_graph(base_url=base_url)
    dataset = next(graph.objects(None, FOAF.primaryTopic), None)
    if dataset is None:
        raise ValueError("Export has no primary dataset")
    return VoidDataset.from_rdf(graph, dataset)


def void_to_minedschema(void_ttl: str) -> MinedSchema:
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
    return void_graph_to_minedschema(g)


def void_graph_to_minedschema(g: Graph) -> MinedSchema:
    """Read VoID RDF without treating metadata predicates as patterns."""
    patterns = _extract_patterns_from_void(g)
    about = _extract_metadata_from_void(g)
    about.pattern_count = len(patterns)
    for partition in g.objects(None, VOID.classPartition):
        class_iri = g.value(partition, VOID["class"])
        count = optional_count(g.value(partition, VOID.entities))
        if class_iri is not None and count is not None:
            about.class_entity_counts[str(class_iri)] = count

    from rdfsolve.schema_models.enrichment import SchemaEnrichment

    schema = MinedSchema(patterns=patterns, about=about)
    schema.enrichment = SchemaEnrichment.from_rdf_graph(
        g, schema.get_classes(), schema.get_properties()
    )
    return schema


def _extract_patterns_from_void(g: Graph) -> list[SchemaPattern]:
    """Extract SchemaPattern list from VoID graph."""
    from rdflib.namespace import RDFS

    patterns = []

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

    for row in g.query(query):
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
                    subject_label=labels.get(subject_class),
                    property_label=labels.get(property_uri),
                )
            )
        else:
            # Property partition without nested partitions - likely Resource or BlankNode
            # For now, default to Resource (untyped URI)
            # We'll need to check if the pattern already exists before adding
            pass

    # Also extract from LinkSets
    linkset_query = """
    PREFIX void: <http://rdfs.org/ns/void#>

    SELECT ?subjectClass ?predicate ?objectClass ?count
    WHERE {
        ?ls a void:Linkset .
        ?ls void:subjectsTarget ?st .
        ?st void:class ?subjectClass .
        ?ls void:linkPredicate ?predicate .
        ?ls void:objectsTarget ?ot .
        ?ot void:class ?objectClass .
        OPTIONAL { ?ls void:triples ?count }
    }
    """

    # Use linksets only if no nested partitions found (avoid duplicates)
    if not patterns:
        for row in g.query(linkset_query):
            if not isinstance(row, ResultRow):
                raise TypeError("Expected a SELECT result row")
            count_val = row.get("count")
            count = optional_count(count_val)
            subject_class = str(row.subjectClass)
            property_uri = str(row.predicate)
            object_class = str(row.objectClass)
            patterns.append(
                SchemaPattern(
                    subject_class=subject_class,
                    property_uri=property_uri,
                    object_class=object_class,
                    count=count,
                    subject_label=labels.get(subject_class),
                    property_label=labels.get(property_uri),
                    object_label=labels.get(object_class),
                )
            )

    return patterns


def _extract_metadata_from_void(g: Graph) -> AboutMetadata:
    """Extract AboutMetadata from VoID graph."""
    from rdflib.namespace import DCTERMS, FOAF, OWL, RDF

    # Find the main dataset
    dataset_uri = None
    for s in g.subjects(RDF.type, VOID.Dataset):
        # Prefer non-partition datasets (those with sparqlEndpoint or title)
        if g.value(s, VOID.sparqlEndpoint) or g.value(s, DCTERMS.title):
            dataset_uri = s
            break

    if not dataset_uri:
        # Fallback to any dataset
        for s in g.subjects(RDF.type, VOID.Dataset):
            dataset_uri = s
            break

    if not dataset_uri:
        return AboutMetadata.build()

    title = g.value(dataset_uri, DCTERMS.title)
    description = g.value(dataset_uri, DCTERMS.description)
    endpoint = g.value(dataset_uri, VOID.sparqlEndpoint)
    classes = g.value(dataset_uri, VOID.classes)
    properties = g.value(dataset_uri, VOID.properties)
    triples = g.value(dataset_uri, VOID.triples)
    version_iri = g.value(dataset_uri, OWL.versionIRI)
    source_version = g.value(dataset_uri, OWL.versionInfo)
    document = g.value(predicate=FOAF.primaryTopic, object=dataset_uri)
    schema_version = g.value(document, OWL.versionInfo) if document is not None else None
    generated_at = g.value(document, DCTERMS.created) if document is not None else None

    return AboutMetadata.build(
        endpoint=str(endpoint) if endpoint else None,
        dataset_name=str(title) if title else None,
        title=str(title) if title else None,
        description=str(description) if description else None,
        class_count=optional_count(classes) or 0,
        property_count=optional_count(properties) or 0,
        triple_count_estimate=optional_count(triples),
        source_version_iri=str(version_iri) if version_iri is not None else None,
        source_version=str(source_version) if source_version is not None else None,
        schema_version=str(schema_version) if schema_version is not None else None,
        finished_at=str(generated_at) if generated_at is not None else None,
    )
