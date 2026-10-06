"""Export VoID partitions; do not infer missing object kinds or totals."""

from __future__ import annotations

from hashlib import md5
from typing import TYPE_CHECKING, Any

from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS

if TYPE_CHECKING:
    from rdflib import Graph

    from rdfsolve.evidence.observed import PropertyUsageCollection
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.void_model import VoidDataset

VOID = "http://rdfs.org/ns/void#"
VOID_EXT = "http://ldf.fi/void-ext#"


def public_endpoint(endpoint: str | None) -> bool:
    """Check whether an endpoint is an HTTP(S) service outside this machine."""
    from urllib.parse import urlsplit

    if not endpoint:
        return False
    parts = urlsplit(endpoint)
    return parts.scheme in {"http", "https"} and parts.hostname not in {
        None,
        "localhost",
        "127.0.0.1",
        "::1",
    }


def to_void_graph(schema: MinedSchema, *, trim_descriptions: int | None = None) -> Graph:
    """Build an rdflib VoID Graph from the mined patterns.

    The dataset has the minted rdfsolve dataset IRI. void:sparqlEndpoint is
    written only for public HTTP(S) endpoints. The generated description is not
    a data dump.
    """
    from rdflib import Graph, Namespace, URIRef
    from rdflib import Literal as RdfLiteral
    from rdflib.namespace import DCTERMS, FOAF, OWL, RDF, RDFS, XSD

    from rdfsolve.config import mint

    # Configure namespaces
    dataset_iri = mint("dataset", schema.about.dataset_name or "unnamed")
    void = Namespace("http://rdfs.org/ns/void#")
    sd = Namespace("http://www.w3.org/ns/sparql-service-description#")
    partition_ns = Namespace(f"{dataset_iri}/partition/")

    g = Graph()

    # void-ext namespace for datatype partitions
    void_ext = Namespace("http://ldf.fi/void-ext#")

    # Bind standard namespaces
    for pfx, ns in (
        ("void", void),
        ("void-ext", void_ext),
        ("sd", sd),
        ("rdf", RDF),
        ("rdfs", RDFS),
        ("xsd", XSD),
        ("dcterms", DCTERMS),
        ("foaf", FOAF),
        ("owl", OWL),
    ):
        g.bind(pfx, ns)

    g.bind("partition", partition_ns)

    endpoint = schema.about.endpoint
    remote = public_endpoint(endpoint)
    dataset_uri = URIRef(dataset_iri)
    base = str(partition_ns)

    # void:DatasetDescription wrapper (W3C VoID spec section 3.1)
    # The VoID document itself is described as a resource
    from rdfsolve.version import VERSION

    void_doc_uri = URIRef("")  # <> represents this document
    g.add((void_doc_uri, RDF.type, void.DatasetDescription))

    # DatasetDescription title includes the source name
    dataset_display_name = schema.about.title or schema.about.dataset_name or "Unknown Dataset"
    g.add((void_doc_uri, DCTERMS.title, RdfLiteral(f"VoID Description of {dataset_display_name}")))
    g.add((void_doc_uri, DCTERMS.creator, RdfLiteral(f"rdfsolve {VERSION}")))
    if schema.about.generated_at:
        g.add((void_doc_uri, DCTERMS.created, RdfLiteral(schema.about.generated_at)))
    g.add((void_doc_uri, FOAF.primaryTopic, dataset_uri))
    # The generated VoID document is an rdfsolve artifact, not an ontology/source
    # release. Its run/snapshot identity is recorded by release provenance instead
    # of owl:versionInfo.

    # void:Dataset represents the source RDF dataset
    g.add((dataset_uri, RDF.type, void.Dataset))

    if remote and endpoint:
        g.add((dataset_uri, void.sparqlEndpoint, URIRef(endpoint)))

    # void:documents (only for local/downloaded datasets)
    if not remote and schema.about.document_count:
        g.add(
            (
                dataset_uri,
                void.documents,
                RdfLiteral(schema.about.document_count, datatype=XSD.integer),
            )
        )

    # Title: prefer explicit title from metadata, fallback to dataset_name
    title_value = schema.about.title or schema.about.dataset_name
    if title_value:
        g.add((dataset_uri, DCTERMS.title, RdfLiteral(title_value)))

    if schema.about.description:
        from rdfsolve.schema_models.exporters.text import clip_description

        g.add(
            (
                dataset_uri,
                DCTERMS.description,
                RdfLiteral(clip_description(schema.about.description, trim_descriptions)),
            )
        )

    # License
    if schema.about.source_license:
        g.add((dataset_uri, DCTERMS.license, URIRef(schema.about.source_license)))

    # Publisher
    if schema.about.source_publisher:
        # Try as URI first, fallback to literal if not a valid URI
        try:
            if schema.about.source_publisher.startswith(("http://", "https://", "urn:")):
                g.add((dataset_uri, DCTERMS.publisher, URIRef(schema.about.source_publisher)))
            else:
                g.add((dataset_uri, DCTERMS.publisher, RdfLiteral(schema.about.source_publisher)))
        except Exception:
            g.add((dataset_uri, DCTERMS.publisher, RdfLiteral(schema.about.source_publisher)))

    # Creators (multiple allowed)
    if schema.about.source_creator:
        for creator in schema.about.source_creator:
            # Try as URI first, fallback to literal
            try:
                if creator.startswith(("http://", "https://", "urn:")):
                    g.add((dataset_uri, DCTERMS.creator, URIRef(creator)))
                else:
                    g.add((dataset_uri, DCTERMS.creator, RdfLiteral(creator)))
            except Exception:
                g.add((dataset_uri, DCTERMS.creator, RdfLiteral(creator)))

    # Homepage
    if schema.about.homepage:
        g.add((dataset_uri, FOAF.homepage, URIRef(schema.about.homepage)))

    # Version info
    if schema.about.source_version_iri:
        g.add((dataset_uri, OWL.versionIRI, URIRef(schema.about.source_version_iri)))
    if schema.about.source_version:
        g.add((dataset_uri, OWL.versionInfo, RdfLiteral(schema.about.source_version)))

    # Dates
    if schema.about.source_issued:
        g.add((dataset_uri, DCTERMS.issued, RdfLiteral(schema.about.source_issued)))
    if schema.about.source_modified:
        g.add((dataset_uri, DCTERMS.modified, RdfLiteral(schema.about.source_modified)))

    # Dataset statistics
    if schema.about.class_count:
        g.add(
            (
                dataset_uri,
                void.classes,
                RdfLiteral(schema.about.class_count, datatype=XSD.integer),
            )
        )
    # The distinct properties of the data, when counted, else those of the patterns.
    properties = schema.about.distinct_predicate_count or schema.about.property_count
    if properties:
        g.add(
            (
                dataset_uri,
                void.properties,
                RdfLiteral(properties, datatype=XSD.integer),
            )
        )
    if schema.about.triple_count_estimate:
        g.add(
            (
                dataset_uri,
                void.triples,
                RdfLiteral(schema.about.triple_count_estimate, datatype=XSD.integer),
            )
        )
    if schema.about.distinct_subject_count:
        g.add(
            (
                dataset_uri,
                void.distinctSubjects,
                RdfLiteral(schema.about.distinct_subject_count, datatype=XSD.integer),
            )
        )
    if schema.about.distinct_object_count:
        g.add(
            (
                dataset_uri,
                void.distinctObjects,
                RdfLiteral(schema.about.distinct_object_count, datatype=XSD.integer),
            )
        )

    # Extract vocabularies from patterns
    vocabs = set()
    for pat in schema.patterns:
        for uri in [pat.subject_class, pat.property_uri, pat.object_class]:
            if uri and uri not in _SENTINEL_OBJECTS:
                # Extract namespace
                if "#" in uri:
                    vocab = uri.rsplit("#", 1)[0] + "#"
                elif "/" in uri:
                    vocab = uri.rsplit("/", 1)[0] + "/"
                else:
                    continue
                # Skip W3C vocabularies
                if not vocab.startswith("http://www.w3.org/"):
                    vocabs.add(vocab)

    # Add vocabulary declarations
    for vocab_uri in sorted(vocabs):
        g.add((dataset_uri, void.vocabulary, URIRef(vocab_uri)))

    # Add discovered graphs as sd:namedGraph if available
    if schema.about.discovered_graphs and remote and endpoint:
        from rdflib import BNode

        service_uri = BNode()
        g.add((service_uri, RDF.type, sd.Service))
        g.add((service_uri, sd.endpoint, URIRef(endpoint)))

        # Link service to dataset description
        g.add((service_uri, sd.defaultDataset, dataset_uri))
        g.add((dataset_uri, RDF.type, sd.Dataset))

        # Add sd:namedGraph entries for each discovered graph
        for graph_info in schema.about.discovered_graphs:
            graph_uri_str = graph_info.get("uri")
            graph_count = graph_info.get("count")

            if not graph_uri_str:
                continue

            # Create sd:namedGraph structure
            named_graph_node = BNode()
            g.add((dataset_uri, sd.namedGraph, named_graph_node))
            g.add((named_graph_node, RDF.type, sd.NamedGraph))
            g.add((named_graph_node, sd.name, URIRef(graph_uri_str)))

            # Create the sd:graph description
            graph_node = BNode()
            g.add((named_graph_node, sd.graph, graph_node))
            g.add((graph_node, RDF.type, sd.Graph))
            g.add((graph_node, RDF.type, void.Dataset))

            # Add triple count
            if graph_count is not None:
                g.add(
                    (
                        graph_node,
                        void.triples,
                        RdfLiteral(graph_count, datatype=XSD.integer),
                    )
                )

            # Add dcterms:title (derive from graph URI if not provided)
            graph_title = graph_info.get("title")
            if not graph_title:
                # Derive title from URI (e.g., "http://example.org/data/" -> "data")
                if "#" in graph_uri_str:
                    graph_title = graph_uri_str.rsplit("#", 1)[-1] or graph_uri_str
                elif "/" in graph_uri_str.rstrip("/"):
                    graph_title = graph_uri_str.rstrip("/").rsplit("/", 1)[-1] or graph_uri_str
                else:
                    graph_title = graph_uri_str

            if graph_title:
                g.add((graph_node, DCTERMS.title, RdfLiteral(graph_title)))

            # Add foaf:homepage (use graph URI as homepage if it's HTTP)
            homepage = graph_info.get("homepage")
            if not homepage and graph_uri_str.startswith(("http://", "https://")):
                # Use the graph URI itself as homepage
                homepage = graph_uri_str.rstrip("/")

            if homepage:
                g.add((graph_node, FOAF.homepage, URIRef(homepage)))

            # Mark ontology graphs with void:vocabulary
            if (
                schema.about.ontology_graph_uris
                and graph_uri_str in schema.about.ontology_graph_uris
            ):
                g.add((dataset_uri, void.vocabulary, URIRef(graph_uri_str)))
                # Also mark the graph itself
                g.add((graph_node, DCTERMS.type, URIRef("http://www.w3.org/2002/07/owl#Ontology")))

    # Dataset property partitions: the exact counts of each property of the data.
    names = {"triples": void.triples, "distinct_subjects": void.distinctSubjects}
    names["distinct_objects"] = void.distinctObjects
    for prop_uri, counts in sorted((schema.about.property_partitions or {}).items()):
        prop_hash = md5(prop_uri.encode(), usedforsecurity=False).hexdigest()[:8]
        dataset_partition = URIRef(f"{base}prop-{prop_hash}")
        g.add((dataset_uri, void.propertyPartition, dataset_partition))
        g.add((dataset_partition, void.property, URIRef(prop_uri)))
        for name, term in names.items():
            if counts.get(name) is not None:
                number = RdfLiteral(counts[name], datatype=XSD.integer)
                g.add((dataset_partition, term, number))

    # Group patterns by subject class for nested VoID structure
    # Structure: class partition -> property partition -> object/datatype partition
    from collections import defaultdict

    # subject_class -> property_uri -> [(object_class, datatype, count), ...]
    class_prop_objects: dict[str, dict[str, list[tuple[str, str | None, int | None]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    # subject_class -> property_uri -> the patterns, for the totals of the property partition
    class_prop_patterns: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))

    # Collect labels for all URIs
    uri_labels: dict[str, str] = {}
    for pat in schema.patterns:
        if not pat.subject_class or pat.subject_class in _SENTINEL_OBJECTS:
            continue
        if not pat.property_uri:
            continue

        class_prop_objects[pat.subject_class][pat.property_uri].append(
            (pat.object_class, pat.datatype, pat.count)
        )
        class_prop_patterns[pat.subject_class][pat.property_uri].append(pat)

        # Collect labels
        if pat.subject_label and pat.subject_class not in _SENTINEL_OBJECTS:
            uri_labels[pat.subject_class] = pat.subject_label
        if pat.property_label:
            uri_labels[pat.property_uri] = pat.property_label
        if pat.object_label and pat.object_class not in _SENTINEL_OBJECTS:
            uri_labels[pat.object_class] = pat.object_label

    # Write rdfs:label triples for all URIs
    for uri, label in uri_labels.items():
        g.add((URIRef(uri), RDFS.label, RdfLiteral(label)))

    # Generate nested VoID class partitions per void-generator spec
    for subject_class in sorted(class_prop_objects.keys()):
        # Create class partition URI
        cls_hash = md5(subject_class.encode(), usedforsecurity=False).hexdigest()[:8]
        class_partition_uri = URIRef(f"{base}class-{cls_hash}")

        # Add class partition to dataset
        g.add((dataset_uri, void.classPartition, class_partition_uri))
        g.add((class_partition_uri, RDF.type, void.Dataset))
        g.add((class_partition_uri, void["class"], URIRef(subject_class)))

        count = schema.about.class_entity_counts.get(subject_class)
        # void:entities states the members; a count that is only a lower bound is not written.
        state = schema.about.class_entity_count_states.get(subject_class, "complete")
        if count is not None and state == "complete":
            g.add((class_partition_uri, void.entities, RdfLiteral(count, datatype=XSD.integer)))

        # Add property partitions within this class partition
        for prop_uri in sorted(class_prop_objects[subject_class].keys()):
            prop_hash = md5(prop_uri.encode(), usedforsecurity=False).hexdigest()[:8]
            prop_partition_uri = URIRef(f"{base}class-{cls_hash}-prop-{prop_hash}")

            g.add((class_partition_uri, void.propertyPartition, prop_partition_uri))
            g.add((prop_partition_uri, RDF.type, void.Dataset))
            g.add((prop_partition_uri, void.property, URIRef(prop_uri)))

            # Object classes can overlap; do not sum their triple counts. The totals of the
            # partition are written only where the patterns give them exactly (_exact_totals).
            for term, total in _exact_totals(class_prop_patterns[subject_class][prop_uri]):
                g.add((prop_partition_uri, term, RdfLiteral(total, datatype=XSD.integer)))

            # Add object class partitions or datatype partitions
            for object_class, datatype, count in class_prop_objects[subject_class][prop_uri]:
                if object_class == "Literal":
                    # Add datatype partition for literals
                    if datatype:
                        dt_hash = md5(datatype.encode(), usedforsecurity=False).hexdigest()[:8]
                        dt_partition_uri = URIRef(
                            f"{base}class-{cls_hash}-prop-{prop_hash}-dt-{dt_hash}"
                        )

                        g.add((prop_partition_uri, void_ext.datatypePartition, dt_partition_uri))
                        g.add((dt_partition_uri, void_ext.datatype, URIRef(datatype)))

                        if count is not None:
                            g.add(
                                (
                                    dt_partition_uri,
                                    void.triples,
                                    RdfLiteral(count, datatype=XSD.integer),
                                )
                            )
                        _add_distinct(
                            g,
                            dt_partition_uri,
                            class_prop_patterns[subject_class][prop_uri],
                            object_class,
                            datatype,
                        )

                elif (
                    object_class
                    and object_class != "Resource"
                    and object_class not in _SENTINEL_OBJECTS
                ):
                    # Add nested class partition for typed objects
                    obj_hash = md5(object_class.encode(), usedforsecurity=False).hexdigest()[:8]
                    obj_partition_uri = URIRef(
                        f"{base}class-{cls_hash}-prop-{prop_hash}-obj-{obj_hash}"
                    )

                    g.add((prop_partition_uri, void.classPartition, obj_partition_uri))
                    g.add((obj_partition_uri, RDF.type, void.Dataset))
                    g.add((obj_partition_uri, void["class"], URIRef(object_class)))

                    if count is not None:
                        g.add(
                            (
                                obj_partition_uri,
                                void.triples,
                                RdfLiteral(count, datatype=XSD.integer),
                            )
                        )
                    _add_distinct(
                        g,
                        obj_partition_uri,
                        class_prop_patterns[subject_class][prop_uri],
                        object_class,
                        None,
                    )

    # A typed relationship alone does not establish a cross-dataset linkset.

    schema.annotate_rdf(g)
    for partition, class_iri in g.subject_objects(void["class"]):
        for example in schema.enrichment.class_examples.get(str(class_iri), []):
            if example.kind == "uri":
                g.add((partition, void.exampleResource, example.to_rdf()))
    return g


def _exact_totals(patterns: list[Any]) -> list[tuple[Any, int]]:
    """Return the counts of a (class, property) partition that its patterns give exactly.

    The rows of literals (one per datatype), of IRIs without a class (Resource) and of blank
    nodes (BlankNode, typed or not) do not overlap; rows of object classes overlap each other
    (an object with two classes) and the blank-node row (a typed blank node). So:
    void-ext:distinctLiterals is the sum of the literal rows (values of different datatypes
    differ), void-ext:distinctBlankNodeObjects the blank-node row, and, when no row has an
    object class, void-ext:distinctIRIReferenceObjects is the Resource row and void:triples the
    sum of all rows. Property usage evidence gives the totals in the other cases
    (add_property_usage).
    """
    from rdflib import URIRef

    literal = [p for p in patterns if p.object_class == "Literal"]
    resource = [p for p in patterns if p.object_class == "Resource"]
    blank = [p for p in patterns if p.object_class == "BlankNode"]
    classed = [p for p in patterns if p.object_class not in _SENTINEL_OBJECTS]
    found: list[tuple[Any, int]] = []
    if literal and all(p.distinct_objects is not None for p in literal):
        found.append(
            (URIRef(VOID_EXT + "distinctLiterals"), sum(p.distinct_objects for p in literal))
        )
    if len(blank) == 1 and blank[0].distinct_objects is not None:
        found.append((URIRef(VOID_EXT + "distinctBlankNodeObjects"), blank[0].distinct_objects))
    if not classed:
        if len(resource) == 1 and resource[0].distinct_objects is not None:
            found.append(
                (URIRef(VOID_EXT + "distinctIRIReferenceObjects"), resource[0].distinct_objects)
            )
        if patterns and all(p.count is not None for p in patterns):
            found.append((URIRef(VOID + "triples"), sum(p.count for p in patterns)))
    return found


def _add_distinct(
    g: Graph, partition: Any, patterns: list[Any], object_class: str, datatype: str | None
) -> None:
    """Add the distinct subjects and objects of the pattern of a nested partition."""
    from rdflib import Literal as RdfLiteral
    from rdflib import URIRef
    from rdflib.namespace import XSD

    for p in patterns:
        if p.object_class == object_class and (object_class != "Literal" or p.datatype == datatype):
            for name, value in (
                ("distinctSubjects", p.distinct_subjects),
                ("distinctObjects", p.distinct_objects),
            ):
                if value is not None:
                    g.add((partition, URIRef(VOID + name), RdfLiteral(value, datatype=XSD.integer)))
            return


def add_property_usage(g: Graph, schema: MinedSchema, usage: PropertyUsageCollection) -> int:
    """Add the totals of each (class, property) partition from property usage evidence.

    The evidence counts the edges of the members of each class with each property over every
    object kind: void:triples, void:distinctSubjects (members with the property) and
    void:distinctObjects. A partition that the patterns do not have is not added. Return the
    partitions completed.
    """
    from rdflib import Literal as RdfLiteral
    from rdflib import URIRef
    from rdflib.namespace import XSD

    from rdfsolve.config import mint

    base = f"{mint('dataset', schema.about.dataset_name or 'unnamed')}/partition/"
    added = 0
    for record in usage.records:
        cls = md5(record.subject_class.encode(), usedforsecurity=False).hexdigest()[:8]
        prop = md5(record.property_uri.encode(), usedforsecurity=False).hexdigest()[:8]
        partition = URIRef(f"{base}class-{cls}-prop-{prop}")
        if (None, URIRef(VOID + "propertyPartition"), partition) not in g:
            continue
        for name, value in (
            ("triples", record.triple_count),
            ("distinctSubjects", record.subjects_with_property),
            ("distinctObjects", record.distinct_objects),
        ):
            if value is not None and record.summary_state.status == "complete":
                g.set((partition, URIRef(VOID + name), RdfLiteral(value, datatype=XSD.integer)))
        added += 1
    return added


def minedschema_to_void(schema: MinedSchema) -> VoidDataset:
    """Read structural VoID fields from the canonical RDF exporter."""
    from rdflib.namespace import FOAF

    from rdfsolve.schema_models.void_model import VoidDataset

    graph = schema.to_void_graph()
    dataset = next(graph.objects(None, FOAF.primaryTopic), None)
    if dataset is None:
        raise ValueError("Export has no primary dataset")
    return VoidDataset.from_rdf(graph, dataset)
