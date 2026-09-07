"""Export VoID partitions; do not infer missing object kinds or totals."""

from __future__ import annotations

import logging
from hashlib import md5
from typing import TYPE_CHECKING, Any

from rdfsolve._uri import uri_to_curie
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS

if TYPE_CHECKING:
    from rdflib import Graph

    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern
    from rdfsolve.schema_models.void_model import VoidDataset

_log = logging.getLogger(__name__)


def to_void_graph(schema: MinedSchema, base_url: str | None = None) -> Graph:
    """Build an rdflib VoID Graph from the mined patterns.

    Args:
        base_url: Base URL for void:dataDump (e.g., "https://rdfsolve.bigcat-bioinformatics.nl").
                 If provided, generates dataDump URL as {base_url}/{dataset_name}/void.ttl

    Allows feeding the result into VoidParser for downstream
    conversion to LinkML, SHACL, RDF-config, etc.
    """
    import json

    from rdflib import Graph, Namespace, URIRef
    from rdflib import Literal as RdfLiteral
    from rdflib.namespace import DCTERMS, FOAF, OWL, RDF, RDFS, XSD

    from rdfsolve.config import get_base_uri

    # Configure namespaces
    base_uri = get_base_uri()
    void = Namespace("http://rdfs.org/ns/void#")
    sd = Namespace("http://www.w3.org/ns/sparql-service-description#")
    vocab_ns = Namespace(f"{base_uri}/vocab#")
    partition_ns = Namespace(f"{base_uri}/schema#")

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

    # Bind rdfsolve-specific namespaces
    g.bind("vocab", vocab_ns)
    g.bind("partition", partition_ns)

    # Dataset URI: represents THE SOURCE RDF dataset
    endpoint = schema.about.endpoint

    if endpoint and not endpoint.startswith(("http://localhost", "http://127.0.0.1")):
        # Remote endpoint: use endpoint URL as dataset identifier
        dataset_uri = URIRef(endpoint)
        # Use schema namespace for partitions
        base = str(partition_ns)
    else:
        # Local or no endpoint: use configured base URI
        dataset_name = schema.about.dataset_name or "unknown"
        dataset_uri = URIRef(f"{base_uri}/dataset/{dataset_name}")
        # Use schema namespace for partitions
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
    if schema.about.schema_version:
        g.add((void_doc_uri, OWL.versionInfo, RdfLiteral(schema.about.schema_version)))

    # void:Dataset represents the source RDF dataset
    g.add((dataset_uri, RDF.type, void.Dataset))

    # SPARQL endpoint (only for remote endpoints)
    if endpoint and not endpoint.startswith(("http://localhost", "http://127.0.0.1")):
        g.add((dataset_uri, void.sparqlEndpoint, URIRef(endpoint)))

    # void:dataDump URL (only for local/downloaded datasets with base_url)
    if (
        base_url
        and schema.about.dataset_name
        and (not endpoint or endpoint.startswith(("http://localhost", "http://127.0.0.1")))
    ):
        # Generate dataDump URL pointing to the dump file location
        datadump_url = f"{base_url.rstrip('/')}/{schema.about.dataset_name}/void.ttl"
        g.add((dataset_uri, void.dataDump, URIRef(datadump_url)))

    # void:documents (only for local/downloaded datasets)
    if (
        not endpoint or endpoint.startswith(("http://localhost", "http://127.0.0.1"))
    ) and schema.about.document_count:
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

    # Description: combine discovered description with provenance info
    provenance_desc = (
        f"Mined with rdfsolve {VERSION} on {schema.about.generated_at or 'unknown date'}"
    )
    if schema.about.description:
        # Strip trailing period from discovered description if present
        desc_clean = schema.about.description.rstrip(".")
        full_desc = f"{desc_clean}. {provenance_desc}"
    else:
        full_desc = provenance_desc
    g.add((dataset_uri, DCTERMS.description, RdfLiteral(full_desc)))

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
    if schema.about.property_count:
        g.add(
            (
                dataset_uri,
                void.properties,
                RdfLiteral(schema.about.property_count, datatype=XSD.integer),
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
    if schema.about.discovered_graphs and endpoint:
        from rdflib import BNode

        # Create sd:Service wrapper for endpoints with discovered graphs
        service_uri = URIRef(f"{endpoint}#service")
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

    # Group patterns by subject class for nested VoID structure
    # Structure: class partition -> property partition -> object/datatype partition
    from collections import defaultdict

    # subject_class -> property_uri -> [(object_class, datatype, count), ...]
    class_prop_objects: dict[str, dict[str, list[tuple[str, str | None, int | None]]]] = (
        defaultdict(lambda: defaultdict(list))
    )

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
        if count is not None:
            g.add((class_partition_uri, void.entities, RdfLiteral(count, datatype=XSD.integer)))

        # Add property partitions within this class partition
        for prop_uri in sorted(class_prop_objects[subject_class].keys()):
            prop_hash = md5(prop_uri.encode(), usedforsecurity=False).hexdigest()[:8]
            prop_partition_uri = URIRef(f"{base}class-{cls_hash}-prop-{prop_hash}")

            g.add((class_partition_uri, void.propertyPartition, prop_partition_uri))
            g.add((prop_partition_uri, RDF.type, void.Dataset))
            g.add((prop_partition_uri, void.property, URIRef(prop_uri)))

            # Object classes can overlap; do not sum their triple counts.

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

    # A typed relationship alone does not establish a cross-dataset linkset.

    _bind_discovered_prefixes(g, schema.patterns)
    schema.annotate_rdf(g)
    for partition, class_iri in g.subject_objects(void["class"]):
        for example in schema.enrichment.class_examples.get(str(class_iri), []):
            if example.kind == "uri":
                g.add((partition, void.exampleResource, example.to_rdf()))
    return g


# LinkML export


def _bind_discovered_prefixes(
    g: Any,
    patterns: list[SchemaPattern],
) -> None:
    """Bind bioregistry-derived prefixes to the graph."""
    for pat in patterns:
        for uri in (
            pat.subject_class,
            pat.property_uri,
            pat.object_class,
        ):
            if uri in _SENTINEL_OBJECTS:
                continue
            _, pfx, ns = uri_to_curie(uri)
            if pfx and ns:
                try:
                    g.bind(pfx, ns, override=False)
                except Exception:
                    _log.debug(
                        "Could not bind %s=%s",
                        pfx,
                        ns,
                        exc_info=True,
                    )


def minedschema_to_void(schema: MinedSchema, base_url: str = "https://example.org") -> VoidDataset:
    """Read structural VoID fields from the canonical RDF exporter."""
    from rdflib.namespace import FOAF

    from rdfsolve.schema_models.void_model import VoidDataset

    graph = schema.to_void_graph(base_url=base_url)
    dataset = next(graph.objects(None, FOAF.primaryTopic), None)
    if dataset is None:
        raise ValueError("Export has no primary dataset")
    return VoidDataset.from_rdf(graph, dataset)
