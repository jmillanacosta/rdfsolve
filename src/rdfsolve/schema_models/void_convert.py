"""VoID to MinedSchema conversion functions."""

from __future__ import annotations

from rdflib import Graph, Namespace, URIRef

from rdfsolve.schema_models.core import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.void_model import VoidDataset


VOID = Namespace("http://rdfs.org/ns/void#")
VOID_EXT = Namespace("http://ldf.fi/void-ext#")


def minedschema_to_void(
    schema: MinedSchema, base_url: str = "https://example.org"
) -> VoidDataset:
    """Convert MinedSchema to VoID Dataset.

    Groups patterns by subject class, creates nested partitions.

    Args:
        schema: MinedSchema to convert
        base_url: Base URL for partition URIs

    Returns:
        VoidDataset with nested class/property/datatype partitions
    """
    from collections import defaultdict
    from hashlib import md5

    from rdfsolve.schema_models.void_model import (
        VoidClassPartition,
        VoidDatatypePartition,
        VoidLinkset,
        VoidPropertyPartition,
    )

    # Group patterns: subject_class -> property -> [(object_class, datatype, count)]
    grouped: dict[str, dict[str, list[tuple[str, str | None, int | None]]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for pat in schema.patterns:
        if pat.subject_class and pat.property_uri:
            grouped[pat.subject_class][pat.property_uri].append(
                (pat.object_class, pat.datatype, pat.count)
            )

    # Build class partitions
    class_partitions = []
    linksets = []

    for subject_class in sorted(grouped.keys()):
        cls_hash = md5(subject_class.encode(), usedforsecurity=False).hexdigest()[:8]

        property_partitions = []
        for prop_uri in sorted(grouped[subject_class].keys()):
            prop_hash = md5(prop_uri.encode(), usedforsecurity=False).hexdigest()[:8]

            nested_class_partitions = []
            datatype_partitions = []

            for obj_class, datatype, count in grouped[subject_class][prop_uri]:
                if obj_class == "Literal" and datatype:
                    dt_hash = md5(datatype.encode(), usedforsecurity=False).hexdigest()[:8]
                    datatype_partitions.append(
                        VoidDatatypePartition(
                            uri=f"{base_url}/dt-{cls_hash}-{prop_hash}-{dt_hash}",
                            datatype_uri=datatype,
                            triples=count,
                        )
                    )
                elif obj_class and obj_class not in ("Literal", "Resource", "BlankNode"):
                    obj_hash = md5(obj_class.encode(), usedforsecurity=False).hexdigest()[:8]
                    nested_class_partitions.append(
                        VoidClassPartition(
                            uri=f"{base_url}/cp-{cls_hash}-{prop_hash}-{obj_hash}",
                            class_uri=obj_class,
                            triples=count,
                        )
                    )
                    # Also create linkset
                    linksets.append(
                        VoidLinkset(
                            uri=f"{base_url}/ls-{cls_hash}-{prop_hash}-{obj_hash}",
                            subjects_target_class=subject_class,
                            link_predicate=prop_uri,
                            objects_target_class=obj_class,
                            triples=count,
                        )
                    )

            prop_triples = sum(c or 0 for _, _, c in grouped[subject_class][prop_uri])
            property_partitions.append(
                VoidPropertyPartition(
                    uri=f"{base_url}/pp-{cls_hash}-{prop_hash}",
                    property_uri=prop_uri,
                    triples=prop_triples if prop_triples > 0 else None,
                    class_partitions=nested_class_partitions,
                    datatype_partitions=datatype_partitions,
                )
            )

        class_triples = sum(
            c or 0 for prop_objs in grouped[subject_class].values() for _, _, c in prop_objs
        )
        class_partitions.append(
            VoidClassPartition(
                uri=f"{base_url}/cp-{cls_hash}",
                class_uri=subject_class,
                triples=class_triples if class_triples > 0 else None,
                property_partitions=property_partitions,
            )
        )

    # Build dataset
    endpoint = schema.about.endpoint
    dataset_uri = endpoint if endpoint else f"{base_url}/dataset"

    return VoidDataset(
        uri=dataset_uri,
        title=schema.about.title or schema.about.dataset_name,
        description=schema.about.description,
        sparql_endpoint=endpoint,
        classes_count=schema.about.class_count,
        properties_count=schema.about.property_count,
        triples=schema.about.triple_count_estimate,
        distinct_subjects=schema.about.distinct_subject_count,
        class_partitions=class_partitions,
        linksets=linksets,
    )


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

    patterns = _extract_patterns_from_void(g)
    about = _extract_metadata_from_void(g)

    return MinedSchema(patterns=patterns, about=about)


def _extract_patterns_from_void(g: Graph) -> list[SchemaPattern]:
    """Extract SchemaPattern list from VoID graph."""
    patterns = []

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

        # Either nested class partition (typed object) or datatype partition, or neither (Resource/BlankNode)
        OPTIONAL {
            ?pp void:classPartition ?objCp .
            ?objCp void:class ?objectClass .
            OPTIONAL { ?objCp void:triples ?count }
        }
        OPTIONAL {
            ?pp void-ext:datatypePartition ?dp .
            ?dp void-ext:datatype ?datatype .
            OPTIONAL { ?dp void:triples ?count }
        }
    }
    """

    for row in g.query(query):
        subject_class = str(row.subjectClass)
        property_uri = str(row.property)

        # Get count safely
        count_val = row.get("count")
        count = int(count_val) if count_val is not None else None

        if row.get("objectClass"):
            # Typed object pattern
            patterns.append(
                SchemaPattern(
                    subject_class=subject_class,
                    property_uri=property_uri,
                    object_class=str(row.objectClass),
                    count=count,
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
            count_val = row.get("count")
            count = int(count_val) if count_val is not None else None
            patterns.append(
                SchemaPattern(
                    subject_class=str(row.subjectClass),
                    property_uri=str(row.predicate),
                    object_class=str(row.objectClass),
                    count=count,
                )
            )

    return patterns


def _extract_metadata_from_void(g: Graph) -> AboutMetadata:
    """Extract AboutMetadata from VoID graph."""
    from rdflib.namespace import DCTERMS, FOAF, RDF

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

    return AboutMetadata.build(
        endpoint=str(endpoint) if endpoint else None,
        dataset_name=str(title) if title else None,
        title=str(title) if title else None,
        description=str(description) if description else None,
        class_count=int(classes) if classes else 0,
        property_count=int(properties) if properties else 0,
        triple_count_estimate=int(triples) if triples else None,
    )
