"""VoID to MinedSchema conversion functions."""

from __future__ import annotations

import logging
import re
from typing import Any

from rdflib import RDF, Graph, Namespace, URIRef
from rdflib.query import ResultRow

from rdfsolve.local_rdf import LocalBackend, LocalRdf
from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

logger = logging.getLogger(__name__)

VOID = Namespace("http://rdfs.org/ns/void#")
VOID_EXT = Namespace("http://ldf.fi/void-ext#")


SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
# The links from a VoID dataset to the parts that describe it. void-ext's object class and
# language partitions are those of the LDF VoID extension.
_PARTS = (
    VOID.classPartition,
    VOID.propertyPartition,
    VOID.subset,
    VOID_EXT.datatypePartition,
    VOID_EXT.objectClassPartition,
    VOID_EXT.languagePartition,
)
# The partitions of a property partition by the class of its objects: nested class partitions
# (void-ext's convention of void:classPartition) or void-ext:objectClassPartition.
_OBJECT_CLASS_PARTS = (VOID.classPartition, VOID_EXT.objectClassPartition)


def service_description_graph_names(g: Graph) -> list[str]:
    """Return the names of the named graphs that a SPARQL service description lists."""
    return sorted(
        {
            str(name)
            for named in g.objects(None, SD.namedGraph)
            for name in g.objects(named, SD.name)
            if isinstance(name, URIRef)
        }
    )


def void_class_populations(
    g: Graph, *, subjects_fallback: bool = False
) -> tuple[dict[str, int], dict[str, str], list[dict[str, object]]]:
    """Return the members of each class that a VoID states, checked against its own counts.

    A class partition states its members with void:entities (with *subjects_fallback*, also
    with void:distinctSubjects). Each of its property partitions bounds them from below: its
    distinct subjects are members, and its triples need at least triples / distinctObjects
    subjects. A stated count below that bound is wrong. Such a class gets the bound, with the state
    "partial" (a lower bound), and the contradiction is returned; a class with no stated count
    has none. A class described by two partitions with different counts
    (two graphs) has at least the larger: "partial".

    Returns the counts, their states ("complete" or "partial") and the contradictions found.
    """
    counts: dict[str, int] = {}
    states: dict[str, str] = {}
    contradictions: list[dict[str, object]] = []
    partitions = set(g.objects(None, VOID.classPartition)) | set(g.subjects(VOID["class"], None))
    for partition in sorted(partitions, key=str):
        class_iri = g.value(partition, VOID["class"])
        if class_iri is None:
            continue
        stated = optional_count(g.value(partition, VOID.entities))
        if stated is None and subjects_fallback:
            stated = optional_count(g.value(partition, VOID.distinctSubjects))
        bound, witness = 0, None
        for part in g.objects(partition, VOID.propertyPartition):
            subjects = optional_count(g.value(part, VOID.distinctSubjects))
            if subjects is None:
                triples = optional_count(g.value(part, VOID.triples))
                objects = optional_count(g.value(part, VOID.distinctObjects))
                subjects = -(-triples // objects) if triples and objects else None
            if subjects is not None and subjects > bound:
                bound, witness = subjects, g.value(part, VOID.property)
        if stated is None:
            continue
        cls = str(class_iri)
        if stated >= bound:
            count, state = stated, "complete"
        else:
            count, state = bound, "partial"
            contradictions.append(
                {
                    "class": cls,
                    "stated": stated,
                    "lower_bound": bound,
                    "property": str(witness) if witness is not None else None,
                }
            )
        if cls in counts and counts[cls] != count:
            count, state = max(count, counts[cls]), "partial"
        if cls in counts and states[cls] == "partial":
            state = "partial"
        counts[cls], states[cls] = count, state
    for item in contradictions:
        logger.warning(
            "VoID states %s members of %s, but its partition of %s has at least %s: the count "
            "is kept as a lower bound",
            item["stated"],
            item["class"],
            item["property"],
            item["lower_bound"],
        )
    return counts, states, contradictions


def void_datasets_of_graphs(g: Graph, graph_names: list[str]) -> list[URIRef]:
    """Return the VoID datasets that describe the named graphs.

    A service description gives the dataset of each named graph (void-generator). A graph that
    it does not name is its own dataset when the VoID describes the graph's IRI with partitions
    (a VoID that names each graph's dataset by the graph).
    """
    found: set[URIRef] = set()
    for name in graph_names:
        stated = {
            dataset
            for named in g.subjects(SD.name, URIRef(name))
            for dataset in g.objects(named, SD.graph)
            if isinstance(dataset, URIRef)
        }
        if not stated and is_partitioned_dataset(g, URIRef(name)):
            stated = {URIRef(name)}
        found |= stated
    return sorted(found)


def described_datasets(g: Graph) -> list[URIRef]:
    """Return the datasets a VoID describes at the top: partitioned and not part of another."""
    parts = {o for link in _PARTS for o in g.objects(None, link)}
    nodes = set(g.subjects(VOID.classPartition, None)) | set(
        g.subjects(VOID.propertyPartition, None)
    )
    return sorted(n for n in nodes if isinstance(n, URIRef) and n not in parts)


def is_partitioned_dataset(g: Graph, node: URIRef) -> bool:
    """Return whether the VoID describes NODE with class or property partitions."""
    return any(g.objects(node, VOID.classPartition)) or any(g.objects(node, VOID.propertyPartition))


def void_issued(g: Graph, dataset: URIRef | None = None) -> str | None:
    """Return when a VoID description was issued or its data last updated, if it says so.

    dcterms:issued, else pav:lastUpdatedOn or dcterms:modified; of
    DATASET when given, else the latest stated anywhere in the description.
    """
    from rdflib.namespace import DCTERMS

    pav_updated = URIRef("http://purl.org/pav/lastUpdatedOn")
    for predicate in (DCTERMS.issued, pav_updated, DCTERMS.modified):
        values = [str(o) for o in g.objects(dataset, predicate)]
        if values:
            return max(values)
    return None


def scope_void_graph(g: Graph, datasets: list[URIRef]) -> Graph:
    """Return the part of a VoID description that describes DATASETS.

    A description of a whole endpoint (each graph and their union) holds every graph's
    partitions. From each dataset its partitions, subsets and linksets are
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
        >>> len(schema.patterns) > 0
        True
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
    left_out: list[dict[str, Any]] | None = None,
) -> MinedSchema:
    """Read VoID RDF without treating metadata predicates as patterns.

    Partitions whose terms a pattern cannot hold are left out; LEFT_OUT, when given, receives
    each with its reason and triples (_append).
    """
    patterns = _extract_patterns_from_void(g, local_backend=local_backend, left_out=left_out)
    if report_untyped:
        warn_untyped_partitions(g, patterns)
    about = _extract_metadata_from_void(g, endpoint=endpoint)
    about.pattern_count = len(patterns)
    counts, states, _ = void_class_populations(g)
    about.class_entity_counts.update(counts)
    about.class_entity_count_states.update(
        {cls: state for cls, state in states.items() if state != "complete"}  # type: ignore[misc]
    )

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


def _append(
    patterns: list[SchemaPattern],
    fields: dict[str, Any],
    left_out: list[dict[str, Any]] | None = None,
) -> None:
    """Add a pattern read from a VoID, unless a term of it cannot be a pattern's term.

    A VoID may state a term that the schema does not take as a class or property, such as an
    IRI whose scheme is not registered. It is not a compact name to expand: a source can use it
    beside the registered IRI it resembles, and expanding it would merge two terms. Such a
    partition is left out, logged and, when LEFT_OUT is given, recorded there with its reason
    and its triples; the rest of the VoID is read.
    """
    from pydantic import ValidationError

    try:
        patterns.append(SchemaPattern(**fields))
    except ValidationError as error:
        problem = error.errors()[0]
        field = str(problem["loc"][0]) if problem["loc"] else "pattern"
        term = str(problem.get("input"))
        unregistered = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", term) is not None
        item = {
            "reason": f"{field}: IRI with an unregistered scheme"
            if unregistered
            else f"{field}: not an IRI",
            "term": problem.get("input"),
            "subject_class": fields.get("subject_class"),
            "property": fields.get("property_uri"),
            "object": fields.get("datatype") or fields.get("object_class"),
            "triples": fields.get("count"),
        }
        logger.warning("VoID partition left out: %s (%s)", item["reason"], item["term"])
        if left_out is not None:
            left_out.append(item)


def _extract_patterns_from_void(
    g: Graph,
    *,
    local_backend: LocalBackend = "oxigraph",
    left_out: list[dict[str, Any]] | None = None,
) -> list[SchemaPattern]:
    """Extract SchemaPattern list from VoID graph."""
    from rdflib.namespace import RDFS

    patterns: list[SchemaPattern] = []
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
            ?pp void:classPartition|void-ext:objectClassPartition ?objCp .
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
            _append(
                patterns,
                left_out=left_out,
                fields={
                    "subject_class": subject_class,
                    "property_uri": property_uri,
                    "object_class": object_class,
                    "count": count,
                    "evidence_source": "void",
                    "subject_label": labels.get(subject_class),
                    "property_label": labels.get(property_uri),
                    "object_label": labels.get(object_class),
                },
            )
        elif row.get("datatype"):
            # Literal pattern with datatype
            _append(
                patterns,
                left_out=left_out,
                fields={
                    "subject_class": subject_class,
                    "property_uri": property_uri,
                    "object_class": "Literal",
                    "datatype": str(row.datatype),
                    "count": count,
                    "evidence_source": "void",
                    "subject_label": labels.get(subject_class),
                    "property_label": labels.get(property_uri),
                },
            )

    # Linksets give the class at each end of a link (as void-generator writes them). Read with the nested partitions: a description with datatype
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
            # of graphs: the larger description holds the other.
            if count is not None and (previous.count is None or count > previous.count):
                previous.count = count
                previous.distinct_subjects = optional_count(row.get("subjects"))
                previous.distinct_objects = optional_count(row.get("objects"))
            continue
        subject_class, property_uri, object_class = key
        made: list[SchemaPattern] = []
        _append(
            made,
            left_out=left_out,
            fields={
                "subject_class": subject_class,
                "property_uri": property_uri,
                "object_class": object_class,
                "count": count,
                "distinct_subjects": optional_count(row.get("subjects")),
                "distinct_objects": optional_count(row.get("objects")),
                "evidence_source": "void",
                "subject_label": labels.get(subject_class),
                "property_label": labels.get(property_uri),
                "object_label": labels.get(object_class),
            },
        )
        if made:
            linked[key] = made[0]
    patterns.extend(linked.values())
    patterns.extend(_untyped_object_partitions(g, labels, left_out))
    return patterns


def _untyped_object_partitions(
    g: Graph, labels: dict[str, str], left_out: list[dict[str, Any]] | None = None
) -> list[SchemaPattern]:
    """Return the IRI objects that an object class partition without a class states.

    A VoID can partition the objects of each class and property by their class, and put the objects
    without a class (literals among them) in a partition without void:class. Its triples beyond
    those of the datatype partitions have objects that are neither literals nor typed: a
    Resource pattern with that count. rdf:type is membership, not a link, and is left out.
    """
    found: list[SchemaPattern] = []
    for cp in set(g.objects(None, VOID.classPartition)):
        subject_class = g.value(cp, VOID["class"])
        if subject_class is None:
            continue
        for pp in g.objects(cp, VOID.propertyPartition):
            prop = g.value(pp, VOID.property)
            if prop is None or prop == RDF.type:
                continue
            untyped = [
                part
                for part in g.objects(pp, VOID_EXT.objectClassPartition)
                if g.value(part, VOID["class"]) is None
            ]
            if not untyped:
                continue
            triples = sum(optional_count(g.value(part, VOID.triples)) or 0 for part in untyped)
            literals = sum(
                optional_count(g.value(d, VOID.triples)) or 0
                for d in g.objects(pp, VOID_EXT.datatypePartition)
            )
            if triples - literals <= 0:
                continue
            _append(
                found,
                left_out=left_out,
                fields={
                    "subject_class": str(subject_class),
                    "property_uri": str(prop),
                    "object_class": "Resource",
                    "count": triples - literals,
                    "evidence_source": "void",
                    "subject_label": labels.get(str(subject_class)),
                    "property_label": labels.get(str(prop)),
                },
            )
    return found


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
