"""VoID Pydantic models per W3C VoID specification.

Specification: https://www.w3.org/TR/void/
Extension: http://ldf.fi/void-ext
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from rdflib import Graph, URIRef


class VoidDatatypePartition(BaseModel):
    """Represents void-ext:datatypePartition."""

    uri: str
    datatype_uri: str = Field(..., description="void-ext:datatype")
    triples: int | None = Field(None, description="void:triples")

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import XSD

        void = Namespace("http://rdfs.org/ns/void#")
        void_ext = Namespace("http://ldf.fi/void-ext#")

        uri = Ref(self.uri)
        graph.add((uri, void_ext.datatype, Ref(self.datatype_uri)))
        if self.triples is not None:
            graph.add((uri, void.triples, RdfLiteral(self.triples, datatype=XSD.integer)))
        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> VoidDatatypePartition:
        """Parse from RDF graph."""
        from rdflib import Namespace

        void = Namespace("http://rdfs.org/ns/void#")
        void_ext = Namespace("http://ldf.fi/void-ext#")

        datatype = graph.value(uri, void_ext.datatype)
        triples = graph.value(uri, void.triples)

        return cls(
            uri=str(uri),
            datatype_uri=str(datatype) if datatype else "",
            triples=int(triples) if triples else None,
        )


class VoidPropertyPartition(BaseModel):
    """Represents void:propertyPartition."""

    uri: str
    property_uri: str = Field(..., description="void:property")
    triples: int | None = Field(None, description="void:triples")
    class_partitions: list[VoidClassPartition] = Field(default_factory=list)
    datatype_partitions: list[VoidDatatypePartition] = Field(default_factory=list)

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import RDF, XSD

        void = Namespace("http://rdfs.org/ns/void#")
        void_ext = Namespace("http://ldf.fi/void-ext#")

        uri = Ref(self.uri)
        graph.add((uri, RDF.type, void.Dataset))
        graph.add((uri, void.property, Ref(self.property_uri)))

        if self.triples is not None:
            graph.add((uri, void.triples, RdfLiteral(self.triples, datatype=XSD.integer)))

        for cp in self.class_partitions:
            cp_uri = cp.to_rdf(graph)
            graph.add((uri, void.classPartition, cp_uri))

        for dp in self.datatype_partitions:
            dp_uri = dp.to_rdf(graph)
            graph.add((uri, void_ext.datatypePartition, dp_uri))

        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> VoidPropertyPartition:
        """Parse from RDF graph."""
        from rdflib import Namespace

        void = Namespace("http://rdfs.org/ns/void#")
        void_ext = Namespace("http://ldf.fi/void-ext#")

        property_uri = graph.value(uri, void.property)
        triples = graph.value(uri, void.triples)

        class_partitions = []
        for cp_uri in graph.objects(uri, void.classPartition):
            class_partitions.append(VoidClassPartition.from_rdf(graph, cp_uri))

        datatype_partitions = []
        for dp_uri in graph.objects(uri, void_ext.datatypePartition):
            datatype_partitions.append(VoidDatatypePartition.from_rdf(graph, dp_uri))

        return cls(
            uri=str(uri),
            property_uri=str(property_uri) if property_uri else "",
            triples=int(triples) if triples else None,
            class_partitions=class_partitions,
            datatype_partitions=datatype_partitions,
        )


class VoidClassPartition(BaseModel):
    """Represents void:classPartition (a nested Dataset with void:class)."""

    uri: str
    class_uri: str = Field(..., description="void:class")
    triples: int | None = Field(None, description="void:triples")
    entities: int | None = Field(None, description="void:entities")
    property_partitions: list[VoidPropertyPartition] = Field(default_factory=list)

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import RDF, XSD

        void = Namespace("http://rdfs.org/ns/void#")

        uri = Ref(self.uri)
        graph.add((uri, RDF.type, void.Dataset))
        graph.add((uri, void["class"], Ref(self.class_uri)))

        if self.triples is not None:
            graph.add((uri, void.triples, RdfLiteral(self.triples, datatype=XSD.integer)))
        if self.entities is not None:
            graph.add((uri, void.entities, RdfLiteral(self.entities, datatype=XSD.integer)))

        for pp in self.property_partitions:
            pp_uri = pp.to_rdf(graph)
            graph.add((uri, void.propertyPartition, pp_uri))

        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> VoidClassPartition:
        """Parse from RDF graph."""
        from rdflib import Namespace

        void = Namespace("http://rdfs.org/ns/void#")

        class_uri = graph.value(uri, void["class"])
        triples = graph.value(uri, void.triples)
        entities = graph.value(uri, void.entities)

        property_partitions = []
        for pp_uri in graph.objects(uri, void.propertyPartition):
            property_partitions.append(VoidPropertyPartition.from_rdf(graph, pp_uri))

        return cls(
            uri=str(uri),
            class_uri=str(class_uri) if class_uri else "",
            triples=int(triples) if triples else None,
            entities=int(entities) if entities else None,
            property_partitions=property_partitions,
        )


class VoidLinkset(BaseModel):
    """Represents void:Linkset."""

    uri: str
    subjects_target_class: str = Field(..., description="void:subjectsTarget void:class")
    link_predicate: str = Field(..., description="void:linkPredicate")
    objects_target_class: str = Field(..., description="void:objectsTarget void:class")
    triples: int | None = Field(None, description="void:triples")

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import BNode, Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import RDF, XSD

        void = Namespace("http://rdfs.org/ns/void#")

        uri = Ref(self.uri)
        graph.add((uri, RDF.type, void.Linkset))

        # subjectsTarget with nested void:class
        subj_target = BNode()
        graph.add((uri, void.subjectsTarget, subj_target))
        graph.add((subj_target, void["class"], Ref(self.subjects_target_class)))

        # linkPredicate
        graph.add((uri, void.linkPredicate, Ref(self.link_predicate)))

        # objectsTarget with nested void:class
        obj_target = BNode()
        graph.add((uri, void.objectsTarget, obj_target))
        graph.add((obj_target, void["class"], Ref(self.objects_target_class)))

        if self.triples is not None:
            graph.add((uri, void.triples, RdfLiteral(self.triples, datatype=XSD.integer)))

        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> VoidLinkset:
        """Parse from RDF graph."""
        from rdflib import Namespace

        void = Namespace("http://rdfs.org/ns/void#")

        # Get subjectsTarget and its class
        subj_target = graph.value(uri, void.subjectsTarget)
        subj_class = graph.value(subj_target, void["class"]) if subj_target else None

        # Get objectsTarget and its class
        obj_target = graph.value(uri, void.objectsTarget)
        obj_class = graph.value(obj_target, void["class"]) if obj_target else None

        link_pred = graph.value(uri, void.linkPredicate)
        triples = graph.value(uri, void.triples)

        return cls(
            uri=str(uri),
            subjects_target_class=str(subj_class) if subj_class else "",
            link_predicate=str(link_pred) if link_pred else "",
            objects_target_class=str(obj_class) if obj_class else "",
            triples=int(triples) if triples else None,
        )


class VoidDataset(BaseModel):
    """Represents void:Dataset (the main dataset)."""

    uri: str
    title: str | None = Field(None, description="dcterms:title")
    description: str | None = Field(None, description="dcterms:description")
    sparql_endpoint: str | None = Field(None, description="void:sparqlEndpoint")
    classes_count: int | None = Field(None, description="void:classes")
    properties_count: int | None = Field(None, description="void:properties")
    triples: int | None = Field(None, description="void:triples")
    distinct_subjects: int | None = Field(None, description="void:distinctSubjects")
    vocabularies: list[str] = Field(default_factory=list, description="void:vocabulary")

    class_partitions: list[VoidClassPartition] = Field(default_factory=list)
    linksets: list[VoidLinkset] = Field(default_factory=list)

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import DCTERMS, RDF, XSD

        void = Namespace("http://rdfs.org/ns/void#")

        uri = Ref(self.uri)
        graph.add((uri, RDF.type, void.Dataset))

        if self.title:
            graph.add((uri, DCTERMS.title, RdfLiteral(self.title)))
        if self.description:
            graph.add((uri, DCTERMS.description, RdfLiteral(self.description)))
        if self.sparql_endpoint:
            graph.add((uri, void.sparqlEndpoint, Ref(self.sparql_endpoint)))
        if self.classes_count is not None:
            graph.add((uri, void.classes, RdfLiteral(self.classes_count, datatype=XSD.integer)))
        if self.properties_count is not None:
            graph.add((uri, void.properties, RdfLiteral(self.properties_count, datatype=XSD.integer)))
        if self.triples is not None:
            graph.add((uri, void.triples, RdfLiteral(self.triples, datatype=XSD.integer)))
        if self.distinct_subjects is not None:
            graph.add((uri, void.distinctSubjects, RdfLiteral(self.distinct_subjects, datatype=XSD.integer)))

        for vocab in self.vocabularies:
            graph.add((uri, void.vocabulary, Ref(vocab)))

        for cp in self.class_partitions:
            cp_uri = cp.to_rdf(graph)
            graph.add((uri, void.classPartition, cp_uri))

        for ls in self.linksets:
            ls.to_rdf(graph)

        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> VoidDataset:
        """Parse from RDF graph."""
        from rdflib import Namespace
        from rdflib.namespace import DCTERMS, RDF

        void = Namespace("http://rdfs.org/ns/void#")

        title = graph.value(uri, DCTERMS.title)
        description = graph.value(uri, DCTERMS.description)
        sparql_endpoint = graph.value(uri, void.sparqlEndpoint)
        classes_count = graph.value(uri, void.classes)
        properties_count = graph.value(uri, void.properties)
        triples = graph.value(uri, void.triples)
        distinct_subjects = graph.value(uri, void.distinctSubjects)

        vocabularies = [str(v) for v in graph.objects(uri, void.vocabulary)]

        class_partitions = []
        for cp_uri in graph.objects(uri, void.classPartition):
            class_partitions.append(VoidClassPartition.from_rdf(graph, cp_uri))

        # Find linksets
        linksets = []
        for ls_uri in graph.subjects(RDF.type, void.Linkset):
            linksets.append(VoidLinkset.from_rdf(graph, ls_uri))

        return cls(
            uri=str(uri),
            title=str(title) if title else None,
            description=str(description) if description else None,
            sparql_endpoint=str(sparql_endpoint) if sparql_endpoint else None,
            classes_count=int(classes_count) if classes_count else None,
            properties_count=int(properties_count) if properties_count else None,
            triples=int(triples) if triples else None,
            distinct_subjects=int(distinct_subjects) if distinct_subjects else None,
            vocabularies=vocabularies,
            class_partitions=class_partitions,
            linksets=linksets,
        )


class VoidDatasetDescription(BaseModel):
    """Represents void:DatasetDescription (the VoID document wrapper)."""

    uri: str
    title: str | None = Field(None, description="dcterms:title")
    creator: str | None = Field(None, description="dcterms:creator")
    created: str | None = Field(None, description="dcterms:created")
    primary_topic: str = Field(..., description="foaf:primaryTopic")

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import DCTERMS, FOAF, RDF

        void = Namespace("http://rdfs.org/ns/void#")

        uri = Ref(self.uri)
        graph.add((uri, RDF.type, void.DatasetDescription))

        if self.title:
            graph.add((uri, DCTERMS.title, RdfLiteral(self.title)))
        if self.creator:
            graph.add((uri, DCTERMS.creator, RdfLiteral(self.creator)))
        if self.created:
            graph.add((uri, DCTERMS.created, RdfLiteral(self.created)))

        graph.add((uri, FOAF.primaryTopic, Ref(self.primary_topic)))

        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> VoidDatasetDescription:
        """Parse from RDF graph."""
        from rdflib import Namespace
        from rdflib.namespace import DCTERMS, FOAF

        void = Namespace("http://rdfs.org/ns/void#")

        title = graph.value(uri, DCTERMS.title)
        creator = graph.value(uri, DCTERMS.creator)
        created = graph.value(uri, DCTERMS.created)
        primary_topic = graph.value(uri, FOAF.primaryTopic)

        return cls(
            uri=str(uri),
            title=str(title) if title else None,
            creator=str(creator) if creator else None,
            created=str(created) if created else None,
            primary_topic=str(primary_topic) if primary_topic else "",
        )


# Update forward references
VoidPropertyPartition.model_rebuild()
VoidClassPartition.model_rebuild()
