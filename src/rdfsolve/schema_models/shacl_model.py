"""SHACL Pydantic models per W3C SHACL specification.

Specification: https://www.w3.org/TR/shacl/
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from rdflib import Graph, URIRef


class ShaclPropertyShape(BaseModel):
    """Represents sh:PropertyShape."""

    uri: str | None = None
    path: str = Field(..., description="sh:path")
    datatype: str | None = Field(None, description="sh:datatype")
    class_constraint: str | None = Field(None, description="sh:class")
    node_kind: str | None = Field(None, description="sh:nodeKind (IRI/Literal/BlankNode)")
    min_count: int | None = Field(None, description="sh:minCount")
    max_count: int | None = Field(None, description="sh:maxCount")
    name: str | None = Field(None, description="sh:name")
    description: str | None = Field(None, description="sh:description")

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import BNode, Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import RDF, XSD

        sh = Namespace("http://www.w3.org/ns/shacl#")

        if self.uri:
            node = Ref(self.uri)
        else:
            node = BNode()

        graph.add((node, RDF.type, sh.PropertyShape))
        graph.add((node, sh.path, Ref(self.path)))

        if self.datatype:
            graph.add((node, sh.datatype, Ref(self.datatype)))
        if self.class_constraint:
            graph.add((node, sh["class"], Ref(self.class_constraint)))
        if self.node_kind:
            nk_map = {
                "IRI": sh.IRI,
                "Literal": sh.Literal,
                "BlankNode": sh.BlankNode,
            }
            if self.node_kind in nk_map:
                graph.add((node, sh.nodeKind, nk_map[self.node_kind]))
        if self.min_count is not None:
            graph.add((node, sh.minCount, RdfLiteral(self.min_count, datatype=XSD.integer)))
        if self.max_count is not None:
            graph.add((node, sh.maxCount, RdfLiteral(self.max_count, datatype=XSD.integer)))
        if self.name:
            graph.add((node, sh.name, RdfLiteral(self.name)))
        if self.description:
            graph.add((node, sh.description, RdfLiteral(self.description)))

        return node

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> ShaclPropertyShape:
        """Parse from RDF graph."""
        from rdflib import Namespace

        sh = Namespace("http://www.w3.org/ns/shacl#")

        path = graph.value(uri, sh.path)
        datatype = graph.value(uri, sh.datatype)
        class_constraint = graph.value(uri, sh["class"])
        node_kind_uri = graph.value(uri, sh.nodeKind)
        min_count = graph.value(uri, sh.minCount)
        max_count = graph.value(uri, sh.maxCount)
        name = graph.value(uri, sh.name)
        description = graph.value(uri, sh.description)

        # Map nodeKind URI to string
        node_kind = None
        if node_kind_uri:
            node_kind_str = str(node_kind_uri)
            if node_kind_str.endswith("IRI"):
                node_kind = "IRI"
            elif node_kind_str.endswith("Literal"):
                node_kind = "Literal"
            elif node_kind_str.endswith("BlankNode"):
                node_kind = "BlankNode"

        return cls(
            uri=str(uri) if not str(uri).startswith("_:") else None,
            path=str(path) if path else "",
            datatype=str(datatype) if datatype else None,
            class_constraint=str(class_constraint) if class_constraint else None,
            node_kind=node_kind,
            min_count=int(min_count) if min_count else None,
            max_count=int(max_count) if max_count else None,
            name=str(name) if name else None,
            description=str(description) if description else None,
        )


class ShaclNodeShape(BaseModel):
    """Represents sh:NodeShape."""

    uri: str
    target_class: str | None = Field(None, description="sh:targetClass")
    closed: bool = Field(False, description="sh:closed")
    ignored_properties: list[str] = Field(default_factory=list, description="sh:ignoredProperties")
    property_shapes: list[ShaclPropertyShape] = Field(default_factory=list)
    name: str | None = Field(None, description="sh:name")
    description: str | None = Field(None, description="sh:description")

    def to_rdf(self, graph: Graph) -> URIRef:
        """Serialize to RDF graph."""
        from rdflib import Namespace, URIRef as Ref
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import RDF

        sh = Namespace("http://www.w3.org/ns/shacl#")

        uri = Ref(self.uri)
        graph.add((uri, RDF.type, sh.NodeShape))

        if self.target_class:
            graph.add((uri, sh.targetClass, Ref(self.target_class)))
        if self.closed:
            graph.add((uri, sh.closed, RdfLiteral(True)))
        if self.name:
            graph.add((uri, sh.name, RdfLiteral(self.name)))
        if self.description:
            graph.add((uri, sh.description, RdfLiteral(self.description)))

        for ps in self.property_shapes:
            ps_node = ps.to_rdf(graph)
            graph.add((uri, sh.property, ps_node))

        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: URIRef) -> ShaclNodeShape:
        """Parse from RDF graph."""
        from rdflib import Namespace
        from rdflib.namespace import RDF

        sh = Namespace("http://www.w3.org/ns/shacl#")

        target_class = graph.value(uri, sh.targetClass)
        closed = graph.value(uri, sh.closed)
        name = graph.value(uri, sh.name)
        description = graph.value(uri, sh.description)

        property_shapes = []
        for ps_uri in graph.objects(uri, sh.property):
            property_shapes.append(ShaclPropertyShape.from_rdf(graph, ps_uri))

        return cls(
            uri=str(uri),
            target_class=str(target_class) if target_class else None,
            closed=bool(closed) if closed else False,
            property_shapes=property_shapes,
            name=str(name) if name else None,
            description=str(description) if description else None,
        )


class ShaclShapesGraph(BaseModel):
    """Collection of SHACL shapes."""

    node_shapes: list[ShaclNodeShape] = Field(default_factory=list)
    base_uri: str | None = None

    def to_rdf(self, graph: Graph | None = None) -> Graph:
        """Serialize all shapes to RDF graph."""
        from rdflib import Graph as RdfGraph, Namespace

        if graph is None:
            graph = RdfGraph()

        sh = Namespace("http://www.w3.org/ns/shacl#")
        graph.bind("sh", sh)

        for ns in self.node_shapes:
            ns.to_rdf(graph)

        return graph

    @classmethod
    def from_rdf(cls, graph: Graph) -> ShaclShapesGraph:
        """Parse all shapes from RDF graph."""
        from rdflib import Namespace
        from rdflib.namespace import RDF

        sh = Namespace("http://www.w3.org/ns/shacl#")

        node_shapes = []
        for ns_uri in graph.subjects(RDF.type, sh.NodeShape):
            node_shapes.append(ShaclNodeShape.from_rdf(graph, ns_uri))

        return cls(node_shapes=node_shapes)
