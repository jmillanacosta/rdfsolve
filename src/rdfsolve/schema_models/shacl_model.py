"""SHACL Pydantic models per W3C SHACL specification.

Specification: https://www.w3.org/TR/shacl/
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast, get_args

from pydantic import BaseModel, Field, model_validator

from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.paths import PropertyPath

if TYPE_CHECKING:
    from rdflib import BNode, Graph, URIRef
    from rdflib.term import Node


NodeKind = Literal[
    "IRI", "Literal", "BlankNode", "BlankNodeOrIRI", "BlankNodeOrLiteral", "IRIOrLiteral"
]


def _count(graph: Graph, shape: Node, predicate: URIRef) -> int | None:
    """Require the SHACL integer datatype and at most one value."""
    from rdflib import XSD
    from rdflib import Literal as RdfLiteral

    value = graph.value(shape, predicate, any=False)
    if value is not None and (not isinstance(value, RdfLiteral) or value.datatype != XSD.integer):
        raise ValueError(f"{predicate} requires an xsd:integer literal")
    return optional_count(value)


class ShaclPropertyShape(BaseModel):
    """Represents sh:PropertyShape."""

    uri: str | None = None
    path: str | PropertyPath = Field(..., description="sh:path")
    datatype: str | None = Field(None, description="sh:datatype")
    class_constraint: str | None = Field(None, description="sh:class")
    node_kind: NodeKind | None = None
    min_count: int | None = Field(None, description="sh:minCount")
    max_count: int | None = Field(None, description="sh:maxCount")
    qualified_shape: ShaclPropertyShape | None = None
    qualified_min_count: int | None = None
    qualified_max_count: int | None = None
    qualified_disjoint: bool = False
    name: str | None = Field(None, description="sh:name")
    description: str | None = Field(None, description="sh:description")
    alternatives: list[ShaclPropertyShape] = Field(
        default_factory=list,
        description="Value constraints combined with sh:or; paths are ignored in these alternatives",
    )

    def to_rdf(self, graph: Graph) -> URIRef | BNode:
        """Serialize to RDF graph."""
        from rdflib import BNode, Namespace
        from rdflib import Literal as RdfLiteral
        from rdflib import URIRef as Ref
        from rdflib.namespace import RDF, XSD

        sh = Namespace("http://www.w3.org/ns/shacl#")

        node: URIRef | BNode
        if self.uri:
            node = Ref(self.uri)
        else:
            node = BNode()

        if self.path:
            graph.add((node, RDF.type, sh.PropertyShape))
            from rdfsolve.schema_models.exporters.paths import path_to_rdf

            path = (
                self.path
                if isinstance(self.path, PropertyPath)
                else PropertyPath(operator="predicate", iri=self.path)
            )
            graph.add((node, sh.path, path_to_rdf(path, graph)))

        if self.datatype:
            graph.add((node, sh.datatype, Ref(self.datatype)))
        if self.class_constraint:
            graph.add((node, sh["class"], Ref(self.class_constraint)))
        if self.node_kind:
            nk_map = {
                "IRI": sh.IRI,
                "Literal": sh.Literal,
                "BlankNode": sh.BlankNode,
                "BlankNodeOrIRI": sh.BlankNodeOrIRI,
                "BlankNodeOrLiteral": sh.BlankNodeOrLiteral,
                "IRIOrLiteral": sh.IRIOrLiteral,
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
        if self.alternatives:
            from rdflib.collection import Collection

            alternatives: list[Node] = [alternative.to_rdf(graph) for alternative in self.alternatives]
            head = BNode()
            Collection(graph, head, alternatives)
            graph.add((node, sh["or"], head))

        if self.qualified_shape is not None:
            graph.add((node, sh.qualifiedValueShape, self.qualified_shape.to_rdf(graph)))
        for predicate, count in (
            (sh.qualifiedMinCount, self.qualified_min_count),
            (sh.qualifiedMaxCount, self.qualified_max_count),
        ):
            if count is not None:
                graph.add((node, predicate, RdfLiteral(count, datatype=XSD.integer)))
        if self.qualified_disjoint:
            graph.add((node, sh.qualifiedValueShapesDisjoint, RdfLiteral(True)))
        return node

    @model_validator(mode="after")
    def check_counts(self) -> ShaclPropertyShape:
        """Reject malformed count parameters, not unsatisfiable bounds."""
        for value in (
            self.min_count,
            self.max_count,
            self.qualified_min_count,
            self.qualified_max_count,
        ):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("SHACL counts must be nonnegative integers")
        if not self.path and (self.min_count is not None or self.max_count is not None):
            raise ValueError("sh:minCount and sh:maxCount require a property path")
        if (
            self.qualified_min_count is not None or self.qualified_max_count is not None
        ) and self.qualified_shape is None:
            raise ValueError("Qualified counts require sh:qualifiedValueShape")
        return self

    @classmethod
    def from_rdf(
        cls, graph: Graph, uri: Node, active: frozenset[Node] = frozenset()
    ) -> ShaclPropertyShape:
        """Parse from RDF graph."""
        from rdflib import BNode, Namespace, URIRef

        sh = Namespace("http://www.w3.org/ns/shacl#")

        from rdfsolve.schema_models.readers.paths import read_path

        if uri in active:
            raise ValueError("Recursive inline SHACL shapes are not supported")
        active = active | {uri}
        path = graph.value(uri, sh.path, any=False)
        datatype = graph.value(uri, sh.datatype)
        class_constraint = graph.value(uri, sh["class"])
        node_kind_uri = graph.value(uri, sh.nodeKind)
        name = graph.value(uri, sh.name)
        description = graph.value(uri, sh.description)

        node_kind = None
        if node_kind_uri is not None:
            namespace = str(sh)
            if not str(node_kind_uri).startswith(namespace):
                raise ValueError(f"Invalid sh:nodeKind: {node_kind_uri}")
            name_kind = str(node_kind_uri)[len(namespace) :]
            if name_kind not in get_args(NodeKind):
                raise ValueError(f"Invalid sh:nodeKind: {node_kind_uri}")
            node_kind = cast(NodeKind, name_kind)

        return cls(
            uri=None if isinstance(uri, BNode) else str(uri),
            path=read_path(graph, path)
            if path is not None and not isinstance(path, URIRef)
            else str(path)
            if path is not None
            else "",
            datatype=str(datatype) if datatype else None,
            class_constraint=str(class_constraint) if class_constraint else None,
            node_kind=node_kind,
            min_count=_count(graph, uri, sh.minCount),
            max_count=_count(graph, uri, sh.maxCount),
            qualified_shape=cls.from_rdf(graph, qualified, active)
            if (qualified := graph.value(uri, sh.qualifiedValueShape, any=False)) is not None
            else None,
            qualified_min_count=_count(graph, uri, sh.qualifiedMinCount),
            qualified_max_count=_count(graph, uri, sh.qualifiedMaxCount),
            qualified_disjoint=bool(graph.value(uri, sh.qualifiedValueShapesDisjoint)),
            name=str(name) if name else None,
            description=str(description) if description else None,
            alternatives=[
                cls.from_rdf(graph, branch, active)
                for head in graph.objects(uri, sh["or"])
                for branch in graph.items(head)
            ],
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

    def to_rdf(self, graph: Graph) -> URIRef | BNode:
        """Serialize to RDF graph."""
        from rdflib import Literal as RdfLiteral
        from rdflib import Namespace
        from rdflib import URIRef as Ref
        from rdflib.namespace import RDF

        sh = Namespace("http://www.w3.org/ns/shacl#")

        from rdflib import BNode

        uri = BNode(self.uri[2:]) if self.uri.startswith("_:") else Ref(self.uri)
        graph.add((uri, RDF.type, sh.NodeShape))

        if self.target_class:
            graph.add((uri, sh.targetClass, Ref(self.target_class)))
        if self.closed:
            graph.add((uri, sh.closed, RdfLiteral(True)))
        if self.name:
            graph.add((uri, sh.name, RdfLiteral(self.name)))
        if self.description:
            graph.add((uri, sh.description, RdfLiteral(self.description)))

        if self.ignored_properties:
            from rdflib.collection import Collection

            head = BNode()
            Collection(graph, head, [Ref(iri) for iri in self.ignored_properties])
            graph.add((uri, sh.ignoredProperties, head))

        for ps in self.property_shapes:
            ps_node = ps.to_rdf(graph)
            graph.add((uri, sh.property, ps_node))

        return uri

    @classmethod
    def from_rdf(cls, graph: Graph, uri: Node) -> ShaclNodeShape:
        """Parse from RDF graph."""
        from rdflib import Namespace
        from rdflib.namespace import RDF

        sh = Namespace("http://www.w3.org/ns/shacl#")

        from rdflib import BNode

        target_class = graph.value(uri, sh.targetClass, any=False)
        closed = graph.value(uri, sh.closed)
        name = graph.value(uri, sh.name)
        description = graph.value(uri, sh.description)

        property_shapes = []
        for ps_uri in graph.objects(uri, sh.property):
            property_shapes.append(ShaclPropertyShape.from_rdf(graph, ps_uri))

        return cls(
            uri=("_:" + str(uri)) if isinstance(uri, BNode) else str(uri),
            target_class=str(target_class) if target_class else None,
            closed=bool(closed) if closed else False,
            ignored_properties=[
                str(item)
                for head in graph.objects(uri, sh.ignoredProperties)
                for item in graph.items(head)
            ],
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
        from rdflib import Graph as RdfGraph
        from rdflib import Namespace

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
