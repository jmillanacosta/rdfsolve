"""SHACL Pydantic models per W3C SHACL specification.

Specification: https://www.w3.org/TR/shacl/
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast, get_args

from pydantic import BaseModel, Field, model_validator

from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import PropertyPath, absolute_iri

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


def _boolean(graph: Graph, shape: Node, predicate: URIRef) -> bool:
    """Read one SHACL boolean without treating nonempty text as true."""
    from rdflib import XSD
    from rdflib import Literal as RdfLiteral

    value = graph.value(shape, predicate, any=False)
    if value is None:
        return False
    if not isinstance(value, RdfLiteral) or value.datatype != XSD.boolean:
        raise ValueError(f"{predicate} requires an xsd:boolean literal")
    parsed = value.toPython()
    if not isinstance(parsed, bool):
        raise ValueError(f"{predicate} requires true or false")
    return parsed


def _text(graph: Graph, shape: Node, predicate: URIRef) -> tuple[str | None, str | None]:
    """Read a preferred display text and retain its language tag."""
    import logging

    from rdflib import Literal as RdfLiteral

    values = list(graph.objects(shape, predicate))
    if not values:
        return None, None
    if not all(isinstance(value, RdfLiteral) for value in values):
        raise ValueError(f"{predicate} requires literal text")
    texts = [value for value in values if isinstance(value, RdfLiteral)]
    texts.sort(
        key=lambda value: (value.language not in ("en", None), value.language or "", str(value))
    )
    if len(texts) > 1:
        logging.getLogger(__name__).warning(
            "SHACL profile retains one display text for %s on %s; %d alternatives are omitted.",
            predicate,
            shape,
            len(texts) - 1,
        )
    return str(texts[0]), texts[0].language


class ShaclPropertyShape(BaseModel):
    """Represents sh:PropertyShape."""

    uri: str | None = None
    path: str | PropertyPath = Field(..., description="sh:path")
    datatype: str | None = Field(None, description="sh:datatype")
    class_constraint: str | None = Field(None, description="sh:class")
    node_kind: NodeKind | None = None
    deactivated: bool = False
    min_count: int | None = Field(None, description="sh:minCount")
    max_count: int | None = Field(None, description="sh:maxCount")
    properties: list[ShaclPropertyShape] = Field(default_factory=list)
    qualified_shape: ShaclPropertyShape | None = None
    qualified_min_count: int | None = None
    qualified_max_count: int | None = None
    qualified_disjoint: bool = False
    name: str | None = Field(None, description="sh:name")
    description: str | None = Field(None, description="sh:description")
    name_language: str | None = None
    description_language: str | None = None
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
        if self.deactivated:
            graph.add((node, sh.deactivated, RdfLiteral(True)))
        if self.min_count is not None:
            graph.add((node, sh.minCount, RdfLiteral(self.min_count, datatype=XSD.integer)))
        if self.max_count is not None:
            graph.add((node, sh.maxCount, RdfLiteral(self.max_count, datatype=XSD.integer)))
        if self.name is not None:
            graph.add((node, sh.name, RdfLiteral(self.name, lang=self.name_language)))
        if self.description is not None:
            graph.add(
                (node, sh.description, RdfLiteral(self.description, lang=self.description_language))
            )
        if self.alternatives:
            from rdflib.collection import Collection

            alternatives: list[Node] = [
                alternative.to_rdf(graph) for alternative in self.alternatives
            ]
            head = BNode()
            Collection(graph, head, alternatives)
            graph.add((node, sh["or"], head))

        for child in self.properties:
            graph.add((node, sh.property, child.to_rdf(graph)))
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
        name, name_language = _text(graph, uri, sh.name)
        description, description_language = _text(graph, uri, sh.description)

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
            properties=[
                cls.from_rdf(graph, child, active) for child in graph.objects(uri, sh.property)
            ],
            qualified_shape=cls.from_rdf(graph, qualified, active)
            if (qualified := graph.value(uri, sh.qualifiedValueShape, any=False)) is not None
            else None,
            qualified_min_count=_count(graph, uri, sh.qualifiedMinCount),
            qualified_max_count=_count(graph, uri, sh.qualifiedMaxCount),
            qualified_disjoint=_boolean(graph, uri, sh.qualifiedValueShapesDisjoint),
            deactivated=_boolean(graph, uri, sh.deactivated),
            name=name,
            description=description,
            name_language=name_language,
            description_language=description_language,
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
    deactivated: bool = False
    ignored_properties: list[str] = Field(default_factory=list, description="sh:ignoredProperties")
    property_shapes: list[ShaclPropertyShape] = Field(default_factory=list)
    name: str | None = Field(None, description="rdfs:label")
    description: str | None = Field(None, description="rdfs:comment")
    name_language: str | None = None
    description_language: str | None = None

    def to_rdf(self, graph: Graph) -> URIRef | BNode:
        """Serialize to RDF graph."""
        from rdflib import Literal as RdfLiteral
        from rdflib import Namespace
        from rdflib import URIRef as Ref
        from rdflib.namespace import RDF, RDFS

        sh = Namespace("http://www.w3.org/ns/shacl#")

        from rdflib import BNode

        uri = BNode(self.uri[2:]) if self.uri.startswith("_:") else Ref(self.uri)
        graph.add((uri, RDF.type, sh.NodeShape))

        if self.target_class:
            graph.add((uri, sh.targetClass, Ref(self.target_class)))
        if self.deactivated:
            graph.add((uri, sh.deactivated, RdfLiteral(True)))
        if self.closed:
            graph.add((uri, sh.closed, RdfLiteral(True)))
        if self.name is not None:
            graph.add((uri, RDFS.label, RdfLiteral(self.name, lang=self.name_language)))
        if self.description is not None:
            graph.add(
                (uri, RDFS.comment, RdfLiteral(self.description, lang=self.description_language))
            )

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
        from rdflib.namespace import RDFS

        sh = Namespace("http://www.w3.org/ns/shacl#")

        from rdflib import BNode

        target_class = graph.value(uri, sh.targetClass, any=False)
        name, name_language = _text(graph, uri, RDFS.label)
        if name is None:
            name, name_language = _text(graph, uri, sh.name)
        description, description_language = _text(graph, uri, RDFS.comment)
        if description is None:
            description, description_language = _text(graph, uri, sh.description)

        property_shapes = []
        for ps_uri in graph.objects(uri, sh.property):
            property_shapes.append(ShaclPropertyShape.from_rdf(graph, ps_uri))

        return cls(
            uri=("_:" + str(uri)) if isinstance(uri, BNode) else str(uri),
            target_class=str(target_class) if target_class else None,
            closed=_boolean(graph, uri, sh.closed),
            deactivated=_boolean(graph, uri, sh.deactivated),
            ignored_properties=[
                str(item)
                for head in graph.objects(uri, sh.ignoredProperties)
                for item in graph.items(head)
            ],
            property_shapes=property_shapes,
            name=name,
            description=description,
            name_language=name_language,
            description_language=description_language,
        )


def _node(identity: str):
    from rdflib import BNode, URIRef

    return BNode(identity[2:]) if identity.startswith("_:") else URIRef(identity)


def _identity(node: Node) -> str:
    from rdflib import BNode

    return ("_:" if isinstance(node, BNode) else "") + str(node)


class ShaclPrefixDeclaration(BaseModel):
    """One SHACL namespace mapping."""

    uri: str
    prefix: str
    namespace: str

    @model_validator(mode="after")
    def check_namespace(self):
        """Validate the namespace and SPARQL prefix syntax."""
        from pyparsing import ParseBaseException
        from rdflib.plugins.sparql.parser import parseQuery

        absolute_iri(self.namespace)
        try:
            parsed = parseQuery(f"PREFIX {self.prefix}: <{self.namespace}> ASK {{}}")
        except ParseBaseException as error:
            raise ValueError(f"Invalid prefix name: {self.prefix!r}") from error
        if len(parsed[0]) != 1 or parsed[1].name != "AskQuery":
            raise ValueError("Invalid prefix declaration")
        return self

    def to_rdf(self, graph: Graph):
        """Write sh:prefix and the required xsd:anyURI namespace."""
        from rdflib import SH, XSD
        from rdflib import Literal as RdfLiteral

        node = _node(self.uri)
        graph.add((node, SH.prefix, RdfLiteral(self.prefix)))
        graph.add((node, SH.namespace, RdfLiteral(self.namespace, datatype=XSD.anyURI)))
        return node

    @classmethod
    def from_rdf(cls, graph: Graph, node: Node):
        """Read exactly one string prefix and one typed namespace."""
        from rdflib import SH, XSD
        from rdflib import Literal as RdfLiteral

        prefix = graph.value(node, SH.prefix, any=False)
        namespace = graph.value(node, SH.namespace, any=False)
        if (
            not isinstance(prefix, RdfLiteral)
            or prefix.language
            or prefix.datatype not in {None, XSD.string}
        ):
            raise ValueError("sh:prefix must be an xsd:string literal")
        if not isinstance(namespace, RdfLiteral) or namespace.datatype != XSD.anyURI:
            raise ValueError("sh:namespace must be an xsd:anyURI literal")
        return cls(uri=_identity(node), prefix=str(prefix), namespace=str(namespace))


class ShaclSparqlExecutable(BaseModel):
    """A SHACL query body, its prefix resources, and retained RDF metadata."""

    uri: str
    text: str
    query_type: Literal["SELECT", "ASK", "CONSTRUCT"]
    prefixes: list[str] = Field(default_factory=list)
    metadata: dict[str, list[RdfTerm]] = Field(default_factory=dict)
    requires_context: bool = False

    @property
    def name(self) -> str:
        """Choose a stable label for named execution."""
        from rdflib import RDFS

        labels = self.metadata.get(str(RDFS.label), [])
        return min(term.value for term in labels) if labels else self.uri

    def to_rdf(self, graph: Graph):
        """Serialize the query body and references to prefix declarations."""
        from rdflib import SH, URIRef
        from rdflib import Literal as RdfLiteral

        node = _node(self.uri)
        graph.add((node, SH[self.query_type.lower()], RdfLiteral(self.text)))
        for resource in self.prefixes:
            graph.add((node, SH.prefixes, _node(resource)))
        for predicate, values in self.metadata.items():
            for value in values:
                graph.add((node, URIRef(predicate), value.to_rdf()))
        return node

    @classmethod
    def from_rdf(cls, graph: Graph, node: Node):
        """Read a query and distinguish validation context from executable examples."""
        from rdflib import RDF, SH, XSD
        from rdflib import Literal as RdfLiteral

        texts = [
            (kind, text)
            for kind in ("SELECT", "ASK", "CONSTRUCT")
            for text in graph.objects(node, SH[kind.lower()])
        ]
        if len(texts) != 1:
            raise ValueError("Expected one SHACL query body")
        kind, text = texts[0]
        if (
            not isinstance(text, RdfLiteral)
            or text.language
            or text.datatype not in {None, XSD.string}
        ):
            raise ValueError("SHACL query bodies must be xsd:string literals")
        types = set(graph.objects(node, RDF.type))
        context = not types.intersection(
            {SH.SPARQLExecutable, SH[f"SPARQL{kind.title()}Executable"]}
        )
        context = context or bool(
            types.intersection({SH.SPARQLConstraint, SH.SPARQLFunction, SH.SPARQLRule})
        )
        context = context or any(
            (None, link, node) in graph
            for link in (SH.sparql, SH.validator, SH.nodeValidator, SH.propertyValidator, SH.rule)
        )
        metadata = {}
        for predicate, value in graph.predicate_objects(node):
            if predicate not in {SH.select, SH.ask, SH.construct, SH.prefixes}:
                metadata.setdefault(str(predicate), []).append(RdfTerm.from_rdf(value))
        return cls(
            uri=_identity(node),
            text=str(text),
            query_type=kind,
            prefixes=[_identity(resource) for resource in graph.objects(node, SH.prefixes)],
            metadata=metadata,
            requires_context=context,
        )


class ShaclShapesGraph(BaseModel):
    """Collection of SHACL shapes."""

    node_shapes: list[ShaclNodeShape] = Field(default_factory=list)
    base_uri: str | None = None
    queries: list[ShaclSparqlExecutable] = Field(default_factory=list)
    prefix_declarations: dict[str, list[ShaclPrefixDeclaration]] = Field(default_factory=dict)
    prefix_imports: dict[str, list[str]] = Field(default_factory=dict)

    def declare_prefixes(self, prefixes: dict[str, str], resource: str | None = None) -> str:
        """Retain typed declarations on a reusable prefix resource."""
        import json
        from hashlib import sha256

        identity = sha256(json.dumps(prefixes, sort_keys=True).encode()).hexdigest()
        resource = resource or "urn:rdfsolve:prefixes:" + identity
        existing = {item.prefix: item for item in self.prefix_declarations.get(resource, [])}
        for prefix, namespace in sorted(prefixes.items()):
            if prefix in existing and existing[prefix].namespace != namespace:
                raise ValueError(f"Conflicting namespace for prefix {prefix}")
            existing[prefix] = ShaclPrefixDeclaration(
                uri="_:prefix-" + sha256(json.dumps([prefix, namespace]).encode()).hexdigest(),
                prefix=prefix,
                namespace=namespace,
            )
        self.prefix_declarations[resource] = list(existing.values())
        return resource

    def compile_query(self, query: ShaclSparqlExecutable) -> str:
        """Resolve sh:prefixes/owl:imports*/sh:declare from retained declarations."""
        from rdflib.plugins.sparql.parser import parseQuery
        from rdflib.plugins.sparql.processor import prepareQuery

        pending, seen, prefixes = list(query.prefixes), set(), {}
        while pending:
            resource = pending.pop()
            if resource in seen:
                continue
            seen.add(resource)
            pending.extend(self.prefix_imports.get(resource, []))
            for declaration in self.prefix_declarations.get(resource, []):
                name, namespace = declaration.prefix, declaration.namespace
                if name in prefixes and prefixes[name] != namespace:
                    raise ValueError(f"Conflicting namespace for prefix {name}")
                prefixes[name] = namespace
        text = (
            "".join(f"PREFIX {name}: <{iri}>\n" for name, iri in sorted(prefixes.items()))
            + query.text
        )
        if not query.requires_context:
            if parseQuery(text)[1].name.upper() != query.query_type + "QUERY":
                raise ValueError("Query type does not match its SHACL property")
            prepareQuery(text)
        return text

    def read_queries(self, graph: Graph) -> None:
        """Read typed executables and prefix declarations, including imported resources."""
        from rdflib import OWL, SH, BNode, URIRef

        nodes = set().union(
            *(set(graph.subjects(p, None)) for p in (SH.select, SH.ask, SH.construct))
        )
        self.queries = [
            ShaclSparqlExecutable.from_rdf(graph, node) for node in sorted(nodes, key=str)
        ]
        pending = list(graph.subjects(SH.declare, None)) + [
            resource
            for query in self.queries
            for resource in graph.objects(_node(query.uri), SH.prefixes)
        ]
        seen = set()
        while pending:
            node = pending.pop()
            if not isinstance(node, (URIRef, BNode)):
                raise ValueError("Prefix resources must be IRIs or blank nodes")
            if node in seen:
                continue
            seen.add(node)
            uri = _identity(node)
            declarations = list(graph.objects(node, SH.declare))
            if any(not isinstance(d, (URIRef, BNode)) for d in declarations):
                raise ValueError("sh:declare must identify a prefix declaration")
            self.prefix_declarations[uri] = [
                ShaclPrefixDeclaration.from_rdf(graph, d) for d in declarations
            ]
            imports = list(graph.objects(node, OWL.imports))
            self.prefix_imports[uri] = [_identity(value) for value in imports]
            pending.extend(imports)
        for query in self.queries:
            self.compile_query(query)

    def to_rdf(self, graph: Graph | None = None) -> Graph:
        """Serialize all shapes to RDF graph."""
        from rdflib import Graph as RdfGraph
        from rdflib import Namespace

        if graph is None:
            graph = RdfGraph()

        sh = Namespace("http://www.w3.org/ns/shacl#")
        graph.bind("sh", sh)

        from rdflib import OWL

        for ns in self.node_shapes:
            ns.to_rdf(graph)
        for query in self.queries:
            query.to_rdf(graph)
        for resource, declarations in self.prefix_declarations.items():
            for declaration in declarations:
                graph.add((_node(resource), sh.declare, declaration.to_rdf(graph)))
                graph.bind(declaration.prefix, declaration.namespace)
        for resource, imports in self.prefix_imports.items():
            for imported in imports:
                graph.add((_node(resource), OWL.imports, _node(imported)))
        return graph

    @classmethod
    def from_rdf(cls, graph: Graph) -> ShaclShapesGraph:
        """Parse all shapes from RDF graph."""
        from rdflib import Namespace
        from rdflib.namespace import RDF

        sh = Namespace("http://www.w3.org/ns/shacl#")

        import logging

        supported = {
            "select",
            "ask",
            "construct",
            "prefixes",
            "declare",
            "prefix",
            "namespace",
            "deactivated",
            "path",
            "datatype",
            "class",
            "nodeKind",
            "minCount",
            "maxCount",
            "name",
            "description",
            "or",
            "qualifiedValueShape",
            "qualifiedMinCount",
            "qualifiedMaxCount",
            "qualifiedValueShapesDisjoint",
            "targetClass",
            "closed",
            "ignoredProperties",
            "property",
            "alternativePath",
            "inversePath",
            "zeroOrMorePath",
            "oneOrMorePath",
            "zeroOrOnePath",
        }
        unknown = sorted(
            {
                str(predicate)
                for predicate in graph.predicates()
                if str(predicate).startswith(str(sh))
                and str(predicate)[len(str(sh)) :] not in supported
            }
        )
        logger = logging.getLogger(__name__)
        if unknown:
            logger.warning("SHACL reader does not retain these predicates: %s", unknown)
        node_shapes = []
        nodes = set(graph.subjects(RDF.type, sh.NodeShape))
        nodes.update(graph.subjects(sh.targetClass, None))
        nodes.update(graph.subjects(sh.property, None))
        for ns_uri in sorted(nodes, key=str):
            if (ns_uri, sh.path, None) in graph:
                continue
            node_shapes.append(ShaclNodeShape.from_rdf(graph, ns_uri))

        shapes = cls(node_shapes=node_shapes)
        shapes.read_queries(graph)
        return shapes
