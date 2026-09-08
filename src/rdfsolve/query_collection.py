"""Named SPARQL queries and SHACL paths. Loading does not execute queries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal as QueryLiteral
from uuid import uuid4

from rdflib import OWL, RDF, RDFS, SH, Graph, Literal, Namespace, URIRef
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.processor import prepareQuery
from rdflib.term import Node

from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.readers.paths import read_path

SCHEMA = Namespace("https://schema.org/")
QueryKind = QueryLiteral["SELECT", "ASK", "CONSTRUCT"]
QUERY_TYPES: dict[URIRef, QueryKind] = {
    SH.select: "SELECT",
    SH.ask: "ASK",
    SH.construct: "CONSTRUCT",
}


@dataclass(frozen=True)
class SavedQuery:
    """Query text and its source identity, separate from execution history."""

    name: str
    query: str
    query_type: QueryKind
    node: Node
    requires_context: bool = False


@dataclass(frozen=True)
class QueryRun:
    """One named execution outcome. Results are not retained in memory."""

    name: str
    endpoint: str
    started_at: str
    elapsed_seconds: float
    success: bool
    error: str = ""


class QueryCollection:
    """Keep named queries, their RDF descriptions, and property-shape paths."""

    def __init__(self) -> None:
        """Create an empty collection without network access."""
        self.graph = Graph()
        self.queries: dict[str, SavedQuery] = {}
        self.paths: dict[str, PropertyPath] = {}

    def add(
        self, name: str, query: str, *, description: str = "", endpoint: str = ""
    ) -> SavedQuery:
        """Add a read query. Reject duplicate names, updates, and invalid syntax."""
        if not name.strip() or name in self.queries:
            raise ValueError("Use a nonempty, unique query name")
        kind = parseQuery(query)[1].name.removesuffix("Query").upper()
        if kind not in QUERY_TYPES.values():
            raise ValueError("Only SELECT, ASK, and CONSTRUCT queries are supported")
        prepareQuery(query)
        if endpoint:
            PropertyPath(operator="predicate", iri=endpoint)
        node = URIRef(f"urn:uuid:{uuid4()}")
        predicate = next(p for p, value in QUERY_TYPES.items() if value == kind)
        saved = SavedQuery(name, query, QUERY_TYPES[predicate], node)
        self.graph.add((node, RDF.type, SH.SPARQLExecutable))
        self.graph.add((node, RDF.type, SH[f"SPARQL{kind.title()}Executable"]))
        self.graph.add((node, RDFS.label, Literal(name)))
        self.graph.add((node, predicate, Literal(query)))
        if description:
            self.graph.add((node, RDFS.comment, Literal(description)))
        if endpoint:
            self.graph.add((node, SCHEMA.target, URIRef(endpoint)))
        self.queries[name] = saved
        return saved

    def load_shacl(self, source: str | Path | Graph) -> list[str]:
        """Load local Turtle or a graph. Retain all RDF; do not fetch imports.

        Queries without an executable type, or attached to validation shapes,
        require SHACL context and cannot run through the helper.
        The load is atomic if names, prefixes, or paths are invalid.
        """
        graph = Graph()
        if isinstance(source, Graph):
            for triple in source:
                graph.add(triple)
        else:
            graph.parse(data=Path(source).read_text(encoding="utf-8"), format="turtle")
        pending: dict[str, SavedQuery] = {}
        paths = {}
        for shape, node in graph.subject_objects(SH.path):
            key = str(shape)
            if key in paths or key in self.paths:
                raise ValueError(f"Duplicate property-shape path: {key}")
            paths[key] = read_path(graph, node)
        nodes = set().union(*(set(graph.subjects(p, None)) for p in QUERY_TYPES))
        for node in sorted(nodes, key=str):
            texts = [(p, text) for p in QUERY_TYPES for text in graph.objects(node, p)]
            if len(texts) != 1 or not isinstance(texts[0][1], Literal):
                raise ValueError(f"Expected one query literal on {node}")
            predicate, text = texts[0]
            labels = sorted(str(label) for label in graph.objects(node, RDFS.label))
            name = labels[0] if labels else str(node)
            if name in pending or name in self.queries:
                raise ValueError(f"Duplicate query name: {name}")
            kind = QUERY_TYPES[predicate]
            standalone = any(
                (node, RDF.type, rdf_type) in graph
                for rdf_type in (SH.SPARQLExecutable, SH[f"SPARQL{kind.title()}Executable"])
            )
            context = not standalone or any(
                (node, RDF.type, rdf_type) in graph
                for rdf_type in (SH.SPARQLConstraint, SH.SPARQLFunction, SH.SPARQLRule)
            )
            context = context or any(
                next(graph.subjects(link, node), None) is not None
                for link in (
                    SH.sparql,
                    SH.validator,
                    SH.nodeValidator,
                    SH.propertyValidator,
                    SH.rule,
                )
            )
            query = _with_prefixes(graph, node, str(text))
            if not context:
                if parseQuery(query)[1].name.upper() != kind + "QUERY":
                    raise ValueError(f"Query type does not match {predicate}")
                prepareQuery(query)
            pending[name] = SavedQuery(name, query, kind, node, context)
        self.graph += graph
        self.queries.update(pending)
        self.paths.update(paths)
        return list(pending)

    def rename(self, name: str, new_name: str) -> None:
        """Rename a saved query and replace its RDF labels with the chosen name."""
        from dataclasses import replace

        if not new_name.strip() or new_name in self.queries:
            raise ValueError("Use a nonempty, unique query name")
        saved = self.queries.pop(name)
        self.queries[new_name] = replace(saved, name=new_name)
        self.graph.set((saved.node, RDFS.label, Literal(new_name)))

    def to_turtle(self, output_file: str | Path | None = None) -> str:
        """Export the retained query and shape RDF, not an execution log."""
        text = self.graph.serialize(format="turtle")
        if output_file is not None:
            Path(output_file).write_text(text, encoding="utf-8")
        return text

    def path_query(self, shape: str, subject: str, *, limit: int = 20) -> str:
        """Build a bounded SELECT for one subject and a loaded property-shape path.

        This does not assert that the path exists or apply SHACL validation.
        LIMIT bounds results, not the cost of traversing a path.
        """
        if limit < 1:
            raise ValueError("Use a positive result limit")
        iri = path_to_sparql(PropertyPath(operator="predicate", iri=subject))
        path = path_to_sparql(self.paths[shape])
        return f"SELECT DISTINCT ?value WHERE {{ {iri} {path} ?value }} LIMIT {limit}"


def _with_prefixes(graph: Graph, node: Node, query: str) -> str:
    """Resolve SHACL prefix declarations from RDF already present in the graph."""
    pending = list(graph.objects(node, SH.prefixes))
    seen = set()
    prefixes: dict[str, str] = {}
    while pending:
        resource = pending.pop()
        if resource in seen:
            continue
        seen.add(resource)
        pending.extend(graph.objects(resource, OWL.imports))
        for declaration in graph.objects(resource, SH.declare):
            names = list(graph.objects(declaration, SH.prefix))
            namespaces = list(graph.objects(declaration, SH.namespace))
            if len(names) != 1 or len(namespaces) != 1:
                raise ValueError("A prefix declaration needs one prefix and namespace")
            name, namespace = str(names[0]), str(namespaces[0])
            PropertyPath(operator="predicate", iri=namespace)
            # Parse the declaration as syntax; do not interpolate unchecked text.
            parsed = parseQuery(f"PREFIX {name}: <{namespace}> ASK {{}}")
            if len(parsed[0]) != 1:
                raise ValueError("Invalid prefix declaration")
            if name in prefixes and prefixes[name] != namespace:
                raise ValueError(f"Conflicting namespace for prefix {name}")
            prefixes[name] = namespace
    # Inline prefixes remain in the original query and take precedence.
    return "".join(f"PREFIX {name}: <{iri}>\n" for name, iri in sorted(prefixes.items())) + query
