"""Named SPARQL queries and SHACL paths. Loading does not execute queries."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any
from typing import Literal as QueryLiteral

from rdflib import RDF, RDFS, SH, Graph, Namespace, URIRef
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.term import Node

from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.readers.paths import read_path
from rdfsolve.schema_models.shacl_model import (
    ShaclShapesGraph,
    ShaclSparqlExecutable,
)

if TYPE_CHECKING:
    from rdfsolve.models.source_model import SourceModel
    from rdfsolve.schema_models.core import MinedSchema

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
        self.shacl = ShaclShapesGraph()
        self._extra = Graph()
        self.paths: dict[str, PropertyPath] = {}
        self.import_report: list[dict[str, Any]] = []

    @property
    def graph(self) -> Graph:
        """Serialize typed queries with retained source shapes and metadata."""
        return self.shacl.to_rdf(self._extra + Graph())

    @property
    def queries(self) -> dict[str, SavedQuery]:
        """Expose compiled query views of the typed SHACL collection."""
        from rdfsolve.schema_models.shacl_model import _node

        return {
            q.name: SavedQuery(
                q.name, self.shacl.compile_query(q), q.query_type, _node(q.uri), q.requires_context
            )
            for q in self.shacl.queries
        }

    def add(
        self,
        name: str,
        query: str,
        *,
        description: str = "",
        endpoint: str = "",
        schema: MinedSchema | None = None,
        prefixes: dict[str, str] | None = None,
    ) -> SavedQuery:
        """Populate the typed SHACL model with a named executable."""
        if not name.strip() or any(q.name == name for q in self.shacl.queries):
            raise ValueError("Use a nonempty, unique query name")
        kind = parseQuery(query)[1].name.removesuffix("Query").upper()
        if kind not in QUERY_TYPES.values():
            raise ValueError("Only SELECT, ASK, and CONSTRUCT queries are supported")
        if prefixes is None:
            prefixes = schema.get_prefixes() if schema is not None else {}
        identity = json.dumps([name, query, prefixes], sort_keys=True)
        uri = "urn:rdfsolve:query:" + sha256(identity.encode()).hexdigest()
        metadata = {
            str(RDFS.label): [RdfTerm(kind="literal", value=name)],
            str(RDF.type): [
                RdfTerm(kind="uri", value=str(t))
                for t in (SH.SPARQLExecutable, SH[f"SPARQL{kind.title()}Executable"])
            ],
        }
        for predicate, value, term_kind in (
            (RDFS.comment, description, "literal"),
            (SCHEMA.target, endpoint, "uri"),
        ):
            if value:
                if term_kind == "uri":
                    PropertyPath(operator="predicate", iri=value)
                metadata[str(predicate)] = [RdfTerm(kind=term_kind, value=value)]
        if schema is not None:
            digest = sha256(json.dumps(schema.to_dict(), sort_keys=True).encode()).hexdigest()
            metadata[str(SCHEMA.isBasedOn)] = [RdfTerm(kind="uri", value="urn:sha256:" + digest)]
        shapes = ShaclShapesGraph()
        resources = []
        if prefixes:
            resources.append(shapes.declare_prefixes(prefixes))
        executable = ShaclSparqlExecutable(
            uri=uri, text=query, query_type=kind, prefixes=resources, metadata=metadata
        )
        compiled = shapes.compile_query(executable)
        self.shacl.prefix_declarations.update(shapes.prefix_declarations)
        self.shacl.queries.append(executable)
        from rdfsolve.schema_models.shacl_model import _node

        return SavedQuery(name, compiled, kind, _node(uri), executable.requires_context)

    def load_shacl(self, source: str | Path | Graph) -> list[str]:
        """Read queries through the typed SHACL model; retain all other RDF."""
        graph = (
            source
            if isinstance(source, Graph)
            else Graph().parse(data=Path(source).read_text(), format="turtle")
        )
        imported = ShaclShapesGraph()
        imported.read_queries(graph)
        names = [q.name for q in imported.queries]
        if len(names) != len(set(names)) or set(names).intersection(self.queries):
            raise ValueError("Use unique query names")
        paths = {}
        for shape, node in graph.subject_objects(SH.path):
            if str(shape) in self.paths or str(shape) in paths:
                raise ValueError(f"Duplicate property-shape path: {shape}")
            paths[str(shape)] = read_path(graph, node)
        combined = self.graph + graph
        typed = ShaclShapesGraph()
        typed.read_queries(combined)
        extra = combined - typed.to_rdf()
        from rdfsolve.schema_models.shacl_model import _node

        for query in typed.queries:
            extra.remove((_node(query.uri), SH[query.query_type.lower()], None))
        for declarations in typed.prefix_declarations.values():
            for declaration in declarations:
                extra.remove((_node(declaration.uri), SH.prefix, None))
                extra.remove((_node(declaration.uri), SH.namespace, None))
        self.shacl, self._extra = typed, extra
        self.paths.update(paths)
        return names

    def load_directory(
        self,
        directory: str | Path,
        *,
        endpoint: str = "",
        schema: MinedSchema | None = None,
        prefixes: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Import local .rq/.sparql or Turtle files and report every rejected file."""
        paths = sorted(
            p for p in Path(directory).rglob("*") if p.suffix in {".rq", ".sparql", ".ttl"}
        )
        for path in paths:
            event: dict[str, Any] = {"file": str(path), "status": "loaded"}
            try:
                text = path.read_text()
                if path.suffix == ".ttl":
                    event["queries"] = self.load_shacl(path)
                else:
                    headers = {
                        key.lower(): value
                        for key, value in re.findall(
                            r"^#\s*(title|description):\s*(.+)$", text, re.MULTILINE | re.IGNORECASE
                        )
                    }
                    title = headers.get("title", path.stem)
                    if re.search(r"\{\{\w+\}\}", text):
                        raise ValueError(
                            "Supply reviewed parameter values before importing this template"
                        )
                    saved = self.add(
                        title,
                        text,
                        description=headers.get("description", ""),
                        endpoint=endpoint,
                        schema=schema,
                        prefixes=prefixes,
                    )
                    self.shacl.queries[-1].metadata[str(SCHEMA.url)] = [
                        RdfTerm(kind="uri", value=path.resolve().as_uri())
                    ]
                    event["queries"] = [saved.name]
            except Exception as exc:
                event.update(status="rejected", error=f"{type(exc).__name__}: {exc}")
            self.import_report.append(event)
        return self.import_report

    @classmethod
    def from_source(
        cls,
        source: SourceModel | dict[str, Any],
        *,
        base_dir: str | Path = ".",
        repository_path: str | Path | None = None,
    ) -> QueryCollection:
        """Load configured graphs/dumps, or an explicitly checked-out repository."""
        import requests

        from rdfsolve.models.source_model import SourceModel
        from rdfsolve.sparql_helper import SparqlHelper

        source = source if isinstance(source, SourceModel) else SourceModel.model_validate(source)
        locations = source.sparql_examples
        if locations is None:
            raise ValueError("The source has no sparql_examples locations")
        collection = cls()
        if locations.shacl_graph_in_endpoint:
            with SparqlHelper.from_source_entry(source.model_dump()) as helper:
                for iri in locations.shacl_graph_in_endpoint:
                    graph = helper.construct_graph(
                        f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{iri}> {{ ?s ?p ?o }} }}"
                    )
                    collection.load_shacl(graph)
        for location in locations.shacl_dumps:
            if location.startswith(("http://", "https://")):
                with requests.get(location, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    content = bytearray()
                    for chunk in response.iter_content(65536):
                        content.extend(chunk)
                        if len(content) > 20_000_000:
                            raise ValueError("Example dump exceeds twenty megabytes")
                collection.load_shacl(Graph().parse(data=content.decode(), format="turtle"))
            else:
                collection.load_shacl(Path(base_dir) / location)
        if repository_path is not None:
            collection.load_directory(repository_path, endpoint=source.endpoint)
        elif not (locations.shacl_dumps or locations.shacl_graph_in_endpoint):
            raise ValueError(
                f"Check out and pin {locations.link_to_repository}, then supply repository_path"
            )
        return collection

    def rename(self, name: str, new_name: str) -> None:
        """Rename a saved query and replace its RDF labels with the chosen name."""
        if not new_name.strip() or new_name in self.queries:
            raise ValueError("Use a nonempty, unique query name")
        query = next((q for q in self.shacl.queries if q.name == name), None)
        if query is None:
            raise KeyError(name)
        query.metadata[str(RDFS.label)] = [RdfTerm(kind="literal", value=new_name)]

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
