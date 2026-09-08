"""Core API for schema mining, VoID parsing, sources management, and format conversion."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rdfsolve.schema_models.metadata import MetadataDocument
    from rdfsolve.schema_models.void_schema import VoidSchema
    from rdfsolve.sources import SourceEntry

import pandas as pd
from rdflib import Graph

from .models import MinedSchema
from .void_discover import VoidParser

logger = logging.getLogger(__name__)

__all__ = [
    "discover_void_graphs",
    "discover_void_source",
    "enrich_source_with_bioregistry",
    "execute_sparql",
    "export_schema_artifacts",
    "get_bioregistry_metadata",
    "graph_to_jsonld",
    "graph_to_linkml",
    "graph_to_schema",
    "graph_to_shacl",
    "load_mapping_jsonld",
    "load_parser_from_file",
    "load_parser_from_graph",
    "load_parser_from_jsonld",
    "load_sources",
    "mine_schema",
    "query_metadata",
    "resolve_void_uri_base",
    "sources_to_jsonld",
    "to_jsonld_from_file",
    "to_linkml_from_file",
    "to_rdfconfig_from_file",
    "to_shacl_from_file",
    "to_void_from_file",
]


# Parser / Export functions


def load_parser_from_file(
    void_file_path: str,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Load a VoID file and return a parser for schema extraction."""
    return VoidParser(
        void_source=void_file_path, graph_uris=graph_uris, exclude_graphs=exclude_graphs
    )


def load_parser_from_graph(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Load a VoID graph and return a parser for schema extraction."""
    return VoidParser(void_source=graph, graph_uris=graph_uris, exclude_graphs=exclude_graphs)


def load_parser_from_jsonld(
    jsonld_path: str,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = True,
) -> VoidParser:
    """Load a mined-schema JSON-LD file and return a VoidParser."""
    from .models import MinedSchema

    schema = MinedSchema.from_jsonld(jsonld_path)
    return VoidParser(
        void_source=schema.to_void_graph(),
        graph_uris=graph_uris,
        exclude_graphs=exclude_graphs,
    )


def to_rdfconfig_from_file(
    void_file_path: str,
    endpoint_url: str | None = None,
    endpoint_name: str | None = None,
    graph_uri: str | None = None,
) -> dict[str, str]:
    """Convert a VoID file to RDF-config YAML files."""
    parser = load_parser_from_file(void_file_path)
    return parser.to_rdfconfig(
        endpoint_url=endpoint_url,
        endpoint_name=endpoint_name,
        graph_uri=graph_uri,
    )


def to_void_from_file(jsonld_path: str) -> Graph:
    """Convert a mined-schema JSON-LD file to a VoID RDF graph."""
    from .models import MinedSchema

    schema = MinedSchema.from_jsonld(jsonld_path)
    return schema.to_void_graph()


def to_linkml_from_file(
    void_file_path: str,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Convert a VoID file to LinkML YAML schema.

    Args:
        void_file_path: Path to VoID file
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema

    Returns:
        LinkML YAML schema string
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_linkml_yaml(
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )


def to_shacl_from_file(
    void_file_path: str,
    schema_base_uri: str = "http://example.org/shapes/",
) -> str:
    """Convert a VoID file to SHACL shapes.

    Generates SHACL (Shapes Constraint Language) shapes from a VoID
    description file. SHACL shapes define constraints on RDF data and
    can be used for validation.

    Args:
        void_file_path: Path to VoID file
        schema_base_uri: Base URI for the SHACL shapes (default: http://example.org/shapes/)

    Returns:
        SHACL shapes as Turtle/RDF string
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_shacl(
        schema_base_uri=schema_base_uri,
    )


def to_jsonld_from_file(
    void_file_path: str,
    endpoint_url: str | None = None,
    dataset_name: str | None = None,
    graph_uris: str | list[str] | None = None,
) -> dict[str, Any]:
    """Convert a VoID file to JSON-LD format."""
    parser = load_parser_from_file(void_file_path)
    graph_uris_list = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    return parser.to_jsonld(
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_jsonld(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    endpoint_url: str | None = None,
    dataset_name: str | None = None,
) -> dict[str, Any]:
    """Convert a VoID graph to JSON-LD format."""
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    graph_uris_list = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    return parser.to_jsonld(
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_schema(
    void_graph: Graph,
    graph_uris: str | list[str] | None = None,
) -> pd.DataFrame:
    """Convert VoID graph to schema DataFrame."""
    parser = VoidParser(void_source=void_graph, graph_uris=graph_uris)
    return parser.to_schema()


def graph_to_linkml(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Convert a VoID graph to LinkML YAML schema.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema

    Returns:
        LinkML YAML schema string
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    return parser.to_linkml_yaml(
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )


def graph_to_shacl(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    schema_base_uri: str = "http://example.org/shapes/",
) -> str:
    """Convert a VoID graph to SHACL shapes.

    Generates SHACL (Shapes Constraint Language) shapes from a VoID
    graph. SHACL shapes define constraints on RDF data and can be used
    for validation.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        schema_base_uri: Base URI for the SHACL shapes (default: http://example.org/shapes/)

    Returns:
        SHACL shapes as Turtle/RDF string
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    return parser.to_shacl(
        schema_base_uri=schema_base_uri,
    )


def export_schema_artifacts(
    void_graph: Graph,
    name: str,
    endpoint: str,
    output_dir: str | Path,
    tag: str = "discovered_remote",
    fmt: str = "all",
) -> dict[str, str]:
    """Write source VoID RDF and canonical schema exports."""
    if fmt not in {"void", "jsonld", "all"}:
        raise ValueError("fmt must be void, jsonld, or all")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}
    schema = VoidParser(void_source=void_graph).to_mined_schema()
    schema.about.endpoint = schema.about.endpoint or endpoint
    schema.about.dataset_name = schema.about.dataset_name or name
    canonical_path = out / f"{name}_{tag}_schema.json"
    canonical_path.write_text(json.dumps(schema.to_dict(), indent=2) + "\n", encoding="utf-8")
    written["schema_json"] = str(canonical_path)

    if fmt in ("void", "all"):
        void_path = out / f"{name}_{tag}_void.ttl"
        void_graph.serialize(destination=str(void_path), format="turtle")
        written["void_ttl"] = str(void_path)

    if fmt in ("jsonld", "all"):
        jsonld_doc = graph_to_jsonld(
            void_graph,
            endpoint_url=endpoint,
            dataset_name=name,
        )
        jsonld_path = out / f"{name}_{tag}_schema.jsonld"
        jsonld_path.write_text(
            json.dumps(jsonld_doc, indent=2) + "\n",
            encoding="utf-8",
        )
        written["schema_jsonld"] = str(jsonld_path)

    if fmt in ("all",):
        try:
            export_parser = VoidParser(void_source=void_graph)
            rdfconfig = export_parser.to_rdfconfig(
                endpoint_url=endpoint,
                endpoint_name=name,
            )
            config_dir = out / f"{name}_{tag}_config"
            config_dir.mkdir(parents=True, exist_ok=True)
            for fname, content in rdfconfig.items():
                (config_dir / f"{fname}.yaml").write_text(
                    content,
                    encoding="utf-8",
                )
            written["rdfconfig_dir"] = str(config_dir)
        except Exception as exc:
            logger.warning("RDF-config export failed for %s: %s", name, exc)

    return written


# Mining functions


def discover_void_graphs(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = False,
    *,
    timeout: float = 30.0,
    max_retries: int = 1,
    batch_size: int = 100,
    graph_batch_size: int = 8,
    max_pages: int = 1000,
) -> dict[str, Any]:
    """Retrieve published VoID RDF and its canonical schema. Failures raise."""
    return VoidParser(graph_uris=graph_uris, exclude_graphs=exclude_graphs).discover_void_graphs(
        endpoint_url, timeout=timeout, max_retries=max_retries,
        batch_size=batch_size, graph_batch_size=graph_batch_size, max_pages=max_pages,
    )


def discover_all_graphs(
    endpoint_url: str, *, include_counts: bool = False,
    timeout: float = 30.0, max_retries: int = 1,
    batch_size: int = 100, max_pages: int = 1000,
) -> dict[str, Any]:
    """List graph names in pages. Set include_counts=True for triple counts.

    A graph name is not evidence that it contains an ontology.
    Query failures raise. Omitted counts are None, not zero.
    """
    return VoidParser().discover_all_graphs(
        endpoint_url, include_counts=include_counts, timeout=timeout,
        max_retries=max_retries, batch_size=batch_size, max_pages=max_pages,
    )


def extract_metadata_from_void_graphs(
    endpoint_url: str, void_graph_uris: list[str]
) -> dict[str, Any]:
    """Extract metadata triples from VoID-named graphs.

    VoID metadata graphs often contain dataset descriptions (title, license,
    publisher, etc.). This function extracts all triples from graphs identified
    as containing metadata.

    Args:
        endpoint_url: SPARQL endpoint URL.
        void_graph_uris: List of graph URIs to extract metadata from.

    Returns:
        Dict with keys:
        - ``metadata_by_graph``: dict mapping graph URI to metadata triples
        - ``total_triples``: total triples extracted

    Example:
        >>> from rdfsolve import discover_all_graphs, extract_metadata_from_void_graphs
        >>> graphs = discover_all_graphs("https://example.org/sparql")
        >>> if graphs["void_graphs"]:
        ...     metadata = extract_metadata_from_void_graphs(
        ...         "https://example.org/sparql", graphs["void_graphs"]
        ...     )
        ...     print(f"Extracted {metadata['total_triples']} metadata triples")
    """
    return VoidParser().extract_metadata_from_void_graphs(endpoint_url, void_graph_uris)


def mine_schema(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    dataset_name: str | None = None,
    chunk_size: int = 10_000,
    class_chunk_size: int | None = None,
    class_batch_size: int = 15,
    delay: float = 0.5,
    timeout: float = 120.0,
    counts: bool = True,
    strategy: str | None = None,
    report_path: str | None = None,
    filter_service_namespaces: bool = True,
    authors: list[dict[str, str]] | None = None,
    get_graphs_from_store: bool = False,
    graph_store_url: str | None = None,
    graph_store_dir: str | Path = "graph-store",
    graph_store_max_bytes: int = 64 * 1024 * 1024,
) -> MinedSchema:
    """Mine RDF schema from a SPARQL endpoint using SELECT queries.

    Parameters
    ----------
    strategy
        Mining strategy to use. Can be "two-phase" (default), "single-pass", or "one-shot".

    Returns
    -------
    MinedSchema
        Schema object with methods to export to JSON-LD, VoID, LinkML, SHACL.
    """
    from .miner import mine_schema as _mine

    is_local = any(host in endpoint_url.lower() for host in ("localhost", "127.0.0.1", "::1"))
    if is_local:
        delay = 0.0

    return _mine(
        endpoint_url=endpoint_url,
        graph_uris=graph_uris,
        dataset_name=dataset_name,
        chunk_size=chunk_size,
        class_chunk_size=class_chunk_size,
        class_batch_size=class_batch_size,
        delay=delay,
        timeout=timeout,
        counts=counts,
        strategy=strategy,
        report_path=report_path,
        filter_service_namespaces=filter_service_namespaces,
        authors=authors,
        get_graphs_from_store=get_graphs_from_store, graph_store_url=graph_store_url,
        graph_store_dir=graph_store_dir, graph_store_max_bytes=graph_store_max_bytes,
    )


# Metadata


def query_metadata(
    endpoint_url: str, timeout: float = 30.0, *,
    graph_uris: list[str] | None = None,
    subject_iris: list[str] | None = None,
) -> MetadataDocument:
    """Retrieve scoped RDF metadata. Use .project(subject_iri) for known fields.

    Explicit subjects can use any vocabulary. Automatic discovery looks for
    declared VoID/DCAT datasets and SPARQL services, not arbitrary entities.
    None graph scope means the default graph, not every named graph.
    """
    from rdfsolve.metadata import query_metadata_document
    from rdfsolve.sparql_helper import SparqlHelper

    with SparqlHelper(endpoint_url, timeout=timeout, max_retries=1) as helper:
        return query_metadata_document(helper, graph_uris=graph_uris, subject_iris=subject_iris)


# Sources / Registry


_VOID_URI_DEFAULT = "https://jmillanacosta.com/rdfsolve/{name}/mined/"


def load_sources(
    path: str | Path | None = None,
    name_filter: str | None = None,
) -> list[SourceEntry]:
    """Load source entries, optionally filtered by name regex."""
    from .sources import load_sources as _load

    entries = _load(path)
    if name_filter:
        pat = re.compile(name_filter, re.IGNORECASE)
        entries = [e for e in entries if pat.search(e.get("name", ""))]
    return entries


def resolve_void_uri_base(
    name: str,
    override: str | None = None,
    entry: SourceEntry | dict[str, Any] | None = None,
) -> str:
    """Return the VoID base URI for a dataset."""
    if override:
        return override.rstrip("/") + "/"
    if entry and entry.get("void_uri_base"):
        return str(entry["void_uri_base"]).rstrip("/") + "/"
    return _VOID_URI_DEFAULT.format(name=name)


def get_bioregistry_metadata(br_prefix: str) -> dict[str, Any]:
    """Return a structured metadata dict for a Bioregistry prefix."""
    from rdfsolve.sources import get_bioregistry_metadata as _impl

    return _impl(br_prefix)


def enrich_source_with_bioregistry(entry: SourceEntry) -> str | None:
    """Populate ``bioregistry_*`` fields on a source entry in-place."""
    from rdfsolve.sources import enrich_source_with_bioregistry as _impl

    return _impl(entry)


def sources_to_jsonld(
    entries: list[SourceEntry],
    *,
    enrich: bool = False,
) -> dict[str, Any]:
    """Serialise source entries to a JSON-LD document."""
    from rdfsolve.sources import sources_to_jsonld as _impl

    return _impl(entries, enrich=enrich)


def discover_void_source(
    endpoint: str,
    name: str,
    output_dir: str | Path | None = None,
    *,
    tag: str = "discovered_remote",
    fmt: str = "all",
    timeout: float = 30.0,
    max_retries: int = 1,
    graph_uris: str | list[str] | None = None,
    batch_size: int = 100,
    graph_batch_size: int = 8,
    max_pages: int = 1000,
    get_graphs_from_store: bool = False,
    graph_store_url: str | None = None,
    graph_store_dir: str | Path = "graph-store",
    graph_store_max_bytes: int = 64 * 1024 * 1024,
) -> VoidSchema:
    """Return published VoID as an object. Export only when output_dir is set.

    Use .to_mined_schema() for canonical patterns, .datasets for typed VoID
    descriptions, and .graph for all retrieved RDF. Empty patterns do not
    imply an empty description. Graph Store mode requires explicit scope.
    """
    from rdfsolve.schema_models.void_schema import VoidSchema

    if fmt not in {"void", "jsonld", "all"}:
        raise ValueError("fmt must be void, jsonld, or all")
    scopes = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    if get_graphs_from_store:
        from rdfsolve.graph_store import download_graphs, load_downloads

        if not graph_store_url or not scopes:
            raise ValueError("Graph Store retrieval requires graph_store_url and graph_uris")
        downloads = download_graphs(
            graph_store_url, scopes, graph_store_dir,
            max_bytes=graph_store_max_bytes, timeout=timeout,
        )
        dataset = load_downloads(downloads, endpoint_url=endpoint, timeout=timeout)
        graph = Graph()
        for uri in scopes:
            graph += dataset.graph(uri)
        document = VoidSchema(graph, endpoint, name, scopes)
    else:
        result = discover_void_graphs(
            endpoint, graph_uris=scopes, timeout=timeout, max_retries=max_retries,
            batch_size=batch_size, graph_batch_size=graph_batch_size, max_pages=max_pages,
        )
        document = VoidSchema(
            result["graph"], endpoint, name, result["found_graphs"], result["default_graph"]
        )
    if output_dir is not None:
        document.files = export_schema_artifacts(
            document.graph, name, endpoint, output_dir, tag=tag, fmt=fmt,
        )
    return document


# SPARQL execution


def execute_sparql(
    query: str,
    endpoint: str,
    method: str = "GET",
    timeout: int = 30,
    variable_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Execute a SPARQL query against a remote endpoint."""
    from rdfsolve.query import execute_sparql as _execute

    qr = _execute(
        query=query,
        endpoint=endpoint,
        method=method,
        timeout=timeout,
        variable_map=variable_map or {},
    )
    return qr.model_dump()


# Mapping utilities


def load_mapping_jsonld(path: str) -> dict[str, Any]:
    """Load a mapping JSON-LD file from disk."""
    result: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
    return result
