"""Core API for schema mining, VoID parsing, sources management, and format conversion."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    filter_void_nodes: bool = True,
    endpoint_url: str | None = None,
    endpoint_name: str | None = None,
    graph_uri: str | None = None,
) -> dict[str, str]:
    """Convert a VoID file to RDF-config YAML files."""
    parser = load_parser_from_file(void_file_path)
    return parser.to_rdfconfig(
        filter_void_nodes=filter_void_nodes,
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
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Convert a VoID file to LinkML YAML schema.

    Args:
        void_file_path: Path to VoID file
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema

    Returns:
        LinkML YAML schema string
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_linkml_yaml(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )


def to_shacl_from_file(
    void_file_path: str,
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
    include_annotations: bool = False,
) -> str:
    """Convert a VoID file to SHACL shapes.

    Generates SHACL (Shapes Constraint Language) shapes from a VoID
    description file. SHACL shapes define constraints on RDF data and
    can be used for validation.

    Args:
        void_file_path: Path to VoID file
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema
        closed: Generate closed shapes (only allow defined properties)
        suffix: Optional suffix for shape names (e.g., "Shape")
        include_annotations: Include class/slot annotations in shapes

    Returns:
        SHACL shapes as Turtle/RDF string
    """
    parser = load_parser_from_file(void_file_path)
    return parser.to_shacl(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
        closed=closed,
        suffix=suffix,
        include_annotations=include_annotations,
    )


def to_jsonld_from_file(
    void_file_path: str,
    filter_void_admin_nodes: bool = True,
    endpoint_url: str | None = None,
    dataset_name: str | None = None,
    graph_uris: str | list[str] | None = None,
) -> dict[str, Any]:
    """Convert a VoID file to JSON-LD format."""
    parser = load_parser_from_file(void_file_path)
    graph_uris_list = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    return parser.to_jsonld(
        filter_void_admin_nodes=filter_void_admin_nodes,
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_jsonld(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_admin_nodes: bool = True,
    endpoint_url: str | None = None,
    dataset_name: str | None = None,
) -> dict[str, Any]:
    """Convert a VoID graph to JSON-LD format."""
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    graph_uris_list = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    return parser.to_jsonld(
        filter_void_admin_nodes=filter_void_admin_nodes,
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris_list,
    )


def graph_to_schema(
    void_graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_admin_nodes: bool = True,
) -> pd.DataFrame:
    """Convert VoID graph to schema DataFrame."""
    parser = VoidParser(void_source=void_graph, graph_uris=graph_uris)
    return parser.to_schema(filter_void_admin_nodes=filter_void_admin_nodes)


def graph_to_linkml(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Convert a VoID graph to LinkML YAML schema.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema

    Returns:
        LinkML YAML schema string
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    return parser.to_linkml_yaml(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )


def graph_to_shacl(
    graph: Graph,
    graph_uris: str | list[str] | None = None,
    filter_void_nodes: bool = True,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
    include_annotations: bool = False,
) -> str:
    """Convert a VoID graph to SHACL shapes.

    Generates SHACL (Shapes Constraint Language) shapes from a VoID
    graph. SHACL shapes define constraints on RDF data and can be used
    for validation.

    Args:
        graph: RDFLib Graph with VoID data
        graph_uris: Graph URIs to filter extraction
        filter_void_nodes: Remove VoID-specific nodes
        schema_name: Name for the schema
        schema_description: Description for the schema
        schema_base_uri: Base URI for the schema
        closed: Generate closed shapes (only allow defined properties)
        suffix: Optional suffix for shape names (e.g., "Shape")
        include_annotations: Include class/slot annotations in shapes

    Returns:
        SHACL shapes as Turtle/RDF string
    """
    parser = load_parser_from_graph(graph, graph_uris=graph_uris)
    return parser.to_shacl(
        filter_void_nodes=filter_void_nodes,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
        closed=closed,
        suffix=suffix,
        include_annotations=include_annotations,
    )


def export_schema_artifacts(
    void_graph: Graph,
    name: str,
    endpoint: str,
    output_dir: str | Path,
    tag: str = "discovered_remote",
    fmt: str = "all",
) -> dict[str, str]:
    """Write VoID / JSON-LD / RDF-config artefacts."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}

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
                filter_void_nodes=True,
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
            logger.debug("RDF-config export failed for %s: %s", name, exc)

    return written


# Mining functions


def discover_void_graphs(
    endpoint_url: str,
    graph_uris: str | list[str] | None = None,
    exclude_graphs: bool = False,
) -> dict[str, Any]:
    """Find VoID graphs at *endpoint_url*."""
    return VoidParser().discover_void_graphs(endpoint_url)


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
    two_phase: bool = True,
    report_path: str | None = None,
    filter_service_namespaces: bool = True,
    authors: list[dict[str, str]] | None = None,
) -> MinedSchema:
    """Mine RDF schema from a SPARQL endpoint using SELECT queries.

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
        two_phase=two_phase,
        report_path=report_path,
        filter_service_namespaces=filter_service_namespaces,
        authors=authors,
    )


# Metadata


def query_metadata(
    endpoint_url: str,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Query SPARQL endpoint for dataset metadata without full schema mining.

    Useful for metadata harvesting, catalog building, and pre-mining checks.
    Queries for license, publisher, creators, version, dates, etc. using
    multiple strategies across common metadata vocabularies (DCTERMS, DC,
    DCAT, PAV, VoID, OWL, FOAF, PROV).

    Args:
        endpoint_url: SPARQL endpoint URL
        timeout: Query timeout in seconds (default: 30)

    Returns:
        dict with metadata fields.
    """
    from .miner import SchemaMiner

    miner = SchemaMiner(endpoint_url=endpoint_url, timeout=timeout)
    return miner.query_dataset_metadata()


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
    output_dir: str | Path = ".",
    *,
    tag: str = "discovered_remote",
    void_uri_base: str | None = None,
    entry: SourceEntry | dict[str, Any] | None = None,
    fmt: str = "all",
) -> dict[str, Any]:
    """Discover VoID descriptions for one source and export artefacts."""
    result = discover_void_graphs(endpoint, exclude_graphs=False)
    partitions = result.get("partitions", [])

    if not partitions:
        return {
            "partitions_found": 0,
            "graphs_found": 0,
            "files": {},
        }

    base_uri = void_uri_base or resolve_void_uri_base(name, entry=entry)
    parser = VoidParser()
    void_graph = parser.build_void_graph_from_partitions(partitions, base_uri=base_uri)

    files = export_schema_artifacts(
        void_graph,
        name,
        endpoint,
        output_dir,
        tag=tag,
        fmt=fmt,
    )

    out = Path(output_dir)
    report = {
        "dataset": name,
        "endpoint": endpoint,
        "source": "discovered",
        "graphs_found": len(result.get("found_graphs", [])),
        "partitions_found": len(partitions),
    }
    report_path = out / f"{name}_{tag}_report.json"
    report_path.write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    files["report"] = str(report_path)

    return {
        "partitions_found": len(partitions),
        "graphs_found": len(result.get("found_graphs", [])),
        "files": files,
    }


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
