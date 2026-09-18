"""Retrieve published VoID from an endpoint and export its canonical schema."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdflib import Graph, URIRef

if TYPE_CHECKING:
    from rdfsolve.schema_models.void_schema import VoidSchema

logger = logging.getLogger(__name__)


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
    """Retrieve published VoID RDF and read it through the canonical VoID reader.

    Named graphs whose names contain 'void' are candidates, not proof of VoID.
    Every named graph and the default graph are inspected and their boundaries
    are kept. An explicit graph scope never expands. Failures raise.
    """
    from rdfsolve.schema_models._constants import SERVICE_NAMESPACE_PREFIXES
    from rdfsolve.schema_models.readers.void import void_graph_to_minedschema
    from rdfsolve.sparql_helper import SparqlHelper
    from rdfsolve.void_retrieval import discover_description

    scope = [graph_uris] if isinstance(graph_uris, str) else graph_uris
    with SparqlHelper(endpoint_url, timeout=timeout, max_retries=max_retries) as helper:
        dataset, found, default_graph = discover_description(
            helper,
            scope,
            batch_size=batch_size,
            graph_batch_size=graph_batch_size,
            max_pages=max_pages,
            excluded_prefixes=tuple(SERVICE_NAMESPACE_PREFIXES) if exclude_graphs else (),
        )
    graph = Graph()
    for context in dataset.contexts():
        graph += context
    schema = void_graph_to_minedschema(graph)
    if scope is not None:
        schema.about.graph_uris = scope
    partitions = [
        {
            "subjectClass": p.subject_class,
            "prop": p.property_uri,
            "objectClass": p.object_class,
            "objectDatatype": p.datatype,
            "count": p.count,
        }
        for p in schema.patterns
    ]
    logger.info("Retrieved %d VoID triples and %d patterns", len(graph), len(partitions))
    return {
        "has_void_descriptions": bool(len(graph)),
        "found_graphs": found,
        "total_graphs": len(found),
        "default_graph": default_graph,
        "partitions": partitions,
        "graph": graph,
        "rdf_dataset": dataset,
        "schema": schema,
        "state": "complete" if len(graph) else "empty",
    }


def discover_all_graphs(
    endpoint_url: str,
    *,
    include_counts: bool = False,
    timeout: float = 30.0,
    max_retries: int = 1,
    batch_size: int = 100,
    max_pages: int = 1000,
) -> dict[str, Any]:
    """List graph names in pages. Set include_counts=True for triple counts.

    A graph name is not evidence that it contains an ontology.
    Query failures raise. Omitted counts are None, not zero.
    """
    from rdfsolve.sparql_helper import SparqlHelper
    from rdfsolve.void_retrieval import discover_graph_names

    with SparqlHelper(endpoint_url, timeout=timeout, max_retries=max_retries) as helper:
        names = discover_graph_names(helper, batch_size=batch_size, max_pages=max_pages)
        graphs: list[dict[str, Any]] = [{"uri": uri, "count": None} for uri in names]
        if include_counts:
            for graph in graphs:
                query = (
                    "SELECT (COUNT(*) AS ?count) WHERE { GRAPH "
                    + URIRef(graph["uri"]).n3()
                    + " { ?s ?p ?o } }"
                )
                rows = helper.select(query, purpose="graph/count")["results"]["bindings"]
                graph["count"] = int(rows[0]["count"]["value"])
    return {
        "graphs": graphs,
        "total_graphs": len(graphs),
        "void_graphs": [uri for uri in names if "void" in uri.lower()],
    }


def export_schema_artifacts(
    void_graph: Graph,
    name: str,
    endpoint: str,
    output_dir: str | Path,
    tag: str = "discovered_remote",
    fmt: str = "all",
) -> dict[str, str]:
    """Write the retrieved VoID RDF and the canonical schema read from it."""
    from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

    if fmt not in {"void", "jsonld", "all"}:
        raise ValueError("fmt must be void, jsonld, or all")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}
    schema = void_graph_to_minedschema(void_graph)
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
        jsonld_path = out / f"{name}_{tag}_schema.jsonld"
        jsonld_path.write_text(json.dumps(schema.to_jsonld(), indent=2) + "\n", encoding="utf-8")
        written["schema_jsonld"] = str(jsonld_path)

    if fmt == "all":
        try:
            rdfconfig = schema.to_rdfconfig(endpoint_url=endpoint, endpoint_name=name)
            config_dir = out / f"{name}_{tag}_config"
            config_dir.mkdir(parents=True, exist_ok=True)
            for fname, content in rdfconfig.items():
                (config_dir / f"{fname}.yaml").write_text(content, encoding="utf-8")
            written["rdfconfig_dir"] = str(config_dir)
        except Exception as exc:
            logger.warning("RDF-config export failed for %s: %s", name, exc)

    return written


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
            graph_store_url,
            scopes,
            graph_store_dir,
            max_bytes=graph_store_max_bytes,
            timeout=timeout,
        )
        dataset = load_downloads(downloads, endpoint_url=endpoint, timeout=timeout)
        graph = Graph()
        for uri in scopes:
            graph += dataset.graph(uri)
        document = VoidSchema(graph, endpoint, name, scopes, rdf_dataset=dataset)
    else:
        result = discover_void_graphs(
            endpoint,
            graph_uris=scopes,
            timeout=timeout,
            max_retries=max_retries,
            batch_size=batch_size,
            graph_batch_size=graph_batch_size,
            max_pages=max_pages,
        )
        document = VoidSchema(
            result["graph"],
            endpoint,
            name,
            result["found_graphs"],
            result["default_graph"],
            rdf_dataset=result["rdf_dataset"],
        )
    if output_dir is not None:
        document.files = export_schema_artifacts(
            document.graph,
            name,
            endpoint,
            output_dir,
            tag=tag,
            fmt=fmt,
        )
        dataset_path = Path(output_dir) / f"{name}_{tag}_dataset.trig"
        dataset_path.write_text(document.to_trig(), encoding="utf-8")
        document.files["dataset_trig"] = str(dataset_path)
    return document
