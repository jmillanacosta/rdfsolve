"""Core mapping models: MappingEdge, InstanceMatchResult, Mapping.

Base class and helpers for all mapping types.
"""

from __future__ import annotations

import json as _json
import logging
from collections import Counter
from collections.abc import Callable, Collection, Iterable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from rdfsolve._uri import (
    _build_br_prefix_map,
    make_expander,
    uri_to_curie,
)
from rdfsolve.schema_models._constants import (
    _GRAPH_SKIP_KEYS,
)
from rdfsolve.schema_models.core import (
    AboutMetadata,
    _merge_into_list,
)

_log = logging.getLogger(__name__)

SKOS_NARROW_MATCH = "http://www.w3.org/2004/02/skos/core#narrowMatch"


# -------------------------------------------------------------------
# Data models
# -------------------------------------------------------------------


class MappingEdge(BaseModel):
    """A single mapping edge between two classes."""

    source_class: str = Field(
        ...,
        description="URI of the source class",
    )
    target_class: str = Field(
        ...,
        description="URI of the target class",
    )
    predicate: str = Field(
        SKOS_NARROW_MATCH,
        description=("Mapping predicate URI (default: skos:narrowMatch)"),
    )
    source_dataset: str = Field(
        ...,
        description=("Dataset name where source_class lives"),
    )
    target_dataset: str = Field(
        ...,
        description=("Dataset name where target_class lives"),
    )
    source_endpoint: str | None = Field(None)
    target_endpoint: str | None = Field(None)
    confidence: float | None = Field(
        None,
        ge=0,
        le=1,
        description="Optional match confidence score 0-1",
    )


class InstanceMatchResult(BaseModel):
    """Raw result of probing one URI format against one endpoint."""

    dataset_name: str = Field(
        ...,
        description="Dataset name",
    )
    endpoint_url: str = Field(
        ...,
        description="SPARQL endpoint URL",
    )
    uri_format: str = Field(
        ...,
        description="URI prefix that was probed",
    )
    matched_class: str | None = Field(
        None,
        description=("Class URI returned by the endpoint for this pattern; None if no match"),
    )


class Mapping(BaseModel):
    """Container for a set of mapping edges with provenance.

    Base class for all mapping types.
    """

    edges: list[MappingEdge] = Field(default_factory=list)
    about: AboutMetadata = Field(...)
    mapping_type: str = Field(
        "unknown",
        description="Mapping strategy identifier",
    )

    # ---- JSON-LD import ------------------------------------

    @classmethod
    def from_jsonld(cls, path: str | Path) -> Mapping:
        """Reconstruct from a mapping JSON-LD file.

        Inverse of :meth:`to_jsonld`.  Expands CURIEs using the
        file's own ``@context`` block.
        """
        br_map = _build_br_prefix_map()
        raw = _json.loads(
            Path(path).read_text(encoding="utf-8"),
        )
        context: dict[str, str] = raw.get("@context", {})
        about_data = raw.get("@about", {})

        # Legacy SSSOM files store curie_map in @about
        curie_map: dict[str, str] = about_data.get("curie_map") or {}
        merged = {**curie_map, **context} if curie_map else context
        expand = make_expander(merged, br_map)

        edges = _parse_mapping_graph(
            raw.get("@graph", []),
            expand,
        )
        about = AboutMetadata.model_validate(about_data)
        strategy = about_data.get("strategy", "unknown")
        return cls(
            edges=edges,
            about=about,
            mapping_type=strategy,
        )

    # ---- NetworkX export -----------------------------------

    def to_networkx(self) -> Any:
        """Export the mapping as an ``nx.MultiDiGraph``."""
        try:
            import networkx as _nx
        except ImportError as exc:
            raise ImportError(
                "networkx is required for to_networkx(); install it with: pip install networkx",
            ) from exc

        graph: Any = _nx.MultiDiGraph()
        for edge in self.edges:
            for uri, ds in (
                (edge.source_class, edge.source_dataset),
                (edge.target_class, edge.target_dataset),
            ):
                if uri not in graph:
                    graph.add_node(uri, dataset=ds)
            graph.add_edge(
                edge.source_class,
                edge.target_class,
                predicate=edge.predicate,
                source_dataset=edge.source_dataset,
                target_dataset=edge.target_dataset,
                strategy=self.mapping_type,
                confidence=edge.confidence,
            )
        return graph

    # ---- Dataset-level graph export ------------------------

    @classmethod
    def dataset_graph(
        cls,
        paths: Iterable[str | Path],
        class_to_datasets: dict[str, set[str]],
        *,
        base_graph: Any | None = None,
        strategies: Collection[str] | None = None,
    ) -> Any:
        """Stream mapping files into a weighted dataset-pair graph.

        For every mapping edge whose both endpoint classes appear
        in *class_to_datasets*, increment the weight of the
        ``(dataset_a, dataset_b)`` pair in the output graph.
        """
        try:
            import networkx as _nx
        except ImportError as exc:
            raise ImportError(
                "networkx is required for dataset_graph(); install it with: pip install networkx",
            ) from exc

        # ujson is ~3-5x faster for large files
        try:
            import ujson as _fast_json
        except ImportError:
            _fast_json = None  # type: ignore[assignment]
        fast_json = _fast_json if _fast_json is not None else _json

        br_map = _build_br_prefix_map()
        skip_keys = frozenset(
            {
                "void:inDataset",
                "dcterms:created",
            }
        )
        weights: Counter[tuple[str, str]] = Counter()

        for p in paths:
            _process_mapping_file(
                p,
                fast_json,
                br_map,
                skip_keys,
                class_to_datasets,
                strategies,
                weights,
            )

        graph: Any = base_graph if base_graph is not None else _nx.Graph()
        for (a, b), w in weights.items():
            if graph.has_edge(a, b):
                graph[a][b]["weight"] += w
            else:
                graph.add_edge(a, b, weight=w)
        return graph

    # ---- JSON-LD export ------------------------------------

    def to_jsonld(self) -> dict[str, Any]:
        """Export as JSON-LD with @context, @graph, @about.

        Edges are grouped by source_class.
        """
        context: dict[str, str] = {
            "skos": ("http://www.w3.org/2004/02/skos/core#"),
            "rdfsolve": "https://w3id.org/rdfsolve/",
            "void": "http://rdfs.org/ns/void#",
            "dcterms": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "sd": ("http://www.w3.org/ns/sparql-service-description#"),
        }
        grouped: dict[str, dict[str, Any]] = {}
        created_at = self.about.generated_at

        for edge in self.edges:
            sc, sc_pfx, sc_ns = uri_to_curie(
                edge.source_class,
            )
            tc, tc_pfx, tc_ns = uri_to_curie(
                edge.target_class,
            )
            pp, pp_pfx, pp_ns = uri_to_curie(
                edge.predicate,
            )
            for pfx, ns in (
                (sc_pfx, sc_ns),
                (tc_pfx, tc_ns),
                (pp_pfx, pp_ns),
            ):
                if pfx and ns:
                    context.setdefault(pfx, ns)

            tgt_obj: dict[str, Any] = {
                "@id": tc,
                "void:inDataset": _dataset_node(
                    edge.target_dataset,
                    edge.target_endpoint,
                ),
            }
            if edge.confidence is not None:
                tgt_obj["rdfsolve:confidence"] = edge.confidence

            if sc not in grouped:
                grouped[sc] = {
                    "@id": sc,
                    "void:inDataset": _dataset_node(
                        edge.source_dataset,
                        edge.source_endpoint,
                    ),
                    "dcterms:created": created_at,
                }

            _merge_into_list(grouped, sc, pp, tgt_obj)

        return {
            "@context": context,
            "@graph": list(grouped.values()),
            "@about": self.about.model_dump(
                exclude_none=True,
            ),
        }


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _dataset_node(
    name: str,
    homepage: str | None = None,
) -> dict[str, Any]:
    """Build a void:inDataset node dict."""
    node: dict[str, Any] = {
        "@id": f"rdfsolve:dataset/{name}",
        "dcterms:title": name,
    }
    if homepage:
        node["foaf:homepage"] = {"@id": homepage}
    return node


def _parse_mapping_graph(
    graph_nodes: list[Any],
    expand: Callable[[str], str],
) -> list[MappingEdge]:
    """Parse @graph nodes from mapping JSON-LD."""
    edges: list[MappingEdge] = []
    for node in graph_nodes:
        src_id = node.get("@id", "")
        if not src_id:
            continue
        src_uri = expand(src_id)
        src_ds_node = node.get("void:inDataset") or {}
        src_ds = src_ds_node.get("dcterms:title", "")
        src_ep_raw = src_ds_node.get("void:sparqlEndpoint") or {}
        src_ep = src_ep_raw.get("@id") if isinstance(src_ep_raw, dict) else None

        for key, val in node.items():
            if key.startswith("@") or key in (_GRAPH_SKIP_KEYS):
                continue
            pred_uri = expand(key)
            targets = val if isinstance(val, list) else [val]
            for tgt in targets:
                if not isinstance(tgt, dict) or not tgt.get(
                    "@id",
                ):
                    continue
                edge = _parse_mapping_target(
                    tgt,
                    expand,
                    pred_uri,
                    src_uri,
                    src_ds,
                    src_ep,
                )
                if edge:
                    edges.append(edge)
    return edges


def _parse_mapping_target(
    tgt: dict[str, Any],
    expand: Callable[[str], str],
    pred_uri: str,
    src_uri: str,
    src_ds: str,
    src_ep: str | None,
) -> MappingEdge | None:
    """Parse one target dict into a MappingEdge or None."""
    tgt_uri = expand(tgt["@id"])
    tgt_ds_node = tgt.get("void:inDataset") or {}
    tgt_ds = tgt_ds_node.get("dcterms:title", "") or src_ds
    tgt_ep_raw = tgt_ds_node.get("void:sparqlEndpoint") or {}
    tgt_ep = tgt_ep_raw.get("@id") if isinstance(tgt_ep_raw, dict) else None
    confidence = tgt.get("rdfsolve:confidence")
    try:
        return MappingEdge(
            source_class=src_uri,
            target_class=tgt_uri,
            predicate=pred_uri,
            source_dataset=src_ds,
            target_dataset=tgt_ds,
            source_endpoint=src_ep,
            target_endpoint=tgt_ep,
            confidence=(float(confidence) if confidence is not None else None),
        )
    except Exception:
        _log.debug(
            "Skipping invalid mapping edge",
            exc_info=True,
        )
        return None


def _process_mapping_file(
    path: str | Path,
    json_mod: Any,
    br_map: dict[str, str],
    skip_keys: frozenset[str],
    class_to_datasets: dict[str, set[str]],
    strategies: Collection[str] | None,
    weights: Counter[tuple[str, str]],
) -> None:
    """Process one mapping file, accumulating weights."""
    try:
        raw = json_mod.loads(Path(path).read_bytes())
    except Exception:
        _log.debug(
            "Could not read %s",
            path,
            exc_info=True,
        )
        return

    about: dict[str, Any] = raw.get("@about", {})
    strategy: str = about.get("strategy", "unknown")
    if strategies is not None and strategy not in strategies:
        return

    context: dict[str, str] = raw.get("@context", {})
    curie_map: dict[str, str] = about.get("curie_map") or {}
    merged = {**curie_map, **context} if curie_map else context
    expand = make_expander(merged, br_map)

    for node in raw.get("@graph", ()):
        src_id: str = node.get("@id", "")
        if not src_id:
            continue
        src_cls = expand(src_id)
        src_datasets = class_to_datasets.get(src_cls)
        if not src_datasets:
            continue
        _accumulate_node_weights(
            node,
            expand,
            skip_keys,
            src_datasets,
            class_to_datasets,
            weights,
        )


def _accumulate_node_weights(
    node: dict[str, Any],
    expand: Callable[[str], str],
    skip_keys: frozenset[str],
    src_datasets: set[str],
    class_to_datasets: dict[str, set[str]],
    weights: Counter[tuple[str, str]],
) -> None:
    """Accumulate dataset-pair weights from one @graph node."""
    for key, val in node.items():
        if key[0] == "@" or key in skip_keys:
            continue
        targets = val if isinstance(val, list) else (val,)
        for tgt in targets:
            if not isinstance(tgt, dict):
                continue
            tgt_id: str = tgt.get("@id", "")
            if not tgt_id:
                continue
            tgt_cls = expand(tgt_id)
            tgt_datasets = class_to_datasets.get(tgt_cls)
            if not tgt_datasets:
                continue
            for sd in src_datasets:
                for td in tgt_datasets:
                    if sd != td:
                        pair = (sd, td) if sd < td else (td, sd)
                        weights[pair] += 1
