"""Class index builder for instance-to-class mapping derivation.

For each entity IRI in a mapping set, expands it to all known URI forms
via bioregistry, then queries the LSLOD QLever endpoint to discover
which RDF classes that entity belongs to and in which named graphs.

The ClassIndex is the central artefact consumed by the derivation engine
(class_derivation.py) and written into enriched instance JSON-LD files.
"""

from __future__ import annotations

import json
import logging
import statistics
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass  # SparqlHelper imported at call-site to avoid circular imports

logger = logging.getLogger(__name__)

__all__ = [
    "ClassIndex",
    "EntityClassInfo",
    "build_class_index",
    "build_class_index_from_endpoints",
    "build_class_index_from_ports",
    "enrich_jsonld_with_classes",
    "expand_iri_alternatives",
    "load_class_index",
    "save_class_index",
]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class EntityClassInfo:
    """Class membership info for a single entity across named graphs.

    Attributes:
        entity_iri: The canonical entity IRI (as it appears in the mapping).
        alternative_iris: All URI forms queried (from bioregistry).
        graph_classes: {graph_uri: [class_uri, ...]} — which classes
            this entity is typed as, in which LSLOD named graphs.
    """

    entity_iri: str
    alternative_iris: list[str] = field(default_factory=list)
    graph_classes: dict[str, list[str]] = field(default_factory=dict)

    def all_classes(self) -> list[str]:
        """Return flat list of all class URIs across all graphs."""
        return [c for cs in self.graph_classes.values() for c in cs]

    def graphs(self) -> list[str]:
        """Return graph URIs where this entity was found."""
        return list(self.graph_classes.keys())

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict for JSON caching."""
        return {
            "entity_iri": self.entity_iri,
            "alternative_iris": self.alternative_iris,
            "graph_classes": self.graph_classes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EntityClassInfo:
        """Deserialise from a plain dict."""
        return cls(
            entity_iri=data["entity_iri"],
            alternative_iris=data.get("alternative_iris", []),
            graph_classes=data.get("graph_classes", {}),
        )


@dataclass
class ClassIndex:
    """Index of class memberships for all entities in a mapping set.

    Attributes:
        entities: {canonical_iri: EntityClassInfo}
        endpoint_url: The SPARQL endpoint used for queries.
    """

    entities: dict[str, EntityClassInfo] = field(default_factory=dict)
    endpoint_url: str = ""

    def classes_for_entity(self, iri: str) -> dict[str, list[str]]:
        """Return {graph: [classes]} for an entity IRI.

        Returns an empty dict if the entity was not found.
        """
        info = self.entities.get(iri)
        return dict(info.graph_classes) if info else {}

    def entity_found(self, iri: str) -> bool:
        """Return True if the entity was found in at least one graph."""
        info = self.entities.get(iri)
        return bool(info and info.graph_classes)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict for JSON caching."""
        return {
            "endpoint_url": self.endpoint_url,
            "entities": {iri: info.to_dict() for iri, info in self.entities.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ClassIndex:
        """Deserialise from a plain dict."""
        return cls(
            endpoint_url=data.get("endpoint_url", ""),
            entities={
                iri: EntityClassInfo.from_dict(info)
                for iri, info in data.get("entities", {}).items()
            },
        )


# ---------------------------------------------------------------------------
# IRI expansion
# ---------------------------------------------------------------------------


def expand_iri_alternatives(iri: str) -> list[str]:
    """Return all known URI forms for an entity IRI using bioregistry.

    Given ``https://identifiers.org/ensembl/ENSG00000139618``, returns
    all registered URI prefix forms for the ``ensembl`` resource,
    each concatenated with ``ENSG00000139618``.

    Falls back to ``[iri]`` if bioregistry cannot parse the IRI.

    Args:
        iri: Full IRI (not a bare CURIE — must start with http/https).

    Returns:
        Deduplicated, sorted list of alternative full IRIs.
        Always includes the input IRI.
    """
    try:
        import bioregistry
    except ImportError as exc:
        raise ImportError(
            "bioregistry is required for IRI expansion. Install with: pip install bioregistry"
        ) from exc

    parsed = bioregistry.parse_iri(iri)
    if not parsed or parsed[0] is None:
        return [iri]

    prefix, identifier = parsed
    resource = bioregistry.get_resource(prefix)
    if resource is None:
        return [iri]

    uri_prefixes = resource.get_uri_prefixes() or set()
    alternatives: set[str] = {iri}
    for uri_pfx in uri_prefixes:
        if uri_pfx:
            alternatives.add(uri_pfx + identifier)

    return sorted(alternatives)


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------


def save_class_index(index: ClassIndex, path: str | Path) -> None:
    """Serialise a ClassIndex to a JSON file.

    Args:
        index: The ClassIndex to save.
        path: Output file path (will be created/overwritten).
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(index.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.debug("ClassIndex saved to %s (%d entities)", p, len(index.entities))


def load_class_index(path: str | Path) -> ClassIndex:
    """Deserialise a ClassIndex from a JSON cache file.

    Args:
        path: Path to a JSON file previously written by save_class_index().

    Returns:
        Populated ClassIndex.

    Raises:
        FileNotFoundError: If the cache file does not exist.
    """
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    index = ClassIndex.from_dict(data)
    logger.debug("ClassIndex loaded from %s (%d entities)", p, len(index.entities))
    return index


# ---------------------------------------------------------------------------
# Class index builder — private helpers
# ---------------------------------------------------------------------------


def _expand_all_entities(
    entity_iris: list[str],
    index: ClassIndex,
) -> dict[str, list[str]]:
    """Expand every IRI to alternatives and seed index.entities."""
    entity_to_alts: dict[str, list[str]] = {}
    for iri in entity_iris:
        alts = expand_iri_alternatives(iri)
        entity_to_alts[iri] = alts
        index.entities[iri] = EntityClassInfo(
            entity_iri=iri,
            alternative_iris=alts,
        )
    return entity_to_alts


def _collect_batch_alts(
    batch: list[str],
    entity_to_alts: dict[str, list[str]],
) -> list[str]:
    """Return flat list of all alternative IRIs for the entities in batch."""
    return [alt for iri in batch for alt in entity_to_alts[iri]]


def _build_alt_to_canonical(
    batch: list[str],
    entity_to_alts: dict[str, list[str]],
) -> dict[str, str]:
    """Return a mapping alt_iri -> canonical_iri for the entities in batch."""
    return {alt: iri for iri in batch for alt in entity_to_alts[iri]}


def _merge_batch_results(
    results: dict[str, dict[str, list[str]]],
    alt_to_canonical: dict[str, str],
    index: ClassIndex,
) -> None:
    """Merge SPARQL results back into the index via the alt->canonical map."""
    for found_iri, graph_classes in results.items():
        canonical = alt_to_canonical.get(found_iri)
        if canonical is None:
            continue
        info = index.entities[canonical]
        for graph_uri, classes in graph_classes.items():
            existing = info.graph_classes.setdefault(graph_uri, [])
            for cls in classes:
                if cls not in existing:
                    existing.append(cls)


def _build_cost_stats(
    n_iris: int,
    iri_alts_total: int,
    query_times: list[float],
    errors: int,
    batch_size: int,
) -> dict[str, Any]:
    """Assemble the cost_stats dict returned by build_class_index."""
    total_time = sum(query_times)
    n_queries = len(query_times)
    mean_time = total_time / n_queries if n_queries else 0.0
    max_time = max(query_times) if query_times else 0.0
    mean_alts = iri_alts_total / n_iris if n_iris else 0.0
    return {
        "iris_total": n_iris,
        "iri_alternatives_total": iri_alts_total,
        "iri_alternatives_mean_per_entity": round(mean_alts, 2),
        "sparql_queries_sent": n_queries,
        "sparql_total_time_s": round(total_time, 3),
        "sparql_mean_time_per_query_s": round(mean_time, 3),
        "sparql_max_time_per_query_s": round(max_time, 3),
        "sparql_errors": errors,
        "batch_size": batch_size,
        "cache_hit": False,
    }


# ---------------------------------------------------------------------------
# Class index builder
# ---------------------------------------------------------------------------


def build_class_index(
    entity_iris: list[str],
    endpoint_url: str,
    *,
    batch_size: int = 50,
    timeout: float = 60.0,
) -> tuple[ClassIndex, dict[str, Any]]:
    """Build a ClassIndex by querying actual IRIs against a SPARQL endpoint.

    For each entity IRI:
    1. Expand to all alternative URI forms via bioregistry.
    2. Batch the alternative IRIs together with other entities' IRIs.
    3. Query the LSLOD QLever endpoint with VALUES-based SPARQL.
    4. Record which graphs contain the entity and its rdf:type classes.

    Args:
        entity_iris: Canonical entity IRIs from the mapping set.
        endpoint_url: SPARQL endpoint (typically LSLOD QLever).
        batch_size: Number of entities (not IRI forms) per SPARQL query.
        timeout: HTTP timeout per request in seconds.

    Returns:
        Tuple of (ClassIndex, cost_stats).
        cost_stats keys:
            iris_total, iri_alternatives_total,
            iri_alternatives_mean_per_entity,
            sparql_queries_sent, sparql_total_time_s,
            sparql_mean_time_per_query_s, sparql_max_time_per_query_s,
            sparql_errors, batch_size, cache_hit.
    """
    from rdfsolve.sparql_helper import SparqlHelper

    index = ClassIndex(endpoint_url=endpoint_url)
    sparql = SparqlHelper(endpoint_url, timeout=timeout)

    entity_to_alts = _expand_all_entities(entity_iris, index)
    iri_alternatives_total = sum(len(v) for v in entity_to_alts.values())
    logger.info(
        "Class index: %d entities, %d total IRI forms",
        len(entity_iris),
        iri_alternatives_total,
    )

    entities_list = list(entity_iris)
    query_times: list[float] = []
    errors = 0

    for batch_start in range(0, len(entities_list), batch_size):
        batch = entities_list[batch_start : batch_start + batch_size]
        all_alts = _collect_batch_alts(batch, entity_to_alts)
        alt_to_canonical = _build_alt_to_canonical(batch, entity_to_alts)

        t0 = time.perf_counter()
        try:
            results = sparql.find_classes_for_iris_by_graph(all_alts)
        except Exception as exc:
            logger.warning(
                "Class index batch query failed (batch %d-%d): %s",
                batch_start,
                batch_start + len(batch) - 1,
                exc,
            )
            errors += 1
            query_times.append(time.perf_counter() - t0)
            continue

        query_times.append(time.perf_counter() - t0)
        _merge_batch_results(results, alt_to_canonical, index)

    cost_stats = _build_cost_stats(
        len(entity_iris),
        iri_alternatives_total,
        query_times,
        errors,
        batch_size,
    )

    found = sum(1 for e in index.entities.values() if e.graph_classes)
    logger.info(
        "Class index built: %d/%d entities found in LSLOD (%d queries, %.1fs total)",
        found,
        len(entity_iris),
        len(query_times),
        sum(query_times),
    )
    return index, cost_stats


# ---------------------------------------------------------------------------
# JSON-LD enrichment — private helpers
# ---------------------------------------------------------------------------

_RDFSOLVE_NS = "https://w3id.org/rdfsolve/"

# Keys that must not be descended into during nested-node enrichment
_ENRICH_SKIP_KEYS: frozenset[str] = frozenset(
    {"void:inDataset", "dcterms:created", "rdfsolve:classifiedIn"}
)


def _extract_not_found_prefix(node_id: str) -> str:
    """Return the bioregistry prefix for a node IRI, or ``'<unknown>'``."""
    try:
        import bioregistry

        parsed = bioregistry.parse_iri(node_id)
        if parsed and parsed[0] is not None:
            return str(parsed[0])
    except Exception as e:
        logger.debug("bioregistry.parse_iri failed for %r: %s", node_id, e)
    return "<unknown>"


def _build_classified_in(
    graph_classes: dict[str, list[str]],
    all_graphs: set[str],
    all_classes: set[str],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Build ``@type`` list and ``classifiedIn`` provenance for one entity.

    Side-effects: updates *all_graphs* and *all_classes* in place.

    Returns:
        Tuple of (all_type_uris, classified_in_list).
    """
    all_type_uris: list[str] = []
    classified_in: list[dict[str, Any]] = []
    for graph_uri, classes in sorted(graph_classes.items()):
        all_graphs.add(graph_uri)
        classified_in.append(
            {
                "void:inDataset": {"@id": graph_uri},
                "@type": sorted(set(classes)),
            }
        )
        for cls in classes:
            all_classes.add(cls)
            if cls not in all_type_uris:
                all_type_uris.append(cls)
    return all_type_uris, classified_in


def _iter_nested_id_nodes(
    node: dict[str, Any],
) -> list[dict[str, Any]]:
    """Yield nested dicts that have an ``@id``, skipping metadata keys."""
    nested: list[dict[str, Any]] = []
    for key, value in node.items():
        if key.startswith("@") or key in _ENRICH_SKIP_KEYS:
            continue
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and "@id" in item:
                    nested.append(item)
        elif isinstance(value, dict) and "@id" in value:
            nested.append(value)
    return nested


# ---------------------------------------------------------------------------
# JSON-LD enrichment
# ---------------------------------------------------------------------------


def enrich_jsonld_with_classes(
    jsonld_doc: dict[str, Any],
    class_index: ClassIndex,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Add @type and rdfsolve:classifiedIn to each entity node.

    Enriches each entity node in the JSON-LD document's ``@graph``
    with class membership information from the ClassIndex.
    Each entity node (subject or object in ``@graph``) that has a
    matching entry in *class_index* gains:

    - ``@type``: flat list of all discovered class URIs across all graphs.
    - ``rdfsolve:classifiedIn``: list of
      ``{"void:inDataset": {"@id": graph_uri}, "@type": [classes]}``
      records showing per-graph provenance.

    The original ``void:inDataset`` from the mapping source is kept
    unchanged.

    Args:
        jsonld_doc: Parsed JSON-LD document (modified in place).
        class_index: Populated ClassIndex from build_class_index().

    Returns:
        Tuple of (mutated_doc, enrichment_stats).
        enrichment_stats keys:
            entities_total, entities_enriched, entities_not_found,
            entities_not_found_pct, classes_added, distinct_classes,
            graphs_referenced, classes_per_entity_mean,
            classes_per_entity_max, not_found_iris, not_found_prefixes.
    """
    # Ensure rdfsolve prefix is in @context
    context: dict[str, Any] = jsonld_doc.setdefault("@context", {})
    context.setdefault("rdfsolve", _RDFSOLVE_NS)

    graph: list[dict[str, Any]] = jsonld_doc.get("@graph", [])

    entities_total = 0
    entities_enriched = 0
    classes_added_total = 0
    classes_per_entity: list[int] = []
    all_graphs: set[str] = set()
    all_classes: set[str] = set()
    not_found_iris: list[str] = []
    not_found_prefix_counter: Counter[str] = Counter()

    def _enrich_node(node: dict[str, Any]) -> None:
        nonlocal entities_total, entities_enriched, classes_added_total

        node_id: str = node.get("@id", "")
        if not node_id:
            return

        entities_total += 1
        graph_classes = class_index.classes_for_entity(node_id)

        if not graph_classes:
            not_found_iris.append(node_id)
            not_found_prefix_counter[_extract_not_found_prefix(node_id)] += 1
            return

        all_type_uris, classified_in = _build_classified_in(graph_classes, all_graphs, all_classes)
        n_classes = len(all_type_uris)
        node["@type"] = sorted(set(all_type_uris))
        node["rdfsolve:classifiedIn"] = classified_in

        entities_enriched += 1
        classes_added_total += n_classes
        classes_per_entity.append(n_classes)

    # Walk the top-level @graph nodes and their nested subject/object nodes
    for node in graph:
        _enrich_node(node)
        for nested in _iter_nested_id_nodes(node):
            _enrich_node(nested)

    entities_not_found = entities_total - entities_enriched
    not_found_pct = round(100.0 * entities_not_found / entities_total, 1) if entities_total else 0.0
    mean_classes = round(statistics.mean(classes_per_entity), 2) if classes_per_entity else 0.0
    max_classes = max(classes_per_entity) if classes_per_entity else 0

    stats: dict[str, Any] = {
        "entities_total": entities_total,
        "entities_enriched": entities_enriched,
        "entities_not_found": entities_not_found,
        "entities_not_found_pct": not_found_pct,
        "classes_added": classes_added_total,
        "distinct_classes": len(all_classes),
        "graphs_referenced": len(all_graphs),
        "classes_per_entity_mean": mean_classes,
        "classes_per_entity_max": max_classes,
        "not_found_iris": not_found_iris,
        "not_found_prefixes": dict(not_found_prefix_counter.most_common()),
    }
    return jsonld_doc, stats


def build_class_index_from_endpoints(
    entity_iris: list[str],
    endpoint_url: str,
    *,
    batch_size: int = 50,
    timeout: float = 60.0,
    cache_path: str | None = None,
) -> tuple[ClassIndex, dict[str, Any]]:
    """Build (or load) a :class:`ClassIndex`, with optional disk cache.

    Queries *endpoint_url* for the RDF classes of every IRI in
    *entity_iris*.  When *cache_path* is given and the file already
    exists, the index is loaded from disk and no network calls are made.

    Args:
        entity_iris: List of entity IRIs to look up.
        endpoint_url: QLever (or SPARQL 1.1) endpoint URL.
        batch_size: Number of IRIs sent per VALUES query (default 50).
        timeout: Per-request timeout in seconds (default 60.0).
        cache_path: Optional path to read/write a cached index JSON.

    Returns:
        ``(class_index, cost_stats)`` where *cost_stats* has keys
        ``"queries"``, ``"found"``, ``"not_found"``, ``"elapsed_s"``.
    """
    if cache_path is not None:
        p = Path(cache_path)
        if p.exists():
            idx = load_class_index(cache_path)
            cost: dict[str, Any] = {
                "queries": 0,
                "found": len(idx.entities),
                "not_found": 0,
                "elapsed_s": 0.0,
                "cached": True,
            }
            return idx, cost

    idx, cost = build_class_index(
        entity_iris,
        endpoint_url,
        batch_size=batch_size,
        timeout=timeout,
    )

    if cache_path is not None:
        save_class_index(idx, cache_path)

    return idx, cost


def build_class_index_from_ports(
    entity_iris: list[str],
    ports_json_path: str,
    *,
    batch_size: int = 50,
    timeout: float = 60.0,
    cache_path: str | None = None,
) -> tuple[ClassIndex, dict[str, Any]]:
    """Build a :class:`ClassIndex` by querying **all** per-dataset QLever instances.

    Each dataset in *ports_json_path* is queried individually using
    :meth:`~rdfsolve.sparql_helper.SparqlHelper.find_classes_for_iris`
    (default-graph ``?s a ?c``).  The dataset name is used as the
    "graph" identifier in the resulting index, so downstream code that
    groups evidence by graph works unchanged.

    Args:
        entity_iris: Canonical entity IRIs from the mapping set.
        ports_json_path: Path to the ``ports.json`` file mapping
            ``{dataset_name: port}``.
        batch_size: Entities per VALUES query (default 50).
        timeout: Per-request timeout in seconds (default 60.0).
        cache_path: Optional path to read/write a cached index JSON.

    Returns:
        ``(class_index, cost_stats)`` — same shape as
        :func:`build_class_index_from_endpoints`.
    """
    if cache_path is not None:
        p = Path(cache_path)
        if p.exists():
            idx = load_class_index(cache_path)
            cost: dict[str, Any] = {
                "queries": 0,
                "found": len(idx.entities),
                "not_found": 0,
                "elapsed_s": 0.0,
                "cached": True,
            }
            return idx, cost

    import json as _json

    from rdfsolve.sparql_helper import SparqlHelper

    with open(ports_json_path) as fh:
        ports_map: dict[str, int] = _json.load(fh)

    idx = ClassIndex(endpoint_url=f"ports:{ports_json_path}")
    entity_to_alts = _expand_all_entities(entity_iris, idx)
    iri_alternatives_total = sum(len(v) for v in entity_to_alts.values())
    logger.info(
        "Class index (multi-endpoint): %d entities, %d IRI forms, %d endpoints",
        len(entity_iris),
        iri_alternatives_total,
        len(ports_map),
    )

    entities_list = list(entity_iris)
    query_times: list[float] = []
    errors = 0

    for ds_name, port in ports_map.items():
        endpoint_url = f"http://localhost:{port}"
        sparql = SparqlHelper(endpoint_url, timeout=timeout)

        # Quick health-check; skip unreachable instances
        try:
            sparql.ask("ASK {}")
        except Exception:
            logger.debug("Skipping unreachable endpoint %s (%s)", ds_name, endpoint_url)
            continue

        for batch_start in range(0, len(entities_list), batch_size):
            batch = entities_list[batch_start : batch_start + batch_size]
            all_alts = _collect_batch_alts(batch, entity_to_alts)
            alt_to_canonical = _build_alt_to_canonical(batch, entity_to_alts)

            t0 = time.perf_counter()
            try:
                flat_results = sparql.find_classes_for_iris(all_alts)
            except Exception as exc:
                logger.warning(
                    "Class index batch failed on %s (batch %d-%d): %s",
                    ds_name,
                    batch_start,
                    batch_start + len(batch) - 1,
                    exc,
                )
                errors += 1
                query_times.append(time.perf_counter() - t0)
                continue

            query_times.append(time.perf_counter() - t0)

            # Convert flat {iri: [classes]} to graph-keyed form using
            # the dataset name as the "graph" identifier.
            graph_keyed: dict[str, dict[str, list[str]]] = {}
            for found_iri, classes in flat_results.items():
                graph_keyed[found_iri] = {ds_name: classes}

            _merge_batch_results(graph_keyed, alt_to_canonical, idx)

    cost_stats = _build_cost_stats(
        len(entity_iris),
        iri_alternatives_total,
        query_times,
        errors,
        batch_size,
    )

    found = sum(1 for e in idx.entities.values() if e.graph_classes)
    logger.info(
        "Class index (multi-endpoint) built: %d/%d entities found (%d queries, %.1fs)",
        found,
        len(entity_iris),
        len(query_times),
        sum(query_times),
    )

    if cache_path is not None:
        save_class_index(idx, cache_path)

    return idx, cost_stats
