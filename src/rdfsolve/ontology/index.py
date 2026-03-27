"""OntologyIndex: in-memory ontology knowledge base for rdfsolve grounding.

Builds a compressed index from OLS4 metadata + rdfsolve schema class URIs
that powers grounding tier 3 (OLS term lookup) and cross-ontology path
expansion in the PlanEngine.

The index contains:

``term_to_classes``
    Normalised term label / synonym string → list of class IRIs.  Used by
    the grounding engine to map a natural-language mention to one or more
    ontology class URIs.

``class_to_ontology``
    Class IRI → OLS4 ``ontologyId``.  Bridges rdfsolve schema class URIs
    to OLS4 ontology nodes.

``ancestors``
    Class IRI → list of ancestor class IRIs (nearest first).  Used for
    hierarchical query expansion (e.g. "chemical compound" subsumes
    "drug").

``ontology_graph``
    NetworkX ``DiGraph`` with one node per OLS4 ontology (``ontologyId``
    as node key; attributes: ``preferred_prefix``, ``base_uris``,
    ``domain``, ``n_classes``).  Edges represent ``importsFrom`` /
    ``exportsTo`` relationships.

``base_uri_to_ontology``
    URI prefix string → ``ontologyId``.  Derived from the OLS4
    ``baseUri`` field; allows matching rdfsolve schema class URIs (which
    often start with ``http://purl.obolibrary.org/obo/CHEBI_``) to the
    owning ontology.

Build::

    from rdfsolve.ontology.index import build_ontology_index, save_ontology_index

    idx = build_ontology_index(
        schema_class_uris={"http://purl.obolibrary.org/obo/CHEBI_15422", ...},
        cache_dir="/tmp/ols_cache",
    )
    save_ontology_index(idx, data_dir="data/")

Load at startup::

    from rdfsolve.ontology.index import load_ontology_index

    idx = load_ontology_index(data_dir="data/")
    classes = idx.lookup("aspirin")
"""

from __future__ import annotations

import gzip
import logging
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import networkx as nx

logger = logging.getLogger(__name__)

_INDEX_FILENAME = "ontology_index.pkl.gz"
_GRAPH_FILENAME = "ontology_graph.graphml"

__all__ = [
    "OntologyIndex",
    "build_ontology_index",
    "load_ontology_index",
    "load_ontology_index_from_db",
    "save_ontology_index",
    "save_ontology_index_to_db",
]


@dataclass
class OntologyIndex:
    """Compressed ontology knowledge base for rdfsolve grounding tier 3.

    Attributes
    ----------
    term_to_classes:
        Normalised term string → list of class IRIs that carry that label
        or synonym.
    class_to_ontology:
        Class IRI → OLS4 ``ontologyId``.
    ancestors:
        Class IRI → ordered list of ancestor class IRIs (nearest first).
    ontology_graph:
        NetworkX ``DiGraph`` of ontology import relationships.  Node key
        is ``ontologyId``; edge attribute ``rel`` is ``"imports"``.
    base_uri_to_ontology:
        URI prefix → ``ontologyId`` (from OLS4 ``baseUri`` field).
    """

    term_to_classes: dict[str, list[str]] = field(default_factory=dict)
    class_to_ontology: dict[str, str] = field(default_factory=dict)
    ancestors: dict[str, list[str]] = field(default_factory=dict)
    ontology_graph: Any = field(default_factory=lambda: None)  # nx.DiGraph
    base_uri_to_ontology: dict[str, str] = field(default_factory=dict)

    # ── query helpers ─────────────────────────────────────────────

    def lookup(self, term: str) -> list[str]:
        """Return class IRIs whose label or synonym matches *term*.

        The lookup is case-insensitive and strips leading/trailing
        whitespace.

        Parameters
        ----------
        term:
            Natural-language label or synonym string.

        Returns
        -------
        list[str]
            Class IRIs associated with *term*, or an empty list if none
            are found.
        """
        return self.term_to_classes.get(_normalise(term), [])

    def ontology_for_class(self, class_iri: str) -> str | None:
        """Return the OLS4 ``ontologyId`` that defines *class_iri*.

        Parameters
        ----------
        class_iri:
            Full class IRI.

        Returns
        -------
        str or None
            OLS4 ontology identifier, or ``None`` if not indexed.
        """
        return self.class_to_ontology.get(class_iri)

    def ontology_for_base_uri(self, uri: str) -> str | None:
        """Return the OLS4 ``ontologyId`` whose ``baseUri`` is a prefix of *uri*.

        Scans all registered base URIs and returns the ontology whose
        prefix is the longest match for *uri*.

        Parameters
        ----------
        uri:
            A class IRI or any URI string.

        Returns
        -------
        str or None
            Best-matching OLS4 ontology identifier, or ``None``.
        """
        best: str | None = None
        best_len = 0
        for prefix, ont_id in self.base_uri_to_ontology.items():
            if uri.startswith(prefix) and len(prefix) > best_len:
                best = ont_id
                best_len = len(prefix)
        return best

    def import_neighbours(self, ontology_id: str, depth: int = 1) -> set[str]:
        """Return ontology IDs reachable via ``importsFrom``/``exportsTo`` edges.

        Traverses the ontology graph up to *depth* hops (undirected)
        from *ontology_id*.

        Parameters
        ----------
        ontology_id:
            Starting OLS4 ontology identifier.
        depth:
            Maximum number of hops (default 1).

        Returns
        -------
        set[str]
            Set of reachable ontology IDs (excluding *ontology_id*
            itself).
        """
        if self.ontology_graph is None or ontology_id not in self.ontology_graph:
            return set()
        import networkx as nx

        subgraph = nx.ego_graph(
            self.ontology_graph.to_undirected(as_view=True),
            ontology_id,
            radius=depth,
        )
        return set(subgraph.nodes) - {ontology_id}

    def stats(self) -> dict[str, int]:
        """Return a summary of index sizes.

        Returns
        -------
        dict[str, int]
            Counts for ``terms``, ``classes``, ``ontologies``,
            ``base_uris``, ``with_ancestors``.
        """
        n_nodes = self.ontology_graph.number_of_nodes() if self.ontology_graph is not None else 0
        return {
            "terms": len(self.term_to_classes),
            "classes": len(self.class_to_ontology),
            "ontologies": n_nodes,
            "base_uris": len(self.base_uri_to_ontology),
            "with_ancestors": len(self.ancestors),
        }


# ── normalisation helper ──────────────────────────────────────────


def _normalise(text: str) -> str:
    """Lower-case and strip *text* for consistent term lookup."""
    return text.strip().lower()


# ── graph building ────────────────────────────────────────────────


def _build_ontology_graph(ontology_metas: list[dict[str, Any]]) -> Any:
    """Build a NetworkX DiGraph from OLS4 ontology metadata dicts.

    Parameters
    ----------
    ontology_metas:
        List of OLS4 ontology metadata objects as returned by
        :meth:`~rdfsolve.ontology.ols_client.OlsClient.get_all_ontologies`.

    Returns
    -------
    nx.DiGraph
        Directed graph with ``ontologyId`` nodes and ``importsFrom`` /
        ``exportsTo`` edges (attribute ``rel="imports"``).
    """
    import networkx as nx

    g: nx.DiGraph = nx.DiGraph()
    for ont in ontology_metas:
        nid = ont.get("ontologyId", "")
        if not nid:
            continue
        g.add_node(
            nid,
            preferred_prefix=ont.get("preferredPrefix", ""),
            base_uris=ont.get("baseUri", []),
            domain=ont.get("domain", ""),
            n_classes=int(ont.get("numberOfClasses") or 0),
        )
        for dep in ont.get("importsFrom", []):
            g.add_edge(nid, dep, rel="imports")
        for exp in ont.get("exportsTo", []):
            g.add_edge(exp, nid, rel="imports")
    return g


# ── index build ───────────────────────────────────────────────────


def build_ontology_index(
    schema_class_uris: set[str] | None = None,
    *,
    cache_dir: str | None = None,
    ontology_ids: list[str] | None = None,
) -> OntologyIndex:
    """Fetch OLS4 metadata and compile an :class:`OntologyIndex`.

    By default, all active OLS4 ontologies are considered.  Pass
    *schema_class_uris* to restrict term fetching to ontologies whose
    ``baseUri`` overlaps with the provided class IRIs — this keeps the
    index lean when only biomedical datasets are of interest.

    Parameters
    ----------
    schema_class_uris:
        Set of class IRIs seen in rdfsolve schemas.  When provided, only
        ontologies whose ``baseUri`` matches at least one URI are fully
        indexed (terms + ancestors).  Other ontologies contribute only to
        the connectivity graph and ``base_uri_to_ontology`` map.
    cache_dir:
        Directory for the :class:`~rdfsolve.ontology.ols_client.OlsClient`
        disk cache (``diskcache``).  ``None`` disables caching.
    ontology_ids:
        Explicit list of OLS4 ontology IDs to index.  When provided,
        *schema_class_uris* filtering is still applied but pagination of
        ``/ontologies`` is skipped in favour of direct lookups.  Useful
        for targeted rebuilds or testing.

    Returns
    -------
    OntologyIndex
        Populated index ready for grounding and path planning.
    """
    from rdfsolve.ontology.ols_client import OlsClient

    idx = OntologyIndex()

    with OlsClient(cache_dir=cache_dir) as client:
        ontology_metas = _fetch_ontology_metas(client, ontology_ids)
        logger.info("OntologyIndex: fetched metadata for %d ontologies", len(ontology_metas))

        idx.ontology_graph = _build_ontology_graph(ontology_metas)
        _populate_base_uri_map(idx, ontology_metas)

        relevant_ids = _resolve_relevant_ids(idx, ontology_metas, schema_class_uris)
        logger.info("OntologyIndex: indexing terms for %d relevant ontologies", len(relevant_ids))

        _index_relevant_ontologies(idx, ontology_metas, relevant_ids, schema_class_uris, client)

        logger.info(
            "OntologyIndex built: %d terms, %d classes",
            len(idx.term_to_classes),
            len(idx.class_to_ontology),
        )

    return idx


def _fetch_ontology_metas(client: Any, ontology_ids: list[str] | None) -> list[dict[str, Any]]:
    """Return a list of OLS4 ontology metadata dicts from *client*.

    When *ontology_ids* is provided, fetches each ontology individually;
    otherwise paginates through all active ontologies.
    """
    if ontology_ids is not None:
        metas: list[dict[str, Any]] = []
        for oid in ontology_ids:
            meta = client.get_ontology(oid)
            if meta:
                metas.append(meta)
        return metas
    return list(client.get_all_ontologies())


def _populate_base_uri_map(idx: OntologyIndex, ontology_metas: list[dict[str, Any]]) -> None:
    """Fill ``idx.base_uri_to_ontology`` from OLS4 ``baseUri`` fields."""
    for ont in ontology_metas:
        oid = ont.get("ontologyId", "")
        for base_uri in ont.get("baseUri", []):
            if base_uri:
                idx.base_uri_to_ontology[base_uri] = oid


def _resolve_relevant_ids(
    idx: OntologyIndex,
    ontology_metas: list[dict[str, Any]],
    schema_class_uris: set[str] | None,
) -> set[str]:
    """Determine which ontology IDs should have their terms fully indexed."""
    if schema_class_uris:
        relevant = _ontologies_matching_uris(schema_class_uris, idx.base_uri_to_ontology)
    else:
        relevant = {ont.get("ontologyId", "") for ont in ontology_metas}
    relevant.discard("")
    return relevant


def _index_relevant_ontologies(
    idx: OntologyIndex,
    ontology_metas: list[dict[str, Any]],
    relevant_ids: set[str],
    schema_class_uris: set[str] | None,
    client: Any,
) -> None:
    """Iterate over *ontology_metas* and index terms for relevant entries."""
    for ont in ontology_metas:
        oid = ont.get("ontologyId", "")
        if oid not in relevant_ids:
            continue

        label_props: list[str] = ont.get("label_property") or ["rdfs:label"]
        synonym_props: list[str] = ont.get("synonym_property") or []
        base_uris = [b for b in ont.get("baseUri", []) if b]
        class_uris_for_ont = _class_uris_for_ontology(schema_class_uris, base_uris)

        for class_iri in class_uris_for_ont:
            term = client.get_term_by_iri(oid, class_iri)
            if term is None:
                continue
            _index_term(idx, term, oid, client, label_props, synonym_props)

        if not class_uris_for_ont:
            _index_ontology_top_terms(idx, oid, client, ont)


def _class_uris_for_ontology(
    schema_class_uris: set[str] | None,
    base_uris: list[str],
) -> set[str]:
    """Return class URIs from *schema_class_uris* that belong to *base_uris*."""
    if not schema_class_uris or not base_uris:
        return set()
    return {cu for cu in schema_class_uris if any(cu.startswith(b) for b in base_uris)}


def _ontologies_matching_uris(
    class_uris: set[str],
    base_uri_to_ontology: dict[str, str],
) -> set[str]:
    """Return ontology IDs whose baseUri is a prefix of any class URI."""
    matched: set[str] = set()
    for base_uri, oid in base_uri_to_ontology.items():
        for cu in class_uris:
            if cu.startswith(base_uri):
                matched.add(oid)
                break
    return matched


def _index_term(
    idx: OntologyIndex,
    term: dict[str, Any],
    ontology_id: str,
    client: Any,
    label_props: list[str],
    synonym_props: list[str],
) -> None:
    """Add a single OLS4 term object to the index in place."""
    iri = term.get("iri") or term.get("@id", "")
    if not iri:
        return

    idx.class_to_ontology[iri] = ontology_id

    for lbl in _collect_labels(term, synonym_props):
        key = _normalise(lbl)
        if key:
            idx.term_to_classes.setdefault(key, [])
            if iri not in idx.term_to_classes[key]:
                idx.term_to_classes[key].append(iri)

    # Fetch ancestors (best-effort — skip on error)
    try:
        anc_terms = client.get_ancestors(ontology_id, iri)
        idx.ancestors[iri] = [a.get("iri", "") for a in anc_terms if a.get("iri")]
    except Exception as exc:
        logger.debug("Could not fetch ancestors for %s: %s", iri, exc)


def _collect_labels(term: dict[str, Any], synonym_props: list[str]) -> list[str]:
    """Return all label and synonym strings for an OLS4 *term* dict.

    Collects:

    * The primary ``label`` field.
    * Any configured synonym properties (*synonym_props*), falling back to
      ``["synonyms", "obo:hasExactSynonym", "oboInOwl:hasExactSynonym"]``.
    * The generic ``synonyms`` key as a safety net.
    """
    labels: list[str] = []

    label = term.get("label") or ""
    if isinstance(label, list):
        labels.extend(str(v) for v in label if v)
    elif label:
        labels.append(label)

    effective_props = synonym_props or [
        "synonyms",
        "obo:hasExactSynonym",
        "oboInOwl:hasExactSynonym",
    ]
    for prop in effective_props:
        syns = term.get(prop, [])
        if isinstance(syns, str):
            syns = [syns]
        labels.extend(syns)

    # Safety net: generic "synonyms" key (de-duplicated)
    for syn in term.get("synonyms", []):
        if syn not in labels:
            labels.append(syn)

    return labels


def _index_ontology_top_terms(
    idx: OntologyIndex,
    ontology_id: str,
    client: Any,
    ont_meta: dict[str, Any],
) -> None:
    """Index the first page of terms for an ontology without specific class URIs.

    Used when *schema_class_uris* is not provided, so we build a broad
    label catalogue across all active ontologies.
    """
    label_props: list[str] = ont_meta.get("label_property") or ["rdfs:label"]
    synonym_props: list[str] = ont_meta.get("synonym_property") or []
    # Paginate terms without a search filter (page_limit=1 keeps the build fast)
    for term in client.get_all_terms(ontology_id, page_limit=1):
        _index_term(idx, term, ontology_id, client, label_props, synonym_props)


# ── persistence ───────────────────────────────────────────────────


def save_ontology_index(
    index: OntologyIndex,
    data_dir: str | Path = "data",
) -> None:
    """Persist *index* to disk under *data_dir*.

    Writes two files:

    * ``ontology_index.pkl.gz`` — gzip-compressed pickle of all fields
      except ``ontology_graph``.
    * ``ontology_graph.graphml`` — the NetworkX ``DiGraph`` in GraphML
      format.

    Parameters
    ----------
    index:
        Populated :class:`OntologyIndex` to persist.
    data_dir:
        Output directory.  Created if absent.
    """
    import networkx as nx

    out = Path(data_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Pickle index fields (without the graph)
    payload = {
        "term_to_classes": index.term_to_classes,
        "class_to_ontology": index.class_to_ontology,
        "ancestors": index.ancestors,
        "base_uri_to_ontology": index.base_uri_to_ontology,
    }
    index_path = out / _INDEX_FILENAME
    with gzip.open(index_path, "wb") as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Saved ontology index → %s", index_path)

    # Write graph as GraphML — lists are not supported by GraphML, so convert
    # any list-valued node attributes to JSON strings before serialising.
    graph_path = out / _GRAPH_FILENAME
    if index.ontology_graph is not None:
        import json as _json

        safe_graph = nx.DiGraph()
        for node, attrs in index.ontology_graph.nodes(data=True):
            safe_attrs = {k: _json.dumps(v) if isinstance(v, list) else v for k, v in attrs.items()}
            safe_graph.add_node(node, **safe_attrs)
        for u, v, attrs in index.ontology_graph.edges(data=True):
            safe_graph.add_edge(u, v, **attrs)
        nx.write_graphml(safe_graph, str(graph_path))
        logger.info("Saved ontology graph → %s", graph_path)


def load_ontology_index(
    data_dir: str | Path = "data",
) -> OntologyIndex:
    """Load a previously saved :class:`OntologyIndex` from *data_dir*.

    Parameters
    ----------
    data_dir:
        Directory containing ``ontology_index.pkl.gz`` and
        ``ontology_graph.graphml`` as written by :func:`save_ontology_index`.

    Returns
    -------
    OntologyIndex
        Restored index.

    Raises
    ------
    FileNotFoundError
        If ``ontology_index.pkl.gz`` does not exist under *data_dir*.
    """
    import networkx as nx

    out = Path(data_dir)
    index_path = out / _INDEX_FILENAME
    if not index_path.exists():
        raise FileNotFoundError(
            f"Ontology index not found at {index_path}. Run build_ontology_index() first."
        )

    with gzip.open(index_path, "rb") as fh:
        payload: dict[str, Any] = pickle.load(fh)  # noqa: S301

    graph: Any = None
    graph_path = out / _GRAPH_FILENAME
    if graph_path.exists():
        graph = nx.read_graphml(str(graph_path))
    else:
        logger.warning("Ontology graph file not found at %s; graph will be None", graph_path)

    idx = OntologyIndex(
        term_to_classes=payload.get("term_to_classes", {}),
        class_to_ontology=payload.get("class_to_ontology", {}),
        ancestors=payload.get("ancestors", {}),
        base_uri_to_ontology=payload.get("base_uri_to_ontology", {}),
        ontology_graph=graph,
    )
    logger.info("Loaded ontology index: %s", idx.stats())
    return idx


# ── database-backed persistence ───────────────────────────────────


def save_ontology_index_to_db(index: OntologyIndex, db: Any) -> None:
    """Persist *index* to the rdfsolve SQLite database.

    Delegates to :meth:`~rdfsolve.backend.database.Database.save_ontology_index`.
    All existing ontology data in the database is replaced atomically.

    Parameters
    ----------
    index:
        Populated :class:`OntologyIndex` to persist.
    db:
        Open :class:`~rdfsolve.backend.database.Database` instance.
    """
    db.save_ontology_index(index)
    logger.info("Ontology index persisted to database (%s)", index.stats())


def load_ontology_index_from_db(db: Any) -> OntologyIndex:
    """Load an :class:`OntologyIndex` from the rdfsolve SQLite database.

    Delegates to :meth:`~rdfsolve.backend.database.Database.load_ontology_index`.

    Parameters
    ----------
    db:
        Open :class:`~rdfsolve.backend.database.Database` instance.

    Returns
    -------
    OntologyIndex
        Reconstructed index.

    Raises
    ------
    RuntimeError
        If the database contains no ontology data.
    """
    if not db.has_ontology_index():
        raise RuntimeError(
            "No ontology index found in database. "
            "Run build_ontology_index() and save_ontology_index_to_db() first."
        )
    idx: OntologyIndex = db.load_ontology_index()
    logger.info("Ontology index loaded from database (%s)", idx.stats())
    return idx
