"""Load data-source definitions from ``data/sources.yaml``.

The canonical source registry is a YAML file containing a flat list
of mappings, one per SPARQL data source.  Each mapping carries:

* **name** - unique human-readable identifier.
* **endpoint** - SPARQL endpoint URL.
* **graph_uris** - named graphs to query.
* **use_graph** - whether to wrap queries in a ``GRAPH`` clause.
* **two_phase** - use two-phase mining (default ``True``).
* Optional tuning knobs: *chunk_size*, *class_batch_size*,
  *class_chunk_size*, *timeout*, *delay*, *counts*, *unsafe_paging*.

Each entry can be enriched with Bioregistry metadata via
:func:`enrich_source_with_bioregistry`, which resolves the canonical
Bioregistry prefix for the underlying dataset (regardless of how rdfsolve
serialises or partitions it) and populates ``bioregistry_*`` fields.

The resolution strategy handles four cases:

1. **Exact match** — source ``name`` is itself a valid Bioregistry prefix
   (e.g. ``"chebi"``, ``"hgnc"``).
2. **Root-prefix match** — the first dot-separated segment of ``name``
   resolves (e.g. ``"drugbank.drugs"`` → ``"drugbank"``).
3. **local_provider field** — the entry declares ``local_provider`` which
   is itself a Bioregistry prefix (e.g. ``local_provider: pubchem``).
4. **Extra-provider reverse lookup** — source name follows the pattern
   ``"{provider}.{dataset}"`` (e.g. ``"bio2rdf.uniprot"``) and the
   dataset resource lists that provider code in its extra providers.

The full metadata dict returned by :func:`get_bioregistry_metadata`
mirrors the fields shown on the Bioregistry resource page (name,
description, homepage, license, domain, keywords, publications,
uri_prefix, synonyms, mappings, extra_providers) and can be exported to
JSON-LD with :func:`sources_to_jsonld`.

Usage::

    from rdfsolve.sources import load_sources, enrich_source_with_bioregistry

    for src in load_sources("data/sources.yaml"):
        enrich_source_with_bioregistry(src)
        print(src["name"], src.get("bioregistry_name"))
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, TypedDict, cast

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


class SourceEntry(TypedDict, total=False):
    """Typed dictionary for a single data-source definition."""

    name: str
    endpoint: str
    void_iri: str
    graph_uris: list[str]
    use_graph: bool
    two_phase: bool
    chunk_size: int
    class_batch_size: int
    class_chunk_size: int | None
    timeout: float
    delay: float
    counts: bool
    unsafe_paging: bool
    notes: str
    # ── Endpoint metadata (populated by probe/discovery scripts) ──
    sparql_engine: str
    sparql_strategy: str
    supports_graph: bool
    endpoint_down: bool
    # ── Bioregistry-derived metadata (populated by enrich_source_with_bioregistry) ──
    bioregistry_prefix: str
    bioregistry_name: str
    bioregistry_description: str
    bioregistry_homepage: str
    bioregistry_license: str
    bioregistry_domain: str
    keywords: list[str]
    bioregistry_publications: list[dict[str, str | None]]
    bioregistry_uri_prefix: str
    bioregistry_uri_prefixes: list[str]
    bioregistry_synonyms: list[str]
    bioregistry_mappings: dict[str, str]
    bioregistry_logo: str
    bioregistry_extra_providers: list[dict[str, str | None]]


# ── default path ──────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SOURCES_YAML = _REPO_ROOT / "data" / "sources.yaml"
DEFAULT_SOURCES_JSONLD = _REPO_ROOT / "data" / "sources.jsonld"
DEFAULT_SOURCES_CSV = _REPO_ROOT / "data" / "sources.csv"


def _default_sources_path() -> Path:
    """Return the default sources file, preferring YAML.

    Raises
    ------
    FileNotFoundError
        If neither ``data/sources.yaml`` nor ``data/sources.jsonld`` exists
        relative to the repository root.
    """
    if DEFAULT_SOURCES_YAML.exists():
        return DEFAULT_SOURCES_YAML
    if DEFAULT_SOURCES_JSONLD.exists():
        return DEFAULT_SOURCES_JSONLD
    raise FileNotFoundError(
        f"Default sources file not found. Looked for:\n"
        f"  {DEFAULT_SOURCES_YAML}\n"
        f"  {DEFAULT_SOURCES_JSONLD}\n"
        "Pass an explicit 'path' argument to load_sources()."
    )


# ── Bioregistry enrichment ────────────────────────────────────────


# Lazily populated reverse index: extra-provider code → list of BR prefixes
# whose resource lists that code in get_extra_providers().
_EXTRA_PROVIDER_INDEX: dict[str, list[str]] | None = None


def _build_extra_provider_index() -> dict[str, list[str]]:
    """Build a mapping from provider code → [bioregistry prefix, ...].

    This allows resolving names like ``"bio2rdf.uniprot"`` to ``"uniprot"``
    by looking up which resource has ``bio2rdf`` as an extra provider.
    The index is computed once and cached in :data:`_EXTRA_PROVIDER_INDEX`.
    """
    try:
        import bioregistry

        index: dict[str, list[str]] = {}
        for prefix, resource in bioregistry.manager.registry.items():
            for ep in resource.get_extra_providers():
                index.setdefault(ep.code, []).append(prefix)
        return index
    except Exception:
        logger.debug("bioregistry extra-provider index unavailable", exc_info=True)
        return {}


def _get_extra_provider_index() -> dict[str, list[str]]:
    global _EXTRA_PROVIDER_INDEX
    if _EXTRA_PROVIDER_INDEX is None:
        _EXTRA_PROVIDER_INDEX = _build_extra_provider_index()
    return _EXTRA_PROVIDER_INDEX


def _resolve_bioregistry_prefix(entry: SourceEntry) -> str | None:
    """Resolve the canonical Bioregistry prefix for a source entry.

    Resolution is attempted in the following order, stopping at the first hit:

    1. **Exact name** — the source ``name`` is itself a valid Bioregistry
       prefix (e.g. ``"chebi"`` → ``"chebi"``).
    2. **Root-prefix** — the first dot-separated segment of ``name``
       resolves (e.g. ``"drugbank.drugs"`` → ``"drugbank"``).
    3. **local_provider field** — the entry declares ``local_provider``
       which is a valid Bioregistry prefix (e.g. ``local_provider: pubchem``).
    4. **Extra-provider reverse lookup** — the name follows
       ``"{provider}.{dataset}"`` and the dataset resource lists that
       provider code among its extra providers (e.g. ``"bio2rdf.uniprot"``
       → ``"uniprot"`` because ``uniprot`` has ``bio2rdf`` as an extra
       provider).

    Returns ``None`` when no match is found.
    """
    try:
        import bioregistry
    except ImportError:
        logger.debug("bioregistry not installed — skipping prefix resolution")
        return None

    name: str = entry.get("name", "") or ""

    # 1. Exact match
    if bioregistry.get_resource(name) is not None:
        return name

    # 2. Root-prefix (first segment before '.')
    parts = name.split(".")
    if len(parts) > 1:
        root = parts[0]
        if bioregistry.get_resource(root) is not None:
            return root

    # 3. local_provider field (e.g. 'pubchem', 'idsm')
    local_provider: str = str(entry.get("local_provider") or "")
    if local_provider and bioregistry.get_resource(local_provider) is not None:
        return local_provider

    # 4. Extra-provider reverse lookup: "{provider_code}.{dataset_name}"
    #    e.g. "bio2rdf.uniprot" -> provider_code="bio2rdf", dataset_name="uniprot"
    if len(parts) == 2:
        provider_code, dataset_name = parts[0], parts[1]
        index = _get_extra_provider_index()
        candidates = index.get(provider_code, [])
        if dataset_name in candidates:
            return dataset_name
        # Also try normalised (lowercase)
        lc = dataset_name.lower()
        for cand in candidates:
            if cand.lower() == lc:
                return cand

    logger.debug("No bioregistry prefix resolved for source %r", name)
    return None


# ── Bioregistry metadata helpers ──────────────────────────────────


def _extract_publications(resource: Any) -> list[dict[str, str | None]]:
    """Extract publication dicts from a Bioregistry resource object."""
    pubs: list[dict[str, str | None]] = []
    raw = resource.get_publications()
    if not raw:
        return pubs
    for pub in raw:
        p: dict[str, str | None] = {}
        if pub.pubmed:
            p["pubmed"] = pub.pubmed
        if pub.doi:
            p["doi"] = pub.doi
        if pub.pmc:
            p["pmc"] = pub.pmc
        if hasattr(pub, "title") and pub.title:
            p["title"] = pub.title
        if p:
            pubs.append(p)
    return pubs


def _extract_extra_providers(resource: Any) -> list[dict[str, str]]:
    """Extract extra-provider dicts from a Bioregistry resource object."""
    raw = resource.get_extra_providers()
    if not raw:
        return []
    result: list[dict[str, str]] = []
    for ep in raw:
        d: dict[str, str] = {
            "code": ep.code,
            "name": ep.name,
            "uri_format": ep.uri_format,
        }
        if ep.homepage:
            d["homepage"] = ep.homepage
        if ep.description:
            d["description"] = ep.description
        result.append(d)
    return result


def _extract_scalar_metadata(resource: Any, meta: dict[str, Any]) -> None:
    """Populate *meta* with scalar fields from a Bioregistry resource object."""
    name = resource.get_name()
    if name:
        meta["name"] = name
    description = resource.get_description()
    if description:
        meta["description"] = description
    homepage = resource.get_homepage()
    if homepage:
        meta["homepage"] = homepage
    license_ = resource.get_license()
    if license_:
        meta["license"] = license_
    if resource.domain:
        meta["domain"] = resource.domain
    logo = resource.get_logo() if hasattr(resource, "get_logo") else getattr(resource, "logo", None)
    if logo:
        meta["logo"] = logo


def _extract_collection_metadata(resource: Any, meta: dict[str, Any]) -> None:
    """Populate *meta* with collection/list fields from a Bioregistry resource object."""
    keywords = resource.get_keywords()
    if keywords:
        meta["keywords"] = sorted(keywords)
    pubs = _extract_publications(resource)
    if pubs:
        meta["publications"] = pubs
    uri_prefix = resource.get_uri_prefix()
    if uri_prefix:
        meta["uri_prefix"] = uri_prefix
    uri_prefixes = resource.get_uri_prefixes()
    if uri_prefixes:
        meta["uri_prefixes"] = sorted(uri_prefixes)
    synonyms = resource.get_synonyms()
    if synonyms:
        meta["synonyms"] = sorted(synonyms)
    mappings = resource.get_mappings()
    if mappings:
        meta["mappings"] = dict(sorted(mappings.items()))
    extra_providers = _extract_extra_providers(resource)
    if extra_providers:
        meta["extra_providers"] = extra_providers


def get_bioregistry_metadata(br_prefix: str) -> dict[str, Any]:
    """Return a structured metadata dict for a Bioregistry prefix.

    The returned dictionary includes all fields visible on the Bioregistry
    resource page and suitable for embedding in JSON-LD or YAML:

    .. code-block:: python

        {
            "prefix": "drugbank",
            "name": "DrugBank",
            "description": "...",
            "homepage": "http://www.drugbank.ca",
            "license": None,
            "domain": "chemical",
            "keywords": ["drug", "chemical structure", ...],
            "publications": [{"pubmed": "...", "doi": "...", "title": "..."}, ...],
            "uri_prefix": "https://go.drugbank.com/drugs/",
            "uri_prefixes": ["https://go.drugbank.com/drugs/", ...],
            "synonyms": ["DrugBank", "DRUGBANK_ID"],
            "mappings": {"wikidata": "P715", ...},
            "logo": "https://...",
            "extra_providers": [
                {"code": "bio2rdf", "name": "Bio2RDF",
                 "uri_format": "http://bio2rdf.org/drugbank:$1"},
                ...
            ],
        }

    Parameters
    ----------
    br_prefix:
        A valid Bioregistry prefix string.

    Returns
    -------
    dict
        All available metadata; missing optional fields are omitted.

    Raises
    ------
    ValueError
        If *br_prefix* is not known to Bioregistry.
    """
    try:
        import bioregistry
    except ImportError as exc:
        raise ImportError("bioregistry must be installed for metadata lookup") from exc

    resource = bioregistry.get_resource(br_prefix)
    if resource is None:
        raise ValueError(f"Unknown Bioregistry prefix: {br_prefix!r}")

    meta: dict[str, Any] = {"prefix": br_prefix}
    _extract_scalar_metadata(resource, meta)
    _extract_collection_metadata(resource, meta)
    return meta


def enrich_source_with_bioregistry(entry: SourceEntry) -> str | None:
    """Populate ``bioregistry_*`` fields on *entry* in-place.

    Resolves the canonical Bioregistry prefix for the source's underlying
    dataset and writes all available metadata into the entry dict.

    Parameters
    ----------
    entry:
        A :class:`SourceEntry` dict, modified in-place.

    Returns
    -------
    str or None
        The resolved Bioregistry prefix, or ``None`` if no match was found.

    Example
    -------
    ::

        src = load_sources()[0]  # e.g. name="drugbank.drugs"
        prefix = enrich_source_with_bioregistry(src)
        print(prefix)  # "drugbank"
        print(src["bioregistry_name"])  # "DrugBank"
    """
    br_prefix = _resolve_bioregistry_prefix(entry)
    if br_prefix is None:
        return None

    try:
        meta = get_bioregistry_metadata(br_prefix)
    except Exception as exc:
        logger.warning("Could not fetch bioregistry metadata for %r: %s", br_prefix, exc)
        return None

    entry["bioregistry_prefix"] = meta.get("prefix", br_prefix)

    _scalar_fields = {
        "bioregistry_name": "name",
        "bioregistry_description": "description",
        "bioregistry_homepage": "homepage",
        "bioregistry_license": "license",
        "bioregistry_domain": "domain",
        "bioregistry_uri_prefix": "uri_prefix",
        "bioregistry_logo": "logo",
    }
    _entry_dict: dict[str, Any] = entry  # type: ignore[assignment]
    for entry_key, meta_key in _scalar_fields.items():
        if meta_key in meta:
            _entry_dict[entry_key] = meta[meta_key]

    _list_fields = {
        "keywords": "keywords",
        "bioregistry_uri_prefixes": "uri_prefixes",
        "bioregistry_synonyms": "synonyms",
        "bioregistry_extra_providers": "extra_providers",
        "bioregistry_publications": "publications",
    }
    for entry_key, meta_key in _list_fields.items():
        if meta_key in meta:
            _entry_dict[entry_key] = meta[meta_key]

    if "mappings" in meta:
        _entry_dict["bioregistry_mappings"] = meta["mappings"]

    return br_prefix


# ── JSON-LD export ────────────────────────────────────────────────

# JSON-LD context for source entries
_SOURCES_JSONLD_CONTEXT: dict[str, Any] = {
    "@vocab": "https://schema.org/",
    "void": "http://rdfs.org/ns/void#",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dcterms": "http://purl.org/dc/terms/",
    "rdfsolve": "https://rdfsolve.io/vocab#",
    "bioregistry": "https://bioregistry.io/registry/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    # Source entry fields
    "name": "schema:name",
    "description": "schema:description",
    "homepage": {"@id": "schema:url", "@type": "@id"},
    "endpoint": {"@id": "void:sparqlEndpoint", "@type": "@id"},
    "void_iri": {"@id": "void:dataDump", "@type": "@id"},
    "graph_uris": {"@id": "void:inDataset", "@type": "@id", "@container": "@set"},
    "domain": "schema:about",
    "license": {"@id": "dcterms:license", "@type": "@id"},
    "keywords": {"@id": "schema:keywords", "@container": "@set"},
    "uri_prefix": {"@id": "void:uriSpace"},
    "uri_prefixes": {"@id": "rdfsolve:uriPrefixes", "@container": "@set"},
    "synonyms": {"@id": "skos:altLabel", "@container": "@set"},
    "logo": {"@id": "schema:logo", "@type": "@id"},
    # Bioregistry mapping registry cross-references
    "bioregistry_prefix": {"@id": "rdfsolve:bioregistryPrefix"},
    "mappings": {"@id": "skos:exactMatch", "@container": "@index"},
    # Publications
    "publications": {"@id": "schema:citation", "@container": "@set"},
    "pubmed": {"@id": "schema:identifier"},
    "doi": {"@id": "schema:sameAs"},
    "pmc": {"@id": "schema:identifier"},
    "title": {"@id": "schema:name"},
    # Extra providers
    "extra_providers": {"@id": "rdfsolve:extraProvider", "@container": "@set"},
    "code": "schema:identifier",
    "uri_format": "rdfsolve:uriFormat",
}

_JSONLD_SCALAR_BR_FIELDS: list[tuple[str, str]] = [
    ("bioregistry_name", "name"),
    ("bioregistry_description", "description"),
    ("bioregistry_homepage", "homepage"),
    ("bioregistry_license", "license"),
    ("bioregistry_domain", "domain"),
    ("bioregistry_uri_prefix", "uri_prefix"),
    ("bioregistry_logo", "logo"),
]

_JSONLD_LIST_BR_FIELDS: list[tuple[str, str]] = [
    ("keywords", "keywords"),
    ("bioregistry_synonyms", "synonyms"),
    ("bioregistry_uri_prefixes", "uri_prefixes"),
]


def _entry_to_jsonld_node(entry: SourceEntry) -> dict[str, Any]:
    """Build a JSON-LD ``@graph`` node dict for a single source entry."""
    node: dict[str, Any] = {}

    src_name: str = entry.get("name", "") or ""
    node["@id"] = f"https://rdfsolve.io/sources/{src_name}"
    node["@type"] = "dcat:Dataset"

    if src_name:
        node["rdfsolve:sourceName"] = src_name

    endpoint = entry.get("endpoint") or ""
    if endpoint:
        node["endpoint"] = endpoint

    void_iri = entry.get("void_iri") or ""
    if void_iri:
        node["void_iri"] = void_iri

    graph_uris: list[str] = entry.get("graph_uris") or []
    if graph_uris:
        node["graph_uris"] = graph_uris

    if entry.get("notes"):
        node["rdfsolve:notes"] = entry["notes"]

    _node_add_bioregistry_fields(node, entry)
    return node


def _node_add_bioregistry_fields(node: dict[str, Any], entry: SourceEntry) -> None:
    """Populate *node* with Bioregistry-derived fields from *entry*."""
    br_prefix = entry.get("bioregistry_prefix") or ""
    if br_prefix:
        node["bioregistry_prefix"] = br_prefix
        node["skos:exactMatch"] = {"@id": f"https://bioregistry.io/registry/{br_prefix}"}

    for field, pred in _JSONLD_SCALAR_BR_FIELDS:
        val = entry.get(field)
        if val:
            node[pred] = val

    for field, pred in _JSONLD_LIST_BR_FIELDS:
        lst = entry.get(field)
        if lst:
            node[pred] = lst

    pubs = entry.get("bioregistry_publications")
    if pubs:
        node["publications"] = pubs

    extra_providers = entry.get("bioregistry_extra_providers")
    if extra_providers:
        node["extra_providers"] = extra_providers

    br_mappings = entry.get("bioregistry_mappings")
    if br_mappings:
        node["mappings"] = br_mappings


def sources_to_jsonld(
    entries: list[SourceEntry],
    *,
    enrich: bool = False,
) -> dict[str, Any]:
    """Serialise a list of source entries to a JSON-LD document.

    Each entry becomes a node in the ``@graph`` array. Bioregistry-derived
    fields (``bioregistry_*``) are mapped to standard vocabulary predicates
    using a compact JSON-LD context.

    Parameters
    ----------
    entries:
        Source entries, typically returned by :func:`load_sources`.
    enrich:
        When ``True``, call :func:`enrich_source_with_bioregistry` on each
        entry before serialisation (entries are **not** modified in place
        when ``enrich=True``; a shallow copy is used per entry).

    Returns
    -------
    dict
        A JSON-LD document with ``@context`` and ``@graph`` keys, ready for
        :func:`json.dump`.

    Example
    -------
    ::

        import json
        from rdfsolve.sources import load_sources, sources_to_jsonld

        entries = load_sources()
        doc = sources_to_jsonld(entries, enrich=True)
        with open("sources.jsonld", "w") as f:
            json.dump(doc, f, indent=2)
    """
    graph: list[dict[str, Any]] = []

    for raw_entry in entries:
        if enrich:
            entry: SourceEntry = cast(SourceEntry, dict(raw_entry))
            enrich_source_with_bioregistry(entry)
        else:
            entry = raw_entry

        graph.append(_entry_to_jsonld_node(entry))

    return {"@context": _SOURCES_JSONLD_CONTEXT, "@graph": graph}


# ── loading ───────────────────────────────────────────────────────


def load_sources(
    path: str | Path | None = None,
) -> list[SourceEntry]:
    """Load data-source definitions from a YAML, JSON-LD, or CSV file.

    Parameters
    ----------
    path:
        Path to the sources file.  When ``None`` the default
        ``data/sources.yaml`` (or ``.jsonld`` / ``.csv`` fallback)
        is used.

    Returns
    -------
    list[SourceEntry]
        One dict per data source, keys normalised to snake_case.
        Sources without an ``endpoint`` are included (callers may
        skip them).
    """
    p = Path(path) if path is not None else _default_sources_path()
    suffix = p.suffix.lower()

    if suffix in (".yaml", ".yml"):
        return _load_yaml(p)
    if suffix in (".jsonld", ".json"):
        return _load_jsonld(p)
    if suffix == ".csv":
        return _load_csv(p)
    raise ValueError(
        f"Unsupported sources file format {suffix!r}: expected .yaml, .yml, .jsonld, .json, or .csv"
    )


# ── YAML reader ───────────────────────────────────────────────────


def _load_yaml(path: Path) -> list[SourceEntry]:
    with open(path, encoding="utf-8") as fh:
        nodes = yaml.safe_load(fh)

    if not isinstance(nodes, list):
        raise ValueError(f"Expected a YAML list of source mappings in {path}")

    entries: list[SourceEntry] = []
    for node in nodes:
        entry = _yaml_node_to_entry(node)
        entries.append(entry)

    logger.info("Loaded %d sources from %s", len(entries), path)
    return entries


def _yaml_node_to_entry(node: dict[str, Any]) -> SourceEntry:
    """Convert a single YAML mapping to a SourceEntry."""
    e: SourceEntry = {}

    e["name"] = node.get("name", "")
    e["endpoint"] = node.get("endpoint", "")
    e["void_iri"] = node.get("void_iri", "")

    raw_g = node.get("graph_uris", [])
    if isinstance(raw_g, str):
        raw_g = [raw_g]
    e["graph_uris"] = list(raw_g)

    e["use_graph"] = bool(node.get("use_graph", False))
    e["two_phase"] = bool(node.get("two_phase", True))
    e["counts"] = bool(node.get("counts", True))
    e["unsafe_paging"] = bool(node.get("unsafe_paging", False))

    for int_key in (
        "chunk_size",
        "class_batch_size",
        "class_chunk_size",
    ):
        if int_key in node and node[int_key] is not None:
            e[int_key] = int(node[int_key])

    for float_key in ("timeout", "delay"):
        if float_key in node and node[float_key] is not None:
            e[float_key] = float(node[float_key])

    if "notes" in node:
        e["notes"] = str(node["notes"])

    # Pass through download_*, local_endpoint, and provider fields so
    # that the CLI (e.g. rdfsolve qleverfile) can see them.
    passthrough = {"local_endpoint", "local_provider", "local_tar_url"}
    e_dict: dict[str, Any] = e  # type: ignore[assignment]
    for key in node:
        if key.startswith("download_") or key in passthrough:
            e_dict[key] = node[key]

    return e


# ── JSON-LD reader ────────────────────────────────────────────────


def _load_jsonld(path: Path) -> list[SourceEntry]:
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)

    graph = doc.get("@graph", [])
    entries: list[SourceEntry] = []

    for node in graph:
        entry = _node_to_entry(node)
        entries.append(entry)

    logger.info("Loaded %d sources from %s", len(entries), path)
    return entries


def _node_to_entry(node: dict[str, Any]) -> SourceEntry:
    """Convert a single JSON-LD ``@graph`` node to a SourceEntry."""
    e: SourceEntry = {}

    e["name"] = node.get("name", "")

    # endpoint can be a plain string or {"@id": "…"}
    ep = node.get("endpoint", "")
    if isinstance(ep, dict):
        ep = ep.get("@id", "")
    e["endpoint"] = ep

    # void_iri - same treatment
    vi = node.get("void_iri", "")
    if isinstance(vi, dict):
        vi = vi.get("@id", "")
    e["void_iri"] = vi

    # graph_uris- normalise to list[str]
    raw_g = node.get("graph_uris", [])
    if isinstance(raw_g, str):
        raw_g = [raw_g]
    e["graph_uris"] = [(g["@id"] if isinstance(g, dict) else g) for g in raw_g]

    # booleans
    e["use_graph"] = bool(node.get("use_graph", False))
    e["two_phase"] = bool(node.get("two_phase", True))
    e["counts"] = bool(node.get("counts", True))
    e["unsafe_paging"] = bool(node.get("unsafe_paging", False))

    # optional numeric overrides (only set when present)
    for int_key in ("chunk_size", "class_batch_size", "class_chunk_size"):
        if int_key in node and node[int_key] is not None:
            e[int_key] = int(node[int_key])

    for float_key in ("timeout", "delay"):
        if float_key in node and node[float_key] is not None:
            e[float_key] = float(node[float_key])

    if "notes" in node:
        e["notes"] = str(node["notes"])

    return e


# ── CSV reader (deprecated now) ──────────────────────────────────────────


def _load_csv(path: Path) -> list[SourceEntry]:
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    entries: list[SourceEntry] = []
    for row in rows:
        e: SourceEntry = {}

        e["name"] = (row.get("dataset_name") or "").strip()
        e["endpoint"] = (row.get("endpoint_url") or "").strip()
        e["void_iri"] = (row.get("void_iri") or "").strip()

        graph_uri = (row.get("graph_uri") or "").strip()
        e["graph_uris"] = [graph_uri] if graph_uri else []

        e["use_graph"] = (row.get("use_graph") or "").strip().lower() in ("true", "1", "yes")
        # two_phase defaults to True unless explicitly off
        tp = (row.get("two_phase") or "").strip().lower()
        e["two_phase"] = tp not in ("false", "0", "no")

        entries.append(e)

    logger.info("Loaded %d sources from CSV %s", len(entries), path)
    return entries


# ── DataFrame conversion (for instance_matcher compat) ────────────


def load_sources_dataframe(
    path: str | Path | None = None,
    *,
    ports_json: str | Path | None = None,
) -> pd.DataFrame:
    """Load sources and return a :class:`~pandas.DataFrame`.

    The DataFrame has columns compatible with
    :func:`~rdfsolve.instance_matcher.probe_resource`:
    ``dataset_name``, ``endpoint_url``, ``graph_uri``, ``use_graph``,
    ``void_iri``.

    Parameters
    ----------
    path:
        Path to the sources file.  ``None`` = auto-detect default.
    ports_json:
        Optional path to a QLever ``ports.json`` file mapping
        ``{dataset_name: port}``.  When supplied, ``endpoint_url`` is
        replaced with ``http://localhost:{port}`` for every dataset
        present in the file, and datasets **not** in the file are
        dropped.  This ensures all queries go to local QLever
        instances instead of remote SPARQL endpoints.
    """
    entries = load_sources(path)
    rows = []
    for e in entries:
        rows.append(
            {
                "dataset_name": e.get("name", ""),
                "endpoint_url": e.get("endpoint", ""),
                "graph_uri": e["graph_uris"][0] if e.get("graph_uris") else "",
                "void_iri": e.get("void_iri", ""),
                "use_graph": e.get("use_graph", False),
            }
        )
    df = pd.DataFrame(rows)

    if ports_json is not None:
        import json as _json

        ports_path = Path(ports_json)
        if not ports_path.exists():
            logger.warning("ports_json %s does not exist — ignoring.", ports_path)
            return df

        port_map: dict[str, int] = _json.loads(ports_path.read_text(encoding="utf-8"))
        logger.info(
            "Overriding endpoint URLs from ports.json (%d datasets).",
            len(port_map),
        )

        # Override endpoint_url for datasets in port_map
        df["endpoint_url"] = df.apply(
            lambda row: f"http://localhost:{port_map[row['dataset_name']]}"
            if row["dataset_name"] in port_map
            else "",
            axis=1,
        )
        # Keep only datasets that have a local endpoint
        df = df[df["endpoint_url"] != ""].reset_index(drop=True)
        logger.info(
            "After ports.json filter: %d datasets with local endpoints.",
            len(df),
        )

    return df


# ── Source mode classification ────────────────────────────────────

# RDF file extensions that indicate a locally-downloadable dump.
_LOCAL_RDF_EXTENSIONS: frozenset[str] = frozenset({
    ".ttl", ".nt", ".nq", ".owl", ".rdf", ".n3",
    ".ttl.gz", ".nt.gz", ".nq.gz", ".owl.gz", ".rdf.gz",
    ".ttl.xz", ".nt.xz", ".nq.xz",
    ".trig", ".trig.gz",
})


def _has_rdf_download(entry: "SourceEntry") -> bool:
    """Return ``True`` if any ``download_*`` field links to an RDF dump.

    A URL is considered an RDF dump when its path (excluding query string)
    ends with one of the extensions in :data:`_LOCAL_RDF_EXTENSIONS`.
    """
    entry_dict: dict[str, Any] = entry  # type: ignore[assignment]
    for key, val in entry_dict.items():
        if not key.startswith("download_"):
            continue
        urls: list[str] = val if isinstance(val, list) else ([val] if val else [])
        for url in urls:
            if not url:
                continue
            url_path = url.lower().split("?")[0]
            for ext in _LOCAL_RDF_EXTENSIONS:
                if url_path.endswith(ext):
                    return True
    return False


def classify_source_mode(entry: "SourceEntry") -> str:
    """Classify a source as ``'local'``, ``'remote'``, ``'both'``, or ``'unknown'``.

    Classification rules (in order):

    * ``'local'``  — at least one ``download_*`` field points to an RDF
      dump file (``.ttl``, ``.nq``, ``.nt``, ``.owl``, etc.).
    * ``'remote'`` — ``endpoint`` is set, ``endpoint_down`` is not
      ``True``, and **no** download links are present.
    * ``'both'``   — download links *and* a live endpoint are both present.
    * ``'unknown'``— neither condition holds (no endpoint, no downloads).

    Parameters
    ----------
    entry:
        A :class:`SourceEntry` dict.

    Returns
    -------
    str
        One of ``'local'``, ``'remote'``, ``'both'``, ``'unknown'``.
    """
    has_download = _has_rdf_download(entry)
    has_endpoint = bool(entry.get("endpoint")) and not entry.get("endpoint_down", False)

    if has_download and has_endpoint:
        return "both"
    if has_download:
        return "local"
    if has_endpoint:
        return "remote"
    return "unknown"
