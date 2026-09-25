"""Load the source registry and enrich entries with Bioregistry metadata."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from rdfsolve.models.source_model import SourceModel, SourcesRegistry

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SOURCES_YAML = _REPO_ROOT / "data" / "sources.yaml"


def load_sources(path: str | Path | None = None) -> list[SourceModel]:
    """Load and validate the source registry YAML; the default is data/sources.yaml."""
    return SourcesRegistry.from_yaml(path if path is not None else DEFAULT_SOURCES_YAML).sources


# Bioregistry enrichment


# Lazily populated reverse index: extra-provider code -> list of BR prefixes
# whose resource lists that code in get_extra_providers().
_EXTRA_PROVIDER_INDEX: dict[str, list[str]] | None = None


def _build_extra_provider_index() -> dict[str, list[str]]:
    """Build a mapping from provider code -> [bioregistry prefix, ...].

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


def _resolve_bioregistry_prefix(source: SourceModel) -> str | None:
    """Use curated identifiers, exact names or registered provider correspondences."""
    try:
        import bioregistry
    except ImportError:
        logger.debug("bioregistry not installed - skipping prefix resolution")
        return None

    if source.bioregistry_prefix:
        return (
            source.bioregistry_prefix
            if bioregistry.get_resource(source.bioregistry_prefix) is not None
            else None
        )
    name = source.name
    if bioregistry.get_resource(name) is not None:
        return name
    parts = name.split(".")
    if len(parts) == 2:
        provider_code, dataset_name = parts[0], parts[1]
        index = _get_extra_provider_index()
        candidates = index.get(provider_code, [])
        if dataset_name in candidates:
            return dataset_name
        lc = dataset_name.lower()
        for cand in candidates:
            if cand.lower() == lc:
                return cand

    logger.debug("No bioregistry prefix resolved for source %r", name)
    return None


# Bioregistry metadata helpers


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


def _optional_bioregistry_getter(module: Any, name: str, prefix: str) -> Any:
    """Call one public Bioregistry getter when available in the installed version."""
    function = getattr(module, name, None)
    if function is None:
        return None
    try:
        return function(prefix)
    except Exception:
        logger.debug("Bioregistry %s failed for %s", name, prefix, exc_info=True)
        return None


def _bioregistry_package_version() -> str:
    try:
        return version("bioregistry")
    except PackageNotFoundError:
        return "unknown"


def get_bioregistry_metadata(br_prefix: str) -> dict[str, Any]:
    """Return a structured metadata dict for a Bioregistry prefix.

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
    # Use public package getters for reference/download metadata. These describe
    # the Bioregistry resource, not files observed in an rdfsolve local bundle.
    for key, getter in {
        "repository": "get_repository",
        "owl_download": "get_owl_download",
        "rdf_download": "get_rdf_download",
        "obo_download": "get_obo_download",
    }.items():
        value = _optional_bioregistry_getter(bioregistry, getter, br_prefix)
        if value:
            meta[key] = str(value)
    meta["package_version"] = _bioregistry_package_version()
    meta["enriched_at"] = datetime.now(timezone.utc).isoformat()
    return meta


def enrich_source_with_bioregistry(source: SourceModel) -> str | None:
    """Resolve the Bioregistry prefix of a source and copy its metadata onto the model.

    The model is updated in place. Return the resolved prefix, or None.
    """
    br_prefix = _resolve_bioregistry_prefix(source)
    if br_prefix is None:
        return None

    try:
        meta = get_bioregistry_metadata(br_prefix)
    except Exception as exc:
        logger.warning("Could not fetch bioregistry metadata for %r: %s", br_prefix, exc)
        return None

    fields = {
        "bioregistry_prefix": "prefix",
        "bioregistry_name": "name",
        "bioregistry_description": "description",
        "bioregistry_homepage": "homepage",
        "bioregistry_license": "license",
        "bioregistry_domain": "domain",
        "bioregistry_uri_prefix": "uri_prefix",
        "bioregistry_logo": "logo",
        "bioregistry_repository": "repository",
        "bioregistry_owl_download": "owl_download",
        "bioregistry_rdf_download": "rdf_download",
        "bioregistry_obo_download": "obo_download",
        "bioregistry_enriched_at": "enriched_at",
        "bioregistry_package_version": "package_version",
        "keywords": "keywords",
        "bioregistry_uri_prefixes": "uri_prefixes",
        "bioregistry_synonyms": "synonyms",
        "bioregistry_extra_providers": "extra_providers",
        "bioregistry_publications": "publications",
        "bioregistry_mappings": "mappings",
    }
    updates = {field: meta[key] for field, key in fields.items() if key in meta}
    updates.setdefault("bioregistry_prefix", br_prefix)
    validated = SourceModel.model_validate({**source.model_dump(), **updates})
    for field in updates:
        setattr(source, field, getattr(validated, field))
    return source.bioregistry_prefix


def enrich_registry_with_bioregistry(
    path: str | Path | None = None,
    *,
    output: str | Path,
    names: set[str] | None = None,
) -> dict[str, Any]:
    """Write a Bioregistry refresh proposal to a separate YAML file."""
    import yaml

    source_path = Path(path or DEFAULT_SOURCES_YAML)
    target_path = Path(output)
    if target_path.resolve() == source_path.resolve() or (
        target_path.exists() and target_path.samefile(source_path)
    ):
        raise ValueError("Write the refresh proposal to a separate file")
    raw = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise ValueError(f"Expected a YAML list in {source_path}")
    selected = names or {str(item.get("name")) for item in raw if item.get("name")}
    resolved = changed = 0
    unresolved: list[str] = []
    enriched_keys = {
        name for name in SourceModel.model_fields if name.startswith("bioregistry_")
    } | {"keywords"}
    for item in raw:
        name = str(item.get("name") or "")
        if not name or name not in selected:
            continue
        model = SourceModel.model_validate(item)
        before = model.model_dump(mode="json")
        prefix = enrich_source_with_bioregistry(model)
        if prefix is None:
            unresolved.append(name)
            continue
        resolved += 1
        after = model.model_dump(mode="json")
        row_changed = False
        for key in enriched_keys:
            value = after.get(key)
            if value in (None, "", [], {}):
                # Remove stale Bioregistry-owned values that disappeared upstream.
                if key in item:
                    item.pop(key, None)
                    row_changed = True
                continue
            if item.get(key) != value:
                item[key] = value
                row_changed = True
        if row_changed or before != after:
            changed += 1
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = target_path.with_name(f".{target_path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
        os.replace(tmp, target_path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return {
        "source": str(source_path),
        "output": str(target_path),
        "selected": len(selected),
        "resolved": resolved,
        "changed": changed,
        "unresolved": sorted(unresolved),
        "bioregistry_version": _bioregistry_package_version(),
    }


# Source mode classification

# RDF file extensions that indicate a locally-downloadable dump.
_LOCAL_RDF_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".ttl",
        ".nt",
        ".nq",
        ".owl",
        ".rdf",
        ".n3",
        ".ttl.gz",
        ".nt.gz",
        ".nq.gz",
        ".owl.gz",
        ".rdf.gz",
        ".ttl.xz",
        ".nt.xz",
        ".nq.xz",
        ".trig",
        ".trig.gz",
    }
)


def _has_rdf_download(source: SourceModel) -> bool:
    """Return ``True`` if any ``download_*`` field links to an RDF dump.

    A URL is considered an RDF dump when its path (excluding query string)
    ends with one of the extensions in :data:`_LOCAL_RDF_EXTENSIONS`.
    """
    if source.graph_sources:
        return True
    fields = {**(source.model_extra or {}), "download_ttl": source.download_ttl}
    for key, val in fields.items():
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


def classify_source_mode(source: SourceModel) -> str:
    """Classify a source as ``'local'``, ``'remote'``, ``'both'``, or ``'unknown'``.

    Classification rules (in order):

    * ``'local'``  - at least one ``download_*`` field points to an RDF
      dump file (``.ttl``, ``.nq``, ``.nt``, ``.owl``, etc.).
    * ``'remote'`` - ``endpoint`` is set, ``endpoint_down`` is not
      ``True``, and **no** download links are present.
    * ``'both'``   - download links *and* a live endpoint are both present.
    * ``'unknown'``- neither condition holds (no endpoint, no downloads).

    Parameters
    ----------
    source:
        A validated registry entry.

    Returns
    -------
    str
        One of ``'local'``, ``'remote'``, ``'both'``, ``'unknown'``.
    """
    has_download = _has_rdf_download(source)
    has_endpoint = bool(source.endpoint) and not source.endpoint_down

    if has_download and has_endpoint:
        return "both"
    if has_download:
        return "local"
    if has_endpoint:
        return "remote"
    return "unknown"
