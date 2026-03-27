"""Build and persist an OntologyIndex from OLS4 metadata.

Usage examples::

    # Index all active OLS4 ontologies (slow — ~276 ontologies)
    python scripts/build_ontology_index.py

    # Index only ChEBI and GO
    python scripts/build_ontology_index.py --ontologies chebi go

    # Filter to ontologies whose classes appear in JSON-LD schema files
    python scripts/build_ontology_index.py --from-schemas docker/schemas

    # Filter to ontologies relevant for schema class URIs in sources.yaml
    python scripts/build_ontology_index.py --from-sources data/sources.yaml

    # Dry-run: print stats without writing to disk
    python scripts/build_ontology_index.py --from-schemas docker/schemas --dry-run

    # Use a custom HTTP cache directory
    python scripts/build_ontology_index.py --cache-dir /tmp/ols_cache

    # Save index to a non-default data directory
    python scripts/build_ontology_index.py --data-dir data/ontology/
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the index builder."""
    parser = argparse.ArgumentParser(
        description="Build an OntologyIndex from OLS4 and persist it to disk.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--ontologies",
        nargs="+",
        metavar="ID",
        default=None,
        help=(
            "Explicit OLS4 ontology IDs to index (e.g. chebi go). "
            "When omitted, all active ontologies are fetched."
        ),
    )
    parser.add_argument(
        "--from-sources",
        metavar="YAML",
        default=None,
        help=(
            "Path to a sources YAML file (e.g. data/sources.yaml). "
            "Schema class URIs extracted from the file restrict indexing "
            "to relevant ontologies."
        ),
    )
    parser.add_argument(
        "--from-schemas",
        metavar="DIR",
        default=None,
        help=(
            "Directory containing *_schema.jsonld files (e.g. docker/schemas). "
            "Every IRI subject found in those files is treated as a candidate "
            "class URI, restricting indexing to relevant ontologies."
        ),
    )
    parser.add_argument(
        "--cache-dir",
        metavar="DIR",
        default=None,
        help=(
            "Directory for the OLS4 HTTP-response cache (diskcache). "
            "Defaults to no caching when omitted."
        ),
    )
    parser.add_argument(
        "--data-dir",
        metavar="DIR",
        default="data",
        help="Directory to write ontology_index.pkl.gz and ontology_graph.graphml. "
        "(default: data/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the index but do not write any files — only print stats.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser.parse_args(argv)


def _load_class_uris_from_sources(sources_path: str) -> set[str]:
    """Extract schema class URIs from a sources YAML file.

    Reads each source entry and collects all ``uri_prefix`` /
    ``uri_prefixes`` values as candidate class URI prefixes.

    Parameters:
        sources_path: Path to the sources YAML (e.g. ``data/sources.yaml``).

    Returns:
        set[str]: Set of URI prefix strings found in the file.
    """
    try:
        from rdfsolve.sources import load_sources
    except ImportError as exc:
        logger.error("Could not import rdfsolve.sources: %s", exc)
        return set()

    path = Path(sources_path)
    if not path.exists():
        logger.warning("Sources file not found: %s", path)
        return set()

    entries = load_sources(path)
    uris: set[str] = set()
    for entry in entries:
        for key in ("uri_prefix", "bioregistry_uri_prefix"):
            val = entry.get(key)
            if isinstance(val, str) and val:
                uris.add(val)
        for val in entry.get("bioregistry_uri_prefixes", []) or []:
            if isinstance(val, str) and val:
                uris.add(val)
    logger.info("Collected %d URI prefixes from %s", len(uris), path)
    return uris


def _load_class_uris_from_schemas(
    schemas_dir: str,
) -> tuple[set[str], dict[str, str]]:
    """Extract candidate class IRIs and prefix map from JSON-LD schema files.

    Walks *schemas_dir* recursively for ``*_schema.jsonld`` files, reads each
    as JSON, resolves all ``@id`` values in ``@graph`` nodes via the file's own
    ``@context`` using :func:`rdfsolve._uri.expand_curie`, and collects every
    resulting full IRI as a candidate class URI.

    No ``owl:Class`` filtering is applied — any IRI that appears as a subject
    is considered a potential class.

    Parameters:
        schemas_dir: Root directory containing schema sub-directories
            (e.g. ``docker/schemas``).

    Returns:
        tuple: ``(uris, context)`` where *uris* is the set of full IRI strings
        found as subjects and *context* is the merged ``prefix → namespace``
        map from all ``@context`` blocks encountered.
    """
    import json

    from rdfsolve._uri import expand_curie

    root = Path(schemas_dir)
    if not root.is_dir():
        logger.warning("Schemas directory not found: %s", root)
        return set(), {}

    uris: set[str] = set()
    merged_context: dict[str, str] = {}
    files = sorted(root.rglob("*_schema.jsonld"))
    logger.info("Scanning %d JSON-LD schema files in %s …", len(files), root)

    for path in files:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not read %s: %s", path, exc)
            continue

        # Build a flat prefix map from @context (may be a list or dict)
        raw_ctx = doc.get("@context", {})
        context: dict[str, str] = {}
        if isinstance(raw_ctx, list):
            for item in raw_ctx:
                if isinstance(item, dict):
                    for k, v in item.items():
                        if isinstance(v, str):
                            context[k] = v
        elif isinstance(raw_ctx, dict):
            for k, v in raw_ctx.items():
                if isinstance(v, str):
                    context[k] = v
        merged_context.update(context)

        # Walk every node in @graph and collect its @id as a full IRI
        for node in doc.get("@graph", []):
            if not isinstance(node, dict):
                continue
            raw_id = node.get("@id")
            if not isinstance(raw_id, str) or not raw_id:
                continue
            expanded = expand_curie(raw_id, context)
            if expanded.startswith(("http://", "https://")):
                uris.add(expanded)

    logger.info("Collected %d candidate class IRIs from schemas", len(uris))
    return uris, merged_context


def _ontology_ids_from_context(
    context: dict[str, str],
    ols_ontology_ids: set[str],
) -> list[str]:
    """Resolve schema ``@context`` prefixes to OLS4 ontology IDs.

    Uses bioregistry to map each prefix to its canonical OLS4 ID, then
    intersects with the set of IDs actually present in OLS4.

    Parameters:
        context: Merged ``prefix → namespace`` map from schema ``@context``
            blocks.
        ols_ontology_ids: Set of ontology IDs known to exist in OLS4.

    Returns:
        list[str]: Sorted list of OLS4 ontology IDs inferred from *context*.
    """
    try:
        import bioregistry
    except ImportError:
        logger.warning("bioregistry not installed — cannot resolve prefixes to OLS4 IDs")
        return []

    matched: set[str] = set()
    for prefix in context:
        # bioregistry stores OLS IDs under the "ols" key
        resource = bioregistry.get_resource(prefix)
        if resource is None:
            continue
        ols_id = resource.get_mappings().get("ols") if resource.get_mappings() else None
        candidate = ols_id or prefix.lower()
        if candidate in ols_ontology_ids:
            matched.add(candidate)
        elif prefix.lower() in ols_ontology_ids:
            matched.add(prefix.lower())

    logger.info(
        "Resolved %d OLS4 ontology IDs from schema @context prefixes", len(matched)
    )
    return sorted(matched)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the build_ontology_index CLI.

    Parameters:
        argv: Optional argument list (uses ``sys.argv`` when ``None``).

    Returns:
        int: Exit code (0 on success, non-zero on failure).
    """
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # ── Collect schema class URIs (optional) ──────────────────────────────
    schema_class_uris: set[str] | None = None
    explicit_ontology_ids: list[str] | None = args.ontologies
    if args.from_schemas:
        schema_class_uris, schema_context = _load_class_uris_from_schemas(args.from_schemas)
        if not schema_class_uris:
            logger.warning("No IRIs found in %s; indexing all ontologies.", args.from_schemas)
            schema_class_uris = None
        elif not args.ontologies:
            # Resolve OLS4 IDs via bioregistry from the merged @context prefixes
            try:
                from rdfsolve.ontology.ols_client import OlsClient

                with OlsClient(cache_dir=args.cache_dir) as _cl:
                    all_metas = list(_cl.get_all_ontologies())
                ols_ids = {m.get("ontologyId", "") for m in all_metas} - {""}
                explicit_ontology_ids = _ontology_ids_from_context(schema_context, ols_ids) or None
                if explicit_ontology_ids:
                    logger.info(
                        "Will target %d OLS4 ontologies: %s",
                        len(explicit_ontology_ids),
                        explicit_ontology_ids,
                    )
            except Exception as exc:
                logger.warning("Could not resolve ontology IDs from context: %s", exc)
    elif args.from_sources:
        schema_class_uris = _load_class_uris_from_sources(args.from_sources)
        if not schema_class_uris:
            logger.warning("No URI prefixes found in %s; indexing all ontologies.", args.from_sources)
            schema_class_uris = None

    # ── Build index ───────────────────────────────────────────────────────
    try:
        from rdfsolve.ontology.index import build_ontology_index, save_ontology_index
    except ImportError as exc:
        logger.error("Cannot import rdfsolve.ontology.index: %s", exc)
        return 1

    logger.info(
        "Building OntologyIndex (ontologies=%s, cache_dir=%s, schema_uris=%s) …",
        explicit_ontology_ids or "all",
        args.cache_dir or "none",
        f"{len(schema_class_uris)} URIs" if schema_class_uris else "none",
    )

    try:
        idx = build_ontology_index(
            schema_class_uris=schema_class_uris,
            cache_dir=args.cache_dir,
            ontology_ids=explicit_ontology_ids,
        )
    except Exception as exc:
        logger.error("Failed to build ontology index: %s", exc, exc_info=args.verbose)
        return 1

    # ── Print stats ───────────────────────────────────────────────────────
    stats = idx.stats()
    col_w = 20
    print("\n── OntologyIndex statistics ─────────────────────────")
    for key, val in stats.items():
        print(f"  {key:<{col_w}}: {val:,}")
    print("─────────────────────────────────────────────────────\n")

    if args.dry_run:
        logger.info("Dry-run mode — skipping write to disk.")
        return 0

    # ── Persist ───────────────────────────────────────────────────────────
    data_dir = Path(args.data_dir)
    try:
        save_ontology_index(idx, data_dir)
    except Exception as exc:
        logger.error("Failed to save ontology index: %s", exc, exc_info=args.verbose)
        return 1

    logger.info("OntologyIndex written to %s/", data_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
