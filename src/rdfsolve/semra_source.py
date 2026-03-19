"""Register rdfsolve as a SeMRA source.

Importing this module (or calling :func:`register`) makes the key
``"rdfsolve_instance"`` available in
``semra.sources.SOURCE_RESOLVER``.  Once registered, the rdfsolve
instance-matcher mappings can be used as inputs to any SeMRA pipeline:

    from semra.sources import SOURCE_RESOLVER
    import rdfsolve.semra_source  # triggers registration
    mappings = SOURCE_RESOLVER.lookup("rdfsolve_instance")()

The returned mappings are read from the JSON-LD files in
``docker/mappings/instance_matching/`` (relative to the repository
root discovered via ``importlib.resources`` / CWD fallback).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdfsolve._uri import expand_curie

if TYPE_CHECKING:
    pass  # semra types only needed at runtime

logger = logging.getLogger(__name__)

_SOURCE_KEY = "rdfsolve_instance"
_DEFAULT_DIR = Path("docker/mappings/instance_matching")
_registered = False


def _find_mappings_dir() -> Path:
    """Locate the instance-matching mappings directory.

    Tries (in order):
    1. ``docker/mappings/instance_matching/`` relative to CWD.
    2. The same path relative to this file's parent chain.

    Returns the first that exists, or the CWD-relative path if none do.
    """
    cwd_path = Path.cwd() / _DEFAULT_DIR
    if cwd_path.exists():
        return cwd_path

    here = Path(__file__).parent
    for _ in range(4):  # walk up at most 4 levels
        candidate = here / _DEFAULT_DIR
        if candidate.exists():
            return candidate
        here = here.parent

    logger.warning(
        "rdfsolve_instance source: mapping directory not found, using CWD-relative path %s",
        cwd_path,
    )
    return cwd_path


def get_rdfsolve_instance_mappings(
    directory: str | None = None,
) -> list[Any]:
    """Return rdfsolve instance-matcher mappings as semra Mappings.

    Reads every ``*.jsonld`` file from *directory* (defaults to
    ``docker/mappings/instance_matching/``), converts the edges via
    :func:`rdfsolve.semra_converter.rdfsolve_edges_to_semra`, and
    returns the flat list of ``semra.Mapping`` objects.

    Args:
        directory: Override the search directory (absolute path).

    Returns:
        List of ``semra.Mapping`` objects.
    """
    import json

    from rdfsolve.mapping_models.core import MappingEdge
    from rdfsolve.semra_converter import rdfsolve_edges_to_semra

    dir_path = Path(directory) if directory else _find_mappings_dir()
    if not dir_path.exists():
        logger.warning(
            "rdfsolve_instance: directory %s does not exist; returning empty list",
            dir_path,
        )
        return []

    all_mappings: list[Any] = []
    for jsonld_file in sorted(dir_path.glob("*.jsonld")):
        try:
            data = json.loads(jsonld_file.read_text(encoding="utf-8"))
            graph = data.get("@graph", [])

            # Reconstruct MappingEdge list from the @graph
            edges: list[MappingEdge] = []
            for node in graph:
                source_id = node.get("@id", "")
                src_ds_node = node.get("void:inDataset", {})
                src_ds = src_ds_node.get("dcterms:title", "")
                src_ep = (
                    src_ds_node.get("void:sparqlEndpoint") or src_ds_node.get("foaf:homepage") or {}
                ).get("@id")
                for key, val in node.items():
                    if key.startswith("@") or key in (
                        "void:inDataset",
                        "dcterms:created",
                    ):
                        continue
                    targets = val if isinstance(val, list) else [val]
                    for tgt in targets:
                        if not isinstance(tgt, dict):
                            continue
                        tgt_id = tgt.get("@id", "")
                        tgt_ds_node = tgt.get("void:inDataset", {})
                        tgt_ds = tgt_ds_node.get("dcterms:title", "")
                        tgt_ep = (
                            tgt_ds_node.get("void:sparqlEndpoint")
                            or tgt_ds_node.get("foaf:homepage")
                            or {}
                        ).get("@id")
                        # Expand CURIE -> URI via context
                        context = data.get("@context", {})
                        pred_uri = expand_curie(key, context)
                        src_uri = expand_curie(source_id, context)
                        tgt_uri = expand_curie(tgt_id, context)

                        edges.append(
                            MappingEdge(
                                source_class=src_uri,
                                target_class=tgt_uri,
                                predicate=pred_uri,
                                source_dataset=src_ds,
                                target_dataset=tgt_ds,
                                source_endpoint=src_ep,
                                target_endpoint=tgt_ep,
                            )
                        )

            semra_mappings = rdfsolve_edges_to_semra(edges, about=None)
            all_mappings.extend(semra_mappings)
            logger.debug(
                "Loaded %d mappings from %s",
                len(semra_mappings),
                jsonld_file.name,
            )
        except Exception as exc:
            logger.warning(
                "Skipping %s: %s",
                jsonld_file.name,
                exc,
            )

    logger.info(
        "rdfsolve_instance: returning %d mappings from %s",
        len(all_mappings),
        dir_path,
    )
    return all_mappings


def register(force: bool = False) -> None:
    """Register the rdfsolve_instance source with SeMRA's resolver.

    Safe to call multiple times - subsequent calls are no-ops unless
    *force* is ``True``.

    Args:
        force: Re-register even if already registered.
    """
    global _registered
    if _registered and not force:
        return
    try:
        from semra.sources import SOURCE_RESOLVER

        SOURCE_RESOLVER.register(
            get_rdfsolve_instance_mappings,
            synonyms=[_SOURCE_KEY, "rdfsolve"],
            raise_on_conflict=False,
        )
        _registered = True
        logger.debug(
            "Registered rdfsolve as SeMRA source %r",
            _SOURCE_KEY,
        )
    except Exception as exc:
        logger.warning(
            "Could not register rdfsolve as SeMRA source: %s",
            exc,
        )


# Auto-register when the module is imported
register()
