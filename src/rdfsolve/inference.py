"""SeMRA-powered inference pipeline for rdfsolve mappings.

Takes one or more mapping JSON-LD files, converts their edges to
``semra.Mapping`` objects, applies the requested inference operations
(inversion, transitivity/chain, generalisation), deduplicates via
``semra.api.assemble_evidences``, and writes the result as an
:class:`~rdfsolve.models.InferencedMapping` JSON-LD file.

Main entry-point
----------------
:func:`infer_mappings` — full pipeline.
:func:`seed_inferenced_mappings` — convenience wrapper for CLI/scripts.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

__all__ = ["infer_mappings", "seed_inferenced_mappings"]


def _load_edges_from_jsonld(
    path: Path,
) -> "list":
    """Load MappingEdge objects from a single JSON-LD mapping file."""
    from rdfsolve.models import MappingEdge

    data = json.loads(path.read_text(encoding="utf-8"))
    context = data.get("@context", {})
    graph = data.get("@graph", [])
    edges: list[MappingEdge] = []

    for node in graph:
        source_id = node.get("@id", "")
        src_ds_node = node.get("void:inDataset", {})
        src_ds = src_ds_node.get("dcterms:title", "")
        src_ep = (
            (src_ds_node.get("void:sparqlEndpoint") or {}).get("@id")
        )
        for key, val in node.items():
            if key.startswith("@") or key in (
                "void:inDataset", "dcterms:created",
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
                    (tgt_ds_node.get("void:sparqlEndpoint") or {})
                    .get("@id")
                )
                pred_uri = _expand_curie(key, context)
                src_uri = _expand_curie(source_id, context)
                tgt_uri = _expand_curie(tgt_id, context)
                edges.append(MappingEdge(
                    source_class=src_uri,
                    target_class=tgt_uri,
                    predicate=pred_uri,
                    source_dataset=src_ds,
                    target_dataset=tgt_ds,
                    source_endpoint=src_ep,
                    target_endpoint=tgt_ep,
                ))

    return edges


def _expand_curie(curie: str, context: dict) -> str:
    """Expand a CURIE or short URI using the JSON-LD @context."""
    if curie.startswith(("http://", "https://", "urn:")):
        return curie
    if ":" in curie:
        prefix, local = curie.split(":", 1)
        ns = context.get(prefix)
        if ns and isinstance(ns, str):
            return ns + local
    return curie


def infer_mappings(
    input_paths: List[str],
    output_path: str,
    *,
    inversion: bool = True,
    transitivity: bool = True,
    generalisation: bool = False,
    chain_cutoff: int = 3,
    dataset_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the inference pipeline over a set of mapping JSON-LD files.

    Loads all mapping edges from *input_paths*, converts them to semra
    Mappings, applies the chosen inference operations, deduplicates via
    ``semra.api.assemble_evidences``, converts back to rdfsolve edges,
    and writes an :class:`~rdfsolve.models.InferencedMapping` JSON-LD to
    *output_path*.

    Args:
        input_paths: Paths to input mapping JSON-LD files.
        output_path: Path to write the inferenced mapping JSON-LD.
        inversion: Apply symmetric inversion of every mapping.
        transitivity: Apply transitive chain inference.
        generalisation: Apply generalisation (broader/narrower).
        chain_cutoff: Max chain length for transitivity inference.
        dataset_name: Override for the ``@about.dataset_name`` field.

    Returns:
        Summary dict with keys ``"input_edges"``, ``"output_edges"``,
        ``"inference_types"``, ``"output_path"``.
    """
    from semra.api import assemble_evidences
    from semra.inference import (
        infer_chains,
        infer_generalizations,
        infer_reversible,
    )

    from rdfsolve.models import AboutMetadata, InferencedMapping
    from rdfsolve.semra_converter import (
        rdfsolve_edges_to_semra,
        semra_evidence_to_jsonld_about,
        semra_to_rdfsolve_edges,
    )

    # ── Load all edges ────────────────────────────────────────────────
    all_edges: list = []
    for p in input_paths:
        pth = Path(p)
        try:
            edges = _load_edges_from_jsonld(pth)
            all_edges.extend(edges)
            logger.info(
                "Loaded %d edges from %s", len(edges), pth.name,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping %s: %s", pth.name, exc)

    logger.info("Total input edges: %d", len(all_edges))

    # ── Convert to semra ──────────────────────────────────────────────
    semra_mappings = rdfsolve_edges_to_semra(all_edges)
    logger.info("Converted to %d semra Mappings", len(semra_mappings))

    # ── Apply inference operations ────────────────────────────────────
    applied: list[str] = []

    if inversion:
        semra_mappings = infer_reversible(semra_mappings)
        applied.append("inversion")
        logger.info(
            "After inversion: %d mappings", len(semra_mappings),
        )

    if transitivity:
        semra_mappings = infer_chains(
            semra_mappings, cutoff=chain_cutoff,
        )
        applied.append("transitivity")
        logger.info(
            "After transitivity (cutoff=%d): %d mappings",
            chain_cutoff, len(semra_mappings),
        )

    if generalisation:
        semra_mappings = infer_generalizations(semra_mappings)
        applied.append("generalisation")
        logger.info(
            "After generalisation: %d mappings", len(semra_mappings),
        )

    # ── Deduplicate ───────────────────────────────────────────────────
    semra_mappings = assemble_evidences(semra_mappings)
    logger.info(
        "After assemble_evidences: %d unique mappings",
        len(semra_mappings),
    )

    # ── Convert back to rdfsolve ──────────────────────────────────────
    result_edges = semra_to_rdfsolve_edges(semra_mappings)

    evidence_chain: list[dict] = []
    for m in semra_mappings:
        evidence_chain.extend(semra_evidence_to_jsonld_about(m.evidence))

    # ── Build InferencedMapping ───────────────────────────────────────
    stem = Path(output_path).stem
    name = dataset_name or f"{stem}_mapping"
    about = AboutMetadata.build(
        dataset_name=name,
        pattern_count=len(result_edges),
        strategy="inferenced",
    )
    mapping = InferencedMapping(
        edges=result_edges,
        about=about,
        inference_types=applied,
        source_mapping_files=[str(p) for p in input_paths],
        evidence_chain=evidence_chain,
        stats={
            "input_edges": len(all_edges),
            "output_edges": len(result_edges),
            "inference_types": applied,
        },
    )

    # ── Write output ──────────────────────────────────────────────────
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(mapping.to_jsonld(), indent=2, ensure_ascii=False), encoding="utf-8",
    )
    logger.info(
        "Written %d inferenced edges to %s", len(result_edges), out,
    )

    return {
        "input_edges": len(all_edges),
        "output_edges": len(result_edges),
        "inference_types": applied,
        "output_path": str(out),
    }


def seed_inferenced_mappings(
    input_dir: str = "docker/mappings",
    output_dir: str = "docker/mappings/inferenced",
    output_name: str = "inferenced_mappings",
    inversion: bool = True,
    transitivity: bool = True,
    generalisation: bool = False,
    chain_cutoff: int = 3,
) -> Dict[str, Any]:
    """Infer over all mappings in *input_dir* and write to *output_dir*.

    Collects all ``*.jsonld`` files under *input_dir*
    (``instance_matching/`` and ``semra/`` subdirs), runs
    :func:`infer_mappings`, and writes
    ``{output_dir}/{output_name}.jsonld``.

    This is the convenience entry-point for the CLI and seed scripts.

    Args:
        input_dir: Directory that contains mapping subdirs.
        output_dir: Directory to write inferenced output.
        output_name: Stem for the output file (without ``.jsonld``).
        inversion: Apply inversion inference.
        transitivity: Apply transitivity inference.
        generalisation: Apply generalisation inference.
        chain_cutoff: Max chain length for transitivity.

    Returns:
        Summary from :func:`infer_mappings`.
    """
    root = Path(input_dir)
    input_paths: list[str] = []

    for subdir_name in ("instance_matching", "semra"):
        subdir = root / subdir_name
        if subdir.exists():
            for f in sorted(subdir.glob("*.jsonld")):
                input_paths.append(str(f))

    if not input_paths:
        logger.warning(
            "No mapping files found under %s; nothing to infer.", root,
        )
        return {
            "input_edges": 0, "output_edges": 0,
            "inference_types": [], "output_path": "",
        }

    output_path = str(
        Path(output_dir) / f"{output_name}.jsonld"
    )
    return infer_mappings(
        input_paths=input_paths,
        output_path=output_path,
        inversion=inversion,
        transitivity=transitivity,
        generalisation=generalisation,
        chain_cutoff=chain_cutoff,
        dataset_name=output_name,
    )
