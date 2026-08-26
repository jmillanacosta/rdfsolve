#!/usr/bin/env python
"""Run inference pipeline on mapping files."""

import json
import logging
from pathlib import Path

from rdfsolve.mapping_models.core import MappingEdge
from rdfsolve.mapping_models.io import load_edges_from_jsonld

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PREDICATE_TO_RELATION = {
    "http://www.w3.org/2004/02/skos/core#exactMatch": "skos:exactMatch",
    "http://www.w3.org/2004/02/skos/core#narrowMatch": "skos:narrowMatch",
    "http://www.w3.org/2004/02/skos/core#broadMatch": "skos:broadMatch",
    "http://www.w3.org/2004/02/skos/core#relatedMatch": "skos:relatedMatch",
    "http://www.w3.org/2004/02/skos/core#closeMatch": "skos:closeMatch",
    "http://www.w3.org/2002/07/owl#equivalentClass": "owl:equivalentClass",
    "http://www.w3.org/2002/07/owl#sameAs": "owl:sameAs",
}
RELATION_TO_PREDICATE = {v: k for k, v in PREDICATE_TO_RELATION.items()}


def rdfsolve_edges_to_semra(edges: list[MappingEdge]):
    from semra.struct import Mapping, Reference, SimpleEvidence

    mappings = []
    for edge in edges:
        parts = edge.source_class.split(":", 1)
        s = Reference(prefix=parts[0], identifier=parts[1] if len(parts) > 1 else parts[0])
        parts = edge.target_class.split(":", 1)
        o = Reference(prefix=parts[0], identifier=parts[1] if len(parts) > 1 else parts[0])

        relation = PREDICATE_TO_RELATION.get(edge.predicate, "skos:exactMatch")
        evidence = [SimpleEvidence(
            justification=edge.mapping_justification or "rdfsolve_mapping",
            mapping_set_name=edge.source_dataset or "unknown",
        )]

        mappings.append(Mapping(s=s, p=relation, o=o, evidence=evidence))
    return mappings


def semra_to_rdfsolve_edges(mappings):
    edges = []
    for m in mappings:
        source_uri = f"{m.s.prefix}:{m.s.identifier}"
        target_uri = f"{m.o.prefix}:{m.o.identifier}"
        predicate = RELATION_TO_PREDICATE.get(m.p, "http://www.w3.org/2004/02/skos/core#exactMatch")

        source_ds = target_ds = "unknown"
        justification = None
        confidence = None

        if m.evidence:
            for ev in m.evidence:
                if hasattr(ev, "mapping_set_name") and ev.mapping_set_name:
                    source_ds = ev.mapping_set_name
                if hasattr(ev, "justification") and ev.justification:
                    justification = ev.justification
                if hasattr(ev, "confidence") and ev.confidence:
                    confidence = ev.confidence

        edges.append(MappingEdge(
            source_class=source_uri,
            target_class=target_uri,
            predicate=predicate,
            source_dataset=source_ds,
            target_dataset=target_ds,
            mapping_justification=justification,
            confidence=confidence,
        ))
    return edges


def load_edges(path: Path) -> list[MappingEdge]:
    data = json.loads(path.read_text())
    edges = []
    for e in data.get("@graph", []):
        edges.append(MappingEdge(**e))
    return edges


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("-o", "--output", type=Path, required=True)
    parser.add_argument("--inversion", action="store_true", default=True)
    parser.add_argument("--transitivity", action="store_true", default=True)
    parser.add_argument("--generalisation", action="store_true")
    parser.add_argument("--chain-cutoff", type=int, default=3)
    args = parser.parse_args()

    from semra.api import assemble_evidences
    from semra.inference import infer_chains, infer_generalizations, infer_reversible

    all_edges = []
    for p in args.inputs:
        try:
            edges = load_edges(p)
            all_edges.extend(edges)
            log.info("Loaded %d edges from %s", len(edges), p.name)
        except Exception as exc:
            log.warning("Skip %s: %s", p.name, exc)

    log.info("Total input edges: %d", len(all_edges))

    semra_mappings = rdfsolve_edges_to_semra(all_edges)
    log.info("Converted to %d semra mappings", len(semra_mappings))

    if args.inversion:
        semra_mappings = infer_reversible(semra_mappings)
        log.info("After inversion: %d", len(semra_mappings))

    if args.transitivity:
        semra_mappings = infer_chains(semra_mappings, cutoff=args.chain_cutoff)
        log.info("After transitivity: %d", len(semra_mappings))

    if args.generalisation:
        semra_mappings = infer_generalizations(semra_mappings)
        log.info("After generalisation: %d", len(semra_mappings))

    semra_mappings = assemble_evidences(semra_mappings)
    log.info("After deduplication: %d", len(semra_mappings))

    output_edges = semra_to_rdfsolve_edges(semra_mappings)
    log.info("Converted back to %d rdfsolve edges", len(output_edges))

    output = {
        "@context": {"@vocab": "https://rdfsolve.io/vocab#"},
        "@type": "InferencedMapping",
        "@about": {"dataset_name": "inferenced", "edge_count": len(output_edges)},
        "@graph": [e.model_dump() for e in output_edges],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))
    log.info("Wrote %s", args.output)


if __name__ == "__main__":
    main()
