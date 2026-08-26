#!/usr/bin/env python
"""Convert SeMRA mapping files to rdfsolve format."""

import json
import logging
from pathlib import Path

from rdfsolve.mapping_models.core import MappingEdge

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


def semra_to_edges(mappings) -> list[MappingEdge]:
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


def main():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path, help="SeMRA file (.sssom.tsv or .pkl)")
    parser.add_argument("-o", "--output", type=Path, required=True)
    args = parser.parse_args()
    
    try:
        from semra import parse_file
    except ImportError:
        log.error("semra not installed")
        return 1
    
    log.info("Loading SeMRA from %s", args.input)
    mappings = parse_file(args.input)
    log.info("Loaded %d SeMRA mappings", len(mappings))
    
    edges = semra_to_edges(mappings)
    log.info("Converted to %d rdfsolve edges", len(edges))
    
    output = {
        "@context": {"@vocab": "https://rdfsolve.io/vocab#"},
        "@type": "Mapping",
        "@about": {"dataset_name": args.input.stem, "edge_count": len(edges)},
        "@graph": [e.model_dump() for e in edges],
    }
    
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))
    log.info("Wrote %s", args.output)


if __name__ == "__main__":
    main()
