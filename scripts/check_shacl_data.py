"""Validate exported shapes and inspect bounded routes on local Turtle files.

Requires pySHACL. This offline diagnostic never changes mining evidence.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections import Counter
from hashlib import file_digest
from itertools import islice, zip_longest
from pathlib import Path

from pyshacl import validate
from rdflib import RDF, SH, XSD, BNode, Graph, Literal, URIRef

logger = logging.getLogger(__name__)


def matching(graph, node, step):
    """Use explicit RDF types, not ontology entailment."""
    kind = step["object_class"]
    if kind == "Literal":
        return isinstance(node, Literal) and (
            not step.get("datatype")
            or str(RDF.langString if node.language else node.datatype or XSD.string)
            == step["datatype"]
        )
    if kind == "Resource":
        return isinstance(node, URIRef)
    if kind == "BlankNode":
        return isinstance(node, BNode)
    return (node, RDF.type, URIRef(kind)) in graph


def route_counts(graph, route, max_values):
    """Count distinct focus/end pairs; do not count intermediate walks."""
    steps = route["steps"]
    focuses = set(graph.subjects(RDF.type, URIRef(steps[0]["subject_class"])))
    if not focuses:
        return {"status": "no_focus_nodes", "focus_nodes": 0}
    example = None
    supported = 0
    pairs = 0
    bare_pairs = 0
    for focus in sorted(focuses):
        filtered = {focus}
        bare = {focus}
        for step in steps:
            predicate = URIRef(step["property_uri"])
            next_bare = set()
            next_filtered = set()
            for node in bare:
                for value in graph.objects(node, predicate):
                    next_bare.add(value)
                    if node in filtered and matching(graph, value, step):
                        next_filtered.add(value)
                    if len(next_bare) > max_values:
                        return {"status": "value_limit", "focus_nodes": len(focuses)}
            bare, filtered = next_bare, next_filtered
        if filtered and example is None:
            example = {"focus": focus.n3(), "value": min(filtered).n3()}
        supported += bool(filtered)
        pairs += len(filtered)
        bare_pairs += len(bare)
    return {
        "status": "complete",
        "focus_nodes": len(focuses),
        "supported_focus_nodes": supported,
        "distinct_focus_end_pairs": pairs,
        "bare_path_focus_end_pairs": bare_pairs,
        "example": example,
    }


def main():
    """Write a separate diagnostic report from local source data."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shapes", type=Path, required=True)
    parser.add_argument("--data", type=Path, nargs="+", required=True)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-routes", type=int, default=24)
    parser.add_argument("--max-values", type=int, default=10000)
    args = parser.parse_args()
    if args.max_routes < 0 or args.max_values < 1:
        parser.error("Use a nonnegative route limit and positive value limit")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    graph = Graph()
    inputs = []
    for path in args.data:
        with path.open("rb") as stream:
            checksum = file_digest(stream, "sha256").hexdigest()
        graph.parse(path, format="turtle")
        inputs.append(
            {"path": str(path.resolve()), "sha256": checksum, "bytes": path.stat().st_size}
        )
    logger.info("Loaded %d triples from %d local files", len(graph), len(inputs))
    shapes = Graph().parse(args.shapes, format="turtle")
    target_counts = {
        str(target): len(set(graph.subjects(RDF.type, target)))
        for target in shapes.objects(None, SH.targetClass)
    }
    checks = {}
    for mode in ("exported", "activated_diagnostic"):
        if mode == "activated_diagnostic":
            # Activate only generated one-hop templates, not imported source shapes.
            for node in list(shapes.subjects(SH.deactivated, Literal(True))):
                if "/ns-" in str(node):
                    shapes.remove((node, SH.deactivated, None))
        conforms, report, _ = validate(
            graph,
            shacl_graph=shapes,
            inference="none",
            meta_shacl=True,
            advanced=False,
            js=False,
            do_owl_imports=False,
        )
        report.serialize(args.output_dir / f"{mode}.ttl", format="turtle")
        results = list(report.subjects(RDF.type, SH.ValidationResult))
        checks[mode] = {
            "conforms": conforms,
            "results": len(results),
            "components": dict(
                Counter(str(report.value(n, SH.sourceConstraintComponent)) for n in results)
            ),
            "result_paths": dict(Counter(str(report.value(n, SH.resultPath)) for n in results)),
        }
        logger.info("%s: conforms=%s; results=%d", mode, conforms, len(results))
    envelope = json.loads(args.schema.read_text())
    if envelope.get("format") != "rdfsolve.mined-schema" or envelope.get("version") != 1:
        raise ValueError("Use canonical mined-schema JSON")
    model = envelope["schema"]
    routes = model.get("navigation", {}).get("paths", [])
    checked = []
    by_length = [[r for r in routes if len(r["steps"]) == hops] for hops in range(2, 7)]
    selection = (r for row in zip_longest(*by_length) for r in row if r is not None)
    for route in islice(selection, args.max_routes):
        checked.append({"steps": route["steps"], **route_counts(graph, route, args.max_values)})
    summary = {
        "inputs": inputs,
        "data_triples": len(graph),
        "shapes": str(args.shapes.resolve()),
        "schema": str(args.schema.resolve()),
        "schema_source_version": model["about"].get("schema_version"),
        "scope": "Union of listed local Turtle files; explicit rdf:type for route filters.",
        "comparability": "Cached dump and mined endpoint snapshot may differ.",
        "target_class_explicit_focus_counts": target_counts,
        "target_classes_absent_from_dump": sorted(k for k, v in target_counts.items() if v == 0),
        "checks": checks,
        "routes_checked": checked,
        "routes_not_checked": len(routes) - len(checked),
        "max_values_per_focus_step": args.max_values,
        "seconds": round(time.perf_counter() - started, 3),
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    logger.info("Checked %d routes; report: %s", len(checked), args.output_dir)


if __name__ == "__main__":
    main()
