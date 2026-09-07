"""Check cached index structure without loading or changing it."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from rdfsolve.qlever.lifecycle import index_name

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper


def has_cached_index(workdir: Path, fallback: str) -> bool:
    """Reject known incomplete indices; file checks do not prove loadability."""
    name = index_name(workdir, fallback)
    metadata_path = workdir / f"{name}.meta-data.json"
    if not metadata_path.is_file():
        if any(workdir.glob(f"{name}.index.*")):
            raise ValueError(f"Incomplete index in {workdir}: missing {metadata_path.name}")
        return False
    try:
        metadata = json.loads(metadata_path.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"Unreadable index metadata: {metadata_path}") from error
    required = {
        "num-subjects",
        "num-predicates",
        "num-objects",
        "num-triples",
        "has-all-permutations",
        "index-format-version",
        "vocabulary-type",
    }
    missing = sorted(required - metadata.keys()) if isinstance(metadata, dict) else sorted(required)
    if missing:
        raise ValueError(f"Incomplete index metadata in {metadata_path}: missing {missing}")
    for key in ("num-subjects", "num-predicates", "num-objects", "num-triples"):
        counts = metadata[key]
        if not isinstance(counts, dict) or any(
            type(counts.get(kind)) is not int or counts[kind] < 0 for kind in ("normal", "internal")
        ):
            raise ValueError(f"Invalid {key} in {metadata_path}")
    if not isinstance(metadata["has-all-permutations"], bool):
        raise ValueError(f"Invalid has-all-permutations in {metadata_path}")
    permutations = ["pso", "pos"]
    if metadata["has-all-permutations"]:
        permutations += ["spo", "sop", "osp", "ops"]
    missing_files = [
        f"{name}.index.{permutation}{suffix}"
        for permutation in permutations
        for suffix in ("", ".meta")
        if not (workdir / f"{name}.index.{permutation}{suffix}").is_file()
        or (workdir / f"{name}.index.{permutation}{suffix}").stat().st_size == 0
    ]
    if missing_files:
        raise ValueError(f"Incomplete index in {workdir}: missing or empty {missing_files}")
    return True


def verify_named_graphs(helper: SparqlHelper, graph_uris: list[str]) -> None:
    """Require at least one triple in each selected cached graph."""
    from rdflib import URIRef

    for offset in range(0, len(graph_uris), 50):
        expected = set(graph_uris[offset : offset + 50])
        terms = " ".join(URIRef(iri).n3() for iri in sorted(expected))
        query = (
            "SELECT DISTINCT ?graph WHERE { VALUES ?graph { "
            + terms
            + " } FILTER EXISTS { GRAPH ?graph { ?s ?p ?o } } }"
        )
        response = helper.select(query, purpose="cached_named_graphs")
        bindings = response.get("results", {}).get("bindings")
        if not isinstance(bindings, list):
            raise ValueError("Cached graph check returned invalid SELECT bindings")
        present = {
            row["graph"]["value"]
            for row in bindings
            if isinstance(row, dict)
            and isinstance(row.get("graph"), dict)
            and row["graph"].get("type") == "uri"
            and "value" in row["graph"]
        }
        missing = sorted(expected - present)
        if missing:
            raise ValueError(
                f"Cached index lacks nonempty graphs: {missing}. "
                "Select matching cached inputs; no unscoped fallback was run."
            )
