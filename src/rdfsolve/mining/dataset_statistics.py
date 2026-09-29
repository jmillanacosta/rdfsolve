"""Exact dataset statistics from a QLever index.

A QLever index keeps the number of distinct subjects and of distinct objects of all its
triples: COUNT(DISTINCT ?s) and COUNT(DISTINCT ?o) answer from the metadata (Bgee, 715,849,799
subjects and 155,731,759 objects: under 0.1 s each, job 114239). The triples of each property
are one grouped query (Bgee: 80 s), and the distinct subjects and objects of one property read
one relation of the index (Bgee RO_0002206, 813,735,712 triples: 9 s each); a grouped query of
distinct objects for all properties was refused (54 GB, job 114239). The counts cover the whole index, so they are given
only when the graph scope holds every graph of it. Other engines read every triple for these
counts, so they are not counted there.
"""

from __future__ import annotations

from typing import Any

from rdflib import URIRef

from rdfsolve.sparql_helper import EndpointError


def count_dataset(helper: Any, graph_uris: list[str] | None) -> dict[str, Any]:
    """Return the exact triples, distinct subjects, objects and properties, or why not."""
    if str(getattr(helper, "sparql_engine", "")).lower() != "qlever":
        return {"state": "not_counted", "reason": "only a QLever index keeps these counts"}

    def select(query: str) -> list[dict[str, Any]]:
        """Return the rows of a query."""
        rows: list[dict[str, Any]] = helper.select(query, purpose="dataset-statistics")["results"][
            "bindings"
        ]
        return rows

    def number(query: str) -> int:
        """Return the count of a query with one row."""
        (row,) = select(query)
        return int(row["n"]["value"])

    try:
        if graph_uris:
            listed = ", ".join(URIRef(g).n3() for g in graph_uris)
            outside = number(
                "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?_g { ?s ?p ?o } "
                f"FILTER(?_g NOT IN ({listed})) }}"
            )
            if outside:
                return {
                    "state": "not_counted",
                    "reason": "the graph scope does not hold the whole index",
                    "triples_outside_scope": outside,
                }
        triples = {
            row["p"]["value"]: int(row["n"]["value"])
            for row in select("SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p")
        }
        subjects = number("SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s ?p ?o }")
        objects = number("SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }")
    except (EndpointError, ValueError) as error:
        return {"state": "not_counted", "reason": f"refused: {error}"}
    # A refused count of one property is recorded; the counts of the others are kept.
    partitions: dict[str, dict[str, int]] = {}
    refused: dict[str, str] = {}
    for prop, n in sorted(triples.items()):
        partitions[prop] = {"triples": n}
        for name, variable in (("distinct_subjects", "?s"), ("distinct_objects", "?o")):
            query = (
                f"SELECT (COUNT(DISTINCT {variable}) AS ?n) WHERE {{ ?s {URIRef(prop).n3()} ?o }}"
            )
            try:
                partitions[prop][name] = number(query)
            except (EndpointError, ValueError) as error:
                refused[prop] = f"{name}: {error}"
    return {
        "state": "counted",
        "source": "QLever index",
        "triples": sum(triples.values()),
        "distinct_subjects": subjects,
        "distinct_objects": objects,
        "distinct_properties": len(triples),
        "property_partitions": partitions,
        "refused_partitions": refused,
    }
