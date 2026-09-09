"""Show observed route rows without asking a model to recreate the data."""

from __future__ import annotations

from typing import Any

import pandas as pd
from rdflib import URIRef

from rdfsolve.schema_models.enrichment import RdfTerm


def connection_table(payload: dict[str, Any]) -> pd.DataFrame:
    """Return one row per retained path match, with its original RDF terms."""
    rows = []
    for match in payload["evidence"]:
        if not match.get("nodes"):
            continue
        nodes = match["nodes"]
        row: dict[str, Any] = {}
        for name, node in (("Source", nodes[0]), ("Target", nodes[-1])):
            row[name] = RdfTerm.model_validate(
                {key: node[key] for key in ("kind", "value")}
            ).to_rdf()
            row[f"{name} name"] = node.get("label")
            row[f"{name} class"] = payload["labels"].get(node["type"], node["type"])
        row.update(
            {
                "Via": [node.get("label") or node["value"] for node in nodes[1:-1]],
                "Predicates": [URIRef(link["predicate"]) for link in match["links"]],
                "Inverse": [link["inverse"] for link in match["links"]],
                "Graph": URIRef(match["graph"]) if match.get("graph") else None,
                "Query": match["query_id"],
            }
        )
        rows.append(row)
    table = pd.DataFrame(
        rows,
        columns=[
            "Source",
            "Source name",
            "Source class",
            "Via",
            "Target",
            "Target name",
            "Target class",
            "Predicates",
            "Inverse",
            "Graph",
            "Query",
        ],
    )
    table.attrs.update(
        {key: payload[key] for key in ("evidence", "coverage", "queries", "reference")}
    )
    return table
