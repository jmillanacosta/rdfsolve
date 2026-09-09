"""Show observed route rows without asking a model to recreate the data."""

from __future__ import annotations

from typing import Any

import pandas as pd
from rdflib import URIRef

from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import RdfTerm


def connection_table(payload: dict[str, Any]) -> pd.DataFrame:
    """Return one row per retained path match, with its original RDF terms."""
    rows: list[dict[str, Any]] = []
    connections = {}
    for match in payload["evidence"]:
        if not match.get("nodes"):
            continue
        nodes = match["nodes"]
        connection = f"{payload['reference']}:{len(rows)}"
        connections[connection] = match
        row: dict[str, Any] = {"Connection": connection}
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
            "Connection",
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
    table.attrs.update(connections=connections, class_labels=payload["labels"])
    return table


def answer_table(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    """Join final references on observed routes, never on shared words or row positions.

    Keep annotation predicates and intermediate nodes. A route is not a new direct
    RDF statement between its endpoints. Unread descriptions remain missing.
    """
    if not payloads:
        table = pd.DataFrame()
        table.attrs.update(title="No linked answer rows returned", connections={}, class_labels={})
        table["Connection"] = []
        return table
    if len({payload["registry_revision"] for payload in payloads}) != 1:
        raise ValueError("Answer references must use the same registry revision")
    schema = MinedSchema.from_dict(payloads[0]["schema"])
    models = {
        str(getattr(model, "rdf_class_iri", "")): model
        for model in schema.to_pydantic_classes().values()
    }
    records = [
        models[item["type"]].model_validate(item["data"])
        for payload in payloads
        for item in payload["records"]
    ]
    descriptions = {}
    for record in records:
        terms = vars(record)["rdf_terms"]
        if "description" in vars(record)["rdf_loaded_fields"]:
            descriptions[(getattr(type(record), "rdf_class_iri", ""), str(vars(record)["uri"]))] = [
                RdfTerm.model_validate(term).to_rdf() for term in terms.get("description", [])
            ]
    labels = payloads[0]["labels"]
    annotations: dict[str, str] = {}
    for annotation in sorted(
        schema.enrichment.labels, key=lambda item: item.text.language not in (None, "", "en")
    ):
        annotations.setdefault(annotation.term_iri, annotation.text.value)
    rows, matches = [], {}
    for payload in payloads:
        links = connection_table(payload)
        for row in links.to_dict(orient="records"):
            identifier = row["Connection"]
            if identifier in matches:
                continue
            match = links.attrs["connections"][identifier]
            matches[identifier] = match
            for role, node in (("Source", match["nodes"][0]), ("Target", match["nodes"][-1])):
                row[f"{role} description"] = descriptions.get((node["type"], node["value"]), pd.NA)
            predicates = [link["predicate"] for link in match["links"]]
            row.update(
                {
                    "Relationship type": [
                        annotations.get(p, p.rsplit("/", 1)[-1].rsplit("#", 1)[-1])
                        for p in predicates
                    ],
                    "Relationship definition": [
                        schema.enrichment.description(p) for p in predicates
                    ],
                    "Via class": [
                        labels.get(node["type"], node["type"]) for node in match["nodes"][1:-1]
                    ],
                }
            )
            rows.append(row)
    table = pd.DataFrame(rows)
    if not rows:
        table["Connection"] = []
    names = {}
    for role in ("Source", "Target"):
        kinds = sorted({row[f"{role} class"] for row in rows})
        names[role] = kinds[0] if len(kinds) == 1 else role
    # Keep distinct headers when a route starts and ends at the same class.
    if names["Source"] == names["Target"]:
        names = {role: f"{name} ({role.lower()})" for role, name in names.items()}
    table.rename(
        columns={
            key: value
            for role, name in names.items()
            for key, value in (
                (role, f"{name} IRI"),
                (f"{role} name", f"{name} name"),
                (f"{role} description", f"{name} description"),
            )
        },
        inplace=True,
    )
    leading = [
        f"{name} {field}" for name in names.values() for field in ("name", "IRI", "description")
    ]
    leading += ["Relationship type", "Relationship definition", "Via", "Via class"]
    table = table[
        [column for column in leading if column in table]
        + [column for column in table if column not in leading]
    ]
    table.attrs.update(
        title=f"{names['Source']} → {names['Target']}: observed relationships"
        if rows
        else "No linked answer rows returned",
        connections=matches,
        class_labels=labels,
        records=records,
        queries=payloads[0]["queries"],
        coverage={
            "status": "partial"
            if any(p["coverage"].get("status") == "partial" for p in payloads)
            else "complete"
        },
        references=[payload["reference"] for payload in payloads],
    )
    return table
