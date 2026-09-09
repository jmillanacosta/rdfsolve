"""Read selected schema routes with their intermediate RDF statements."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from rdfsolve.hydration import _iri, _term

if TYPE_CHECKING:
    from rdfsolve.client_api import Client, Results

Route = list[tuple[str, str, str, bool]]


def read_routes(client: Client, source: Results, routes: dict[str, Route]) -> Results:
    """Join each chosen route inside one graph and batch source records."""
    from rdfsolve.client_api import Results, _name_fields, _title

    evidence: list[dict[str, Any]] = []
    nodes: dict[str, set[str]] = defaultdict(set)
    targets: dict[str, set[str]] = defaultdict(set)
    partial = source.coverage.get("status") == "partial"
    node_ids: set[str] = set()
    target_ids: set[tuple[str, str]] = set()
    for route_id, route in routes.items():
        iris = sorted(
            {
                str(vars(record)["uri"])
                for record in source
                if getattr(type(record), "rdf_class_iri", "") == route[0][0]
            }
        )
        for start in range(0, len(iris), client.batch_size):
            remaining = client.max_rows - len(evidence)
            if remaining <= 0:
                partial = True
                break
            values = " ".join(_iri(iri) for iri in iris[start : start + client.batch_size])
            body = f"VALUES ?n0 {{ {values} }} "
            for i, (s, p, o, backward) in enumerate(route):
                left, right = (i + 1, i) if backward else (i, i + 1)
                body += (
                    f"?n{left} {_iri(p)} ?n{right} . ?n{i} a {_iri(s)} . ?n{i + 1} a {_iri(o)} . "
                )
            for i in range(len(route) + 1):
                for j in range(i):
                    body += f"FILTER(!sameTerm(?n{i}, ?n{j})) "
            variables = " ".join(f"?n{i}" for i in range(len(route) + 1))
            rows = client._select(
                f"SELECT DISTINCT {variables} ?_graph WHERE {{ {client._scope(body)} }} "
                f"LIMIT {remaining + 1}"
            )
            partial |= len(rows) > remaining
            for row in rows[:remaining]:
                classes = [route[0][0], *[edge[2] for edge in route]]
                terms = [_term(row[f"n{i}"]) for i in range(len(classes))]
                if any(term.kind == "literal" for term in terms):
                    raise ValueError("A class route returned a literal node")
                new_nodes = {term.value for term in terms if term.kind == "uri"}
                target_key = (classes[-1], terms[-1].value)
                if len(node_ids | new_nodes) > client.max_subjects * 4 or (
                    terms[-1].kind == "uri"
                    and target_key not in target_ids
                    and len(target_ids) >= client.max_subjects
                ):
                    partial = True
                    continue
                node_ids.update(new_nodes)
                for cls, term in zip(classes, terms, strict=True):
                    if term.kind == "uri":
                        nodes[cls].add(term.value)
                if terms[-1].kind == "uri":
                    target_ids.add(target_key)
                    targets[classes[-1]].add(terms[-1].value)
                else:
                    partial = True  # Retain blank nodes as evidence, not followable identifiers.
                evidence.append(
                    {
                        "path": route_id,
                        "query_id": len(client._records()),
                        "graph": row.get("_graph", {}).get("value"),
                        "nodes": [
                            {"type": cls, **term.model_dump(mode="json")}
                            for cls, term in zip(classes, terms, strict=True)
                        ],
                        "links": [{"predicate": edge[1], "inverse": edge[3]} for edge in route],
                    }
                )
    records = []
    labels: dict[tuple[str, str], str] = {}
    for cls, ids in sorted(nodes.items()):
        model = client.model(cls)
        ordered = sorted(ids)
        for start in range(0, len(ordered), client.max_subjects):
            found = client.get_many(
                model, ordered[start : start + client.max_subjects], fields=_name_fields(model)
            )
            for record in found:
                labels[(cls, str(vars(record)["uri"]))] = _title(record)
                if str(vars(record)["uri"]) in targets[cls]:
                    records.append(record)
    for match in evidence:
        for node in match["nodes"]:
            node["label"] = labels.get((node["type"], node["value"]))
    return Results(
        client,
        records,
        evidence=evidence,
        coverage={
            "status": "partial" if partial else "complete",
            "basis": "Observed paths for the selected records and routes, not proof of causation",
            "source": source.coverage,
            "selected_paths": list(routes),
            "limit_reached": partial,
        },
    )
