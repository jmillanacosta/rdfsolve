"""Relate requested answer classes to possible and observed RDF routes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rdfsolve.rdf_operations import Paths, Plan, Search
from rdfsolve.schema_catalogue import words

if TYPE_CHECKING:
    from rdfsolve.client_session import ClientSession


def build_plan(session: ClientSession, args: Plan) -> dict[str, Any]:
    """Resolve class choices and list bounded routes without querying data."""
    Search(terms=args.terms)
    source = str(getattr(session.client.model(args.source), "rdf_class_iri", ""))
    targets = sorted(
        {str(getattr(session.client.model(target), "rdf_class_iri", "")) for target in args.targets}
        - {source}
    )
    descriptions = {item.id: item for item in session.registry.types}
    # Match only explicit class words. This does not resolve synonyms or infer intent.
    generic = {"id", "identifier", "entity", "record", "class"}
    concepts = {item.id: set(words(item.label)) - generic for item in descriptions.values()}
    represented = set().union(*(concepts[kind] for kind in [source, *targets]))
    requested = set(words(args.selection))
    missing = [
        item.label
        for item in descriptions.values()
        if concepts[item.id]
        and concepts[item.id] <= requested
        and not concepts[item.id] & represented
    ]
    if missing:
        raise ValueError(
            "The selection names classes missing from the answer columns: "
            + ", ".join(missing)
            + ". Choose the requested entity classes, not only intermediate classes."
        )
    columns = [
        {"class": descriptions[kind].label, "type": kind, "role": role}
        for kind, role in [(source, "source"), *[(target, "target") for target in targets]]
    ]
    routes = []
    for target in targets:
        found = session._paths(
            Paths(
                source=source, target=target, text=args.evidence, max_hops=args.max_hops, limit=30
            )
        )
        ordered = sorted(found["paths"], key=lambda path: path["hops"])
        chosen, seen = [], set()
        for _ in range(min(3, len(ordered))):
            path = min(
                ordered,
                key=lambda item: (
                    item["hops"],
                    tuple(step["to"] for step in item["steps"]) in seen,
                ),
            )
            chain = tuple(step["to"] for step in path["steps"])
            chosen.append(path)
            seen.add(chain)
            ordered.remove(path)
        found.update(paths=chosen, next_offset=0 if len(chosen) < found["matched_paths"] else None)
        routes.append({"target": target, **found})
    return {
        "columns": columns,
        "selection": args.selection,
        "terms": args.terms,
        "evidence": args.evidence,
        "routes": routes,
        "max_hops": args.max_hops,
        "row": "One observed source-to-target route; retain intermediate records and query IDs.",
        "basis": "Proposed answer structure, not a claim that records or links exist",
        "data_queried": False,
    }


def plan_status(session: ClientSession) -> dict[str, Any]:
    """Report retrieved classes, attempted routes and gaps from execution records."""
    plan = session.answer_plan
    if not plan:
        return {"plan": None, "pending": ["Choose the answer classes with plan before searching."]}
    source = plan["columns"][0]["type"]
    references: dict[str, list[str]] = {}
    for reference, result in session.results.items():
        for kind in {getattr(type(record), "rdf_class_iri", "") for record in result}:
            references.setdefault(kind, []).append(reference)
    attempts: set[str] = set()
    failed: set[str] = set()
    for operation in session.client._operations:
        if operation["operation"] == "read" and operation.get("query_ids"):
            (failed if operation["status"] == "failed" else attempts).update(
                operation["arguments"].get("paths", [])
            )
    observed: dict[str, set[str]] = {}
    for reference, final in session.final_queries.items():
        attempts.update(final["paths"])
        for row in final["bindings"]:
            branch = final["branches"][int(row["_route"]["value"])]
            if branch["nodes"][0]["type"] == source:
                references.setdefault(source, []).append(reference)
                observed.setdefault(branch["nodes"][-1]["type"], set()).add(reference)
    for reference, result in session.results.items():
        for match in result.evidence:
            if match.get("nodes") and match["nodes"][0]["type"] == source:
                observed.setdefault(match["nodes"][-1]["type"], set()).add(reference)
    pending = []
    if source not in references:
        searched = any(
            op["operation"] == "search" and op["status"] != "failed"
            for op in session.client._operations
        )
        if not searched and not session.final_queries:
            pending.append("Search the planned topic values; no records have been queried.")
    rows = []
    for group in plan["routes"]:
        target = group["target"]
        paths = [path["id"] for path in group["paths"]]
        related = {
            key
            for key, route in session.routes.items()
            if route[0][0] == source and route[-1][2] == target
        }
        untried = set(paths) - attempts - failed
        if target in observed:
            status = "linked records retrieved"
        elif source not in references:
            status = "no source candidates retrieved"
        elif not paths:
            status = "no route found within the chosen hop limit"
        elif untried and not (related & attempts):
            status = "route not tried"
        elif untried:
            status = "no matches yet; alternatives not tried"
        elif related & failed:
            status = "route query failed"
        else:
            status = "no matches on tried routes"
        if source in references and target not in observed and untried:
            pending.append(
                f"Follow a route to {target} from {references[source][0]}: "
                + ", ".join(sorted(untried))
            )
        rows.append(
            {
                "class": session.client.type_name(session.client.model(target)),
                "type": target,
                "status": status,
                "routes": paths,
                "references": sorted(observed.get(target, set())),
            }
        )
    searches = [
        {
            "terms": op["arguments"].get("terms"),
            "class": op["arguments"].get("kind"),
            "status": op["status"],
            "query_ids": op.get("query_ids", []),
        }
        for op in session.client._operations
        if op["operation"] == "search"
    ]
    routes = [
        {
            "path": path["id"],
            "route": " → ".join(
                [path["steps"][0]["from"], *[step["to"] for step in path["steps"]]]
            ),
            "fields": path["fields"],
        }
        for group in plan["routes"]
        for path in group["paths"]
    ]
    return {
        "plan": plan,
        "routes": routes,
        "coverage": rows,
        "searches": searches,
        "pending": pending,
    }
