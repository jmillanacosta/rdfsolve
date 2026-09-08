"""Verify mined class routes against selected records."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import pandas as pd
from rdflib import RDF, Literal

from rdfsolve.client_paths import _budget, class_paths, resource_path_table
from rdfsolve.exploration import SEARCH_PREDICATES
from rdfsolve.hydration import HydrationLimitError, _iri

if TYPE_CHECKING:
    from rdfsolve.client_api import Client


def value_paths(
    client: Client,
    source: str,
    value: str | None,
    *,
    source_iri: str | None = None,
    target_class: str | None = None,
    max_hops: int,
    both_directions: bool,
    max_paths: int,
) -> pd.DataFrame:
    """Find observed paths along simple mined class routes, within one graph."""
    _budget(max_hops, max_paths)
    if (value is None) == (target_class is None):
        raise ValueError("Supply a target name or class")
    if value is not None and not value.strip():
        raise ValueError("Enter a word or name for target_value")
    if source_iri is not None:
        _iri(source_iri)
    predicates = " ".join(_iri(p) for p in sorted(SEARCH_PREDICATES))
    models = {str(getattr(model, "rdf_class_iri", "")) for model in client.models.values()}
    name_query = (
        f"{{ SELECT DISTINCT ?target WHERE {{ VALUES ?labelPredicate {{ {predicates} }} "
        f"?target ?labelPredicate ?label . FILTER(isIRI(?target) && !isBlank(?label) && "
        f"CONTAINS(LCASE(STR(?label)), LCASE({Literal(value).n3()}))) }} }} ?target a ?class"
    )
    routes: list[dict[str, Any]] = []
    seen: set[str] = set()
    with client.step(f"Find paths to {value if value is not None else target_class}"):
        targets: dict[str, set[tuple[str, str]] | None] = {}
        if target_class is not None:
            targets[target_class] = None
        else:
            found = client._select(
                "SELECT DISTINCT ?target ?class ?_graph WHERE { "
                + client._scope(name_query)
                + f" }} LIMIT {client.max_rows + 1}"
            )
            if len(found) > client.max_rows:
                raise HydrationLimitError("Name lookup exceeded max_rows")
            matched: dict[str, set[tuple[str, str]]] = defaultdict(set)
            for row in found:
                if row["class"]["value"] in models:
                    matched[row["class"]["value"]].add(
                        (row["target"]["value"], row.get("_graph", {}).get("value", ""))
                    )
            targets.update(matched)
        candidates: list[tuple[list[tuple[str, str, str, bool]], set[tuple[str, str]] | None]] = []
        for end_class in sorted(targets):
            if end_class == source:
                continue
            table = class_paths(
                client,
                source,
                end_class,
                max_hops=max_hops,
                both_directions=both_directions,
                max_paths=max_paths,
            )
            candidates.extend(
                (route, targets[end_class])
                for route in table.attrs["routes"]
                if all(edge[1] != str(RDF.type) for edge in route)
            )
            if len(candidates) > max_paths:
                raise HydrationLimitError(
                    "Too many class routes; reduce max_hops or raise max_paths"
                )
        queries: dict[int, list[str]] = defaultdict(list)
        for route, matches in sorted(candidates, key=lambda item: (len(item[0]), item[0])):
            hops = len(route)
            body = ""
            if matches is not None:
                columns = [f"?n{hops}"]
                rows = [[_iri(iri)] for iri, _ in sorted(matches)]
                if client.graph_uris:
                    columns.append("?_graph")
                    for terms, (_, graph) in zip(rows, sorted(matches), strict=True):
                        terms.append(_iri(graph))
                if source_iri is not None:
                    columns.insert(0, "?n0")
                    for terms in rows:
                        terms.insert(0, _iri(source_iri))
                values = " ".join("(" + " ".join(row) + ")" for row in rows)
                body = f"VALUES ({' '.join(columns)}) {{ {values} }} "
            elif source_iri is not None:
                body = f"VALUES ?n0 {{ {_iri(source_iri)} }} "
            for i, (s, p, o, backward) in enumerate(route):
                left, right = (i + 1, i) if backward else (i, i + 1)
                body += (
                    f"?n{left} {_iri(p)} ?n{right} . ?n{i} a {_iri(s)} . ?n{i + 1} a {_iri(o)} . "
                )
            # Keep the connected triple patterns together before adding output fields.
            for i, (_s, p, _o, backward) in enumerate(route):
                body += (
                    f"BIND({_iri(p)} AS ?p{i}) BIND({'true' if backward else 'false'} AS ?back{i}) "
                )
            for i in range(hops + 1):
                for j in range(i):
                    body += f"FILTER(!sameTerm(?n{i}, ?n{j})) "
            queries[hops].append(body)
        for hops, bodies in sorted(queries.items()):
            for start in range(0, len(bodies), client.batch_size):
                body = " UNION ".join(
                    "{ " + part + " }" for part in bodies[start : start + client.batch_size]
                )
                variables = " ".join(
                    [f"?n{i}" for i in range(hops + 1)] + [f"?p{i} ?back{i}" for i in range(hops)]
                )
                bindings = client._select(
                    f"SELECT DISTINCT {variables} ?_graph WHERE {{ "
                    + client._scope(body)
                    + f" }} LIMIT {client.max_rows + 1}"
                )
                if len(bindings) > client.max_rows:
                    raise HydrationLimitError("Path lookup exceeded max_rows")
                for binding in bindings:
                    key = json.dumps(binding, sort_keys=True)
                    if any(term["type"] == "bnode" for term in binding.values()):
                        key = f"{len(client._records())}:{key}"
                    if key in seen:
                        continue
                    seen.add(key)
                    routes.append(
                        {"hops": hops, "bindings": binding, "query_id": len(client._records())}
                    )
                    if len(routes) > max_paths:
                        raise HydrationLimitError(
                            "Too many connections; reduce max_hops or raise max_paths"
                        )
    table = resource_path_table(client, routes, max_hops)
    table.attrs.update(
        source_class=source,
        source_iri=source_iri,
        target_value=value,
        target_class=target_class,
        basis="queried mined class routes",
    )
    return table
