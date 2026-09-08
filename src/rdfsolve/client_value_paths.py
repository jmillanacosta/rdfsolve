"""Verify mined class routes against named records."""

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
    value: str,
    *,
    max_hops: int,
    both_directions: bool,
    max_paths: int,
) -> pd.DataFrame:
    """Find observed paths along simple mined class routes, within one graph."""
    _budget(max_hops, max_paths)
    if not value.strip():
        raise ValueError("Enter a word or name for target_value")
    predicates = " ".join(_iri(p) for p in sorted(SEARCH_PREDICATES))
    models = {str(getattr(model, "rdf_class_iri", "")) for model in client.models.values()}
    body = (
        f"{{ SELECT DISTINCT ?target WHERE {{ VALUES ?labelPredicate {{ {predicates} }} "
        f"?target ?labelPredicate ?label . FILTER(isIRI(?target) && !isBlank(?label) && "
        f"CONTAINS(LCASE(STR(?label)), LCASE({Literal(value).n3()}))) }} }} ?target a ?class"
    )
    routes: list[dict[str, Any]] = []
    seen: set[str] = set()
    with client.step(f"Find paths to {value}"):
        found = client._select(
            "SELECT DISTINCT ?target ?class ?_graph WHERE { "
            + client._scope(body)
            + f" }} LIMIT {client.max_rows + 1}"
        )
        if len(found) > client.max_rows:
            raise HydrationLimitError("Name lookup exceeded max_rows")
        targets: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for row in found:
            if row["class"]["value"] not in models:
                continue
            targets[row["class"]["value"]].add(
                (row["target"]["value"], row.get("_graph", {}).get("value", ""))
            )
        candidates: list[tuple[list[tuple[str, str, str, bool]], set[tuple[str, str]]]] = []
        for target_class in sorted(targets):
            if target_class == source:
                continue
            table = class_paths(
                client,
                source,
                target_class,
                max_hops=max_hops,
                both_directions=both_directions,
                max_paths=max_paths,
            )
            candidates.extend(
                (route, targets[target_class])
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
            if client.graph_uris:
                values = " ".join(f"({_iri(iri)} {_iri(graph)})" for iri, graph in sorted(matches))
                body = f"VALUES (?n{hops} ?_graph) {{ {values} }} "
            else:
                values = " ".join(_iri(iri) for iri, _ in sorted(matches))
                body = f"VALUES ?n{hops} {{ {values} }} "
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
    table.attrs.update(source_class=source, target_value=value, basis="queried mined class routes")
    return table
