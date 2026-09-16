"""Evaluate generated paths against retained sets of typed resources."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from rdfsolve.client.hydration import HydrationLimitError, _iri
from rdfsolve.client.paths import _budget, class_paths, resource_path_table
from rdfsolve.schema_models.enrichment import RdfTerm

if TYPE_CHECKING:
    from rdfsolve.client.api import Client


def value_paths(
    client: Client,
    sources,
    targets,
    *,
    max_hops: int,
    both_directions: bool,
    max_paths: int,
    allow_partial: bool = False,
    allow_repeated_classes: bool = False,
    meaning: str = "",
    via: tuple[str, ...] = (),
):
    """Query whole selected sets, preserving path witnesses and graph-local bindings."""
    _budget(max_hops, max_paths)
    fragments, observations, routes, warnings = [], [], [], []
    partial = False
    for source, source_iris in sources.items():
        for target, target_iris in targets.items():
            if source == target and not allow_repeated_classes:
                continue
            table = class_paths(
                client,
                source,
                target,
                max_hops=max_hops,
                both_directions=both_directions,
                max_paths=max_paths,
                allow_partial=allow_partial,
                allow_repeated_classes=allow_repeated_classes,
                meaning=meaning,
                via=via,
            )
            partial |= table.attrs["truncated"]
            warnings.extend(table.attrs["warnings"])
            for fragment in table.attrs["fragments"]:
                anchors = {
                    i: [RdfTerm(kind="uri", value=iri) for iri in iris]
                    for i, iris in ((0, source_iris), (-1, target_iris))
                    if iris is not None
                }
                fragments.append(replace(fragment, anchors=anchors))
    if len(fragments) > max_paths:
        if not allow_partial:
            raise HydrationLimitError("Class path budget exhausted; narrow the endpoint classes")
        fragments = fragments[:max_paths]
        partial = True
    observations = [{"status": "not_tested", "matches": 0, "query_ids": []} for _ in fragments]
    with client.step("Evaluate generated paths for selected resources"):
        for start in range(0, len(fragments), client.batch_size):
            bodies = []
            batch = fragments[start : start + client.batch_size]
            for index, fragment in enumerate(batch, start):
                hops = len(fragment.steps)
                pattern = fragment.render([f"?n{i}" for i in range(hops + 1)], lambda: "?_unused")
                bindings = [f"BIND({index} AS ?route)"]
                for i, (_, predicate, _, back) in enumerate(fragment.steps):
                    bindings += [
                        f"BIND({_iri(predicate)} AS ?p{i})",
                        f"BIND({'true' if back else 'false'} AS ?back{i})",
                    ]
                distinct = [
                    f"FILTER(!sameTerm(?n{i}, ?n{j}))" for i in range(hops + 1) for j in range(i)
                ]
                bodies.append("{ " + " ".join([pattern, *distinct, *bindings]) + " }")
            variables = [
                "?route",
                "?_graph",
                *[f"?n{i}" for i in range(max_hops + 1)],
                *[f"?p{i} ?back{i}" for i in range(max_hops)],
            ]
            limit = min(client.max_rows, max_paths - len(routes)) + 1
            found = client._select(
                f"SELECT DISTINCT {' '.join(variables)} WHERE {{ {client._scope(' UNION '.join(bodies))} }} LIMIT {limit}"
            )
            capped = len(found) >= limit
            if capped and not allow_partial:
                raise HydrationLimitError(
                    "Observed path budget exhausted; narrow endpoints or increase max_paths and max_rows"
                )
            query_id = len(client._records())
            for i in range(start, start + len(batch)):
                observations[i].update(
                    status="not_observed_in_sample" if capped else "no_match", query_ids=[query_id]
                )
            for binding in found[: limit - 1]:
                index = int(binding.pop("route")["value"])
                hops = len(fragments[index].steps)
                routes.append(
                    {"hops": hops, "bindings": binding, "query_id": query_id, "candidate": index}
                )
                observations[index]["status"] = "matched"
                observations[index]["matches"] += 1
            if capped:
                partial = True
                break
        if partial:
            client._steps[-1]["status"] = "partial"
    table = resource_path_table(client, routes, max_hops)
    table.attrs.update(
        fragments=fragments,
        observations=observations,
        warnings=list(dict.fromkeys(warnings)),
        basis="evaluated generated model paths",
        truncated=partial,
        status="partial" if partial else "complete",
        scope={"sources": sources, "targets": targets, "graphs": list(client.graph_uris)},
        source_class=next(iter(sources)) if len(sources) == 1 else None,
        target_class=next(iter(targets)) if len(targets) == 1 else None,
    )
    return table
