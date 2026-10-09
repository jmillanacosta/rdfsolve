"""Measure the relations between the member sets of the classes of a schema (QLever).

One grouped query for each class counts the members it shares with every class. The classes
are measured from the smallest; those left after the time budget are recorded as not checked.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from rdfsolve.mining.query_builders import _build_class_overlap_query
from rdfsolve.schema_models.class_extensions import ClassExtensions, relate
from rdfsolve.sparql_helper import EndpointError

# Seconds for the queries of all classes.
BUDGET_S = 3600.0


def measure_class_extensions(
    helper: Any,
    classes: list[str],
    sizes: dict[str, int],
    graph_uris: list[str] | None,
    type_context_graph_uris: list[str] | None,
    record: Callable[[str, float, bool], None] | None = None,
) -> ClassExtensions | None:
    """Return the relations of *classes*, or None on an engine other than QLever.

    *record* receives the purpose, time and success of each query (the mining report).
    """
    if str(getattr(helper, "sparql_engine", "")).lower() != "qlever":
        return None
    wanted = set(classes)
    overlaps: dict[str, dict[str, int]] = {}
    not_checked: dict[str, str] = {}
    started = time.monotonic()
    for cls in sorted(classes, key=lambda c: (sizes.get(c, 0), c)):
        if getattr(cls, "members", None):
            not_checked[str(cls)] = "a group of ontology terms"
            continue
        if time.monotonic() - started > BUDGET_S:
            not_checked[str(cls)] = f"time budget of {BUDGET_S:.0f} s"
            continue
        query = _build_class_overlap_query(str(cls), graph_uris, type_context_graph_uris)
        asked = time.monotonic()
        try:
            rows = helper.select(query, purpose="class-extensions")["results"]["bindings"]
        except (EndpointError, ValueError) as error:
            if record:
                record("class-extensions", time.monotonic() - asked, False)
            not_checked[str(cls)] = f"refused: {error}"
            continue
        if record:
            record("class-extensions", time.monotonic() - asked, True)
        overlaps[str(cls)] = {
            row["other"]["value"]: int(row["n"]["value"])
            for row in rows
            if "other" in row and row["other"]["value"] in wanted
        }
    found = relate(overlaps)
    found.not_checked = dict(sorted(not_checked.items()))
    return found
