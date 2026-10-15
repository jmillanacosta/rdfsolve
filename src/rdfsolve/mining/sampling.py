"""Answer a refused query over a bounded sample instead of losing its rows.

An endpoint refuses some queries as a whole: a time or cost limit, a gateway that cuts the
response (STRING: HTTP 502 after about 540 s for the 354 M triples of one property; med2rdf:
502 at a fixed 120 s), or a row cap. When every other fallback of a query (smaller batches,
one property at a time, pages) is refused too, the same question is asked over a bounded
sample: the first *N* edges of the property (a sub-select ``{ SELECT ?s ?o WHERE { … } LIMIT N }``)
or the first *N* members of the class. The rows of a sample that the endpoint answers stand:
the patterns they show exist, and every count they give is a lower bound. Their rows carry the
provenance of the sample (SAMPLE_KEY), the patterns made from them are flagged sampled with
count_bound lower_bound, and the report lists the sampled queries (not as failures). A sample
of *N* that is refused too is tried at N/10 and N/100; when every sample is refused, the
refusal stands as before.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Literal

from rdfsolve._outcomes import SAMPLE_KEY, Bindings, QueryFailure, QueryOutcome, QuerySample

if TYPE_CHECKING:
    from rdfsolve.schema_models.pattern import SchemaPattern
    from rdfsolve.schema_models.structural import StructuralPattern
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_SAMPLE_SIZE",
    "REFUSALS",
    "SAMPLE_SIZE",
    "accepts_sample",
    "flag",
    "mark_sampled",
    "refusal",
    "sample_of",
    "sample_query",
    "sample_size",
    "sample_sizes",
    "sampled_select",
    "with_sample",
]

# Edges (or members) of the first sample of a refused query.
DEFAULT_SAMPLE_SIZE = 100_000
# The sample size of the mining run; 0 turns sampling off (set by sample_size()).
SAMPLE_SIZE: ContextVar[int] = ContextVar("sample_size", default=DEFAULT_SAMPLE_SIZE)
# Categories of a refusal: a time or cost limit or a gateway cut (timeout), or a row cap or a
# page that failed (truncated). Not an invalid query, a rate limit or an endpoint that is down.
REFUSALS = frozenset({"timeout", "truncated"})
# The smallest sample tried after a refused one.
SMALLEST_SAMPLE = 1_000

Covers = Literal["patterns", "counts"]


@contextmanager
def sample_size(size: int | None) -> Iterator[None]:
    """Use *size* as the first sample of a refused query in the block (0: no samples)."""
    token = SAMPLE_SIZE.set(DEFAULT_SAMPLE_SIZE if size is None else max(int(size), 0))
    try:
        yield
    finally:
        SAMPLE_SIZE.reset(token)


def sample_sizes() -> list[int]:
    """Return the sizes to try, largest first: N, N/10 and N/100, not below SMALLEST_SAMPLE."""
    first = SAMPLE_SIZE.get()
    if first <= 0:
        return []
    floor = min(first, SMALLEST_SAMPLE)
    return list(dict.fromkeys(max(first // step, floor) for step in (1, 10, 100)))


def refusal(outcome: QueryOutcome) -> QueryFailure | None:
    """Return the refusal of an outcome that is not complete, or None when it is not refused."""
    if outcome.state == "complete":
        return None
    return next((f for f in outcome.failures if f.category in REFUSALS), None)


def with_sample(rows: Bindings, sample: QuerySample) -> Bindings:
    """Return the rows of a sample, each with the provenance of the sample."""
    provenance = sample.provenance()
    return [{**row, SAMPLE_KEY: provenance} for row in rows]


def accepts_sample(builder: Callable[..., str]) -> bool:
    """Return whether a query builder can bound its edges or members to a sample."""
    import inspect

    try:
        return "sample" in inspect.signature(builder).parameters
    except (TypeError, ValueError):
        return False


def sample_query(query: str, size: int) -> str:
    """Bound a grouped query to the first *size* solutions of its WHERE group.

    ``SELECT … WHERE { body } GROUP BY …`` becomes ``SELECT … WHERE { { SELECT * WHERE { body }
    LIMIT size } } GROUP BY …``: the aggregates count the solutions of the sample. For a query
    whose WHERE clause is one group (the count queries of ontology terms); the ORDER BY of a
    paged query is dropped.
    """
    start = query.index("WHERE {") + len("WHERE {")
    group = query.rfind("GROUP BY")
    end = query.rindex("}", 0, group if group != -1 else len(query))
    tail = query[end + 1 :]
    order = tail.find("ORDER BY")
    if order != -1:
        tail = tail[:order].rstrip() + "\n"
    return (
        f"{query[: start - 1]}{{ {{ SELECT * WHERE {{{query[start:end]}}} LIMIT {size} }} }}{tail}"
    )


def sample_of(row: dict[str, Any]) -> dict[str, Any] | None:
    """Return the provenance of the sample that answered a row, or None."""
    found = row.get(SAMPLE_KEY)
    return found if isinstance(found, dict) else None


def sampled_select(
    build: Callable[[int], str],
    purpose: str,
    helper: SparqlHelper,
    refused: QueryOutcome,
    *,
    unit: str,
    classes: Sequence[str] = (),
    graph_uris: list[str] | None = None,
    property_uri: str | None = None,
) -> QueryOutcome:
    """Ask a refused query again over samples built by *build(size)*.

    Return *refused* unchanged when it is not a refusal or when every sample is refused (the
    item stays a failure or a gap, as before). Otherwise the outcome is complete: the rows of
    the sample, which carry its provenance, then the rows that the refused query read (pages
    before a cut), and the sample is recorded in ``samples``.
    """
    from rdfsolve.mining.query_fallbacks import select_outcome

    cause = refusal(refused)
    if cause is None:
        return refused
    for size in sample_sizes():
        found = select_outcome(build(size), f"{purpose}/sample", helper, list(classes), graph_uris)
        if found.state == "complete":
            sample = QuerySample(
                purpose,
                size,
                unit,
                cause.category,
                cause.message[:500],
                list(classes),
                property_uri,
                graph_uris,
            )
            logger.info(
                "%s: refused (%s); answered over a sample of %d %s (%d rows)",
                purpose,
                cause.category,
                size,
                unit,
                len(found.rows),
            )
            # The rows read before a cut come last: a count row of a page is exact, and a
            # reader that keeps the last row of a group keeps it.
            return QueryOutcome(
                [*with_sample(found.rows, sample), *refused.rows],
                "complete",
                gaps=[*refused.gaps, *found.gaps],
                samples=[*refused.samples, sample],
            )
        if refusal(found) is None:
            break
    return refused


def mark_sampled(
    patterns: list[SchemaPattern],
    rows: Bindings,
    covers: Sequence[Covers] = ("patterns",),
    class_variable: str = "class",
) -> None:
    """Flag the patterns whose subject class and property a sampled row shows."""
    found: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        provenance = sample_of(row)
        if provenance is not None:
            key = (
                row.get(class_variable, {}).get("value", ""),
                row.get("p", {}).get("value", ""),
            )
            found.setdefault(key, provenance)
    if not found:
        return
    for pattern in patterns:
        provenance = found.get((pattern.subject_class, pattern.property_uri))
        if provenance is not None:
            flag(pattern, provenance, covers)


def flag(
    pattern: SchemaPattern | StructuralPattern, provenance: dict[str, Any], covers: Sequence[Covers]
) -> None:
    """Flag one pattern as sampled for *covers*, keeping what an earlier sample covered."""
    from rdfsolve.schema_models.pattern import PatternSample

    earlier = list(pattern.sampled.covers) if pattern.sampled is not None else []
    merged: list[Covers] = [c for c in ("patterns", "counts") if c in earlier or c in covers]
    pattern.sampled = PatternSample(
        size=int(provenance["size"]),
        unit=str(provenance["unit"]),
        reason=str(provenance["reason"]),
        covers=merged,
    )
    if "counts" in merged:
        pattern.count_bound = "lower_bound"
