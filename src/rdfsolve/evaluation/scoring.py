"""Compare the rows of an answer with the rows of a reference, by term or by resource.

Level "term": a cell matches only the same RDF term (a plain and an xsd:string literal are
the same term). Level "resource": a cell matches when both cells show the same resource
(its IRI, a name, an identifier, a page or a key field of it), and numbers match by value.
Level "linked": as "resource", and resources linked by owl:sameAs or skos:exactMatch also
match. At each level, equal rows count once, and each answer row can match one reference
row: the score uses a maximum matching of rows. The fields that replace the fields of the
reference are counted at level "resource", for example "keid: label -> identifier".
"""

from __future__ import annotations

import re
from collections import Counter, deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from rdfsolve.evaluation.views import FIELD_ORDER, NUMBERS, Key, View, resolve, term_key

LEVELS = ("term", "resource", "linked")
Row = dict[str, dict[str, str]]
Cell = frozenset[Any]


@dataclass
class Score:
    """How well the rows of an answer agree with the rows of a reference."""

    level: str
    expected: int
    actual: int
    matched: int
    substitutions: Counter[str] = field(default_factory=Counter)

    @property
    def precision(self) -> float:
        """Give the part of the answer rows that match a reference row."""
        return self.matched / self.actual if self.actual else float(not self.expected)

    @property
    def recall(self) -> float:
        """Give the part of the reference rows that an answer row matches."""
        return self.matched / self.expected if self.expected else float(not self.actual)

    @property
    def f1(self) -> float:
        """Give the harmonic mean of precision and recall."""
        total = self.actual + self.expected
        return 2 * self.matched / total if total else 1.0

    @property
    def exact(self) -> bool:
        """Tell whether each answer row matches one reference row, and the reverse."""
        return self.matched == self.actual == self.expected


def _cell(term: dict[str, str] | None, views: dict[Key, frozenset[View]], level: str) -> Cell:
    """Give the keys that a cell can match."""
    if term is None:
        return frozenset({("unbound",)})
    key = term_key(term)
    if level != "term" and key[3] in NUMBERS:
        try:
            return frozenset({("number", Decimal(key[1]))})
        except InvalidOperation:
            pass
    shown = [v for v in views.get(key, ()) if level == "linked" or v.kind != "link"]
    if level != "term" and shown:
        return frozenset(("entity", view.entity) for view in shown)
    return frozenset({key})


def maximum_matching(edges: Sequence[Sequence[int]], right: int) -> dict[int, int]:
    """Give a maximum matching of a bipartite graph as left index to right index."""
    left_match = [-1] * len(edges)
    right_match = [-1] * right
    for start in range(len(edges)):
        parent: dict[int, int] = {}
        queue, found = deque([start]), -1
        while queue and found < 0:
            node = queue.popleft()
            for target in edges[node]:
                if target in parent:
                    continue
                parent[target] = node
                if right_match[target] < 0:
                    found = target
                    break
                queue.append(right_match[target])
        while found >= 0:
            node = parent[found]
            following = left_match[node]
            left_match[node], right_match[found] = found, node
            found = following if node != start else -1
    return {node: target for node, target in enumerate(left_match) if target >= 0}


def _short(iri: str) -> str:
    """Give the last part of a field IRI."""
    return iri if iri == "self" else re.split(r"[#/]", iri.rstrip("/#"))[-1]


def score(
    reference: Sequence[Row],
    answer: Sequence[Row],
    columns: Sequence[str],
    *,
    level: str = "term",
    views: dict[Key, frozenset[View]] | None = None,
) -> Score:
    """Score the rows of an answer against the rows of a reference at one level."""
    if level not in LEVELS:
        raise ValueError(f"Use one of the levels {LEVELS}")
    views = views or {}

    def rows(source: Sequence[Row]) -> dict[tuple[Cell, ...], Row]:
        """Give the distinct rows as tuples of cell keys, each with one example row."""
        found: dict[tuple[Cell, ...], Row] = {}
        for row in source:
            found.setdefault(tuple(_cell(row.get(c), views, level) for c in columns), row)
        return found

    wanted, given = rows(reference), rows(answer)
    targets = list(wanted)
    index: dict[Any, list[int]] = {}
    for j, cells in enumerate(targets):
        for key in cells[0] if cells else ():
            index.setdefault(key, []).append(j)
    edges = []
    for cells in given:
        candidates = {j for key in cells[0] for j in index.get(key, ())} if cells else set()
        edges.append(
            sorted(
                j for j in candidates if all(a & b for a, b in zip(cells, targets[j], strict=True))
            )
        )
    pairs = maximum_matching(edges, len(targets))
    result = Score(level, len(wanted), len(given), len(pairs))
    sources = list(given.values())
    for i, j in pairs.items():
        for column in columns:
            result.substitutions.update(
                _substitution(column, wanted[targets[j]], sources[i], views)
            )
    return result


def _substitution(
    column: str, reference: Row, answer: Row, views: dict[Key, frozenset[View]]
) -> list[str]:
    """Name the field of the reference and the other field of the answer in one column."""
    ref, ans = reference.get(column), answer.get(column)
    if ref is None or ans is None or term_key(ref) == term_key(ans):
        return []
    ref_views, ans_views = (
        [v for v in views.get(term_key(cell), ()) if v.kind != "link"] for cell in (ref, ans)
    )
    shared = {v.entity for v in ref_views} & {v.entity for v in ans_views}

    def best(found: list[View]) -> str:
        """Give the preferred field of the views of the shared resource."""
        fields = sorted(
            {v.field for v in found if v.entity in shared},
            key=lambda f: (FIELD_ORDER.get(f, 99), f),
        )
        return _short(fields[0]) if fields else "value"

    before, after = [best(ref_views)], [best(ans_views)]
    return [f"{column}: {before[0]} -> {after[0]}"]


def score_levels(
    reference: Sequence[Row],
    answer: Sequence[Row],
    columns: Sequence[str],
    select: Callable[[str], list[dict[str, Any]]],
    key_cache: dict[str, bool] | None = None,
) -> dict[str, Score]:
    """Resolve the views of all cells with select, and score at each level."""
    cells = [row[c] for rows in (reference, answer) for row in rows for c in columns if c in row]
    views = resolve(cells, select, key_cache=key_cache)
    return {level: score(reference, answer, columns, level=level, views=views) for level in LEVELS}
