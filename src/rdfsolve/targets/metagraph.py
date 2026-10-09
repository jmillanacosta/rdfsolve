"""A property-graph metagraph as a target model: node kinds and edge types between kinds.

The format lists node kinds, and edge types as (from kind, to kind, verb, direction) tuples. It
states no hierarchy, no identifier namespaces, no definitions, no qualifiers and no IRIs; edges
whose direction is "both" are undirected. Its terms get IRIs minted under rdfsolve's namespace,
so that the converted data still has an RDF view to check against.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from rdfsolve.target_model import KindInfo, RelationInfo, TargetModel

__all__ = ["Metagraph"]


class Metagraph(TargetModel):
    """A metagraph of node kinds and edge-type tuples (JSON)."""

    def __init__(self, name: str, kinds: list[str], edges: list[tuple[str, str, str, str]]) -> None:
        """Keep the node kinds and the edge types of the metagraph."""
        self.name, self.version = name, ""
        self._kinds = list(kinds)
        self._edges = [tuple(e) for e in edges]
        self.capabilities = frozenset(
            {"directed"} if any(e[3] != "both" for e in self._edges) else set()
        )
        from rdfsolve.config import mint

        # The format has no IRIs: its terms are minted under rdfsolve's namespace.
        self.prefix, self.base = name, mint("target", name) + "/"

    @classmethod
    def read(cls, path: str | Path, name: str | None = None) -> Metagraph:
        """Read a metagraph JSON file: metanode_kinds and metaedge_tuples."""
        data = json.loads(Path(path).read_text())
        return cls(name or Path(path).stem, data["metanode_kinds"], data["metaedge_tuples"])

    def _iri(self, kind: str, name: str) -> str:
        """Return the minted IRI of a term (the format has none)."""
        from rdfsolve.config import mint

        return mint("target", self.name, kind, name.replace(" ", "_"))

    def kinds(self) -> list[KindInfo]:
        """Return the node kinds; the format states nothing else about them."""
        return [KindInfo(k, self._iri("kind", k)) for k in self._kinds]

    def relations(self) -> list[RelationInfo]:
        """Return one relation per verb, with the kind pairs it is listed between."""
        pairs: dict[str, list[tuple[str, str]]] = defaultdict(list)
        directed: dict[str, bool] = defaultdict(bool)
        for source, target, verb, direction in self._edges:
            pairs[verb].append((source, target))
            directed[verb] |= direction != "both"
        out = []
        for verb, between in pairs.items():
            sources, targets = {a for a, _ in between}, {b for _, b in between}
            out.append(
                RelationInfo(
                    verb,
                    self._iri("relation", verb),
                    domain=next(iter(sources)) if len(sources) == 1 else None,
                    range=next(iter(targets)) if len(targets) == 1 else None,
                    pairs=tuple(between),
                    directed=directed[verb],
                )
            )
        return out
