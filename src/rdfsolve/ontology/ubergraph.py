"""UberGraph: about 60 OBO ontologies merged and reasoned (INCATools, RENCI).

Its named graphs (https://github.com/INCATools/ubergraph): ontology (the merged axioms and
labels), redundant (the closure of subclass and existential relations), nonredundant (the
same relations without the redundant edges) and the Biolink model with term categories. In
the relation graphs X R Y means X SubClassOf (R some Y); rdfs:subClassOf is a named
superclass.

Biolink categories follow OBO class semantics: a category of a term says what an instance of
the term is. Every NCBITaxon class is an AnatomicalEntity (an organism is a material anatomical
entity in COB), and none is an OrganismTaxon; read a category as the kind of an ontology term,
not as the kind of node an identifier names.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import Any

from rdfsolve.ontology.vocabulary import OWL, SUBCLASS_OF

UBERGRAPH = "https://ubergraph.apps.renci.org/sparql"
REDUNDANT = "http://reasoner.renci.org/redundant"
ONTOLOGY_GRAPH = "http://reasoner.renci.org/ontology"
NONREDUNDANT = "http://reasoner.renci.org/nonredundant"
BIOLINK_GRAPH = "https://biolink.github.io/biolink-model/"
BIOLINK = "https://w3id.org/biolink/vocab/"
CATEGORY = BIOLINK + "category"
IS_A = "https://w3id.org/linkml/is_a"
MIXINS = "https://w3id.org/linkml/mixins"

Select = Callable[[str], list[dict[str, Any]]]


def _values(terms: Iterable[str]) -> str:
    return " ".join(f"<{term}>" for term in terms)


class UberGraph:
    """Ancestors, descendants, relations and Biolink categories of OBO terms from UberGraph.

    *select* runs a SPARQL SELECT and returns its bindings; by default a SparqlHelper on
    *endpoint* (the public endpoint, or a local index of the UberGraph files).
    """

    def __init__(
        self,
        select: Select | None = None,
        *,
        endpoint: str = UBERGRAPH,
        batch_size: int = 200,
        timeout: float = 60,
    ) -> None:
        """Configure the source; no request is made before a question."""
        self.endpoint = endpoint
        self.batch_size = batch_size
        self._select = select
        self._helper: Any = None
        self.timeout = timeout

    def select(self, query: str) -> list[dict[str, Any]]:
        """Return the bindings of a SELECT query."""
        if self._select is not None:
            return self._select(query)
        if self._helper is None:
            from rdfsolve.sparql_helper import SparqlHelper

            self._helper = SparqlHelper(self.endpoint, timeout=self.timeout, max_retries=1)
        rows: list[dict[str, Any]] = self._helper.select(query, purpose="ontology/ubergraph")[
            "results"
        ]["bindings"]
        return rows

    def close(self) -> None:
        """Release the connection."""
        if self._helper is not None:
            self._helper.close()

    def _pairs(self, terms: Iterable[str], pattern: str) -> dict[str, set[str]]:
        """Return ?t -> {?x} for each batch of terms bound to ?t in *pattern*."""
        found: dict[str, set[str]] = defaultdict(set)
        ordered = sorted(set(terms))
        for start in range(0, len(ordered), self.batch_size):
            batch = ordered[start : start + self.batch_size]
            query = f"SELECT ?t ?x WHERE {{ VALUES ?t {{ {_values(batch)} }} {pattern} }}"
            for row in self.select(query):
                found[row["t"]["value"]].add(row["x"]["value"])
        return {term: found.get(term, set()) for term in ordered}

    def known(self, terms: Iterable[str]) -> set[str]:
        """Return the terms that UberGraph holds as classes; it holds OBO PURL IRIs only.

        An answer about a term it does not hold is empty, which must not be read as a term
        without ancestors (EFO, EDAM, SIO terms are asked of OLS instead).
        """
        held: set[str] = set()
        ordered = sorted(set(terms))
        for start in range(0, len(ordered), self.batch_size):
            batch = ordered[start : start + self.batch_size]
            query = (
                f"SELECT DISTINCT ?t WHERE {{ VALUES ?t {{ {_values(batch)} }} "
                f"GRAPH <{ONTOLOGY_GRAPH}> {{ ?t a <{OWL}Class> }} }}"
            )
            held |= {row["t"]["value"] for row in self.select(query)}
        return held

    def parents(self, terms: Iterable[str]) -> dict[str, set[str]]:
        """Return the direct named superclasses of each term."""
        return self._pairs(
            terms,
            f"GRAPH <{NONREDUNDANT}> {{ ?t <{SUBCLASS_OF}> ?x }} FILTER(isIRI(?x) && ?x != ?t)",
        )

    def ancestors(self, terms: Iterable[str]) -> dict[str, set[str]]:
        """Return all named superclasses of each term (the reasoner's closure), without itself."""
        return self._pairs(
            terms,
            f"GRAPH <{REDUNDANT}> {{ ?t <{SUBCLASS_OF}> ?x }} "
            f"FILTER(isIRI(?x) && ?x != ?t && ?x != <{OWL}Thing>)",
        )

    def descendants(self, term: str) -> set[str]:
        """Return all named subclasses of a term (the reasoner's closure), without itself."""
        query = (
            f"SELECT ?x WHERE {{ GRAPH <{REDUNDANT}> {{ ?x <{SUBCLASS_OF}> <{term}> }} "
            f"FILTER(isIRI(?x) && ?x != <{term}>) }}"
        )
        return {row["x"]["value"] for row in self.select(query)}

    def relations(self, terms: Iterable[str]) -> list[tuple[str, str, str]]:
        """Return the existential relations of each term: (term, property, filler).

        (X, R, Y) means X SubClassOf (R some Y); the nonredundant edges are returned.
        """
        out: list[tuple[str, str, str]] = []
        ordered = sorted(set(terms))
        for start in range(0, len(ordered), self.batch_size):
            batch = ordered[start : start + self.batch_size]
            query = (
                f"SELECT ?t ?p ?x WHERE {{ VALUES ?t {{ {_values(batch)} }} "
                f"GRAPH <{NONREDUNDANT}> {{ ?t ?p ?x }} "
                f"FILTER(isIRI(?x) && ?p != <{SUBCLASS_OF}>) }}"
            )
            out += [(r["t"]["value"], r["p"]["value"], r["x"]["value"]) for r in self.select(query)]
        return sorted(set(out))

    def between(
        self, terms: Iterable[str], predicates: Iterable[str]
    ) -> list[tuple[str, str, str]]:
        """Return the relations among *terms* by *predicates*, from the closure (both ways)."""
        ordered = sorted(set(terms))
        if len(ordered) < 2:
            return []
        query = (
            f"SELECT ?t ?p ?x WHERE {{ VALUES ?t {{ {_values(ordered)} }} "
            f"VALUES ?x {{ {_values(ordered)} }} VALUES ?p {{ {_values(predicates)} }} "
            f"GRAPH <{REDUNDANT}> {{ ?t ?p ?x }} FILTER(?t != ?x) }}"
        )
        return sorted(
            {(r["t"]["value"], r["p"]["value"], r["x"]["value"]) for r in self.select(query)}
        )

    def categories(
        self, terms: Iterable[str], *, most_specific: bool = True
    ) -> dict[str, set[str]]:
        """Return the Biolink categories of each term.

        UberGraph gives every category up to the root (GO_0006915 has nine); with
        *most_specific* only the categories that no other category of the term is under are
        kept, by the is_a and mixins links of the Biolink model in the same graph.
        """
        found = self._pairs(terms, f"GRAPH <{BIOLINK_GRAPH}> {{ ?t <{CATEGORY}> ?x }}")
        if not most_specific:
            return found
        above = self._category_ancestors({c for cs in found.values() for c in cs})
        return {
            term: {c for c in cs if not any(c in above.get(other, set()) for other in cs)}
            for term, cs in found.items()
        }

    def _category_ancestors(self, categories: set[str]) -> dict[str, set[str]]:
        """Return the categories above each Biolink category, by is_a and mixins."""
        if not categories:
            return {}
        query = (
            f"SELECT ?c ?p WHERE {{ GRAPH <{BIOLINK_GRAPH}> {{ ?c ?link ?p }} "
            f"VALUES ?link {{ <{IS_A}> <{MIXINS}> }} }}"
        )
        parent: dict[str, set[str]] = defaultdict(set)
        for row in self.select(query):
            parent[row["c"]["value"]].add(row["p"]["value"])
        out: dict[str, set[str]] = {}
        for category in categories:
            seen: set[str] = set()
            frontier = list(parent.get(category, ()))
            while frontier:
                current = frontier.pop()
                if current not in seen:
                    seen.add(current)
                    frontier.extend(parent.get(current, ()))
            out[category] = seen
        return out


__all__ = ["BIOLINK", "UBERGRAPH", "UberGraph"]
