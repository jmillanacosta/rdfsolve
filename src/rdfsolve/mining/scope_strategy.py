"""Mine the neighbourhood of chosen resources instead of the whole dataset.

The scope is a list of seed subjects, a sample of the members of chosen classes, and the
resources that chosen predicates reach from them in one hop. Every statement of the scope is
read. Resources are classified by rdf:type, by a membership property when the data uses one
(P31, "instance of", on Wikidata), and by the class that an IRI prefix implies, for stores
that leave out these type statements. The schema is small and enough for a client that reads
these resources; a new run shows when their statements change.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from rdflib import Literal, URIRef

from rdfsolve._outcomes import QueryFailure, QueryOutcome
from rdfsolve.mining.query_fallbacks import select_outcome
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.models import SchemaPattern
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, TermAnnotation

LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
BATCH = 50
Key = tuple[str, str, str, str | None]


class ScopeStrategy(MiningStrategy):
    """Every statement of seed subjects, class samples and one hop along chosen predicates."""

    scoped = True

    def __init__(
        self,
        subjects: Iterable[str] = (),
        classes: Iterable[str] = (),
        follow: Iterable[str] = (),
        *,
        window: int = 1000,
        membership: str | None = None,
        prefix_classes: Mapping[str, str] | None = None,
        language: str = "en",
    ) -> None:
        """Mine *subjects*, up to *window* members of each class, and what *follow* reaches.

        *membership* classifies resources in addition to rdf:type. *prefix_classes* gives
        the class of the IRIs that start with a prefix (the Wikidata Query Service leaves
        out wikibase:Item and wikibase:Statement). Labels are read in *language*.
        """
        self.subjects = list(dict.fromkeys(subjects))
        self.classes = list(dict.fromkeys(classes))
        self.follow = list(dict.fromkeys(follow))
        self.window = window
        self.membership_property = membership
        self.prefix_classes = dict(prefix_classes or {})
        self.language = language
        self.examples: list[PatternExample] = []
        self.labels: list[TermAnnotation] = []

    @property
    def name(self) -> str:
        """Return the strategy name for reporting."""
        return "scope"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Read every statement of the scope, classified by type, membership and IRI prefix."""
        links = [RDF_TYPE, *([self.membership_property] if self.membership_property else [])]
        classify = "|".join(f"<{link}>" for link in links)
        scope: dict[str, Any] = {"subjects": len(self.subjects), "classes": {}}
        scope.update(follow=self.follow, followed=0, unclassified_subjects=0)
        context.report.report.config["scope"] = scope
        seeds, sampled = list(self.subjects), []
        for cls in self.classes:
            query = (
                f"SELECT DISTINCT ?s {self._dataset(context)} WHERE {{ ?s {classify} <{cls}> "
                f"FILTER(isIRI(?s)) }} LIMIT {self.window}"
            )
            members = [r["s"]["value"] for r in self._select(context, query, "scope/members")]
            state = "sampled" if len(members) >= self.window else "complete"
            scope["classes"][cls] = {"members": len(members), "state": state}
            if state == "sampled":
                sampled.append(cls)
            seeds += members
        seeds = list(dict.fromkeys(seeds))
        reached: list[str] = []
        predicates = " ".join(URIRef(p).n3() for p in self.follow)
        for batch in _batches(seeds) if self.follow else []:
            query = (
                f"SELECT DISTINCT ?o {self._dataset(context)} WHERE {{ VALUES ?s {{ {batch} }} "
                f"VALUES ?p {{ {predicates} }} ?s ?p ?o FILTER(isIRI(?o)) }}"
            )
            reached += [r["o"]["value"] for r in self._select(context, query, "scope/follow")]
        followed = [iri for iri in dict.fromkeys(reached) if iri not in set(seeds)]
        scope["followed"] = len(followed)
        subjects = seeds + followed
        classes = {iri: set(self._implied(iri)) for iri in subjects}
        values = " ".join(f"<{link}>" for link in links)
        for batch in _batches(subjects):
            query = (
                f"SELECT ?s ?c {self._dataset(context)} WHERE {{ VALUES ?s {{ {batch} }} "
                f"VALUES ?link {{ {values} }} ?s ?link ?c FILTER(isIRI(?c)) }}"
            )
            for row in self._select(context, query, "scope/classes"):
                classes[row["s"]["value"]].add(row["c"]["value"])
        patterns: dict[Key, SchemaPattern] = {}
        unclassified: set[str] = set()
        for batch in _batches(subjects):
            for row in self._select(context, self._query(context, batch), "scope/statements"):
                subject = row["s"]["value"]
                if classes[subject]:
                    self._add(patterns, sorted(classes[subject]), row)
                else:
                    unclassified.add(subject)
        scope["unclassified_subjects"] = len(unclassified)
        named = {c for key in patterns for c in (key[0], key[2])}
        found = self.predicate_labels(context, {key[1] for key in patterns})
        found.update(self._labels(context, named - {"Literal", "BlankNode", "Resource"}))
        self.labels += [
            TermAnnotation(
                term_iri=iri,
                predicate=LABEL,
                text=RdfTerm(kind="literal", value=label, language=self.language),
            )
            for iri, label in sorted(found.items())
        ]
        if sampled:
            failure = QueryFailure(
                "sampled",
                f"{len(sampled)} classes filled their sample of {self.window} members; "
                "other members may add rows.",
                "scope/members",
                sampled,
            )
            context.report.record_outcome(QueryOutcome(state="partial", failures=[failure]))
        return list(patterns.values())

    def predicate_labels(self, context: MiningContext, predicates: set[str]) -> dict[str, str]:
        """Return the label of each predicate, as the data states it."""
        return self._labels(context, predicates)

    def _add(
        self, patterns: dict[Key, SchemaPattern], classes: list[str], row: dict[str, Any]
    ) -> None:
        """Add the rows of one grouped statement result, with one example each."""
        kind = row["kind"]["value"]
        datatype = row.get("dt", {}).get("value") if kind == "literal" else None
        objects = {"literal": ["Literal"], "bnode": ["BlankNode"]}.get(kind) or [
            row[key]["value"] for key in ("ot", "om", "on") if key in row
        ]
        for subject_class in classes:
            for object_class in objects or ["Resource"]:
                key = (subject_class, row["p"]["value"], object_class, datatype)
                if key in patterns:
                    continue
                patterns[key] = SchemaPattern(
                    subject_class=subject_class,
                    property_uri=key[1],
                    object_class=object_class,
                    datatype=datatype,
                )
                self.examples.append(
                    PatternExample(
                        subject_class=subject_class,
                        property_uri=key[1],
                        subject=RdfTerm(kind="uri", value=row["s"]["value"]),
                        value=RdfTerm.model_validate(_term(row["value"])),
                    )
                )

    def _implied(self, iri: str) -> list[str]:
        """Return the class of the longest prefix that the IRI starts with."""
        found = [p for p in self.prefix_classes if iri.startswith(p)]
        return [self.prefix_classes[max(found, key=len)]] if found else []

    def _query(self, context: MiningContext, batch: str) -> str:
        """Group the statements of the subjects by predicate, value kind and value classes."""
        member = self.membership_property
        extra = f"OPTIONAL {{ ?o <{member}> ?om }} " if member else ""
        if self.prefix_classes:
            chain = "?none"  # Never bound: an IRI with no listed prefix gets no implied class.
            for prefix, cls in sorted(self.prefix_classes.items(), key=lambda item: len(item[0])):
                chain = f"IF(STRSTARTS(STR(?o), {Literal(prefix).n3()}), <{cls}>, {chain})"
            extra += f"BIND(IF(isIRI(?o), {chain}, ?none) AS ?on) "
        return (
            f"SELECT ?s ?p ?ot ?om ?on ?kind ?dt (SAMPLE(?o) AS ?value) {self._dataset(context)} "
            f"WHERE {{ VALUES ?s {{ {batch} }} ?s ?p ?o FILTER(?p != <{RDF_TYPE}>) "
            f"OPTIONAL {{ ?o <{RDF_TYPE}> ?ot }} {extra}"
            'BIND(IF(isLiteral(?o), "literal", IF(isBlank(?o), "bnode", "uri")) AS ?kind) '
            "BIND(DATATYPE(?o) AS ?dt) } GROUP BY ?s ?p ?ot ?om ?on ?kind ?dt"
        )

    def _labels(self, context: MiningContext, terms: set[str]) -> dict[str, str]:
        """Return the rdfs:label of each term in the language of the strategy."""
        language = Literal(self.language).n3()
        found: dict[str, str] = {}
        for batch in _batches(sorted(terms)):
            query = (
                f"SELECT ?term ?label {self._dataset(context)} WHERE {{ VALUES ?term {{ {batch} }} "
                f"?term <{LABEL}> ?label FILTER(LANG(?label) = {language}) }}"
            )
            for row in self._select(context, query, "scope/labels"):
                found[row["term"]["value"]] = row["label"]["value"]
        return found

    def _dataset(self, context: MiningContext) -> str:
        """Return the FROM clauses of the data graphs, if the miner selected graphs."""
        return " ".join(f"FROM <{graph}>" for graph in context.graph_uris or [])

    def _select(self, context: MiningContext, query: str, purpose: str) -> list[dict[str, Any]]:
        """Return the rows of a query; record a failed query and give no rows."""
        outcome = select_outcome(query, purpose, context.helper, [])
        if outcome.state != "complete":
            context.report.record_outcome(outcome)
            return []
        return outcome.rows


def _batches(iris: list[str]) -> Iterator[str]:
    """Yield the IRIs as SPARQL terms, in groups that fit in one VALUES block."""
    for start in range(0, len(iris), BATCH):
        yield " ".join(URIRef(iri).n3() for iri in iris[start : start + BATCH])


def _term(binding: dict[str, Any]) -> dict[str, Any]:
    """Return the RDF term fields of a SPARQL JSON binding."""
    kind = "literal" if binding["type"] in ("literal", "typed-literal") else binding["type"]
    return {
        "kind": kind,
        "value": binding["value"],
        "datatype": binding.get("datatype"),
        "language": binding.get("xml:lang"),
    }
