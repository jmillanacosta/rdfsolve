"""Applications: a rule run on records of a source release, in batches.

An application records what was run on what (PROV): the rule, the source release and the
mapping sets used, and for each batch the records it bound and the query sent. Whether each
batch was answered completely is a quality measurement (W3C DQV) of the completeness dimension
(Zaveri et al. 2016), so that silence is read only where the answer was complete. A batch that
fails is recorded as incomplete; the results of the others are kept.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rdflib import PROV, RDF, SKOS, Graph, Literal, Namespace, URIRef

from rdfsolve.config import mint
from rdfsolve.mining.query_fallbacks import select_outcome
from rdfsolve.reconciliation.rules import Rule
from rdfsolve.sparql_helper import SparqlHelper, SparqlHelperError

if TYPE_CHECKING:
    from nanopub.nanopub import Nanopub

__all__ = ["Application", "Batch", "apply"]

DQV = Namespace("http://www.w3.org/ns/dqv#")
LDQD = Namespace("http://www.w3.org/2016/05/ldqd#")
COMPLETE = URIRef(mint("metric", "batch-answered-completely"))


@dataclass(frozen=True)
class Batch:
    """The records one query bound, the query sent, and whether it was answered completely."""

    records: tuple[str, ...]
    query: str
    complete: bool
    error: str = ""


@dataclass
class Application:
    """A rule run on records of a source release, with the mapping sets it read."""

    rule: Rule
    source: str
    release: str
    mapping_sets: tuple[str, ...]
    batches: list[Batch] = field(default_factory=list)

    @property
    def iri(self) -> str:
        """Return the IRI of the application: it follows from the rule, release and records."""
        key = "\t".join(
            [
                self.rule.iri,
                self.release,
                *self.mapping_sets,
                *(r for b in self.batches for r in b.records),
            ]
        )
        return mint("application", hashlib.sha256(key.encode()).hexdigest()[:24])

    @property
    def complete(self) -> bool:
        """Return whether every batch was answered completely."""
        return all(b.complete for b in self.batches)

    def to_graph(self) -> Graph:
        """Return the application in PROV, with a DQV measurement of each batch."""
        graph = Graph()
        activity = URIRef(self.iri)
        graph.add((activity, RDF.type, PROV.Activity))
        for used in (self.rule.iri, self.release, *self.mapping_sets):
            graph.add((activity, PROV.used, URIRef(used)))
        graph.add((URIRef(self.release), PROV.specializationOf, URIRef(self.source)))
        graph.add((COMPLETE, RDF.type, DQV.Metric))
        graph.add((COMPLETE, DQV.inDimension, LDQD.completeness))
        graph.add(
            (COMPLETE, SKOS.definition, Literal("Every query of the batch was answered in full"))
        )
        for number, batch in enumerate(self.batches, 1):
            entity = URIRef(f"{self.iri}/batch/{number}")
            graph.add((entity, RDF.type, PROV.Collection))
            graph.add((entity, PROV.wasGeneratedBy, activity))
            graph.add((entity, PROV.value, Literal(batch.query)))
            for record in batch.records:
                graph.add((entity, PROV.hadMember, URIRef(record)))
            measurement = URIRef(f"{entity}/completeness")
            graph.add((measurement, RDF.type, DQV.QualityMeasurement))
            graph.add((measurement, DQV.computedOn, entity))
            graph.add((measurement, DQV.isMeasurementOf, COMPLETE))
            graph.add((measurement, DQV.value, Literal(batch.complete)))
        return graph

    def nanopublication(self, *, attributed_to: str, created: str) -> Nanopub:
        """Return the application as an unsigned nanopublication."""
        from rdfsolve.reconciliation.nanopubs import nanopublication

        return nanopublication(
            self.to_graph(), kinds=[PROV.Activity], attributed_to=attributed_to, created=created
        )


def apply(
    rule: Rule,
    records: Sequence[str],
    helper: SparqlHelper,
    *,
    source: str,
    release: str,
    mapping_sets: Sequence[str] = (),
    batch_size: int = 100,
) -> tuple[Any, Application]:
    """Run *rule* on *records* with *helper*, in batches, and return its result and application.

    The result is a Graph for a conversion and the SPARQL JSON rows for a check, from the
    batches that were answered.
    """
    application = Application(rule, source, release, tuple(mapping_sets))
    result: Any = Graph() if rule.form == "construct" else []
    for start in range(0, len(records), batch_size):
        batch = tuple(records[start : start + batch_size])
        query = rule.bound(batch)
        if rule.form == "construct":
            try:
                result += helper.construct_graph(query)
                application.batches.append(Batch(batch, query, True))
            except SparqlHelperError as error:
                application.batches.append(Batch(batch, query, False, str(error)))
            continue
        outcome = select_outcome(query, f"rule/{rule.iri}", helper)
        result.extend(outcome.rows)
        reason = "; ".join(f.message for f in outcome.failures)
        application.batches.append(Batch(batch, query, outcome.state == "complete", reason))
    return result, application
