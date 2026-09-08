"""Query source ontology axioms for used terms and their ancestors."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from rdfsolve.mining.query_builders import _graph_clause
from rdfsolve.schema_models.ontology import (
    DomainAssertion,
    InverseRelation,
    OntologyStructure,
    PropertyCharacteristic,
    RangeAssertion,
    SubClassRelation,
)

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)
_PREFIXES = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"""


class OntologyMiner:
    """Query selected RDFS/OWL axioms, not a complete OWL ontology."""

    def __init__(
        self,
        helper: SparqlHelper,
        graph_uris: list[str] | None = None,
        *,
        class_iris: list[str] | None = None,
        property_iris: list[str] | None = None,
        batch_size: int = 50,
        delay: float = 0.0,
    ) -> None:
        """Use None for unrestricted terms; an empty list selects none."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.helper = helper
        self.graph_uris = graph_uris
        self.class_iris = class_iris
        self.property_iris = property_iris
        self.batch_size = batch_size
        self.delay = delay
        self._last_request = 0.0

    def _query(
        self,
        variables: str,
        body: str,
        purpose: str,
        scope_variable: str,
        iris: list[str] | None,
    ) -> Iterator[dict[str, Any]]:
        """Run serial VALUES batches without copying the full source hierarchy."""
        selected = sorted(set(iris)) if iris is not None else None
        batches = (
            [None]
            if selected is None
            else [
                selected[start : start + self.batch_size]
                for start in range(0, len(selected), self.batch_size)
            ]
        )
        g_open, g_close = _graph_clause(self.graph_uris)
        for batch in batches:
            values = ""
            if batch is not None:
                values = (
                    f"VALUES ?{scope_variable} {{ " + " ".join(f"<{iri}>" for iri in batch) + " }"
                )
            query = f"{_PREFIXES}\nSELECT DISTINCT {variables} WHERE {{ {values} {g_open} {body} {g_close} }}"
            pause = self.delay - (time.monotonic() - self._last_request)
            if pause > 0:
                time.sleep(pause)
            try:
                result = self.helper.select(query, purpose=f"ontology/{purpose}")
            except Exception as exc:
                raise RuntimeError(f"Failed to query ontology {purpose}: {exc}") from exc
            finally:
                self._last_request = time.monotonic()
            yield from result.get("results", {}).get("bindings", [])

    def mine(self) -> OntologyStructure:
        """Read used property axioms, then traverse class ancestors."""
        domain = self.query_domain()
        ranges = self.query_range()
        classes = set(self.query_classes())
        subclass: list[SubClassRelation] = []
        if self.class_iris is None:
            subclass = self.query_subclass_of()
        else:
            pending = set(self.class_iris)
            pending.update(item.domain for item in domain)
            pending.update(item.range for item in ranges)
            visited: set[str] = set()
            while pending:
                relations = self.query_subclass_of(sorted(pending))
                visited.update(pending)
                subclass.extend(relations)
                pending = {item.parent for item in relations} - visited
        for relation in subclass:
            classes.update((relation.child, relation.parent))
        classes.update(item.domain for item in domain)
        classes.update(item.range for item in ranges)
        return OntologyStructure(
            classes=sorted(classes),
            subclass_relations=subclass,
            domain_assertions=domain,
            range_assertions=ranges,
            inverse_properties=self.query_inverse_of(),
            property_characteristics=self.query_property_characteristics(),
        )

    def query_classes(self) -> list[str]:
        """Keep declared classes and selected superclass references, not used types alone."""
        if self.class_iris is None:
            body = "?class a ?kind . VALUES ?kind { owl:Class rdfs:Class } FILTER(isIRI(?class))"
        else:
            body = """
                FILTER(
                    EXISTS { ?class a owl:Class } ||
                    EXISTS { ?class a rdfs:Class } ||
                    EXISTS { ?child rdfs:subClassOf ?class } ||
                    EXISTS { ?class rdfs:subClassOf ?parent }
                )
            """
        return [
            row["class"]["value"]
            for row in self._query("?class", body, "classes", "class", self.class_iris)
        ]

    def query_subclass_of(self, class_iris: list[str] | None = None) -> list[SubClassRelation]:
        """Read direct parent edges. The caller controls ancestor traversal."""
        return [
            SubClassRelation(child=row["child"]["value"], parent=row["parent"]["value"])
            for row in self._query(
                "?child ?parent",
                "?child rdfs:subClassOf ?parent . FILTER(isIRI(?child) && isIRI(?parent))",
                "subclass",
                "child",
                class_iris,
            )
        ]

    def query_domain(self) -> list[DomainAssertion]:
        """Read named domains of selected properties."""
        return [
            DomainAssertion(property_uri=row["property"]["value"], domain=row["domain"]["value"])
            for row in self._query(
                "?property ?domain",
                "?property rdfs:domain ?domain . FILTER(isIRI(?property) && isIRI(?domain))",
                "domain",
                "property",
                self.property_iris,
            )
        ]

    def query_range(self) -> list[RangeAssertion]:
        """Read named ranges of selected properties."""
        return [
            RangeAssertion(property_uri=row["property"]["value"], range=row["range"]["value"])
            for row in self._query(
                "?property ?range",
                "?property rdfs:range ?range . FILTER(isIRI(?property) && isIRI(?range))",
                "range",
                "property",
                self.property_iris,
            )
        ]

    def query_inverse_of(self) -> list[InverseRelation]:
        """Keep either orientation of an inverse assertion."""
        body = "?property1 owl:inverseOf ?property2 ."
        if self.property_iris is not None:
            body = """
                { BIND(?selected AS ?property1) ?property1 owl:inverseOf ?property2 }
                UNION { BIND(?selected AS ?property2) ?property1 owl:inverseOf ?property2 }
            """
        return [
            InverseRelation(
                property1=row["property1"]["value"], property2=row["property2"]["value"]
            )
            for row in self._query(
                "?property1 ?property2",
                body + " FILTER(isIRI(?property1) && isIRI(?property2))",
                "inverse",
                "selected",
                self.property_iris,
            )
        ]

    def query_property_characteristics(self) -> list[PropertyCharacteristic]:
        """Read the supported OWL property kinds in one query per batch."""
        return [
            PropertyCharacteristic(
                property_uri=row["property"]["value"], characteristic=row["kind"]["value"]
            )
            for row in self._query(
                "?property ?kind",
                """
                ?property a ?kind .
                VALUES ?kind {
                    owl:FunctionalProperty owl:InverseFunctionalProperty
                    owl:TransitiveProperty owl:SymmetricProperty owl:AsymmetricProperty
                    owl:ReflexiveProperty owl:IrreflexiveProperty
                }
                FILTER(isIRI(?property))
                """,
                "characteristic",
                "property",
                self.property_iris,
            )
        ]


__all__ = ["OntologyMiner"]
