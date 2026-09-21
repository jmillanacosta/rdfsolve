"""Query source ontology axioms for used terms and their ancestors."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from rdfsolve.mining.query_builders import _graph_clause
from rdfsolve.schema_models.ontology import (
    DisjointClassRelation,
    DomainAssertion,
    EquivalentClassRelation,
    EquivalentPropertyRelation,
    InverseRelation,
    OntologyStructure,
    PropertyCharacteristic,
    RangeAssertion,
    SubClassRelation,
    SubPropertyRelation,
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
        """Read named axioms around used terms and traverse named ancestors."""
        domain = self.query_domain()
        ranges = self.query_range()
        classes = set(self.query_classes())

        equivalent_classes = self.query_equivalent_classes()
        disjoint_classes = self.query_disjoint_classes()

        subclass: list[SubClassRelation] = []
        if self.class_iris is None:
            subclass = self.query_subclass_of()
        else:
            pending = set(self.class_iris)
            pending.update(item.domain for item in domain)
            pending.update(item.range for item in ranges)
            for eq_class in equivalent_classes:
                if eq_class.class1 in pending or eq_class.class2 in pending:
                    pending.update((eq_class.class1, eq_class.class2))
            visited: set[str] = set()
            while pending:
                current = sorted(pending - visited)
                if not current:
                    break
                relations = self.query_subclass_of(current)
                visited.update(current)
                subclass.extend(relations)
                pending.update(item.parent for item in relations)

        for sub in subclass:
            classes.update((sub.child, sub.parent))
        for eq_class in equivalent_classes:
            classes.update((eq_class.class1, eq_class.class2))
        for disjoint in disjoint_classes:
            if disjoint.class1 in classes or disjoint.class2 in classes:
                classes.update((disjoint.class1, disjoint.class2))
        classes.update(item.domain for item in domain)
        classes.update(item.range for item in ranges)

        subproperty: list[SubPropertyRelation] = []
        equivalent_properties = self.query_equivalent_properties()
        if self.property_iris is None:
            subproperty = self.query_subproperty_of()
        else:
            pending_properties = set(self.property_iris)
            for eq_prop in equivalent_properties:
                if (
                    eq_prop.property1 in pending_properties
                    or eq_prop.property2 in pending_properties
                ):
                    pending_properties.update((eq_prop.property1, eq_prop.property2))
            visited_properties: set[str] = set()
            while pending_properties:
                current = sorted(pending_properties - visited_properties)
                if not current:
                    break
                prop_relations = self.query_subproperty_of(current)
                visited_properties.update(current)
                subproperty.extend(prop_relations)
                pending_properties.update(item.parent for item in prop_relations)

        retained_terms = set(classes) | set(self.property_iris or [])
        retained_terms.update(item.child for item in subproperty)
        retained_terms.update(item.parent for item in subproperty)
        deprecated_terms = self.query_deprecated_terms(sorted(retained_terms))

        return OntologyStructure(
            classes=sorted(classes),
            subclass_relations=subclass,
            subproperty_relations=subproperty,
            equivalent_classes=equivalent_classes,
            equivalent_properties=equivalent_properties,
            disjoint_classes=disjoint_classes,
            deprecated_terms=deprecated_terms,
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

    def query_subproperty_of(
        self, property_iris: list[str] | None = None
    ) -> list[SubPropertyRelation]:
        """Read direct named superproperty edges."""
        selected = self.property_iris if property_iris is None else property_iris
        return [
            SubPropertyRelation(child=row["child"]["value"], parent=row["parent"]["value"])
            for row in self._query(
                "?child ?parent",
                "?child rdfs:subPropertyOf ?parent . FILTER(isIRI(?child) && isIRI(?parent))",
                "subproperty",
                "child",
                selected,
            )
        ]

    def query_equivalent_classes(self) -> list[EquivalentClassRelation]:
        """Keep named equivalent-class assertions touching selected classes."""
        body = "?class1 owl:equivalentClass ?class2 ."
        scope_var = "class1"
        if self.class_iris is not None:
            body = """
                { BIND(?selected AS ?class1) ?class1 owl:equivalentClass ?class2 }
                UNION { BIND(?selected AS ?class2) ?class1 owl:equivalentClass ?class2 }
            """
            scope_var = "selected"
        return [
            EquivalentClassRelation(class1=row["class1"]["value"], class2=row["class2"]["value"])
            for row in self._query(
                "?class1 ?class2",
                body + " FILTER(isIRI(?class1) && isIRI(?class2))",
                "equivalent-class",
                scope_var,
                self.class_iris,
            )
        ]

    def query_disjoint_classes(self) -> list[DisjointClassRelation]:
        """Keep named owl:disjointWith assertions touching selected classes."""
        body = "?class1 owl:disjointWith ?class2 ."
        scope_var = "class1"
        if self.class_iris is not None:
            body = """
                { BIND(?selected AS ?class1) ?class1 owl:disjointWith ?class2 }
                UNION { BIND(?selected AS ?class2) ?class1 owl:disjointWith ?class2 }
            """
            scope_var = "selected"
        return [
            DisjointClassRelation(class1=row["class1"]["value"], class2=row["class2"]["value"])
            for row in self._query(
                "?class1 ?class2",
                body + " FILTER(isIRI(?class1) && isIRI(?class2))",
                "disjoint-class",
                scope_var,
                self.class_iris,
            )
        ]

    def query_equivalent_properties(self) -> list[EquivalentPropertyRelation]:
        """Keep named equivalent-property assertions touching selected properties."""
        body = "?property1 owl:equivalentProperty ?property2 ."
        scope_var = "property1"
        if self.property_iris is not None:
            body = """
                { BIND(?selected AS ?property1) ?property1 owl:equivalentProperty ?property2 }
                UNION { BIND(?selected AS ?property2) ?property1 owl:equivalentProperty ?property2 }
            """
            scope_var = "selected"
        return [
            EquivalentPropertyRelation(
                property1=row["property1"]["value"], property2=row["property2"]["value"]
            )
            for row in self._query(
                "?property1 ?property2",
                body + " FILTER(isIRI(?property1) && isIRI(?property2))",
                "equivalent-property",
                scope_var,
                self.property_iris,
            )
        ]

    def query_deprecated_terms(self, iris: list[str] | None = None) -> list[str]:
        """Read named terms explicitly marked owl:deprecated true."""
        return sorted(
            {
                row["term"]["value"]
                for row in self._query(
                    "?term",
                    "?term owl:deprecated true . FILTER(isIRI(?term))",
                    "deprecated",
                    "term",
                    iris,
                )
            }
        )

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
