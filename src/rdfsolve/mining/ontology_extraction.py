"""Ontology structure extraction (TBox mining)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rdfsolve.schema_models.ontology import (
    DomainAssertion,
    InverseRelation,
    OntologyStructure,
    PropertyCharacteristic,
    RangeAssertion,
    Restriction,
    SubClassRelation,
)

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


class OntologyMiner:
    """Extract ontology axioms (TBox) from SPARQL endpoint."""

    def __init__(
        self,
        helper: SparqlHelper,
        graph_uris: list[str] | None = None,
    ) -> None:
        self.helper = helper
        self.graph_uris = graph_uris

    def _graph_clause(self) -> tuple[str, str]:
        """Return (open, close) for GRAPH clause."""
        if not self.graph_uris:
            return "", ""
        if len(self.graph_uris) == 1:
            return f"GRAPH <{self.graph_uris[0]}> {{", "}"
        values = " ".join(f"(<{u}>)" for u in self.graph_uris)
        return f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{", "}"

    def mine(self) -> OntologyStructure:
        """Extract ontology structure."""
        logger.info("Mining ontology structure (TBox)")

        subclass = self.query_subclass_of()
        domain = self.query_domain()
        range_ = self.query_range()
        inverse = self.query_inverse_of()
        characteristics = self.query_property_characteristics()

        logger.info(
            f"Found {len(subclass)} subclass, {len(domain)} domain, "
            f"{len(range_)} range, {len(inverse)} inverse, "
            f"{len(characteristics)} property characteristics"
        )

        return OntologyStructure(
            subclass_relations=subclass,
            domain_assertions=domain,
            range_assertions=range_,
            inverse_properties=inverse,
            property_characteristics=characteristics,
            restrictions=[],
        )

    def query_subclass_of(self) -> list[SubClassRelation]:
        """Query rdfs:subClassOf relations."""
        g_open, g_close = self._graph_clause()
        query = f"""\
SELECT DISTINCT ?child ?parent
WHERE {{
  {g_open}
    ?child <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?parent .
    FILTER(isURI(?child))
    FILTER(isURI(?parent))
  {g_close}
}}"""

        try:
            result = self.helper.select(query, purpose="ontology/subclass")
            bindings = result.get("results", {}).get("bindings", [])
            return [
                SubClassRelation(
                    child=row.get("child", {}).get("value", ""),
                    parent=row.get("parent", {}).get("value", ""),
                )
                for row in bindings
                if row.get("child", {}).get("value") and row.get("parent", {}).get("value")
            ]
        except Exception as e:
            logger.warning(f"Failed to query subClassOf: {e}")
            return []

    def query_domain(self) -> list[DomainAssertion]:
        """Query rdfs:domain assertions."""
        g_open, g_close = self._graph_clause()
        query = f"""\
SELECT DISTINCT ?property ?domain
WHERE {{
  {g_open}
    ?property <http://www.w3.org/2000/01/rdf-schema#domain> ?domain .
    FILTER(isURI(?property))
    FILTER(isURI(?domain))
  {g_close}
}}"""

        try:
            result = self.helper.select(query, purpose="ontology/domain")
            bindings = result.get("results", {}).get("bindings", [])
            return [
                DomainAssertion(
                    property_uri=row.get("property", {}).get("value", ""),
                    domain=row.get("domain", {}).get("value", ""),
                )
                for row in bindings
                if row.get("property", {}).get("value") and row.get("domain", {}).get("value")
            ]
        except Exception as e:
            logger.warning(f"Failed to query domain: {e}")
            return []

    def query_range(self) -> list[RangeAssertion]:
        """Query rdfs:range assertions."""
        g_open, g_close = self._graph_clause()
        query = f"""\
SELECT DISTINCT ?property ?range
WHERE {{
  {g_open}
    ?property <http://www.w3.org/2000/01/rdf-schema#range> ?range .
    FILTER(isURI(?property))
    FILTER(isURI(?range))
  {g_close}
}}"""

        try:
            result = self.helper.select(query, purpose="ontology/range")
            bindings = result.get("results", {}).get("bindings", [])
            return [
                RangeAssertion(
                    property_uri=row.get("property", {}).get("value", ""),
                    range=row.get("range", {}).get("value", ""),
                )
                for row in bindings
                if row.get("property", {}).get("value") and row.get("range", {}).get("value")
            ]
        except Exception as e:
            logger.warning(f"Failed to query range: {e}")
            return []

    def query_inverse_of(self) -> list[InverseRelation]:
        """Query owl:inverseOf relations."""
        g_open, g_close = self._graph_clause()
        query = f"""\
SELECT DISTINCT ?property1 ?property2
WHERE {{
  {g_open}
    ?property1 <http://www.w3.org/2002/07/owl#inverseOf> ?property2 .
    FILTER(isURI(?property1))
    FILTER(isURI(?property2))
  {g_close}
}}"""

        try:
            result = self.helper.select(query, purpose="ontology/inverse")
            bindings = result.get("results", {}).get("bindings", [])
            return [
                InverseRelation(
                    property1=row.get("property1", {}).get("value", ""),
                    property2=row.get("property2", {}).get("value", ""),
                )
                for row in bindings
                if row.get("property1", {}).get("value") and row.get("property2", {}).get("value")
            ]
        except Exception as e:
            logger.warning(f"Failed to query inverseOf: {e}")
            return []

    def query_property_characteristics(self) -> list[PropertyCharacteristic]:
        """Query OWL property characteristics."""
        g_open, g_close = self._graph_clause()
        characteristics = [
            "http://www.w3.org/2002/07/owl#FunctionalProperty",
            "http://www.w3.org/2002/07/owl#InverseFunctionalProperty",
            "http://www.w3.org/2002/07/owl#TransitiveProperty",
            "http://www.w3.org/2002/07/owl#SymmetricProperty",
            "http://www.w3.org/2002/07/owl#AsymmetricProperty",
            "http://www.w3.org/2002/07/owl#ReflexiveProperty",
            "http://www.w3.org/2002/07/owl#IrreflexiveProperty",
        ]

        results = []
        for char in characteristics:
            query = f"""\
SELECT DISTINCT ?property
WHERE {{
  {g_open}
    ?property a <{char}> .
    FILTER(isURI(?property))
  {g_close}
}}"""

            try:
                result = self.helper.select(query, purpose="ontology/characteristic")
                bindings = result.get("results", {}).get("bindings", [])
                for row in bindings:
                    prop = row.get("property", {}).get("value")
                    if prop:
                        results.append(
                            PropertyCharacteristic(property_uri=prop, characteristic=char)
                        )
            except Exception as e:
                logger.debug(f"Failed to query {char}: {e}")
                continue

        return results


__all__ = ["OntologyMiner"]
