"""Mining for endpoints that use ontology classes as data.

Some endpoints (e.g., Rhea with CHEBI) declare individual entities as owl:Class
instances and use them in properties. This module mines patterns showing how
these owl:Class instances are used, attributing them to their superclasses.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rdfsolve.schema_models.core import SchemaPattern

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


def detect_ontology_as_data(
    helper: SparqlHelper,
    graph_uris: list[str] | None = None,
    threshold: int = 1000,
) -> bool:
    """Detect if endpoint uses ontology-as-data pattern.

    Checks if owl:Class instances are used as subjects/objects in
    non-ontology properties (e.g., reactions use CHEBI compounds).

    Args:
        helper: SPARQL helper instance
        graph_uris: Optional list of graph URIs to restrict query
        threshold: Minimum count to trigger ontology-as-data mode

    Returns:
        True if ontology-as-data pattern detected
    """
    g_clause = ""
    if graph_uris:
        if len(graph_uris) == 1:
            g_clause = f"GRAPH <{graph_uris[0]}> {{"
        else:
            values = " ".join(f"(<{u}>)" for u in graph_uris)
            g_clause = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"

    query = f"""\
SELECT (COUNT(*) as ?count)
WHERE {{
  {g_clause}
    ?owlClass a <http://www.w3.org/2002/07/owl#Class> .
    ?owlClass ?p ?o .
    FILTER(?p NOT IN (
      <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,
      <http://www.w3.org/2000/01/rdf-schema#subClassOf>,
      <http://www.w3.org/2000/01/rdf-schema#label>,
      <http://www.w3.org/2000/01/rdf-schema#comment>
    ))
  {"}}" if g_clause else ""}
}}
LIMIT 10000"""

    try:
        result = helper.select(query, purpose="detect-ontology-as-data")
        count_str = (
            result.get("results", {}).get("bindings", [])[0].get("count", {}).get("value", "0")
        )
        count = int(count_str)
        detected = count >= threshold
        logger.info(
            f"Ontology-as-data detection: {count} triples (threshold: {threshold}) - {'DETECTED' if detected else 'not detected'}"
        )
        return detected
    except Exception as e:
        logger.warning(f"Failed to detect ontology-as-data pattern: {e}")
        return False


def mine_ontology_as_data_patterns(
    helper: SparqlHelper,
    graph_uris: list[str] | None = None,
    superclasses: list[str] | None = None,
    bnode_namespace: str = "http://example.com/.well-known/genid/",
) -> list[SchemaPattern]:
    """Mine patterns where owl:Class instances are used as data.

    Queries for properties that have owl:Class instances as objects,
    and attributes them to the object's superclass for aggregation.
    Aggregates at superclass level to avoid per-instance bloat.

    Args:
        helper: SPARQL helper instance
        graph_uris: Optional list of graph URIs to restrict query
        superclasses: List of superclass URIs to aggregate into
        bnode_namespace: Namespace for blank nodes (default: example.com)

    Returns:
        List of schema patterns showing superclass-level usage
    """
    if not superclasses:
        logger.info("No superclasses provided, skipping ontology-as-data object patterns")
        return []

    g_clause = ""
    if graph_uris:
        if len(graph_uris) == 1:
            g_clause = f"GRAPH <{graph_uris[0]}> {{"
        else:
            values = " ".join(f"(<{u}>)" for u in graph_uris)
            g_clause = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"

    # Build VALUES clause for superclasses (limit to prevent query explosion)
    sc_values = " ".join(f"(<{sc}>)" for sc in superclasses[:500])

    # Query aggregated at superclass level for OBJECTS
    # Now includes ?subjectClass for better granularity
    query = f"""\
SELECT ?subjectClass ?property ?objectSuperclass (SUM(?cnt) as ?count)
WHERE {{
  {{
    SELECT ?subjectClass ?property ?objectSuperclass (COUNT(*) as ?cnt)
    WHERE {{
      {g_clause}
        VALUES (?objectSuperclass) {{ {sc_values} }}
        ?subject a ?subjectClass .
        ?subject ?property ?object .
        ?object a <http://www.w3.org/2002/07/owl#Class> .
        ?object <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?objectSuperclass .
        FILTER(?property NOT IN (
          <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,
          <http://www.w3.org/2000/01/rdf-schema#subClassOf>,
          <http://www.w3.org/2000/01/rdf-schema#label>,
          <http://www.w3.org/2000/01/rdf-schema#comment>
        ))
        FILTER(isURI(?subjectClass))
        FILTER(?subjectClass NOT IN (
          <http://www.w3.org/2002/07/owl#Class>,
          <http://www.w3.org/2000/01/rdf-schema#Class>
        ))
      {"}}" if g_clause else ""}
    }}
    GROUP BY ?subjectClass ?property ?objectSuperclass
  }}
}}
GROUP BY ?subjectClass ?property ?objectSuperclass
ORDER BY DESC(?count)
LIMIT 1000"""

    try:
        logger.info("Mining ontology-as-data patterns (aggregated at superclass level)...")
        result = helper.select(query, purpose="ontology-as-data-aggregated")
        bindings = result.get("results", {}).get("bindings", [])

        patterns = []
        for row in bindings:
            subject_class = row.get("subjectClass", {}).get("value")
            property_uri = row.get("property", {}).get("value")
            object_class = row.get("objectSuperclass", {}).get("value")
            count_str = row.get("count", {}).get("value", "1")

            if not subject_class or not property_uri or not object_class:
                continue

            # Skip blank nodes
            if object_class.startswith("_:") or "genid" in object_class.lower():
                continue
            if subject_class.startswith("_:") or "genid" in subject_class.lower():
                continue

            pattern = SchemaPattern(
                subject_class=subject_class,
                property_uri=property_uri,
                object_class=object_class,
                count=int(count_str),
            )
            patterns.append(pattern)

        logger.info(f"Found {len(patterns)} aggregated ontology-as-data patterns")
        return patterns

    except Exception as e:
        logger.warning(f"Failed to mine ontology-as-data patterns: {e}")
        return []


def mine_ontology_as_data_subject_patterns(
    helper: SparqlHelper,
    graph_uris: list[str] | None = None,
    superclasses: list[str] | None = None,
    bnode_namespace: str = "http://example.com/.well-known/genid/",
) -> list[SchemaPattern]:
    """Mine patterns where owl:Class instances are subjects.

    Aggregates at superclass level to avoid per-instance bloat.

    Args:
        helper: SPARQL helper instance
        graph_uris: Optional list of graph URIs to restrict query
        superclasses: List of superclass URIs to aggregate into
        bnode_namespace: Namespace for blank nodes

    Returns:
        List of schema patterns
    """
    if not superclasses:
        logger.info("No superclasses provided, skipping ontology-as-data subject patterns")
        return []

    g_clause = ""
    if graph_uris:
        if len(graph_uris) == 1:
            g_clause = f"GRAPH <{graph_uris[0]}> {{"
        else:
            values = " ".join(f"(<{u}>)" for u in graph_uris)
            g_clause = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"

    # Build VALUES clause for superclasses
    sc_values = " ".join(f"(<{sc}>)" for sc in superclasses[:500])

    query = f"""\
SELECT ?subjectSuperclass ?property (SAMPLE(?objType) as ?objectType) (COUNT(*) as ?count)
WHERE {{
  {g_clause}
    VALUES (?subjectSuperclass) {{ {sc_values} }}
    ?subject a <http://www.w3.org/2002/07/owl#Class> .
    ?subject <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?subjectSuperclass .
    ?subject ?property ?o .
    BIND(IF(isLiteral(?o), "literal", "uri") as ?objType)
    FILTER(?property NOT IN (
      <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,
      <http://www.w3.org/2000/01/rdf-schema#subClassOf>,
      <http://www.w3.org/2000/01/rdf-schema#label>,
      <http://www.w3.org/2000/01/rdf-schema#comment>
    ))
  {"}}" if g_clause else ""}
}}
GROUP BY ?subjectSuperclass ?property
ORDER BY DESC(?count)
LIMIT 1000"""

    try:
        logger.info("Mining ontology-as-data subject patterns (aggregated)...")
        result = helper.select(query, purpose="ontology-as-data-subject-aggregated")
        bindings = result.get("results", {}).get("bindings", [])

        patterns = []
        for row in bindings:
            subject_class = row.get("subjectSuperclass", {}).get("value")
            property_uri = row.get("property", {}).get("value")
            obj_type = row.get("objectType", {}).get("value", "uri")
            count_str = row.get("count", {}).get("value", "1")

            if not subject_class or not property_uri:
                continue

            # Skip blank nodes
            if subject_class.startswith("_:") or "genid" in subject_class.lower():
                continue

            # Determine object class
            if obj_type == "literal":
                object_class = "Literal"
            else:
                object_class = "Resource"

            pattern = SchemaPattern(
                subject_class=subject_class,
                property_uri=property_uri,
                object_class=object_class,
                count=int(count_str),
            )
            patterns.append(pattern)

        logger.info(f"Found {len(patterns)} aggregated ontology-as-data subject patterns")
        return patterns

    except Exception as e:
        logger.warning(f"Failed to mine ontology-as-data subject patterns: {e}")
        return []
