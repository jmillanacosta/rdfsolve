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
    bnode_namespace: str = "http://example.com/.well-known/genid/",
) -> list[SchemaPattern]:
    """Mine patterns where owl:Class instances are used as data.

    Queries for properties that have owl:Class instances as objects,
    and attributes them to the object's superclass for aggregation.

    Args:
        helper: SPARQL helper instance
        graph_uris: Optional list of graph URIs to restrict query
        bnode_namespace: Namespace for blank nodes (default: example.com)

    Returns:
        List of schema patterns showing superclass-level usage
    """
    g_clause = ""
    if graph_uris:
        if len(graph_uris) == 1:
            g_clause = f"GRAPH <{graph_uris[0]}> {{"
        else:
            values = " ".join(f"(<{u}>)" for u in graph_uris)
            g_clause = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"

    # Query for properties with owl:Class objects, attributed to superclass
    query = f"""\
SELECT DISTINCT ?property ?objectSuperclass (COUNT(*) as ?count)
WHERE {{
  {g_clause}
    ?subject ?property ?object .
    ?object a <http://www.w3.org/2002/07/owl#Class> .
    ?object <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?objectSuperclass .
    FILTER(?property NOT IN (
      <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,
      <http://www.w3.org/2000/01/rdf-schema#subClassOf>,
      <http://www.w3.org/2000/01/rdf-schema#label>,
      <http://www.w3.org/2000/01/rdf-schema#comment>
    ))
  {"}}" if g_clause else ""}
}}
GROUP BY ?property ?objectSuperclass
ORDER BY DESC(?count)"""

    try:
        logger.info(
            "Querying ontology-as-data patterns (owl:Class objects with superclass attribution)..."
        )
        result = helper.select(query, purpose="ontology-as-data-patterns")
        bindings = result.get("results", {}).get("bindings", [])

        patterns = []
        for row in bindings:
            property_uri = row.get("property", {}).get("value")
            object_class = row.get("objectSuperclass", {}).get("value")
            count_str = row.get("count", {}).get("value", "1")

            if not property_uri or not object_class:
                continue

            # Handle blank nodes
            if object_class.startswith("_:") or "genid" in object_class.lower():
                # Replace blank node with namespace
                bnode_id = object_class.split(":")[-1].split("/")[-1]
                object_class = f"{bnode_namespace}{bnode_id}"

            # Create pattern: rdfs:Resource --property--> objectSuperclass
            # Subject is rdfs:Resource since we don't track subject types in this query
            pattern = SchemaPattern(
                subject_class="http://www.w3.org/2000/01/rdf-schema#Resource",
                property_uri=property_uri,
                object_class=object_class,
                count=int(count_str),
            )
            patterns.append(pattern)

        logger.info(f"Found {len(patterns)} ontology-as-data patterns")
        return patterns

    except Exception as e:
        logger.warning(f"Failed to mine ontology-as-data patterns: {e}")
        return []


def mine_ontology_as_data_subject_patterns(
    helper: SparqlHelper,
    graph_uris: list[str] | None = None,
    bnode_namespace: str = "http://example.com/.well-known/genid/",
) -> list[SchemaPattern]:
    """Mine patterns where owl:Class instances are subjects with literal/resource objects.

    Args:
        helper: SPARQL helper instance
        graph_uris: Optional list of graph URIs to restrict query
        bnode_namespace: Namespace for blank nodes

    Returns:
        List of schema patterns
    """
    g_clause = ""
    if graph_uris:
        if len(graph_uris) == 1:
            g_clause = f"GRAPH <{graph_uris[0]}> {{"
        else:
            values = " ".join(f"(<{u}>)" for u in graph_uris)
            g_clause = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"

    # Query for properties where owl:Class instances are subjects
    # Attribute to subject's superclass
    query = f"""\
SELECT DISTINCT ?subjectSuperclass ?property (SAMPLE(?o) as ?sampleObject) (COUNT(*) as ?count)
WHERE {{
  {g_clause}
    ?subject a <http://www.w3.org/2002/07/owl#Class> .
    ?subject <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?subjectSuperclass .
    ?subject ?property ?o .
    FILTER(?property NOT IN (
      <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,
      <http://www.w3.org/2000/01/rdf-schema#subClassOf>,
      <http://www.w3.org/2000/01/rdf-schema#label>,
      <http://www.w3.org/2000/01/rdf-schema#comment>
    ))
  {"}}" if g_clause else ""}
}}
GROUP BY ?subjectSuperclass ?property
ORDER BY DESC(?count)"""

    try:
        logger.info("Querying ontology-as-data subject patterns...")
        result = helper.select(query, purpose="ontology-as-data-subject")
        bindings = result.get("results", {}).get("bindings", [])

        patterns = []
        for row in bindings:
            subject_class = row.get("subjectSuperclass", {}).get("value")
            property_uri = row.get("property", {}).get("value")
            sample_obj = row.get("sampleObject", {})
            count_str = row.get("count", {}).get("value", "1")

            if not subject_class or not property_uri:
                continue

            # Handle blank nodes in subject class
            if subject_class.startswith("_:") or "genid" in subject_class.lower():
                bnode_id = subject_class.split(":")[-1].split("/")[-1]
                subject_class = f"{bnode_namespace}{bnode_id}"

            # Determine object class
            obj_type = sample_obj.get("type", "uri")
            if obj_type == "literal":
                object_class = "http://www.w3.org/2000/01/rdf-schema#Literal"
            else:
                object_class = "http://www.w3.org/2000/01/rdf-schema#Resource"

            pattern = SchemaPattern(
                subject_class=subject_class,
                property_uri=property_uri,
                object_class=object_class,
                count=int(count_str),
            )
            patterns.append(pattern)

        logger.info(f"Found {len(patterns)} ontology-as-data subject patterns")
        return patterns

    except Exception as e:
        logger.warning(f"Failed to mine ontology-as-data subject patterns: {e}")
        return []
