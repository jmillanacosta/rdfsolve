"""Metadata querying utilities for SPARQL endpoints.

This module provides reusable SPARQL query patterns and strategies for
discovering dataset metadata from SPARQL endpoints. Useful for:
- Pre-mining metadata checks
- Metadata harvesting and catalog building
- Understanding endpoint characteristics before full schema mining

The query patterns cover common vocabularies:
- DCTERMS (Dublin Core Terms)
- DC (Dublin Core Elements)
- DCAT (Data Catalog Vocabulary)
- PAV (Provenance, Authoring and Versioning)
- VoID (Vocabulary of Interlinked Datasets)
- OWL (Web Ontology Language)
- FOAF (Friend of a Friend)
- PROV (Provenance Ontology)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


# Reusable SPARQL patterns for common metadata predicates

LICENSE_PATTERN = """
  OPTIONAL { ?subject dcterms:license ?license }
  OPTIONAL { ?subject dc:license ?license }
  OPTIONAL { ?subject dcat:license ?license }
"""

PUBLISHER_PATTERN = """
  OPTIONAL { ?subject dcterms:publisher ?publisher }
  OPTIONAL { ?subject dc:publisher ?publisher }
"""

CREATOR_PATTERN = """
  OPTIONAL { ?subject dcterms:creator ?creator }
  OPTIONAL { ?subject dc:creator ?creator }
  OPTIONAL { ?subject pav:authoredBy ?creator }
  OPTIONAL { ?subject pav:createdBy ?creator }
"""

VERSION_PATTERN = """
  OPTIONAL { ?subject pav:version ?version }
  OPTIONAL { ?subject owl:versionInfo ?version }
  OPTIONAL { ?subject dcat:version ?version }
  OPTIONAL { ?subject owl:versionIRI ?versionIRI }
"""

DATE_PATTERN = """
  OPTIONAL { ?subject dcterms:issued ?issued }
  OPTIONAL { ?subject dcat:issued ?issued }
  OPTIONAL { ?subject dcterms:created ?created }
  OPTIONAL { ?subject pav:createdOn ?created }
  OPTIONAL { ?subject dcterms:modified ?modified }
  OPTIONAL { ?subject dcat:modified ?modified }
  OPTIONAL { ?subject pav:lastUpdateOn ?modified }
"""

TITLE_PATTERN = """
  OPTIONAL { ?subject dcterms:title ?title }
  OPTIONAL { ?subject dc:title ?title }
"""

HOMEPAGE_PATTERN = """
  OPTIONAL { ?subject foaf:homepage ?homepage }
"""

DESCRIPTION_PATTERN = """
  OPTIONAL { ?subject dcterms:description ?description }
  OPTIONAL { ?subject dc:description ?description }
"""


# Complete query strategies


def build_void_dataset_query() -> str:
    """Build query for void:Dataset or dcat:Dataset pattern.

    Most common in LOD endpoints with VoID descriptions.
    """
    return f"""
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX void: <http://rdfs.org/ns/void#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX pav: <http://purl.org/pav/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?license ?publisher ?creator ?version ?versionIRI
           ?issued ?modified ?created ?homepage ?title ?description
    WHERE {{
      {{ ?subject a void:Dataset }} UNION {{ ?subject a dcat:Dataset }}

      {LICENSE_PATTERN}
      {PUBLISHER_PATTERN}
      {CREATOR_PATTERN}
      {VERSION_PATTERN}
      {DATE_PATTERN}
      {TITLE_PATTERN}
      {HOMEPAGE_PATTERN}
      {DESCRIPTION_PATTERN}
    }}
    """


def build_owl_ontology_query() -> str:
    """Build query for owl:Ontology pattern.

    Common in OWL ontologies (e.g., nano-safety, AOP-Wiki, WikiPathways).
    """
    return f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX pav: <http://purl.org/pav/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?license ?creator ?version ?versionIRI
           ?issued ?modified ?created ?title ?description
    WHERE {{
      ?subject a owl:Ontology .

      {LICENSE_PATTERN}
      {CREATOR_PATTERN}
      {VERSION_PATTERN}
      {DATE_PATTERN}
      {TITLE_PATTERN}
      {DESCRIPTION_PATTERN}
    }}
    """


def build_named_graph_query() -> str:
    """Build query for named graph metadata pattern.

    Some endpoints store metadata on the graph URI itself.
    """
    return f"""
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX void: <http://rdfs.org/ns/void#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX pav: <http://purl.org/pav/>

    SELECT ?license ?publisher ?creator ?issued ?modified ?title ?description
    WHERE {{
      GRAPH ?g {{
        {{ ?g dcterms:license ?license }} UNION {{ ?g dc:license ?license }}
      }}
      OPTIONAL {{ GRAPH ?g {{ {PUBLISHER_PATTERN.replace("?subject", "?g")} }} }}
      OPTIONAL {{ GRAPH ?g {{ {CREATOR_PATTERN.replace("?subject", "?g")} }} }}
      OPTIONAL {{ GRAPH ?g {{ {DATE_PATTERN.replace("?subject", "?g")} }} }}
      OPTIONAL {{ GRAPH ?g {{ {TITLE_PATTERN.replace("?subject", "?g")} }} }}
      OPTIONAL {{ GRAPH ?g {{ {DESCRIPTION_PATTERN.replace("?subject", "?g")} }} }}
    }}
    """


def build_service_description_query() -> str:
    """Build query for SPARQL service description pattern.

    Endpoints with sd:Service descriptions.
    """
    return f"""
    PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX void: <http://rdfs.org/ns/void#>
    PREFIX pav: <http://purl.org/pav/>

    SELECT ?license ?publisher ?creator ?issued ?modified ?title ?description
    WHERE {{
      ?service a sd:Service .
      OPTIONAL {{ ?service sd:defaultDataset ?subject }}

      {LICENSE_PATTERN}
      {PUBLISHER_PATTERN}
      {CREATOR_PATTERN}
      {DATE_PATTERN}
      {TITLE_PATTERN}
      {DESCRIPTION_PATTERN}
    }}
    """


def build_broad_scan_query() -> str:
    """Build broad metadata scan query.

    Look for ANY metadata triples with common predicates.
    This is a fallback when more specific patterns don't work.
    """
    return """
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX pav: <http://purl.org/pav/>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX dcat: <http://www.w3.org/ns/dcat#>

    SELECT ?license ?publisher ?creator ?version ?issued ?modified ?title ?description
    WHERE {
      {
        ?s dcterms:license ?license .
      } UNION {
        ?s dc:license ?license .
      } UNION {
        ?s dcat:license ?license .
      } UNION {
        ?s dcterms:publisher ?publisher .
      } UNION {
        ?s dc:publisher ?publisher .
      } UNION {
        ?s dcterms:creator ?creator .
      } UNION {
        ?s dc:creator ?creator .
      } UNION {
        ?s pav:authoredBy ?creator .
      } UNION {
        ?s dcterms:issued ?issued .
      } UNION {
        ?s dcat:issued ?issued .
      } UNION {
        ?s dcterms:modified ?modified .
      } UNION {
        ?s dcat:modified ?modified .
      } UNION {
        ?s owl:versionInfo ?version .
      } UNION {
        ?s pav:version ?version .
      } UNION {
        ?s dcterms:title ?title .
      } UNION {
        ?s dc:title ?title .
      } UNION {
        ?s dcterms:description ?description .
      } UNION {
        ?s dc:description ?description .
      }
    } LIMIT 100
    """


# Result processing


def process_metadata_results(bindings: list[dict[str, Any]]) -> dict[str, Any]:
    """Process SPARQL query results into standardized metadata dict.

    Args:
        bindings: SPARQL result bindings

    Returns:
        dict with metadata fields:
            - source_license: License URI or literal
            - source_publisher: Publisher URI or name
            - source_creator: List of creator URIs or names (ALL creators)
            - source_version: Version string
            - source_version_iri: Version IRI
            - source_issued: Issued/created date
            - source_modified: Modified date
            - homepage: Homepage URI
            - title: Dataset title
            - description: Dataset description
    """
    metadata: dict[str, Any] = {}
    creators = []

    for row in bindings:
        # License (first found wins)
        if "license" in row and not metadata.get("source_license"):
            metadata["source_license"] = row["license"].get("value")

        # Publisher (first found wins)
        if "publisher" in row and not metadata.get("source_publisher"):
            metadata["source_publisher"] = row["publisher"].get("value")

        # Creator (collect ALL unique values)
        if "creator" in row:
            creator_val = row["creator"].get("value")
            if creator_val and creator_val not in creators:
                creators.append(creator_val)

        # Version (first found wins)
        if "version" in row and not metadata.get("source_version"):
            metadata["source_version"] = row["version"].get("value")

        # Version IRI (first found wins)
        if "versionIRI" in row and not metadata.get("source_version_iri"):
            metadata["source_version_iri"] = row["versionIRI"].get("value")

        # Issued/Created (first found wins)
        if "issued" in row and not metadata.get("source_issued"):
            metadata["source_issued"] = row["issued"].get("value")
        elif "created" in row and not metadata.get("source_issued"):
            metadata["source_issued"] = row["created"].get("value")

        # Modified (first found wins)
        if "modified" in row and not metadata.get("source_modified"):
            metadata["source_modified"] = row["modified"].get("value")

        # Homepage (first found wins)
        if "homepage" in row and not metadata.get("homepage"):
            metadata["homepage"] = row["homepage"].get("value")

        # Title (first found wins)
        if "title" in row and not metadata.get("title"):
            metadata["title"] = row["title"].get("value")

        # Description (first found wins)
        if "description" in row and not metadata.get("description"):
            metadata["description"] = row["description"].get("value")

    # Store all creators
    if creators:
        metadata["source_creator"] = creators

    return metadata


# Main query function


def query_endpoint_metadata(
    sparql_helper: SparqlHelper,
) -> dict[str, Any]:
    """Query SPARQL endpoint for dataset metadata using multiple strategies.

    Tries multiple query strategies to discover metadata from different
    vocabularies (DCTERMS, DC, DCAT, PAV, VoID, OWL, FOAF, PROV) across
    different patterns (void:Dataset, owl:Ontology, named graphs, etc.).

    Args:
        sparql_helper: SparqlHelper instance configured for the endpoint

    Returns:
        dict with metadata fields (see process_metadata_results for schema)
        Returns empty dict if all queries fail.

    Example:
        >>> from rdfsolve.sparql_helper import SparqlHelper
        >>> helper = SparqlHelper("https://sparql.uniprot.org/sparql")
        >>> metadata = query_endpoint_metadata(helper)
        >>> print(metadata.get("source_license"))
    """
    strategies = [
        ("void:Dataset/dcat:Dataset", build_void_dataset_query()),
        ("owl:Ontology", build_owl_ontology_query()),
        ("named-graph", build_named_graph_query()),
        ("service-description", build_service_description_query()),
        ("broad-scan", build_broad_scan_query()),
    ]

    for strategy_name, query in strategies:
        try:
            logger.debug("Trying metadata strategy: %s", strategy_name)
            raw = sparql_helper.select(query, purpose=f"metadata_{strategy_name}")
            bindings = raw.get("results", {}).get("bindings", [])

            if not bindings:
                continue

            metadata = process_metadata_results(bindings)

            if metadata:
                logger.info(
                    "Found metadata via %s: %s",
                    strategy_name,
                    list(metadata.keys()),
                )
                creators = metadata.get("source_creator", [])
                if creators:
                    logger.info("  - Captured %d creator(s)", len(creators))
                return metadata

        except Exception as e:
            logger.debug("Metadata strategy %s failed: %s", strategy_name, e)
            continue

    logger.debug("No dataset metadata found via any strategy")
    return {}


# Utility functions for building custom queries with patterns


def build_custom_metadata_query(
    subject_pattern: str,
    include_license: bool = True,
    include_publisher: bool = True,
    include_creator: bool = True,
    include_version: bool = True,
    include_dates: bool = True,
    include_title: bool = True,
    include_homepage: bool = True,
    include_description: bool = True,
) -> str:
    """Build a custom metadata query with selected patterns.

    Args:
        subject_pattern: WHERE clause pattern to identify the subject
            Example: "?subject a void:Dataset"
        include_*: Whether to include each metadata pattern

    Returns:
        Complete SPARQL query string

    Example:
        >>> query = build_custom_metadata_query(
        ...     "?subject a void:Dataset", include_description=False
        ... )
    """
    prefixes = """
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX void: <http://rdfs.org/ns/void#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX pav: <http://purl.org/pav/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    """

    select_vars = []
    patterns = []

    if include_license:
        select_vars.append("?license")
        patterns.append(LICENSE_PATTERN)

    if include_publisher:
        select_vars.append("?publisher")
        patterns.append(PUBLISHER_PATTERN)

    if include_creator:
        select_vars.append("?creator")
        patterns.append(CREATOR_PATTERN)

    if include_version:
        select_vars.extend(["?version", "?versionIRI"])
        patterns.append(VERSION_PATTERN)

    if include_dates:
        select_vars.extend(["?issued", "?modified", "?created"])
        patterns.append(DATE_PATTERN)

    if include_title:
        select_vars.append("?title")
        patterns.append(TITLE_PATTERN)

    if include_homepage:
        select_vars.append("?homepage")
        patterns.append(HOMEPAGE_PATTERN)

    if include_description:
        select_vars.append("?description")
        patterns.append(DESCRIPTION_PATTERN)

    select_clause = "SELECT " + " ".join(select_vars)
    where_clause = f"{subject_pattern}\n\n" + "\n".join(patterns)

    return f"{prefixes}\n{select_clause}\nWHERE {{\n{where_clause}\n}}"
