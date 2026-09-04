"""Mining package - modular schema mining with ontology separation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rdfsolve.mining.metadata_mining import MetadataMiner
from rdfsolve.mining.ontology_as_data import (
    detect_ontology_as_data,
    mine_ontology_as_data_patterns,
    mine_ontology_as_data_subject_patterns,
)
from rdfsolve.mining.ontology_extraction import OntologyMiner
from rdfsolve.mining.types import ONTOLOGY_METACLASSES
from rdfsolve.schema_models import MiningResult

if TYPE_CHECKING:
    from rdfsolve.miner import SchemaMiner
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


def _query_owl_class_superclasses(
    helper: SparqlHelper,
    graph_uris: list[str] | None = None,
    limit: int = 500,
) -> list[str]:
    """Query for superclasses of owl:Class instances.

    For endpoints that use ontology classes as data (e.g., Rhea using CHEBI
    compounds), this returns the parent classes which represent the actual
    class vocabulary being used, rather than the 200K+ individual compounds.

    Args:
        helper: SPARQL helper instance
        graph_uris: Optional list of graph URIs to restrict query
        limit: Maximum number of superclasses to return (default 500 to prevent OOM)

    Returns:
        List of superclass URIs (parents of owl:Class instances), limited to top N
    """
    g_clause = ""
    if graph_uris:
        if len(graph_uris) == 1:
            g_clause = f"GRAPH <{graph_uris[0]}> {{"
        else:
            values = " ".join(f"(<{u}>)" for u in graph_uris)
            g_clause = f"VALUES (?_g) {{ {values} }} GRAPH ?_g {{"

    # Query with COUNT to get most-used superclasses first
    query = f"""\
SELECT ?parent (COUNT(DISTINCT ?child) as ?count)
WHERE {{
  {g_clause}
    ?child a <http://www.w3.org/2002/07/owl#Class> .
    ?child <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?parent .
    FILTER(isURI(?parent))
    FILTER(!isBlank(?parent))
  {"}}" if g_clause else ""}
}}
GROUP BY ?parent
ORDER BY DESC(?count)
LIMIT {limit}"""

    try:
        result = helper.select(query, purpose="owl-class-superclasses")
        bindings = result.get("results", {}).get("bindings", [])
        classes = []
        for row in bindings:
            class_uri = row.get("parent", {}).get("value")
            # Filter out metaclasses and blank nodes
            if class_uri and class_uri not in ONTOLOGY_METACLASSES:
                classes.append(class_uri)
        logger.info(f"Found {len(classes)} superclasses of owl:Class instances (top {limit})")
        return classes
    except Exception as e:
        logger.warning(f"Failed to query owl:Class superclasses: {e}")
        return []


def mine_with_ontology(
    miner: SchemaMiner,
    extract_ontology: bool = False,
    extract_metadata: bool = False,
    dataset_name: str | None = None,
) -> MiningResult:
    """Mine schema with optional ontology and metadata extraction.

    Args:
        miner: Configured SchemaMiner instance
        extract_ontology: Extract TBox (class hierarchies, domain/range)
        extract_metadata: Extract infrastructure metadata (VoID/DCAT)
        dataset_name: Optional dataset name to attach to schema metadata

    Returns:
        MiningResult with data schema, ontology, and metadata
    """
    # Detect if endpoint uses ontology-as-data pattern
    # (owl:Class instances used as subjects/objects in properties)
    uses_ontology_as_data = detect_ontology_as_data(
        miner._helper,
        miner.graph_uris,
        threshold=1000,
    )

    ontology = None
    if extract_ontology:
        logger.info("Extracting ontology structure (TBox)")
        ontology_miner = OntologyMiner(miner._helper, miner.graph_uris)
        ontology = ontology_miner.mine()

        # For ontology-as-data endpoints, query superclasses for aggregation
        if uses_ontology_as_data:
            logger.info("Querying for superclasses of owl:Class instances")
            superclasses = _query_owl_class_superclasses(miner._helper, miner.graph_uris)
            # Inject these as additional classes for pattern mining
            if superclasses:
                miner._ontology_classes = superclasses

    logger.info("Mining schema patterns (ABox)")
    data_schema = miner.mine(dataset_name=dataset_name)

    # If ontology-as-data detected, mine additional patterns
    # showing how owl:Class instances are used as data
    if uses_ontology_as_data:
        logger.info("Mining ontology-as-data patterns (owl:Class usage as data)")

        # Mine patterns where owl:Class instances are objects (attributed to superclass)
        object_patterns = mine_ontology_as_data_patterns(
            miner._helper,
            miner.graph_uris,
        )

        # Mine patterns where owl:Class instances are subjects (attributed to superclass)
        subject_patterns = mine_ontology_as_data_subject_patterns(
            miner._helper,
            miner.graph_uris,
        )

        # Merge with existing patterns
        all_patterns = object_patterns + subject_patterns
        logger.info(f"Adding {len(all_patterns)} ontology-as-data patterns to schema")
        data_schema.patterns.extend(all_patterns)

        # Update pattern count in metadata
        data_schema.about.pattern_count = len(data_schema.patterns)

    metadata = None
    if extract_metadata:
        logger.info("Extracting infrastructure metadata")
        metadata_miner = MetadataMiner(miner._helper, miner.graph_uris)
        metadata = metadata_miner.mine()

    return MiningResult(
        data_schema=data_schema,
        ontology=ontology,
        metadata=metadata,
    )


__all__ = [
    "MetadataMiner",
    "OntologyMiner",
    "mine_with_ontology",
]
