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
  {"}" if g_clause else ""}
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
    ontology_graph_uris: list[str] | None = None,
) -> MiningResult:
    """Mine schema with optional ontology and metadata extraction.

    Args:
        miner: Configured SchemaMiner instance
        extract_ontology: Extract TBox (class hierarchies, domain/range)
        extract_metadata: Extract infrastructure metadata (VoID/DCAT)
        dataset_name: Optional dataset name to attach to schema metadata
        ontology_graph_uris: Discovered .owl graph URIs to mine for ontology.
            If provided, these graphs are mined specifically for ontology triples.
            If None, uses miner.graph_uris or default graph.

    Returns:
        MiningResult with data schema, ontology, and metadata
    """
    with miner._session(dataset_name):
        result = _mine_with_ontology(
            miner, extract_ontology, extract_metadata, dataset_name, ontology_graph_uris
        )
        result.data_schema = miner._finish_schema(result.data_schema)
        return result


def _mine_with_ontology(
    miner: SchemaMiner,
    extract_ontology: bool,
    extract_metadata: bool,
    dataset_name: str | None,
    ontology_graph_uris: list[str] | None,
) -> MiningResult:
    """Run optional phases within the miner's active report session."""
    # Detect if endpoint uses ontology-as-data pattern
    # (owl:Class instances used as subjects/objects in properties)
    uses_ontology_as_data = detect_ontology_as_data(
        miner._helper,
        miner.graph_uris,
        threshold=1000,
    )

    ontology = None
    superclasses: list[str] = []
    ontology_graphs_used = []
    if extract_ontology:
        phase = miner._report.start_phase("ontology-extraction")
        # If .owl graphs were discovered, mine those specifically for ontology
        if ontology_graph_uris:
            logger.info(f"Extracting ontology structure (TBox) from {len(ontology_graph_uris)} .owl graphs:")
            for owl_graph in ontology_graph_uris[:5]:  # Log first 5
                logger.info(f"  - {owl_graph}")
            if len(ontology_graph_uris) > 5:
                logger.info(f"  ... and {len(ontology_graph_uris) - 5} more")

            # Mine .owl graphs for ontology
            ontology_miner = OntologyMiner(miner._helper, ontology_graph_uris)
            ontology = ontology_miner.mine()
            ontology_graphs_used = ontology_graph_uris

        else:
            # Fallback: mine from configured graphs or default
            graph_desc = ", ".join(miner.graph_uris) if miner.graph_uris else "default graph"
            logger.info(f"Extracting ontology structure (TBox) from {graph_desc}")
            ontology_miner = OntologyMiner(miner._helper, miner.graph_uris)
            ontology = ontology_miner.mine()
            ontology_graphs_used = miner.graph_uris or []

        # Log ontology extraction results
        if ontology:
            logger.info(
                f"Ontology extracted: {len(ontology.subclass_relations)} subclass, "
                f"{len(ontology.domain_assertions)} domain, "
                f"{len(ontology.range_assertions)} range relations"
            )

            # Add ontology extraction info to report
            miner._report.report.ontology_extraction = {
                "graphs_mined": ontology_graphs_used,
                "graph_count": len(ontology_graphs_used),
                "subclass_relations": len(ontology.subclass_relations),
                "domain_assertions": len(ontology.domain_assertions),
                "range_assertions": len(ontology.range_assertions),
                "inverse_properties": len(ontology.inverse_properties),
                "property_characteristics": len(ontology.property_characteristics),
            }
        miner._report.finish_phase(phase)

    # Aggregation does not require separate ontology export.
    if uses_ontology_as_data:
        logger.info("Querying for superclasses of owl:Class instances")
        superclasses = _query_owl_class_superclasses(miner._helper, miner.graph_uris)
        if superclasses:
            miner._ontology_classes = superclasses

    logger.info("Mining schema patterns (ABox)")
    data_schema = miner._mine_schema(dataset_name=dataset_name)

    # If ontology-as-data detected, mine additional patterns
    # showing how owl:Class instances are used as data
    if uses_ontology_as_data:
        phase = miner._report.start_phase("ontology-as-data")
        logger.info("Mining ontology-as-data patterns (owl:Class usage as data)")

        # Mine patterns where owl:Class instances are objects (attributed to superclass)
        object_patterns = mine_ontology_as_data_patterns(
            miner._helper,
            miner.graph_uris,
            superclasses=superclasses,
        )

        # Mine patterns where owl:Class instances are subjects (attributed to superclass)
        subject_patterns = mine_ontology_as_data_subject_patterns(
            miner._helper,
            miner.graph_uris,
            superclasses=superclasses,
        )

        # Merge with existing patterns
        all_patterns = object_patterns + subject_patterns
        logger.info(f"Adding {len(all_patterns)} ontology-as-data patterns to schema")
        data_schema.patterns.extend(all_patterns)
        miner._report.finish_phase(phase, items=len(all_patterns))

        # Update metadata to reflect ontology-as-data strategy
        data_schema.about.pattern_count = len(data_schema.patterns)
        if data_schema.about.strategy:
            data_schema.about.strategy += "+ontology-as-data"
        else:
            data_schema.about.strategy = "ontology-as-data"

    metadata = None
    if extract_metadata:
        phase = miner._report.start_phase("infrastructure-metadata")
        logger.info("Extracting infrastructure metadata")
        metadata_miner = MetadataMiner(miner._helper, miner.graph_uris)
        metadata = metadata_miner.mine()
        miner._report.finish_phase(phase)

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
