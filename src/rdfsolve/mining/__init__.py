"""Mining package - modular schema mining with ontology separation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

from rdfsolve.mining.metadata_mining import MetadataMiner
from rdfsolve.mining.ontology_extraction import OntologyMiner
from rdfsolve.schema_models import MiningResult

if TYPE_CHECKING:
    from rdfsolve.mining.miner import SchemaMiner
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)


def mine_with_ontology(
    miner: SchemaMiner,
    extract_ontology: bool = False,
    extract_metadata: bool = False,
    dataset_name: str | None = None,
    ontology_graph_uris: list[str] | None = None,
    ontology_scope: Literal["schema", "full"] = "schema",
    ontology_as_data: bool = False,
    ontology_term_budget: int = 300,
) -> MiningResult:
    """Mine schema with optional ontology and metadata extraction.

    Args:
        miner: Configured SchemaMiner instance
        extract_ontology: Extract TBox (class hierarchies, domain/range)
        extract_metadata: Extract infrastructure metadata (VoID/DCAT)
        ontology_as_data: Retain exact term bindings in data_schema.term_patterns.
            Group typed patterns under their rdfs:subClassOf ancestors when
            their class count exceeds ontology_term_budget. Grouped patterns
            are inferred; exact term observations retain their original IRIs.
        ontology_term_budget: Target class count for the typed schema view.
        dataset_name: Optional dataset name to attach to schema metadata
        ontology_graph_uris: Graphs for ontology extraction and superclass lookup.
            Named graphs must hold triples. None keeps extraction in the data
            scope and superclass lookup in the endpoint default dataset.
        ontology_scope: Keep the schema's classes and ancestors, or all queried axioms.

    Returns:
        MiningResult with data schema, ontology, and metadata
    """
    if ontology_scope not in ("schema", "full"):
        raise ValueError("ontology_scope must be schema or full")
    with miner._session(dataset_name, ontology_graph_uris):
        miner._report.report.config["ontology_scope"] = ontology_scope
        miner._report.report.config["ontology_as_data"] = ontology_as_data
        miner._verify_graph_scope()
        if extract_ontology or ontology_as_data:
            miner._verify_context_graphs("ontology_context", ontology_graph_uris)
        if ontology_as_data:
            if ontology_term_budget < 1:
                raise ValueError("ontology_term_budget must be positive")
            miner._ontology_term_budget = ontology_term_budget
        result = _mine_with_ontology(
            miner,
            extract_ontology,
            extract_metadata,
            dataset_name,
            ontology_graph_uris,
            ontology_scope,
        )
        ontology_iris = result.ontology.term_iris() if result.ontology else []
        annotations = None
        shared_scope = ontology_graph_uris is None or ontology_graph_uris == miner.graph_uris
        if ontology_iris and (not miner.enrich or not shared_scope):
            from rdfsolve.mining.enrichment import query_enrichment
            from rdfsolve.schema_models.core import MinedSchema

            annotations = query_enrichment(
                MinedSchema(patterns=[], about=result.data_schema.about),
                miner._helper,
                ontology_graph_uris if ontology_graph_uris is not None else miner.graph_uris,
                examples_per_pattern=0,
                delay=miner.delay,
                report=miner._report,
                annotation_iris=ontology_iris,
            )
        result.data_schema = miner._finish_schema(
            result.data_schema, annotation_iris=ontology_iris if shared_scope else None
        )
        if result.ontology is not None:
            annotations = annotations or result.data_schema.enrichment
            terms = set(ontology_iris)
            result.ontology.annotations = [
                annotation
                for annotation in annotations.labels + annotations.definitions
                if annotation.term_iri in terms
            ]
        return result


def _mine_with_ontology(
    miner: SchemaMiner,
    extract_ontology: bool,
    extract_metadata: bool,
    dataset_name: str | None,
    ontology_graph_uris: list[str] | None,
    ontology_scope: Literal["schema", "full"],
) -> MiningResult:
    """Run optional phases within the miner's active report session."""
    ontology = None
    logger.info("Mining schema patterns (ABox)")
    data_schema = miner._mine_schema(dataset_name=dataset_name)
    subsumption = miner._report.report.config.get("ontology_term_subsumption") or {}
    if subsumption.get("subsumed"):
        data_schema.about.strategy = (
            f"{data_schema.about.strategy}+ontology-as-data"
            if data_schema.about.strategy
            else "ontology-as-data"
        )

    if extract_ontology:
        phase = miner._report.start_phase("ontology-extraction")
        try:
            visible = data_schema
            if miner.filter_service_namespaces:
                visible = visible.filter_service_namespaces()
            scope = ontology_graph_uris if ontology_graph_uris is not None else miner.graph_uris
            logger.info("Querying %s-scoped ontology axioms", ontology_scope)
            ontology = OntologyMiner(
                miner._helper,
                scope,
                class_iris=visible.get_classes() if ontology_scope == "schema" else None,
                property_iris=visible.get_properties() if ontology_scope == "schema" else None,
                batch_size=min(miner.class_batch_size, 50),
                delay=miner.delay,
            ).mine()
            miner._report.report.ontology_extraction = {
                "scope": ontology_scope,
                "graphs_mined": scope or [],
                "graph_count": len(scope or []),
                "classes": len(ontology.classes),
                "subclass_relations": len(ontology.subclass_relations),
                "subproperty_relations": len(ontology.subproperty_relations),
                "equivalent_classes": len(ontology.equivalent_classes),
                "equivalent_properties": len(ontology.equivalent_properties),
                "disjoint_classes": len(ontology.disjoint_classes),
                "deprecated_terms": len(ontology.deprecated_terms),
                "domain_assertions": len(ontology.domain_assertions),
                "range_assertions": len(ontology.range_assertions),
                "inverse_properties": len(ontology.inverse_properties),
                "property_characteristics": len(ontology.property_characteristics),
            }
        except Exception as error:
            miner._report.finish_phase(phase, error=str(error))
        else:
            miner._report.finish_phase(phase)

    metadata = None
    if extract_metadata:
        phase = miner._report.start_phase("infrastructure-metadata")
        logger.info("Extracting infrastructure metadata")
        try:
            metadata_miner = MetadataMiner(miner._helper, miner.graph_uris)
            metadata = metadata_miner.mine()
        except Exception as error:
            miner._report.finish_phase(phase, error=str(error))
        else:
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
