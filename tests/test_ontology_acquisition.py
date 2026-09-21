from rdfsolve.evidence.ontology import (
    ObservedOntologyTerms,
    OntologyGraphCandidate,
    OntologyVersionEvidence,
)
from rdfsolve.evidence.ontology_acquisition import build_ontology_acquisition_plan


def test_provider_graph_without_empirical_overlap_is_not_usage():
    observed = ObservedOntologyTerms(classes={"http://example.org/domain#A"})
    graph = OntologyGraphCandidate(
        graph_uri="http://example.org/ontology",
        explicit_ontology_iris=["http://example.org/onto"],
        candidate_reasons=["owl_ontology_declaration"],
    )
    plan = build_ontology_acquisition_plan("dataset", observed, graph_candidates=[graph])
    item = plan.candidates[0]
    assert item.namespace == "http://example.org/domain#"
    assert item.graph_evidence == []
    assert item.ontology_id is None


def test_provider_graph_with_empirical_overlap_is_attributed_but_not_reference_download():
    term = "http://example.org/domain#A"
    observed = ObservedOntologyTerms(classes={term})
    graph = OntologyGraphCandidate(
        graph_uri="http://example.org/graphs/ontology",
        explicit_ontology_iris=["http://example.org/onto"],
        version_evidence=[
            OntologyVersionEvidence(
                subject="http://example.org/onto",
                predicate="http://www.w3.org/2002/07/owl#versionInfo",
                value="2026-01",
                scope="ontology",
            )
        ],
        candidate_reasons=["owl_ontology_declaration"],
        observed_class_overlap=[term],
    )
    plan = build_ontology_acquisition_plan(
        "dataset",
        observed,
        graph_candidates=[graph],
        endpoint_url="https://example.org/sparql",
    )
    item = plan.candidates[0]
    assert item.ontology_id == "http://example.org/onto"
    assert item.identity_basis == "declared_ontology_iri"
    assert item.has_declared_graph_evidence
    assert not item.has_reference_source
    assert item.graph_evidence[0].endpoint_url == "https://example.org/sparql"
    assert item.graph_evidence[0].version_evidence[0].scope == "ontology"


def test_obo_reference_and_provider_graph_are_kept_separate():
    term = "http://purl.obolibrary.org/obo/CHEBI_15377"
    observed = ObservedOntologyTerms(classes={term})
    graph = OntologyGraphCandidate(
        graph_uri="http://example.org/chebi-ontology",
        explicit_ontology_iris=["http://purl.obolibrary.org/obo/chebi.owl"],
        candidate_reasons=["owl_ontology_declaration"],
        observed_class_overlap=[term],
    )
    plan = build_ontology_acquisition_plan(
        "dataset", observed, graph_candidates=[graph], endpoint_url="https://example.org/sparql"
    )
    item = plan.candidates[0]
    assert item.ontology_id == "chebi"
    assert item.identity_basis == "reference_source"
    assert item.reference_sources[0].source_url == "https://purl.obolibrary.org/obo/chebi.owl"
    assert item.graph_evidence[0].graph_uri == "http://example.org/chebi-ontology"


def test_explicit_reference_source_resolves_non_obo_namespace():
    term = "http://example.org/vocab#Thing"
    observed = ObservedOntologyTerms(classes={term})
    plan = build_ontology_acquisition_plan(
        "dataset",
        observed,
        explicit_sources={
            "http://example.org/vocab#": ("example-vocab", "https://example.org/vocab.owl")
        },
    )
    item = plan.candidates[0]
    assert item.ontology_id == "example-vocab"
    assert item.has_reference_source
    assert item.identity_basis == "reference_source"


def test_infrastructure_namespace_is_not_domain_ontology_candidate():
    observed = ObservedOntologyTerms(
        classes={"http://www.w3.org/2004/02/skos/core#Concept"},
        properties={"http://www.w3.org/2004/02/skos/core#prefLabel"},
    )
    plan = build_ontology_acquisition_plan("dataset", observed)
    assert not plan.candidates
    assert "http://www.w3.org/2004/02/skos/core#" in plan.infrastructure_namespaces
