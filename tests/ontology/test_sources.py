"""rdfsolve.ontology.sources: ontology sources and their acquisition."""

from rdfsolve.ontology.discovery import OntologyGraphCandidate
from rdfsolve.ontology.sources import build_ontology_acquisition_plan, resolve_ontology_sources
from rdfsolve.ontology.usage import ObservedOntologyTerms


def test_provider_or_registry_source_can_resolve_non_obo_namespace():
    ns = "http://example.org/vocab#"
    observed = ObservedOntologyTerms(properties={ns + "p"})
    result = resolve_ontology_sources(
        observed, explicit_sources={ns: ("example-vocab", "https://example.org/vocab.ttl")}
    )
    assert result.resolved[0].resolution_basis == "explicit"
    assert result.resolved[0].source_url == "https://example.org/vocab.ttl"


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
