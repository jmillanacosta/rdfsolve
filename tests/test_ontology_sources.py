from rdfsolve.evidence.ontology import ObservedOntologyTerms
from rdfsolve.evidence.ontology_sources import resolve_ontology_sources, term_namespace


def test_obo_terms_resolve_to_full_ontology_candidate_without_claiming_usage():
    observed = ObservedOntologyTerms(
        classes={"http://purl.obolibrary.org/obo/CHEBI_15377"},
        properties={"http://purl.obolibrary.org/obo/RO_0000056"},
    )
    result = resolve_ontology_sources(observed)
    assert [(r.ontology_id, r.source_url) for r in result.resolved] == [
        ("chebi", "https://purl.obolibrary.org/obo/chebi.owl"),
        ("ro", "https://purl.obolibrary.org/obo/ro.owl"),
    ]
    assert result.unresolved == []


def test_non_obo_namespace_is_not_given_a_guessed_download_url():
    observed = ObservedOntologyTerms(classes={"http://purl.bioontology.org/ontology/NCBITAXON/9606"})
    result = resolve_ontology_sources(observed)
    assert not result.resolved
    assert result.unresolved[0].namespace == "http://purl.bioontology.org/ontology/NCBITAXON/"


def test_provider_or_registry_source_can_resolve_non_obo_namespace():
    ns = "http://example.org/vocab#"
    observed = ObservedOntologyTerms(properties={ns + "p"})
    result = resolve_ontology_sources(
        observed,
        explicit_sources={ns: ("example-vocab", "https://example.org/vocab.ttl")},
    )
    assert result.resolved[0].resolution_basis == "explicit"
    assert result.resolved[0].source_url == "https://example.org/vocab.ttl"


def test_standard_rdf_namespaces_are_not_ontology_download_candidates():
    observed = ObservedOntologyTerms(
        classes={"http://www.w3.org/2002/07/owl#Class"},
        properties={"http://www.w3.org/2000/01/rdf-schema#label"},
    )
    result = resolve_ontology_sources(observed)
    assert result.resolved == [] and result.unresolved == []


def test_obo_namespace_grouping_is_prefix_specific():
    assert term_namespace("http://purl.obolibrary.org/obo/GO_0008150") == "http://purl.obolibrary.org/obo/GO_"


def test_metadata_vocabulary_is_classified_as_infrastructure_not_unresolved_domain_ontology():
    observed = ObservedOntologyTerms(properties={"http://purl.org/dc/terms/title"})
    result = resolve_ontology_sources(observed)
    assert not result.resolved and not result.unresolved
    assert [item.namespace for item in result.infrastructure] == ["http://purl.org/dc/terms/"]
