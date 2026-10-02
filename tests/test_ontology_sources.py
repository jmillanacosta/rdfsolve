from rdfsolve.ontology.sources import resolve_ontology_sources
from rdfsolve.ontology.usage import ObservedOntologyTerms


def test_provider_or_registry_source_can_resolve_non_obo_namespace():
    ns = "http://example.org/vocab#"
    observed = ObservedOntologyTerms(properties={ns + "p"})
    result = resolve_ontology_sources(
        observed, explicit_sources={ns: ("example-vocab", "https://example.org/vocab.ttl")}
    )
    assert result.resolved[0].resolution_basis == "explicit"
    assert result.resolved[0].source_url == "https://example.org/vocab.ttl"
