from rdflib import Graph, Literal, RDF, URIRef

from rdfsolve.api import resolve_identifiers
from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models.enrichment import RdfTerm


def test_identifier_policy_preserves_terms_and_reports_target_evidence():
    source = SourceModel(
        name="mesh", bioregistry_prefix="mesh", bioregistry_synonyms=["MESH"],
        bioregistry_uri_prefixes=["http://id.nlm.nih.gov/mesh/", "https://example.org/mesh/"],
    )
    graph = Graph().parse(data="""
        @prefix mesh: <http://id.nlm.nih.gov/mesh/> .
        @prefix e: <https://example.org/> .
        mesh:D000001 a e:Concept .
        e:gene e:ref mesh:D000001 .
        mesh:D000002 a e:Concept .
        <https://example.org/mesh/D000002> a e:Concept .
    """, format="turtle")
    target = {str(s) for s in graph.subjects(RDF.type, URIRef("https://example.org/Concept"))}
    values = [RdfTerm.from_rdf(v) for v in [
        Literal("MESH:D000001"), Literal("MESH:D000002"), Literal("MESH:D999999"),
        URIRef("http://id.nlm.nih.gov/mesh/D000001"),
        URIRef("https://example.org/mesh/D000001"),
        Literal("MESH:D000001", lang="en"), Literal("UNKNOWN:D000001"),
    ]]
    original = [v.model_dump() for v in values]
    observed = resolve_identifiers(values, source, target_iris=target)
    assert [r.status for r in observed.results] == [
        "unresolved", "unresolved", "unresolved", "exact", "unresolved", "unresolved", "unresolved"
    ]
    resolved = resolve_identifiers(values, source, target_iris=target, mode="namespace")
    assert [r.status for r in resolved.results] == [
        "resolved", "ambiguous", "unresolved", "exact", "resolved", "unresolved", "unresolved"
    ]
    assert resolved.results[0].matches == ["http://id.nlm.nih.gov/mesh/D000001"]
    assert resolved.results[1].matches == sorted(target - {resolved.results[0].matches[0]})
    assert list(graph.subjects(URIRef("https://example.org/ref"),
                               URIRef(resolved.results[0].matches[0]))) == [URIRef("https://example.org/gene")]
    assert type(resolved).model_validate_json(resolved.model_dump_json()) == resolved
    assert [v.model_dump() for v in values] == original
    assert len(graph) == 4
