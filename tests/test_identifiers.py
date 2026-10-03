"""rdfsolve.identifiers: identifiers are parsed with their validity and resolved in any registered form."""

import pytest
from rdflib import RDF, Graph, Literal, URIRef

from rdfsolve.api import resolve_identifiers
from rdfsolve.identifiers import candidates, canonical_iri, curie, parse
from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models.enrichment import RdfTerm

OBO = "http://purl.obolibrary.org/obo/"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (OBO + "CHEBI_15377", ("chebi", "15377", True)),
        ("CHEBI:15377", ("chebi", "15377", True)),
        ("https://identifiers.org/chebi/72297", ("chebi", "72297", True)),
        ("http://identifiers.org/mgi/101757", ("mgi", "101757", True)),
        ("http://purl.uniprot.org/uniprot/P53_HUMAN", ("uniprot", "P53_HUMAN", False)),
        ("vega:OTTHUMG1", ("vega", "OTTHUMG1", None)),
    ],
)
def test_iris_and_curies_are_read_with_their_validity(value, expected):
    found = parse(value)
    assert (found.prefix, found.local, found.valid) == expected


def test_what_is_not_a_registered_identifier_is_none():
    assert parse("hello") is None and parse("urn:x:y") is None
    assert parse("http://example.org/thing") is None


def test_forms_keys_and_candidates_are_built_on_parse():
    assert curie(OBO + "GO_0006915") == "go:0006915"
    assert curie("http://example.org/thing") == "http://example.org/thing"
    assert canonical_iri("https://identifiers.org/GO:0006915") == OBO + "GO_0006915"
    iris, coverage = candidates("CHEBI:15377")
    assert OBO + "CHEBI_15377" in iris and coverage["basis"] == "registered namespace candidates"
    assert candidates("http://example.org/thing")[0] == ["http://example.org/thing"]
    with pytest.raises(ValueError, match="Invalid"):
        candidates("uniprot:P53_HUMAN")


def test_identifier_policy_preserves_terms_and_reports_target_evidence():
    source = SourceModel(
        name="mesh",
        bioregistry_prefix="mesh",
        bioregistry_synonyms=["MESH"],
        bioregistry_uri_prefixes=["http://id.nlm.nih.gov/mesh/", "https://example.org/mesh/"],
    )
    graph = Graph().parse(
        data="""
        @prefix mesh: <http://id.nlm.nih.gov/mesh/> .
        @prefix e: <https://example.org/> .
        mesh:D000001 a e:Concept .
        e:gene e:ref mesh:D000001 .
        mesh:D000002 a e:Concept .
        <https://example.org/mesh/D000002> a e:Concept .
    """,
        format="turtle",
    )
    target = {str(s) for s in graph.subjects(RDF.type, URIRef("https://example.org/Concept"))}
    values = [
        RdfTerm.from_rdf(v)
        for v in [
            Literal("MESH:D000001"),
            Literal("MESH:D000002"),
            Literal("MESH:D999999"),
            URIRef("http://id.nlm.nih.gov/mesh/D000001"),
            URIRef("https://example.org/mesh/D000001"),
            Literal("MESH:D000001", lang="en"),
            Literal("UNKNOWN:D000001"),
        ]
    ]
    original = [v.model_dump() for v in values]
    observed = resolve_identifiers(values, source, target_iris=target)
    assert [r.status for r in observed.results] == [
        "unresolved",
        "unresolved",
        "unresolved",
        "exact",
        "unresolved",
        "unresolved",
        "unresolved",
    ]
    resolved = resolve_identifiers(values, source, target_iris=target, mode="namespace")
    assert [r.status for r in resolved.results] == [
        "resolved",
        "ambiguous",
        "unresolved",
        "exact",
        "resolved",
        "unresolved",
        "unresolved",
    ]
    assert resolved.results[0].matches == ["http://id.nlm.nih.gov/mesh/D000001"]
    assert resolved.results[1].matches == sorted(target - {resolved.results[0].matches[0]})
    assert list(
        graph.subjects(URIRef("https://example.org/ref"), URIRef(resolved.results[0].matches[0]))
    ) == [URIRef("https://example.org/gene")]
    assert type(resolved).model_validate_json(resolved.model_dump_json()) == resolved
    assert [v.model_dump() for v in values] == original
    assert len(graph) == 4

    query = """SELECT ?resolution ?input ?target ?gene WHERE {
        %s
        ?gene <https://example.org/ref> ?target .
    }"""
    rows = list(graph.query(query % resolved.to_sparql_values()))
    assert {int(row.resolution) for row in rows} == {0, 3, 4}
    assert all(row.gene == URIRef("https://example.org/gene") for row in rows)
    assert next(row.input for row in rows if int(row.resolution) == 0) == Literal("MESH:D000001")
    empty = resolve_identifiers(values[:1], source, target_iris=target)
    assert not list(graph.query(query % empty.to_sparql_values()))
