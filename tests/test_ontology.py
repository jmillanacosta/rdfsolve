"""Ontology evidence must improve local retrieval without changing its meaning."""

import json

import pytest
import requests
from rdflib import Graph, Literal, RDF, RDFS, URIRef

from rdfsolve.api import Client
from rdfsolve.catalogue import Catalogue
from rdfsolve.ontology import OntologyLookup, canonical_iri
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

MMO = "http://purl.obolibrary.org/obo/MMO_0000000"
TAXON = "http://purl.bioontology.org/ontology/NCBITAXON/131567"
HUMAN = "http://purl.bioontology.org/ontology/NCBITAXON/9606"
EVENT = "https://example.org/Event"


def fixture(lookup=False):
    schema = MinedSchema(about={"dataset_name": "test"}, patterns=[
        SchemaPattern(subject_class=EVENT, property_uri=MMO, object_class="Literal"),
        SchemaPattern(subject_class=TAXON, property_uri=str(RDFS.label), object_class="Literal"),
    ])
    graph = Graph()
    for triple in [(URIRef(HUMAN), RDF.type, URIRef(TAXON)),
                   (URIRef(HUMAN), RDFS.label, Literal("Homo sapiens")),
                   (URIRef("urn:impostor"), RDF.type, URIRef(TAXON)),
                   (URIRef("urn:impostor"), RDFS.label, Literal("Homo sapiens"))]:
        graph.add(triple)
    return Client(schema, graph, graph_uris=[], ontology_grounding=lookup)


def provider(monkeypatch, **kwargs):
    lookup = OntologyLookup(**kwargs)
    def response(path, **params):
        if path == "/search":
            iri = canonical_iri(HUMAN) if params["q"] == "human" else MMO
            return {"response": {"docs": [{"iri": iri, "label": "Homo sapiens"}]}}
        if path == "/terms":
            iri = params["iri"]
            return {"_embedded": {"terms": [{"iri": iri, "ontology_name": "test", "is_defining_ontology": True,
                "label": "measurement method" if iri == MMO else "Homo sapiens",
                "description": ["A procedure to measure a biological state."], "obo_synonym": [{"name":"human", "scope":"hasExactSynonym"}] if iri == canonical_iri(HUMAN) else []}]}}
        return {"_embedded": {"terms": [{"iri": "urn:parent", "label": "organism"}]}}
    monkeypatch.setattr(lookup, "_json", response)
    return lookup


def test_overlay_is_separate_and_available_through_client(monkeypatch):
    with fixture(provider(monkeypatch)) as client:
        before = client._schema.to_dict()
        index = Catalogue(client)
        ref = next(r for r in index.field_refs.values() if index.fragments[r].path.iri == MMO)
        index.explain(ref)
        assert index.metadata[ref]["ontology"][0]["label"] == "measurement method"
        matches = index.search("measurement method", owners=[EVENT])
        assert matches and index.fragments[matches[0]].path.iri == MMO
        evidence = index.metadata[matches[0]]["ontology"][0]
        assert evidence["label"] == "measurement method" and evidence["parents"]
        assert client.field_name(client.model(EVENT), "measurement method") == index.fragments[matches[0]].field_name
        assert "organism" not in index.schema_documents[matches[0]][0]
        assert client._schema.to_dict() == before
        assert not client.queries and "urn:parent" not in index.known_iris
        assert client.session_metadata()["ontology"]["evidence"]
        assert client.describe("measurement method", owners=[EVENT]).iloc[0]["Ontology evidence"]


def test_alias_requires_an_observed_matching_identity(monkeypatch):
    with fixture(provider(monkeypatch)) as client:
        result = client.find("human", kind=TAXON)
        assert {str(r.uri) for r in result} == {HUMAN}
        assert result.coverage["terms"] == ["human"]
        assert result.evidence and all(e["id"] == HUMAN for e in result.evidence)
        assert client.trace()["source_queries"] > 0
        assert client.trace()["ontology"]["requests"] > 0


def test_disabled_lookup_makes_no_external_requests(monkeypatch):
    monkeypatch.setattr(requests.Session, "get", lambda *a, **k: pytest.fail("external request"))
    with fixture() as client:
        assert not client.find("human", kind=TAXON)
        assert client.vocabulary(MMO) is None
        assert Catalogue(client).search("measurement method", owners=[EVENT]) == []


def test_cache_and_unavailable_are_distinct(monkeypatch, tmp_path):
    path = tmp_path / "ontology.json"
    lookup = provider(monkeypatch, cache=path)
    term = lookup.lookup(MMO)
    frozen = OntologyLookup(cache=path, offline=True)
    assert frozen.lookup(MMO)["label"] == term["label"]
    assert frozen.lookup("urn:missing") is None
    assert frozen.events[-1]["status"] == "cache_miss"
    failing = OntologyLookup()
    def timeout(*args, **kwargs):
        raise requests.Timeout("provider timed out")
    monkeypatch.setattr(failing, "_json", timeout)
    assert failing.lookup(MMO) is None
    assert failing.events[-1]["status"] == "unavailable" and not failing.cache
    assert json.loads(path.read_text())
    assert frozen.search("measurement method")[0]["iri"] == MMO
    assert frozen.events[-1]["cached"] and frozen.diagnostics()["requests"] == 0
    healthy = provider(monkeypatch)
    with fixture(failing) as client:
        assert client.vocabulary(MMO) is None
        monkeypatch.setattr(failing, "_json", healthy._json)
        assert client.vocabulary(MMO)["label"] == "measurement method"


def test_wrong_or_obsolete_terms_cannot_supply_evidence(monkeypatch):
    lookup = OntologyLookup()
    monkeypatch.setattr(lookup, "_json", lambda *a, **k: {"_embedded": {"terms": [
        {"iri": "urn:other", "label": "measurement method"},
        {"iri": MMO, "label": "outdated", "is_obsolete": True},
    ]}})
    assert lookup.lookup(MMO) is None


def test_ontobee_uses_shared_helper(monkeypatch):
    from rdfsolve.sparql_helper import SparqlHelper
    calls = []
    def select(self, query, **kwargs):
        calls.append(query)
        return {"results": {"bindings": [{"p": {"value": str(RDFS.label)}, "value": {"value": "measurement method"}}]}}
    monkeypatch.setattr(SparqlHelper, "select_with_fallback", select)
    with fixture(OntologyLookup("ontobee")) as client:
        assert client.ontology.lookup(MMO, hierarchy=False)["label"] == "measurement method"
        assert len(calls) == 1 and MMO in calls[0]


def test_diagram_has_stable_ids_and_does_not_query():
    with fixture() as client:
        chart = client.diagram(EVENT, TAXON)
        assert 'flowchart LR' in chart and 'C0[' in chart and 'C1[' in chart
        assert not client.queries


def test_ontobee_search_preserves_label_and_synonym_branches(monkeypatch):
    from rdfsolve.ontology import SYNONYMS
    from rdfsolve.sparql_helper import SparqlHelper

    graph = Graph().parse(data=f'<urn:term> <{RDFS.label}> "Homo sapiens"; <{SYNONYMS[0]}> "human" .', format="turtle")
    monkeypatch.setattr(SparqlHelper, "select_with_fallback", lambda self, query, **kwargs: json.loads(graph.query(query).serialize(format="json")))
    lookup = OntologyLookup("ontobee")
    assert lookup.search("human")[0]["iri"] == "urn:term"
    assert lookup.search("Homo sapiens")[0]["iri"] == "urn:term"
    assert not lookup.search('"} UNION {?s ?p ?o} #')
    graph.add((URIRef("urn:term"), RDFS.subClassOf, URIRef("urn:parent")))
    graph.add((URIRef("urn:parent"), RDFS.label, Literal("organism", lang="en")))
    graph.add((URIRef("urn:parent"), RDFS.label, Literal("organisme", lang="fr")))
    assert lookup.parents({"iri": "urn:term"}) == [{"iri": "urn:parent", "label": "organism"}]
    lookup.close()


def test_related_synonyms_do_not_identify_an_entity():
    term = OntologyLookup._term({"iri": HUMAN, "label": "Homo sapiens", "synonyms": ["human", "primate"],
        "obo_synonym": [{"name":"human", "scope":"hasExactSynonym"}, {"name":"primate", "scope":"hasBroadSynonym"}]})
    assert term["synonyms"] == ["human"]


def test_ols_nullable_metadata_is_empty_evidence():
    term = OntologyLookup._term({"iri": MMO, "label": "measurement method", "annotation": None,
        "exact_synonyms": None, "obo_synonym": None, "description": None})
    assert term["synonyms"] == term["namespace"] == term["description"] == []
