"""rdfsolve.ontology.service: ontology terms are looked up, cached and answered with their sources."""

import json

import requests
from rdflib import RDF, RDFS, Graph, Literal, URIRef

from rdfsolve.api import Client
from rdfsolve.identifiers import canonical_iri
from rdfsolve.ontology import Ontologies
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

MMO = "http://purl.obolibrary.org/obo/MMO_0000000"
TAXON = "http://purl.bioontology.org/ontology/NCBITAXON/131567"
HUMAN = "http://purl.bioontology.org/ontology/NCBITAXON/9606"
EVENT = "https://example.org/Event"


def fixture(lookup=False):
    schema = MinedSchema(
        about={"dataset_name": "test"},
        patterns=[
            SchemaPattern(subject_class=EVENT, property_uri=MMO, object_class="Literal"),
            SchemaPattern(
                subject_class=TAXON, property_uri=str(RDFS.label), object_class="Literal"
            ),
        ],
    )
    graph = Graph()
    for node in (URIRef(HUMAN), URIRef("urn:impostor")):  # The impostor has the name, not the IRI.
        graph += [(node, RDF.type, URIRef(TAXON)), (node, RDFS.label, Literal("Homo sapiens"))]
    return Client(schema, graph, graph_uris=[], ontology_grounding=lookup)


def provider(monkeypatch, **kwargs):
    lookup = Ontologies(**kwargs)
    lookup.asked = []

    def response(path, **params):
        lookup.asked.append(params)
        if path == "/search":
            iri = canonical_iri(HUMAN) if params["q"] == "human" else MMO
            docs = [{"iri": iri, "label": "Homo sapiens"}]
            docs += [{"iri": "urn:other", "label": "human"}] if params["q"] == "human" else []
            return {"response": {"docs": docs * (params["rows"] if params["q"] == "many" else 1)}}
        if path == "/terms":
            iri = params["iri"]
            return {
                "_embedded": {
                    "terms": [
                        {
                            "iri": iri,
                            "ontology_name": "test",
                            "is_defining_ontology": True,
                            "label": "measurement method" if iri == MMO else "Homo sapiens",
                            "description": ["A procedure to measure a biological state."],
                            "obo_synonym": [{"name": "human", "scope": "hasExactSynonym"}]
                            if iri == canonical_iri(HUMAN)
                            else [],
                        }
                    ]
                }
            }
        return {"_embedded": {"terms": [{"iri": "urn:parent", "label": "organism"}]}}

    monkeypatch.setattr(lookup, "_json", response)
    return lookup


def test_cache_and_unavailable_are_distinct(monkeypatch, tmp_path):
    path = tmp_path / "ontology.json"
    lookup = provider(monkeypatch, cache=path)
    term = lookup.lookup(MMO)
    frozen = Ontologies(cache=path, offline=True)
    assert frozen.lookup(MMO)["label"] == term["label"]
    assert frozen.lookup("urn:missing") is None
    assert frozen.events[-1]["status"] == "cache_miss"
    failing = Ontologies()

    def timeout(*args, **kwargs):
        raise requests.Timeout("provider timed out")

    monkeypatch.setattr(failing, "_json", timeout)
    assert failing.lookup(MMO) is None
    assert failing.events[-1]["status"] == "unavailable" and (not failing.cache)
    assert json.loads(path.read_text())
    assert frozen.search("measurement method")[0]["iri"] == MMO
    assert frozen.events[-1]["cached"] and frozen.diagnostics()["requests"] == 0
    healthy = provider(monkeypatch)
    with fixture(failing) as client:
        assert client.vocabulary(MMO) is None
        monkeypatch.setattr(failing, "_json", healthy._json)
        assert client.vocabulary(MMO)["label"] == "measurement method"
    online = provider(monkeypatch)
    online.lookup(HUMAN)
    found = [term["iri"] for term in online.search("human")]
    assert found == [canonical_iri(HUMAN), "urn:other"], "Retained terms must not replace a search"
    assert online.events[-1]["possibly_truncated"] is False
    online.search("many")
    assert online.events[-1]["possibly_truncated"], "A full page may hide further candidates"
    asked = len(online.asked)
    assert online.search("human", exact=False), "Candidates for a phrase"
    assert len(online.asked) == asked + 1 and online.asked[-1]["exact"] == "false", (
        "Not exact names"
    )


def test_a_term_ols_files_under_another_iri_is_found_by_its_obo_id(monkeypatch):
    """Bioregistry gives SBO terms as OBO PURLs; OLS files them under biomodels.net/SBO/."""
    from rdfsolve.ontology import Ontologies

    term = {
        "iri": "http://biomodels.net/SBO/SBO_0000027",
        "obo_id": "SBO:0000027",
        "label": "Michaelis constant",
        "ontology_name": "sbo",
        "is_defining_ontology": True,
        "description": [],
        "synonyms": [],
    }

    def answer(path, **params):
        if params.get("obo_id") == "SBO:0000027":
            return {"_embedded": {"terms": [term]}}
        return {"_embedded": {"terms": []}}

    lookup = Ontologies()
    monkeypatch.setattr(lookup, "_json", answer)
    found = lookup.lookup("http://purl.obolibrary.org/obo/SBO_0000027", hierarchy=False)
    assert found is not None and found["label"] == "Michaelis constant"
