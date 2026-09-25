import json

import requests
from rdflib import RDF, RDFS, Graph, Literal, URIRef
from rdfsolve.api import Client
from rdfsolve.client.ontology import OntologyLookup, canonical_iri
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
    for triple in [
        (URIRef(HUMAN), RDF.type, URIRef(TAXON)),
        (URIRef(HUMAN), RDFS.label, Literal("Homo sapiens")),
        (URIRef("urn:impostor"), RDF.type, URIRef(TAXON)),
        (URIRef("urn:impostor"), RDFS.label, Literal("Homo sapiens")),
    ]:
        graph.add(triple)
    return Client(schema, graph, graph_uris=[], ontology_grounding=lookup)


def provider(monkeypatch, **kwargs):
    lookup = OntologyLookup(**kwargs)

    def response(path, **params):
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
    frozen = OntologyLookup(cache=path, offline=True)
    assert frozen.lookup(MMO)["label"] == term["label"]
    assert frozen.lookup("urn:missing") is None
    assert frozen.events[-1]["status"] == "cache_miss"
    failing = OntologyLookup()

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
