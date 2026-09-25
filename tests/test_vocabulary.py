"""Generate typed records from a published vocabulary's domain and range declarations."""

import pytest
from rdflib import DCTERMS, FOAF, RDFS, SH, XSD, Graph, Literal, Namespace, URIRef

from rdfsolve.api import Client
from rdfsolve.schema_models import MinedSchema

S = Namespace("https://schema.org/")
VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix ex: <urn:ex:> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> . @prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix dcterms: <http://purl.org/dc/terms/> .
schema:Thing a rdfs:Class; rdfs:label "Thing" .
schema:Person a rdfs:Class; rdfs:label "Person"; rdfs:subClassOf schema:Thing; rdfs:comment "A person." .
schema:Organization a rdfs:Class; rdfs:label "Organization"; rdfs:subClassOf schema:Thing .
schema:ResearchOrganization a rdfs:Class; rdfs:label "ResearchOrganization"; rdfs:subClassOf schema:Organization .
schema:Text a schema:DataType, rdfs:Class . schema:URL rdfs:subClassOf schema:Text .
schema:Date a schema:DataType, rdfs:Class .
schema:name rdfs:label "name"; schema:domainIncludes schema:Thing; schema:rangeIncludes schema:Text .
schema:sameAs rdfs:label "sameAs"; schema:domainIncludes schema:Thing; schema:rangeIncludes schema:URL .
schema:birthDate rdfs:label "birthDate"; schema:domainIncludes schema:Person; schema:rangeIncludes schema:Date .
schema:affiliation rdfs:label "affiliation"; schema:domainIncludes schema:Person;
    schema:rangeIncludes schema:Organization .
ex:rank rdfs:label "rank"; rdfs:domain schema:Person; rdfs:range xsd:integer .
foaf:Person a rdfs:Class . foaf:name rdfs:domain <http://www.w3.org/2002/07/owl#Thing>; rdfs:range rdfs:Literal .
dcterms:date a rdf:Property; rdfs:range rdfs:Literal . dcterms:source a rdf:Property; rdfs:label "Source" .
foaf:givenName a owl:DatatypeProperty; rdfs:label "Given name" . foaf:givenname a owl:DatatypeProperty; rdfs:comment "Old" .
dcterms:creator a rdf:Property; <http://purl.org/dc/dcam/rangeIncludes> dcterms:Agent . ex:mentioned a rdf:Property .
"""


def test_vocabulary_declarations_become_models_and_rdf(caplog):
    schema = MinedSchema.from_vocabulary(VOCABULARY, classes=[S.Person, S.Organization, S.ResearchOrganization])
    assert schema.class_hierarchy == {str(S.ResearchOrganization): [str(S.Organization)]}
    assert {p.evidence_source for p in schema.patterns} == {"vocabulary"}, "Declared, not observed"
    person_rows = {p.property_uri for p in schema.patterns if p.subject_class == str(S.Person)}
    assert str(S.name) in person_rows, "Properties of ancestor classes are inherited"

    client = Client(schema, Graph(), contract=True)
    org = client.create("ResearchOrganization", uri="urn:org", name="Institute")
    ada = client.create(
        "Person", uri="urn:ada", name="Ada", birthDate="1815-12", rank=1, affiliation=org,
        sameAs=URIRef("https://www.wikidata.org/wiki/Q7259"),
    )
    graph = ada.to_graph() + org.to_graph()
    ada_iri = URIRef("urn:ada")
    assert graph.value(ada_iri, S.birthDate) == Literal("1815-12", datatype=XSD.gYearMonth)
    assert graph.value(ada_iri, S.affiliation) == URIRef("urn:org"), "A subclass fills its parent's range"
    assert not [r for r in caplog.records if r.levelname in {"ERROR", "WARNING"}], "Clean coercion"
    assert graph.value(URIRef("urn:org"), S.name) == Literal("Institute", datatype=XSD.string)
    assert graph.value(ada_iri, URIRef("urn:ex:rank")) == Literal(1, datatype=XSD.integer)
    assert "A person." in (client.model("Person").__doc__ or ""), "Definitions document the model"
    assert "name" in client.model("Person").model_fields, "Classes with rows keep the plain name"
    assert "mentioned" not in client.model("Person").model_fields, "Mentioned, not described"
    year = Literal("1815", datatype=XSD.gYear)
    tagged = client.create("Person", uri="urn:en", name="Ada", foaf_name="Ada", foaf_givenName="A", dcterms_date=year, language="en")
    en = tagged.to_graph()
    assert en.value(URIRef("urn:en"), S.name) == Literal("Ada", lang="en")
    assert en.value(URIRef("urn:en"), FOAF.name) == Literal("Ada", lang="en"), "owl:Thing domain; prefixed field"
    assert en.value(URIRef("urn:en"), FOAF.givenName) == Literal("A", lang="en"), "CURIEs keep case"
    assert en.value(URIRef("urn:en"), DCTERMS.date) == year, "No domain: any class; rdfs:Literal: any literal"
    cited = client.create("Person", uri="urn:c", dcterms_source=URIRef("urn:s"), dcterms_creator="urn:ada")
    assert set(cited.to_graph().objects()) >= {URIRef("urn:s"), URIRef("urn:ada")}, "No range; dcam ranges"
    with pytest.raises(ValueError, match="Ambiguous"):
        client.create("Person", uri="urn:c", dcterms_source="https://example.org/"), "IRI or text?"
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert not set(shapes.triples((None, SH.datatype, RDFS.Literal))), "Any literal: a node kind"
    with pytest.raises(ValueError, match="urn:ex:Unknown"):
        MinedSchema.from_vocabulary(VOCABULARY, classes=["urn:ex:Unknown"])
