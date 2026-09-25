"""Generate typed records from a published vocabulary's domain and range declarations."""

import pytest
from rdflib import XSD, Graph, Literal, Namespace, URIRef

from rdfsolve.api import Client
from rdfsolve.schema_models import MinedSchema

S = Namespace("https://schema.org/")
VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix ex: <urn:ex:> .
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
ex:nick rdfs:label "nick"; rdfs:domain <http://www.w3.org/2002/07/owl#Thing>; rdfs:range rdfs:Literal .
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
    tagged = client.create("Organization", uri="urn:en", name="Institute", nick="I", language="en")
    assert tagged.to_graph().value(URIRef("urn:en"), S.name) == Literal("Institute", lang="en")
    assert tagged.to_graph().value(URIRef("urn:en"), URIRef("urn:ex:nick")) == Literal("I", lang="en"), "owl:Thing"
    with pytest.raises(ValueError, match="urn:ex:Unknown"):
        MinedSchema.from_vocabulary(VOCABULARY, classes=["urn:ex:Unknown"])
