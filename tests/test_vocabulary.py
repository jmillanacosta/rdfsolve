"""Generate typed records from a published vocabulary's domain and range declarations."""

import pytest
from rdflib.collection import Collection
from rdflib import DCTERMS, FOAF, RDFS, SH, XSD, Graph, Literal, Namespace, URIRef

from rdfsolve.api import Client, RDFList
from rdfsolve.schema_models import MinedSchema

S = Namespace("https://schema.org/")
VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix ex: <urn:ex:> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> . @prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix dcterms: <http://purl.org/dc/terms/> .
schema:Thing a rdfs:Class; rdfs:label "Thing" . schema:Person a rdfs:Class; rdfs:label "Person"; rdfs:subClassOf schema:Thing; rdfs:comment "A person." .
schema:Organization a rdfs:Class; rdfs:label "Organization"; rdfs:subClassOf schema:Thing .
schema:ResearchOrganization a rdfs:Class; rdfs:label "ResearchOrganization"; rdfs:subClassOf schema:Organization .
schema:Text a schema:DataType, rdfs:Class . schema:URL rdfs:subClassOf schema:Text . schema:Date a schema:DataType, rdfs:Class . schema:DateTime a schema:DataType, rdfs:Class .
schema:name rdfs:label "name"; schema:domainIncludes schema:Thing; schema:rangeIncludes schema:Text .
schema:sameAs rdfs:label "sameAs"; schema:domainIncludes schema:Thing; schema:rangeIncludes schema:URL .
schema:birthDate rdfs:label "birthDate"; schema:domainIncludes schema:Person; schema:rangeIncludes schema:Date, schema:DateTime .
schema:affiliation rdfs:label "affiliation"; schema:domainIncludes schema:Person; schema:rangeIncludes schema:Organization .
schema:knowsAbout schema:domainIncludes schema:Person; schema:rangeIncludes schema:Thing .
schema:familyName schema:domainIncludes schema:Person; schema:rangeIncludes schema:Text .
schema:Number a schema:DataType, rdfs:Class . schema:Integer rdfs:subClassOf schema:Number . schema:Boolean a schema:DataType .
schema:value schema:domainIncludes schema:Person; schema:rangeIncludes schema:Boolean, schema:Number, schema:Text .
schema:numberOfChildren schema:domainIncludes schema:Person; schema:rangeIncludes schema:Integer .
schema:colleague schema:domainIncludes schema:Person; schema:rangeIncludes schema:Person, rdf:List .
ex:rank rdfs:label "rank"; rdfs:domain schema:Person; rdfs:range xsd:integer .
foaf:Person a rdfs:Class . foaf:name rdfs:domain <http://www.w3.org/2002/07/owl#Thing>; rdfs:range rdfs:Literal .
dcterms:date a rdf:Property; rdfs:range rdfs:Literal; rdfs:label "Birth date" . dcterms:source a rdf:Property; rdfs:label "Source" .
foaf:family_name a owl:DatatypeProperty; rdfs:label "family_name" . foaf:givenName a owl:DatatypeProperty; rdfs:label "Given name" . foaf:givenname a owl:DatatypeProperty; rdfs:comment "Old" .
dcterms:creator a rdf:Property; <http://purl.org/dc/dcam/rangeIncludes> dcterms:Agent . ex:mentioned a rdf:Property .
"""


def test_vocabulary_declarations_become_models_and_rdf(caplog, recwarn, tmp_path):
    head, tail = VOCABULARY.split("schema:familyName")
    (tmp_path / "base.ttl").write_text(head)
    prefixes = "\n".join(line for line in head.splitlines() if line.startswith("@prefix"))
    alias = "@prefix dct: <http://purl.org/dc/terms/> . <urn:ex:page> dct:title 'Page' ."
    parts = [alias, tmp_path / "base.ttl", f"{prefixes}\nschema:familyName{tail}"]
    classes = [S.Person, S.Organization, S.ResearchOrganization]
    schema = MinedSchema.from_vocabulary(VOCABULARY, classes=classes)
    from_parts = MinedSchema.from_vocabulary(parts, classes=classes)
    assert from_parts.patterns == schema.patterns, "Files and text"
    assert from_parts.prefixes["dcterms"] == str(DCTERMS) and "dct" not in from_parts.prefixes, "Owner"
    assert schema.prefixes["foaf"] == str(FOAF) and "owl" not in schema.prefixes, "Declared prefixes in use"
    parents = {S.ResearchOrganization: S.Organization, S.Organization: S.Thing, S.Person: S.Thing}
    assert schema.class_hierarchy == {str(c): [str(p)] for c, p in parents.items()}, "Range-only Thing joins"
    assert {p.evidence_source for p in schema.patterns} == {"vocabulary"}, "Declared, not observed"
    person_rows = {p.property_uri for p in schema.patterns if p.subject_class == str(S.Person)}
    assert str(S.name) in person_rows, "Properties of ancestor classes are inherited"

    client = Client(schema, contract=True)  # Records are written without a data source.
    assert Client(schema, contract=True).model("Person") is client.model("Person"), "Built once"
    with pytest.raises(ValueError, match="No data source"):
        client.select("SELECT * WHERE { ?s ?p ?o }")
    org = client.create("ResearchOrganization", uri="urn:org", name="Institute")
    ada = client.create(
        "Person", uri="urn:ada", name="Ada", birthDate="1815-12", rank=1, affiliation=org, knowsAbout=org,
        sameAs=URIRef("https://www.wikidata.org/wiki/Q7259"), value="0000-0002",
    )
    graph, ada_iri = ada.to_graph() + org.to_graph(), URIRef("urn:ada")
    assert graph.value(ada_iri, S.birthDate) == Literal("1815-12", datatype=XSD.gYearMonth)
    assert graph.value(ada_iri, S.affiliation) == URIRef("urn:org"), "A subclass fills its parent's range"
    assert graph.value(ada_iri, S.knowsAbout) == URIRef("urn:org"), "Also when the range is not requested"
    assert not [r for r in caplog.records if r.levelname in {"ERROR", "WARNING"}], "Clean coercion"
    assert not [w for w in recwarn if "rdflib" in w.filename], "Values are not parsed to be tried"
    assert graph.value(URIRef("urn:org"), S.name) == Literal("Institute", datatype=XSD.string)
    assert graph.value(ada_iri, URIRef("urn:ex:rank")) == Literal(1, datatype=XSD.integer)
    person = client.model("Person")
    assert "A person." in (person.__doc__ or "") and "name" in person.model_fields, "Documented; plain names"
    research = client.model("ResearchOrganization")
    assert "name" in research.model_fields and "name" not in research.__annotations__, "Inherited"
    assert "mentioned" not in client.model("Person").model_fields, "Mentioned, not described"
    year = Literal("1815-12-10", datatype=XSD.date)
    tagged = client.create("Person", uri="urn:en", name="Ada", foaf_name="Ada", foaf_givenName="A", familyName="L", numberOfChildren=3, dcterms_date=year, language="en")
    assert {(p, o) for _, p, o in tagged.to_graph()} >= {
        (S.name, Literal("Ada", lang="en")),
        (FOAF.name, Literal("Ada", lang="en")),  # owl:Thing domain; a prefixed field
        (FOAF.givenName, Literal("A", lang="en")),  # A CURIE keeps its case: not foaf:givenname
        (S.familyName, Literal("L", lang="en")),  # The local name, in the vocabulary of the class
        (S.numberOfChildren, Literal(3)),  # Integer, not its parent Number
        (DCTERMS.date, year),  # No domain: any class. rdfs:Literal: any literal
    }
    cited = client.create("Person", uri="urn:c", dcterms_source=URIRef("urn:s"), dcterms_creator="urn:ada")
    assert set(cited.to_graph().objects()) >= {URIRef("urn:s"), URIRef("urn:ada")}, "No range; dcam ranges"
    with pytest.raises(ValueError, match="Ambiguous"):
        client.create("Person", uri="urn:c", dcterms_source="https://example.org/"), "IRI or text?"
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert not set(shapes.triples((None, SH.datatype, RDFS.Literal))), "Any literal: a node kind"
    team = client.create("Person", uri="urn:t", colleague=RDFList(items=[ada, "urn:b"])).to_graph()
    assert list(Collection(team, team.value(URIRef("urn:t"), S.colleague))) == [ada_iri, URIRef("urn:b")]
    linked = client.create("Person", uri="urn:l", sameAs=org).to_graph()
    assert (URIRef("urn:org"), S.name, None) in linked, "A record fills an IRI field with its statements"
    with pytest.raises(ValueError, match=r"^affiliation: a Person record cannot be the value\. Use"):
        client.create("Person", uri="urn:w", affiliation=client.create("Person", uri="urn:p"))
    with pytest.raises(ValueError, match=r"name\[1\]: missing value"):
        client.create("Person", uri="urn:m", name=["Ada", None])
    with pytest.raises(ValueError, match="urn:ex:Unknown"):
        MinedSchema.from_vocabulary(VOCABULARY, classes=["urn:ex:Unknown"])
