"""A link can reach an OWL construct and not an entity: an owl:Restriction or an owl:Axiom
holds the identifier as a value. The route is resolved to the term that the construct
describes (the subclass of the restriction, the annotated source of the axiom), so that it
ends at an entity. A path of rdf:type steps is not a relation and is not tested."""

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings.routes import Route, check_routes, federated_query, propose_segments
from rdfsolve.mappings.signatures import Link, LinkEvidence
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

OWL, RDFS = "http://www.w3.org/2002/07/owl#", "http://www.w3.org/2000/01/rdf-schema#"
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
UBERON = "http://purl.obolibrary.org/obo/UBERON_"
SOURCE = f"""
<urn:ke/1> a <urn:KeyEvent> ; <urn:organ> <{UBERON}0002107> .
<urn:ke/2> a <urn:KeyEvent> ; <urn:organ> <{UBERON}0000948> .
"""
TARGET = f"""
@prefix owl: <{OWL}> . @prefix rdfs: <{RDFS}> .
<urn:doid/liver-disease> a owl:Class ; rdfs:label "liver disease" ;
  rdfs:subClassOf [ a owl:Restriction ; owl:onProperty <urn:located_in> ;
                    owl:someValuesFrom <{UBERON}0002107> ] .
[] a owl:Axiom ; owl:annotatedSource <urn:doid/heart-disease> ;
  owl:annotatedTarget <{UBERON}0000948> .
<urn:doid/heart-disease> a owl:Class ; rdfs:label "heart disease" .
"""
ORGAN = SchemaPattern(subject_class="urn:KeyEvent", property_uri="urn:organ", object_class="Resource")
LABEL = SchemaPattern(subject_class=OWL + "Class", property_uri=RDFS + "label", object_class="Literal")
SUPER = SchemaPattern(subject_class=OWL + "Class", property_uri=RDFS + "subClassOf", object_class=OWL + "Class")
TYPED = SchemaPattern(subject_class=OWL + "Class", property_uri=TYPE, object_class=OWL + "Class")
EVENTS = MinedSchema(about=AboutMetadata.build(dataset_name="events"), patterns=[ORGAN])
DISEASES = MinedSchema(about=AboutMetadata.build(dataset_name="diseases"), patterns=[LABEL, SUPER, TYPED])


def _link(construct, prop):
    return Link("shared", "events", "urn:KeyEvent", "urn:organ", "uberon", "diseases", OWL + construct, OWL + prop)


def _check(link):
    befores, afters = propose_segments(link, EVENTS, DISEASES)
    source = Dataset().parse(format="turtle", data=SOURCE)
    target = Dataset().parse(format="turtle", data=TARGET)
    with Client(EVENTS, source) as s, Client(DISEASES, target) as t:
        return check_routes(link, befores, afters, s, t, sample=None)


def test_the_paths_after_a_construct_start_at_the_term_and_have_no_type_step():
    _, afters = propose_segments(_link("Restriction", "someValuesFrom"), EVENTS, DISEASES)
    assert [[p.property_uri for p in a] for a in afters] == [[], [RDFS + "subClassOf"]]


def test_a_route_to_a_restriction_or_an_axiom_ends_at_the_described_term():
    for construct, prop in (("Restriction", "someValuesFrom"), ("Axiom", "annotatedTarget")):
        result = _check(_link(construct, prop))
        (resolved,) = [e for e in result.routes if not e.route.after]
        assert (resolved.starts, resolved.matched) == (2, 1)
        assert resolved.route.end_class == OWL + "Class" and resolved.route.resolved == construct


def test_the_federated_query_resolves_the_construct():
    link = _link("Restriction", "someValuesFrom")
    evidence = LinkEvidence(link, 2, 1, {UBERON + "{id}": 1}, [(UBERON + "0002107", UBERON + "0002107")])
    query = federated_query(Route(link), evidence, {"events": "https://e/sparql", "diseases": "https://d/sparql"})
    assert f"?e <{RDFS}subClassOf> ?x ." in query and f"?x <{OWL}someValuesFrom> ?t ." in query
