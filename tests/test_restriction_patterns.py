"""An ontology states relations between its terms with OWL restrictions: a class is a subclass
of (a property some filler). The restriction is a blank node, so the class patterns show only
(owl:Class, rdfs:subClassOf, owl:Restriction). The relations are mined as patterns of their
own (the owner decision of 2026-09-30): the terms are grouped by namespace, and each pattern
has the axiom, the property, the form, the filler, its counts, one example and a label in
Manchester syntax. The blank node is joined through and is not returned."""

from rdflib import Graph

from rdfsolve import SchemaMiner
from rdfsolve.mining.restrictions import mine_restriction_patterns
from rdfsolve.schema_models import AboutMetadata, MinedSchema

OBO = "http://purl.obolibrary.org/obo/"
DATA = f"""
@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix obo: <{OBO}> .
obo:RO_0001025 a owl:ObjectProperty ; rdfs:label "located in" .
obo:RO_0002452 a owl:ObjectProperty ; rdfs:label "has symptom" .
obo:DOID_1 a owl:Class ; rdfs:label "liver disease" ;
  rdfs:subClassOf [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ; owl:someValuesFrom obo:UBERON_0002107 ] ,
                  [ a owl:Restriction ; owl:onProperty obo:RO_0002452 ; owl:someValuesFrom obo:SYMP_1 ] .
obo:DOID_2 a owl:Class ; rdfs:label "heart disease" ;
  rdfs:subClassOf [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ; owl:someValuesFrom obo:UBERON_0000948 ] ,
                  [ a owl:Restriction ; owl:onProperty obo:RO_0002452 ; owl:allValuesFrom obo:SYMP_2 ] .
obo:DOID_3 a owl:Class ; owl:equivalentClass [ owl:intersectionOf ( obo:DOID_2
    [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ; owl:someValuesFrom obo:UBERON_0000948 ] ) ] .
obo:DOID_4 a owl:Class ; rdfs:subClassOf [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ;
    owl:someValuesFrom [ owl:unionOf ( obo:UBERON_1 obo:UBERON_2 ) ] ] .
obo:UBERON_0002107 rdfs:label "liver" .
"""


def _mine():
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        return mine_restriction_patterns(miner.helper)


def _key(p):
    return p.axiom, p.property_uri.rsplit("/", 1)[-1], p.form, p.filler_namespace.rsplit("/", 1)[-1]


def test_each_relation_of_the_ontology_is_a_pattern_with_its_counts():
    found = _mine()
    assert found.state == "complete" and found.query_count > 0
    by = {_key(p): p for p in found.patterns}
    assert set(by) == {
        ("SubClassOf", "RO_0001025", "some", "UBERON_"),
        ("SubClassOf", "RO_0001025", "some", "(class expression)"),
        ("SubClassOf", "RO_0002452", "some", "SYMP_"),
        ("SubClassOf", "RO_0002452", "only", "SYMP_"),
        ("EquivalentTo", "RO_0001025", "some", "UBERON_"),
    }
    located = by["SubClassOf", "RO_0001025", "some", "UBERON_"]
    assert (located.count, located.classes) == (2, 2) and located.subject_namespace == OBO + "DOID_"
    assert located.example_subject.startswith(OBO + "DOID_")
    assert located.example_filler.startswith(OBO + "UBERON_")


def test_the_label_is_in_manchester_syntax_with_the_labels_of_the_source():
    by = {_key(p): p for p in _mine().patterns}
    assert by["SubClassOf", "RO_0001025", "some", "UBERON_"].label == (
        "DOID SubClassOf 'located in' some UBERON"
    )
    assert by["SubClassOf", "RO_0002452", "only", "SYMP_"].label == (
        "DOID SubClassOf 'has symptom' only SYMP"
    )
    assert by["EquivalentTo", "RO_0001025", "some", "UBERON_"].label == (
        "DOID EquivalentTo (… and 'located in' some UBERON)"
    )


def test_the_patterns_are_kept_in_the_schema_file():
    schema = MinedSchema(about=AboutMetadata.build(dataset_name="x"), patterns=[])
    schema.restriction_patterns = _mine()
    read = MinedSchema.from_dict(schema.to_dict())
    assert read.restriction_patterns == schema.restriction_patterns
    assert MinedSchema.from_dict(MinedSchema(about=schema.about, patterns=[]).to_dict()).restriction_patterns is None


RELATIONS = "http://reasoner.renci.org/nonredundant"
MATERIALIZED = f"""
<{OBO}CL_1> <{OBO}BFO_0000050> <{OBO}UBERON_1> .
<{OBO}CL_2> <{OBO}BFO_0000050> <{OBO}UBERON_2> .
<{OBO}CL_2> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <{OBO}CL_1> .
<{OBO}CL_1> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <{OBO}UBERON_9> .
<{OBO}CL_1> <{OBO}BFO_0000050> "not a term" .
"""


def test_materialized_relations_are_patterns_with_their_evidence():
    """UberGraph stores X part_of Y for X SubClassOf (part_of some Y) in a relation graph; those
    edges are patterns of the same kind, marked materialized, next to the asserted ones."""
    from rdflib import Dataset, URIRef

    data = Dataset()
    data.default_graph.parse(data=DATA, format="turtle")
    data.graph(URIRef(RELATIONS)).parse(data=MATERIALIZED, format="nt")
    with SchemaMiner.from_graph(data, delay=0) as miner:
        found = mine_restriction_patterns(miner.helper, materialized_graph_uris=[RELATIONS])
    assert found.state == "complete"
    made = {(_key(p), p.subject_namespace.rsplit("/", 1)[-1]): p for p in found.patterns if p.evidence == "materialized"}
    part = made[("SubClassOf", "BFO_0000050", "some", "UBERON_"), "CL_"]
    assert (part.count, part.classes, part.graph_uri) == (2, 2, RELATIONS)
    assert part.label == "CL SubClassOf BFO_0000050 some UBERON", "No label of BFO_0000050 in the data"
    within = made[("SubClassOf", "rdf-schema#subClassOf", "named", "CL_"), "CL_"]
    assert within.label == "CL SubClassOf CL" and within.count == 1
    assert (("SubClassOf", "rdf-schema#subClassOf", "named", "UBERON_"), "CL_") in made
    assert len([p for p in found.patterns if p.evidence == "asserted"]) == 5, "The axioms as before"
    assert MinedSchema.from_dict(
        MinedSchema(about=AboutMetadata.build(dataset_name="x"), patterns=[], restriction_patterns=found).to_dict()
    ).restriction_patterns == found
