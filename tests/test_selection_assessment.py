"""Separate selected-data violations from source completeness and inactive checks."""
from rdflib import Dataset, Graph, SH, URIRef, Literal
from rdfsolve import SchemaMiner
from rdfsolve.client.api import Client

def test_selection_assessment_retains_evidence_and_limits():
    data=Dataset().parse(data='''@prefix e: <urn:example:> .
        e:data { e:a a e:Cell; e:ref e:r . e:b a e:Cell . e:r a e:Reference . }
    ''',format="trig")
    with SchemaMiner.from_graph(data,graph_uris=["urn:example:data"],delay=0) as miner:
        schema=miner.mine("references")
    with Client(schema,data) as client:
        selected=client.extract(schema.select(fields=[("urn:example:Cell","urn:example:ref")]),
                                root_class="urn:example:Cell")
    shapes=Graph().parse(data='''@prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:shape> a sh:NodeShape; sh:targetClass <urn:example:Cell>;
            sh:property [sh:path <urn:example:ref>; sh:minCount 1; sh:class <urn:example:Reference>] .
    ''',format="turtle")
    before=set(shapes)
    checked=selected.assess(shapes)
    assert checked.state=="violations" and checked.conforms is False
    assert any(item.focus.value=="urn:example:b" for item in checked.violations)
    assert checked.source_conforms is None, "A selected export cannot certify its whole source"
    assert checked.selection_rows==1 and checked.ontology_consistency=="not_checked"
    assert set(shapes)==before, "Validation must preserve declarations"
    shapes.set((URIRef("urn:shape"),SH.deactivated,Literal(True)))
    inactive=selected.assess(shapes)
    assert inactive.state=="not_checked" and inactive.conforms is None
    assert inactive.deactivated_shapes==1
    shapes.remove((URIRef("urn:shape"),SH.deactivated,None))
    prop=shapes.value(URIRef("urn:shape"),SH.property)
    shapes.set((prop,SH.path,URIRef("urn:example:unselected")))
    assert selected.assess(shapes).scope_warnings, "Name requirements outside extracted fields"

    ontology=Graph().parse(data="""@prefix owl: <http://www.w3.org/2002/07/owl#> .
        <urn:example:Cell> owl:disjointWith <urn:example:Reference> .
        <urn:example:a> a <urn:example:Reference> .
    """,format="turtle")
    reasoned=selected.assess(shapes,ontology=ontology,inference="owlrl")
    assert reasoned.ontology_errors, "External OWL-RL must report incompatible asserted classes"
    assert reasoned.ontology_consistency=="contradiction_reported"
