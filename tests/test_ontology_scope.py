"""Keep the ontology needed by shapes, not every imported descendant."""

from rdflib import RDF, RDFS, URIRef

from rdfsolve.schema_models.ontology import DomainAssertion, OntologyStructure, SubClassRelation

GENE = "http://purl.obolibrary.org/obo/SO_0000704"
ENTITY = "http://identifiers.org/ensembl/ENSG00000144229"


def test_gene_parent_remains_without_entity_leaf():
    ontology = OntologyStructure(subclass_relations=[SubClassRelation(child=ENTITY, parent=GENE)])
    selected = ontology.for_schema({GENE}, set())
    assert selected.classes == [GENE]
    assert selected.subclass_relations == []
    graph = selected.to_rdf_graph()
    assert (URIRef(GENE), RDF.type, RDFS.Class) in graph
    assert not list(graph.triples((URIRef(ENTITY), None, None)))
    assert len(ontology.subclass_relations) == 1


def test_used_leaf_class_is_not_removed_by_its_identifier():
    ontology = OntologyStructure(subclass_relations=[SubClassRelation(child=ENTITY, parent=GENE)])
    selected = ontology.for_schema({ENTITY}, set())
    assert selected.subclass_relations == ontology.subclass_relations
    assert set(selected.classes) == {ENTITY, GENE}


def test_walks_up_hierarchy_and_handles_cycles():
    ontology = OntologyStructure(
        subclass_relations=[
            SubClassRelation(child="urn:A", parent="urn:B"),
            SubClassRelation(child="urn:B", parent="urn:A"),
            SubClassRelation(child="urn:Unused", parent="urn:A"),
        ]
    )
    selected = ontology.for_schema({"urn:A"}, set())
    assert selected.classes == ["urn:A", "urn:B"]
    assert len(selected.subclass_relations) == 2


def test_domain_of_used_property_seeds_scope():
    ontology = OntologyStructure(
        domain_assertions=[
            DomainAssertion(property_uri="urn:p", domain=GENE),
            DomainAssertion(property_uri="urn:unused", domain="urn:Other"),
        ]
    )
    selected = ontology.for_schema(set(), {"urn:p"})
    assert selected.classes == [GENE]
    assert len(selected.domain_assertions) == 1


def test_empty_schema_has_empty_ontology_slice():
    ontology = OntologyStructure(subclass_relations=[SubClassRelation(child=ENTITY, parent=GENE)])
    assert len(ontology.for_schema(set(), set()).to_rdf_graph()) == 0


def test_used_types_do_not_create_an_embedded_ontology():
    ontology = OntologyStructure().for_schema(["urn:UsedType"], ["urn:p"])
    assert ontology.classes == []
    assert len(ontology.to_rdf_graph()) == 0
