from rdflib import OWL, URIRef
from rdfsolve.schema_models import AboutMetadata, MinedSchema
from rdfsolve.schema_models.ontology import OntologyStructure


def test_generated_ontology_annotation_has_no_owl_version_info():
    schema = MinedSchema(
        patterns=[],
        about=AboutMetadata.build(dataset_name="test", finished_at="2026-09-18T10:00:00+00:00"),
    )
    graph = OntologyStructure(classes=["urn:C"]).to_rdf_graph()
    schema.annotate_rdf(graph, include_examples=False)
    assert not list(graph.triples((URIRef(""), OWL.versionInfo, None)))
    assert not list(graph.triples((None, OWL.versionInfo, None)))
