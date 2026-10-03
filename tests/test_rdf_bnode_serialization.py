from rdflib import Graph
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, SchemaEnrichment


def test_unsafe_provider_blank_node_label_serializes_as_valid_turtle():
    value = "example_51_nodeID://b12577"
    enrichment = SchemaEnrichment(
        examples=[
            PatternExample(
                subject_class="urn:A",
                property_uri="urn:p",
                subject=RdfTerm(kind="uri", value="urn:s"),
                value=RdfTerm(kind="bnode", value=value),
            )
        ]
    )
    ttl = enrichment.to_rdf_graph().serialize(format="turtle")
    Graph().parse(data=ttl, format="turtle")
    assert "nodeID://" not in ttl
    assert enrichment.examples[0].value.value == value
