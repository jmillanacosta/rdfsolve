from pathlib import Path

from rdflib import Dataset, Graph
from rdflib.compare import isomorphic

DATA = Path(__file__).parent / "test_data" / "aopwikirdf_generated_void.ttl"


def test_void_conversion_preserves_source_metadata_and_contexts_through_json():
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.void_schema import VoidSchema

    graph = Graph().parse(DATA.with_name("aopwikirdf_metadata_excerpt.ttl"))
    dataset = Dataset()
    dataset.graph("http://aopwiki.org/").__iadd__(graph)
    dataset.default_context.__iadd__(graph)
    void = VoidSchema(
        graph,
        "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql",
        "aopwikirdf",
        graph_uris=["http://aopwiki.org/"],
        default_graph=True,
        rdf_dataset=dataset,
    )
    schema = void.to_mined_schema()
    assert schema.about.description == "AOP-Wiki RDF -- complete dataset"
    assert schema.about.source_version == "2026.09.05"
    assert schema.about.source_issued is None
    assert schema.about.triple_count_estimate is None
    assert not schema.patterns
    restored = MinedSchema.from_dict(schema.to_dict())
    for result in (schema, restored):
        metadata = result.get_metadata()
        assert isomorphic(metadata.graph, graph)
        assert isomorphic(metadata.for_graph("http://aopwiki.org/").graph, graph)
        assert isomorphic(metadata.for_graph(None).graph, graph)
        assert metadata.scope == "retained VoID RDF"
        exported = Dataset().parse(data=metadata.to_trig(), format="trig")
        assert isomorphic(exported.graph("http://aopwiki.org/"), graph)
