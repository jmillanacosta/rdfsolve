import json
from rdflib import BNode, Dataset, Literal, Namespace, RDF

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.edge_graph_split import split_by_edge_graph


def test_counts_preserve_blank_nodes_and_graph_units(tmp_path):
    e = Namespace("urn:counts:")
    data = Dataset(default_union=True)
    graphs = [str(e.left), str(e.right)]
    node = BNode()
    for graph in graphs:
        target = data.graph(graph)
        target.add((e.s, RDF.type, e.Class))
        target.add((e.s, e.blank, node))
        target.add((e.s, e.value, Literal("same")))
        target.add((e.s, e.link, e.o))
    data.graph(e.types).add((e.o, RDF.type, e.Target))
    with SchemaMiner.from_graph(data, graph_uris=graphs, type_context_graph_uris=[str(e.types)], delay=0) as miner:
        schema = miner.mine("fixture")
        assert miner.last_report.completion_state == "complete"
    wanted = {str(e.blank), str(e.value), str(e.link)}
    patterns = [p for p in schema.patterns if p.property_uri in wanted]
    assert len(patterns) == 3
    for pattern in patterns:
        assert pattern.count == 2, pattern.property_uri
        assert pattern.count_semantics == "quad_occurrences"
        assert pattern.graphs == dict.fromkeys(graphs, 1)
        assert pattern.distinct_subjects is None
    blank = next(p for p in patterns if p.property_uri == str(e.blank))
    assert blank.object_class == "BlankNode" and blank.pattern_type == "blank_node_property"
    part = split_by_edge_graph(schema, graphs[0], "part")
    assert {p.property_uri for p in part.patterns} >= wanted
    assert all(p.count_semantics == "triples_in_graph" for p in part.patterns)
    with SchemaMiner.from_graph(data, delay=0) as miner:
        union = miner.mine("fixture")
    same = next(p for p in union.patterns if p.property_uri == str(e.value))
    assert same.count == 1 and same.count_semantics == "endpoint_default"
    (tmp_path / "schema.json").write_text(json.dumps(schema.to_dict()))
    assert type(schema).from_json(tmp_path / "schema.json").patterns == schema.patterns

    from rdfsolve.mining.ontology_as_data import subsume_patterns

    overlap = [blank, blank.model_copy(update={"subject_class": str(e.Other)})]
    merged = subsume_patterns(overlap, {str(e.Class): str(e.Parent), str(e.Other): str(e.Parent)})
    assert len(merged) == 1 and merged[0].count == 4
    assert merged[0].count_semantics == "upper_bound"
    assert merged[0].evidence_source == "inferred" and merged[0].distinct_subjects is None
