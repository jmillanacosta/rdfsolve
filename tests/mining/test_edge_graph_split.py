"""rdfsolve.mining.edge_graph_split: a schema is split by the graph of its edges, with its
measurements."""

import json

from rdflib import RDF, BNode, Dataset, Literal, Namespace

from rdfsolve.config import mint
from rdfsolve.mining.edge_graph_split import split_by_edge_graph
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

SUBSTANCE = "urn:graph:substance"
COMPOUND = "urn:graph:compound"


def _group() -> MinedSchema:
    return MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="urn:Substance",
                property_uri="urn:cid",
                object_class="urn:Compound",
                count=5,
                graphs={SUBSTANCE: 5},
                distinct_subjects=5,
                distinct_objects=4,
            ),
            SchemaPattern(
                subject_class="urn:Compound",
                property_uri="urn:label",
                object_class="Literal",
                count=7,
                graphs={SUBSTANCE: 2, COMPOUND: 5},
                distinct_subjects=6,
            ),
            SchemaPattern(subject_class="urn:X", property_uri="urn:p", object_class="Resource"),
        ],
        raw_patterns=[
            SchemaPattern(
                subject_class="urn:Leaf",
                property_uri="urn:value",
                object_class="Literal",
                count=3,
                graphs={SUBSTANCE: 1, COMPOUND: 2},
            ),
            SchemaPattern(
                subject_class="urn:Other",
                property_uri="urn:value",
                object_class="Literal",
                count=4,
                graphs={SUBSTANCE: 4},
            ),
        ],
        about=AboutMetadata.build(
            dataset_name="pubchem.ftp",
            graph_uris=[SUBSTANCE, COMPOUND],
            started_at="2026-09-21T10:00:00+00:00",
            content_sha256="abc",
        ),
    )


def test_the_dataset_gets_its_own_identity_and_counts():
    part = split_by_edge_graph(
        _group(), COMPOUND, "pubchem.ftp.compound", declared_classes=frozenset({"urn:Compound"})
    )
    assert part.raw_patterns is not None
    assert [(p.subject_class, p.count, p.graphs) for p in part.raw_patterns] == [
        ("urn:Leaf", 2, {COMPOUND: 2})
    ], "Raw evidence must follow the selected edge graphs"
    about = part.about
    assert about.dataset_name == "pubchem.ftp.compound"
    assert about.graph_uris == [COMPOUND]
    assert about.snapshot_id == mint(
        "snapshot", "pubchem.ftp.compound", "2026-09-21T10:00:00+00:00"
    )
    assert about.content_sha256 is None
    assert about.schema_uri == mint("schema", "pubchem.ftp.compound")
    assert (about.pattern_count, about.class_count, about.declared_class_count) == (1, 1, 1)
    assert part.navigation is None

    merged = split_by_edge_graph(_group(), [SUBSTANCE, COMPOUND], "combined")
    assert merged.about.graph_uris == [SUBSTANCE, COMPOUND]
    literal = next(p for p in merged.patterns if p.object_class == "Literal")
    assert literal.count == 7 and literal.graphs == {SUBSTANCE: 2, COMPOUND: 5}
    assert literal.distinct_subjects is None, "Distinct populations cannot be summed"


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
    with SchemaMiner.from_graph(
        data, graph_uris=graphs, type_context_graph_uris=[str(e.types)], delay=0
    ) as miner:
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
