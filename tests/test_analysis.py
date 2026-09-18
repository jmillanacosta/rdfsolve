"""Check dataset scope, evidence counts and reproducible connectivity exports."""

import json
from pathlib import Path

import pytest
from rdflib import RDF, Dataset, URIRef

from rdfsolve.analysis import build_connectivity, compare_schemas, load_schemas, read_class_mappings
from rdfsolve.mappings import (
    ClassIndex,
    EntityClassInfo,
    Mapping,
    MappingEdge,
    derive_class_mappings,
    shared_entity_links,
)
from rdfsolve.schema_models import MinedSchema, SchemaPattern


def schema(name, left, right):
    return MinedSchema(
        about={"dataset_name": name},
        patterns=[SchemaPattern(subject_class=left, property_uri="urn:link", object_class=right)],
    )


def index(dataset, records):
    return ClassIndex(
        endpoint_url="urn:local",
        entities={
            iri: EntityClassInfo(entity_iri=iri, graph_classes={dataset: classes})
            for iri, classes in records.items()
        },
    )


def test_associations_deduplicate_entities_and_keep_dataset_types():
    combined = ClassIndex(
        endpoint_url="urn:local",
        dataset_graphs={"a": ["urn:ga"], "b": ["urn:gb"]},
        entities={
            "urn:one": EntityClassInfo(
                entity_iri="urn:one", graph_classes={"urn:ga": ["urn:A"], "urn:gb": ["urn:Wrong"]}
            ),
            "urn:two": EntityClassInfo(entity_iri="urn:two", graph_classes={"urn:gb": ["urn:B"]}),
            "urn:three": EntityClassInfo(
                entity_iri="urn:three", graph_classes={"urn:ga": ["urn:A"]}
            ),
        },
    )
    edge = MappingEdge(
        source_class="urn:one",
        target_class="urn:two",
        source_dataset="a",
        target_dataset="b",
        predicate="urn:corresponds",
    )
    pairs, stats = derive_class_mappings([edge, edge], combined)
    assert len(pairs) == 1
    pair = pairs[0]
    assert (pair.source_class, pair.target_class, pair.instance_count) == ("urn:A", "urn:B", 1)
    assert pair.source_coverage == 0.5 and pair.target_coverage == 1
    assert not hasattr(pair, "confidence")
    assert pair.supporting_entity_predicates == {"urn:corresponds": 1}
    assert pair.class_relation is None
    assert pair.derivation_method == "mapped_instance_types"
    assert stats["supporting_entity_predicates"] == {"urn:corresponds": 1}
    assert stats["processed_edges"] == 2
    assert derive_class_mappings([edge], combined, min_instance_count=2)[0] == []


def test_one_association_per_class_pair_counts_each_supporting_predicate():
    indices = {
        "a": index("a", {"urn:x": ["urn:A"], "urn:y": ["urn:A"]}),
        "b": index("b", {"urn:u": ["urn:B"], "urn:v": ["urn:B"]}),
    }
    edges = [
        MappingEdge(source_class=s, target_class=t, source_dataset="a", target_dataset="b", predicate=p)
        for s, t, p in (
            ("urn:x", "urn:u", "urn:exact"),
            ("urn:x", "urn:u", "urn:close"),
            ("urn:y", "urn:v", "urn:close"),
        )
    ]
    pairs, stats = derive_class_mappings(edges, indices)
    assert len(pairs) == 1
    assert pairs[0].instance_count == 2
    assert pairs[0].supporting_entity_predicates == {"urn:close": 2, "urn:exact": 1}
    assert stats["supporting_entity_predicates"] == {"urn:close": 1, "urn:exact": 1}


def test_shared_identity_does_not_merge_dataset_nodes_or_assert_class_equivalence():
    indices = {
        "a": index("a", {"urn:shared": ["urn:A"]}),
        "b": index("b", {"urn:shared": ["urn:B"]}),
    }
    pairs, _ = shared_entity_links(indices)
    schemas = {"a": schema("a", "urn:A", "urn:Common"), "b": schema("b", "urn:B", "urn:Common")}
    graph = build_connectivity(schemas, associations=pairs)
    assert len(graph) == 4
    assert ("a", "urn:Common") in graph and ("b", "urn:Common") in graph
    assert sorted(d["kind"] for _, _, d in graph.edges(data=True)) == [
        "entity_association",
        "schema",
        "schema",
        "shared_class",
    ]
    association = [d for _, _, d in graph.edges(data=True) if d["kind"] == "entity_association"][0]
    assert association["instance_count"] == 1 and "confidence" not in association
    assert association["predicate"] is None
    assert association["supporting_entity_predicates"] == {}
    assert pairs[0].instance_count == 1
    assert association["derivation_method"] == "shared_entity_iri"
    overlap = compare_schemas(schemas)[0]
    assert overlap["shared_classes"] == 1 and overlap["class_jaccard"] == 1 / 3
    edge = MappingEdge(
        source_class="urn:Common",
        target_class="urn:Common",
        source_dataset="a",
        target_dataset="b",
        predicate="urn:explicit",
    )
    assert len(Mapping(edges=[edge], about={}).to_networkx()) == 2


def test_class_index_queries_keep_graph_scope_and_report_failures(monkeypatch):
    graph = Dataset()
    graph.graph(URIRef("urn:selected")).add((URIRef("urn:e"), RDF.type, URIRef("urn:A")))
    graph.graph(URIRef("urn:other")).add((URIRef("urn:e"), RDF.type, URIRef("urn:Wrong")))

    class Helper:
        last_select_execution = {"status": "complete"}

        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def select_with_fallback(self, query, **kwargs):
            assert kwargs["exhaustive"]
            return json.loads(graph.query(query).serialize(format="json"))

    monkeypatch.setattr("rdfsolve.mappings.index.SparqlHelper", Helper)
    data = ClassIndex(endpoint_url="urn:local")
    report = data.index_entities(["urn:e", "urn:absent"], graph_uris=["urn:selected"])
    assert report == {"total_entities": 2, "indexed": 1, "not_found": 1, "errors": 0}
    assert data.get_classes("urn:e") == {"urn:A"}

    def fail(*args, **kwargs):
        raise TimeoutError("source unavailable")

    monkeypatch.setattr(Helper, "select_with_fallback", fail)
    assert data.index_entities(["urn:e"])["errors"] == 1


def test_sssom_projection_preserves_predicate_prefixes_and_mapping_source(tmp_path):
    path = tmp_path / "classes.sssom.tsv"
    path.write_text("""# mapping_set_id: https://example.org/mappings
# license: https://creativecommons.org/publicdomain/zero/1.0/
# curie_map:
#   ex: https://example.org/
#   skos: http://www.w3.org/2004/02/skos/core#
#   semapv: https://w3id.org/semapv/vocab/
subject_id\tpredicate_id\tobject_id\tmapping_justification
ex:A\tskos:closeMatch\tex:B\tsemapv:ManualMappingCuration
ex:Missing\tskos:exactMatch\tex:B\tsemapv:ManualMappingCuration
""")
    schemas = {
        "a": schema("a", "https://example.org/A", "urn:End"),
        "b": schema("b", "https://example.org/B", "urn:End"),
        "c": schema("c", "https://example.org/A", "urn:End"),
    }
    edges, report = read_class_mappings(path, schemas)
    assert len(edges) == 2 and report["unrepresented_rows"] == 1
    assert {e.source_dataset for e in edges} == {"a", "c"}
    assert all(
        e.predicate.endswith("closeMatch")
        and e.mapping_source == "https://example.org/mappings"
        and e.confidence is None
        for e in edges
    )


def test_analysis_stage_reads_canonical_schema_and_includes_zero_pairs(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    from pipeline_stages.analysis import AnalysisStage
    from pipeline_stages.config import PipelineConfig

    for name, left, right in [("a", "urn:A", "urn:B"), ("b", "urn:C", "urn:D")]:
        (tmp_path / f"{name}_schema.json").write_text(
            json.dumps(schema(name, left, right).to_dict())
        )
    report = AnalysisStage(PipelineConfig(base_dir=tmp_path, output_dir=tmp_path)).run()
    assert report["success"] and report["total_schemas"] == 2 and report["class_nodes"] == 4
    rows = json.loads((tmp_path / "schema_overlaps.json").read_text())
    assert len(rows) == 1 and rows[0]["class_jaccard"] == 0
    (tmp_path / "duplicate_schema.json").write_text((tmp_path / "a_schema.json").read_text())
    with pytest.raises(ValueError, match="one named schema"):
        load_schemas(tmp_path)


def test_mapping_export_keeps_dataset_scope_and_provenance(tmp_path):
    edges = [
        MappingEdge(
            source_class="urn:Common",
            target_class="urn:Target",
            source_dataset=name,
            target_dataset="target",
            predicate="urn:maps",
            mapping_source="urn:community",
            mapping_justification="urn:manual",
        )
        for name in ("a", "b")
    ]
    path = tmp_path / "mapping.jsonld"
    path.write_text(json.dumps(Mapping(edges=edges, about={}).to_jsonld()))
    restored = Mapping.from_jsonld(path)
    assert {e.source_dataset for e in restored.edges} == {"a", "b"}
    assert all(
        e.mapping_source == "urn:community" and e.mapping_justification == "urn:manual"
        for e in restored.edges
    )
