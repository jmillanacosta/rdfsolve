"""rdfsolve.schema_models.core: a mined schema is cleaned of service data in every part,
serialized and read back with its versions, provenance, blank nodes and metadata."""

import json
from pathlib import Path

import pytest
from rdflib import OWL, Dataset, Graph, URIRef
from rdflib.compare import isomorphic

from rdfsolve.ontology.structure import OntologyStructure
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, SchemaEnrichment
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary
from rdfsolve.schema_models.structural import StructuralPattern

V = "http://www.openlinksw.com/schemas/virtrdf#"
ENGINE = "http://www.openlinksw.com/"


def pattern(subject, prop, count, graphs=None):
    return SchemaPattern(
        subject_class=subject, property_uri=prop, object_class="Literal", count=count, graphs=graphs
    )


SCHEMA = MinedSchema(
    about=AboutMetadata.build(dataset_name="x"),
    patterns=[
        pattern("urn:A", "urn:p", 10, {"urn:data": 10}),
        pattern(V + "QuadMap", V + "qmTableName", 7),
        pattern("urn:A", "urn:q", 3, {V: 3}),
    ],
)


def test_the_removed_patterns_and_their_triples_are_recorded():
    cleaned = SCHEMA.clean_schema(namespaces=[ENGINE], graph_uris=[ENGINE])
    assert [p.property_uri for p in cleaned.patterns] == ["urn:p"]
    record = MinedSchema.from_dict(cleaned.to_dict()).about.cleaned
    assert record["patterns_removed"] == 2
    assert record["removed_by_namespace"] == {ENGINE: {"patterns": 1, "triples": 7}}
    assert record["removed_by_graph"] == {ENGINE: {"patterns": 1, "triples": 3}}
    assert {(p["property_uri"], p["count"]) for p in record["removed_patterns"]} == {
        (V + "qmTableName", 7),
        ("urn:q", 3),
    }, "Users see which patterns were in the source"


CLEAN_SCHEMA_PARTS_V = ENGINE + "schemas/virtrdf#"
DATA = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B", count=5)
NEXT = SchemaPattern(subject_class="urn:B", property_uri="urn:q", object_class="urn:C", count=5)
QUAD = SchemaPattern(
    subject_class="urn:B",
    property_uri=CLEAN_SCHEMA_PARTS_V + "item",
    object_class=CLEAN_SCHEMA_PARTS_V + "QuadMap",
)


def _untyped(prop):
    return StructuralPattern(
        subject_properties=[prop],
        subject_kind="IRI",
        property_uri=prop,
        object_kind="Literal",
        count=2,
        distinct_subjects=2,
        distinct_objects=1,
        witness_query="q",
        recount_query="q",
    )


def _example(subject_class, prop):
    node = RdfTerm(kind="uri", value="urn:x")
    return PatternExample(subject_class=subject_class, property_uri=prop, subject=node, value=node)


def _path(*steps):
    return NavigationPath(steps=list(steps), evidence="instance_tested", instance_support="matched")


CLEAN_SCHEMA_PARTS_SCHEMA = MinedSchema(
    about=AboutMetadata.build(dataset_name="x").model_copy(
        update={
            "class_entity_counts": {"urn:A": 3, CLEAN_SCHEMA_PARTS_V + "QuadMap": 9},
            "class_entity_count_states": {
                "urn:A": "complete",
                CLEAN_SCHEMA_PARTS_V + "QuadMap": "complete",
            },
        }
    ),
    patterns=[DATA, NEXT, QUAD],
    raw_patterns=[DATA, NEXT, QUAD],
    structural_patterns=[_untyped("urn:p"), _untyped(CLEAN_SCHEMA_PARTS_V + "isGcResistantType")],
    enrichment=SchemaEnrichment(
        examples=[
            _example("urn:A", "urn:p"),
            _example(CLEAN_SCHEMA_PARTS_V + "QuadMap", CLEAN_SCHEMA_PARTS_V + "item"),
        ],
        class_examples={"urn:A": [], CLEAN_SCHEMA_PARTS_V + "QuadMap": []},
    ),
    navigation=NavigationSummary(
        max_hops=2,
        max_paths_per_length=0,
        edge_count=3,
        walk_counts={2: 2},
        strategy="tested",
        paths=[_path(DATA, NEXT), _path(DATA, QUAD)],
    ),
)


def test_every_part_is_cleaned_and_the_record_counts_each_part():
    cleaned = MinedSchema.from_dict(
        CLEAN_SCHEMA_PARTS_SCHEMA.clean_schema(namespaces=[ENGINE]).to_dict()
    )
    assert [p.property_uri for p in cleaned.patterns] == ["urn:p", "urn:q"]
    assert [p.property_uri for p in cleaned.raw_patterns] == ["urn:p", "urn:q"]
    assert [p.property_uri for p in cleaned.structural_patterns] == ["urn:p"]
    assert [e.property_uri for e in cleaned.enrichment.examples] == ["urn:p"]
    assert list(cleaned.enrichment.class_examples) == ["urn:A"]
    assert cleaned.about.class_entity_counts == {"urn:A": 3}
    assert cleaned.about.class_entity_count_states == {"urn:A": "complete"}
    assert [[s.property_uri for s in p.steps] for p in cleaned.navigation.paths] == [
        ["urn:p", "urn:q"]
    ]
    assert cleaned.about.cleaned["removed_from_other_parts"] == {
        "raw_patterns": 1,
        "structural_patterns": 1,
        "examples": 1,
        "class_examples": 1,
        "class_entity_counts": 1,
        "paths": 1,
    }


def test_a_schema_without_service_data_keeps_every_part():
    cleaned = CLEAN_SCHEMA_PARTS_SCHEMA.clean_schema(namespaces=["urn:none:"])
    assert (
        cleaned.patterns == CLEAN_SCHEMA_PARTS_SCHEMA.patterns
        and cleaned.navigation == CLEAN_SCHEMA_PARTS_SCHEMA.navigation
    )
    assert cleaned.about.cleaned["removed_from_other_parts"] == {}


def _schema(*patterns: SchemaPattern) -> MinedSchema:
    return MinedSchema(patterns=list(patterns), about=AboutMetadata(dataset_name="test"))


MODELS_DATA = SchemaPattern(
    subject_class="http://ex.org/Person",
    property_uri="http://ex.org/name",
    object_class="Literal",
    count=7,
    graphs={"https://example.org/data": 7},
)


def test_per_graph_counts_survive_the_canonical_round_trip():
    schema = _schema(MODELS_DATA)
    restored = MinedSchema.from_dict(schema.to_dict())
    assert restored.patterns[0].graphs == {"https://example.org/data": 7}


@pytest.fixture
def schema():
    return MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="urn:A",
                property_uri="urn:p",
                object_class=kind,
                count=count,
                subject_label="Class A",
                confidence=0.4,
                evidence_source="imported",
                datatype=datatype,
            )
            for kind, count, datatype in [
                ("urn:B", 2**54 + 1, None),
                ("Literal", 0, "http://www.w3.org/2001/XMLSchema#string"),
                ("Resource", None, None),
                ("BlankNode", 3, None),
            ]
        ],
        about=AboutMetadata(
            dataset_name="test",
            graph_uris=["urn:g"],
            description='Quotes: "hello"; Unicode: α',
            custom_note={"a": [1, None]},
        ),
    )


def test_canonical_round_trip_preserves_all_model_fields(schema, tmp_path):
    raw = schema.to_dict()
    assert raw["format"] == "rdfsolve.mined-schema"
    assert raw["version"] == 1
    assert MinedSchema.from_dict(raw) == schema
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert MinedSchema.from_json(path) == schema
    raw["version"] = 999
    with pytest.raises(ValueError):
        MinedSchema.from_dict(raw)


def test_version_iri_survives_rdf_exports():
    schema = MinedSchema(
        patterns=[],
        about=AboutMetadata.build(
            dataset_name="test",
            endpoint="https://example.org/sparql",
            source_version_iri="urn:release",
        ),
    )
    assert MinedSchema.from_dict(schema.to_jsonld()).about.source_version_iri == "urn:release"
    assert (
        MinedSchema.from_void(
            schema.to_void_graph().serialize(format="turtle")
        ).about.schema_version
        == "urn:release"
    )
    assert schema.to_linkml().version == "urn:release"


def test_generated_ontology_annotation_has_no_owl_version_info():
    schema = MinedSchema(
        patterns=[],
        about=AboutMetadata.build(dataset_name="test", finished_at="2026-09-18T10:00:00+00:00"),
    )
    graph = OntologyStructure(classes=["urn:C"]).to_rdf_graph()
    schema.annotate_rdf(graph, include_examples=False)
    assert not list(graph.triples((URIRef(""), OWL.versionInfo, None)))
    assert not list(graph.triples((None, OWL.versionInfo, None)))


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


AB = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
BC = SchemaPattern(subject_class="urn:B", property_uri="urn:q", object_class="urn:C")


def test_tested_paths_can_be_left_out_of_a_dump():
    summary = NavigationSummary(
        max_hops=2,
        max_paths_per_length=0,
        edge_count=2,
        walk_counts={2: 1},
        strategy="tested",
        paths=[
            NavigationPath(steps=[AB, BC], evidence="instance_tested", instance_support="matched")
        ],
    )
    written = summary.model_dump(mode="json", exclude={"paths"})
    assert "paths" not in written and written["strategy"] == "tested"
    assert summary.model_dump(mode="json")["paths"]["format"] == "edges-1", (
        "The full dump is compact"
    )


METADATA_DOCUMENT_DATA = Path(__file__).parents[1] / "test_data" / "aopwikirdf_generated_void.ttl"


def test_void_conversion_preserves_source_metadata_and_contexts_through_json():
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.void_schema import VoidSchema

    graph = Graph().parse(METADATA_DOCUMENT_DATA.with_name("aopwikirdf_metadata_excerpt.ttl"))
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
