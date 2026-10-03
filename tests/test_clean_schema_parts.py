"""Engine and service data are removed from every part of a schema, not only from its patterns
(the owner decision of 2026-09-30): the raw patterns, the patterns of untyped nodes, the examples,
the class counts and the tested paths. The record of the cleaning gives the number removed from
each part, so a cleaned schema still says what the endpoint served."""

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, SchemaEnrichment
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary
from rdfsolve.schema_models.structural import StructuralPattern

ENGINE = "http://www.openlinksw.com/"
V = ENGINE + "schemas/virtrdf#"
DATA = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B", count=5)
NEXT = SchemaPattern(subject_class="urn:B", property_uri="urn:q", object_class="urn:C", count=5)
QUAD = SchemaPattern(subject_class="urn:B", property_uri=V + "item", object_class=V + "QuadMap")


def _untyped(prop):
    return StructuralPattern(
        subject_properties=[prop], subject_kind="IRI", property_uri=prop, object_kind="Literal",
        count=2, distinct_subjects=2, distinct_objects=1, witness_query="q", recount_query="q",
    )


def _example(subject_class, prop):
    node = RdfTerm(kind="uri", value="urn:x")
    return PatternExample(subject_class=subject_class, property_uri=prop, subject=node, value=node)


def _path(*steps):
    return NavigationPath(steps=list(steps), evidence="instance_tested", instance_support="matched")


SCHEMA = MinedSchema(
    about=AboutMetadata.build(dataset_name="x").model_copy(
        update={
            "class_entity_counts": {"urn:A": 3, V + "QuadMap": 9},
            "class_entity_count_states": {"urn:A": "complete", V + "QuadMap": "complete"},
        }
    ),
    patterns=[DATA, NEXT, QUAD],
    raw_patterns=[DATA, NEXT, QUAD],
    structural_patterns=[_untyped("urn:p"), _untyped(V + "isGcResistantType")],
    enrichment=SchemaEnrichment(
        examples=[_example("urn:A", "urn:p"), _example(V + "QuadMap", V + "item")],
        class_examples={"urn:A": [], V + "QuadMap": []},
    ),
    navigation=NavigationSummary(
        max_hops=2, max_paths_per_length=0, edge_count=3, walk_counts={2: 2}, strategy="tested",
        paths=[_path(DATA, NEXT), _path(DATA, QUAD)],
    ),
)


def test_every_part_is_cleaned_and_the_record_counts_each_part():
    cleaned = MinedSchema.from_dict(SCHEMA.clean_schema(namespaces=[ENGINE]).to_dict())
    assert [p.property_uri for p in cleaned.patterns] == ["urn:p", "urn:q"]
    assert [p.property_uri for p in cleaned.raw_patterns] == ["urn:p", "urn:q"]
    assert [p.property_uri for p in cleaned.structural_patterns] == ["urn:p"]
    assert [e.property_uri for e in cleaned.enrichment.examples] == ["urn:p"]
    assert list(cleaned.enrichment.class_examples) == ["urn:A"]
    assert cleaned.about.class_entity_counts == {"urn:A": 3}
    assert cleaned.about.class_entity_count_states == {"urn:A": "complete"}
    assert [[s.property_uri for s in p.steps] for p in cleaned.navigation.paths] == [["urn:p", "urn:q"]]
    assert cleaned.about.cleaned["removed_from_other_parts"] == {
        "raw_patterns": 1, "structural_patterns": 1, "examples": 1, "class_examples": 1,
        "class_entity_counts": 1, "paths": 1,
    }


def test_a_schema_without_service_data_keeps_every_part():
    cleaned = SCHEMA.clean_schema(namespaces=["urn:none:"])
    assert cleaned.patterns == SCHEMA.patterns and cleaned.navigation == SCHEMA.navigation
    assert cleaned.about.cleaned["removed_from_other_parts"] == {}
