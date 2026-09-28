"""Cleaning removes service and engine patterns and records what it removed."""

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

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
