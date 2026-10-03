"""Select publication information without losing its source evidence."""

import pytest

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.collections import CollectionProfile


def test_select_publication_paths_and_preserve_evidence():
    def row(subject, predicate, target):
        return SchemaPattern(subject_class=f"urn:{subject}", property_uri=f"urn:{predicate}",
                             object_class=target if target == "Literal" else f"urn:{target}",
                             count=4, graphs={"urn:data": 4})

    schema = MinedSchema(
        about=AboutMetadata(dataset_name="publications", graph_uris=["urn:data"],
                            class_entity_counts={"urn:Article": 7},
                            class_entity_count_states={"urn:Article": "partial"}),
        patterns=[row("Article", "author", "Person"), row("Person", "name", "Literal"),
                  row("Person", "affiliation", "Organization"),
                  row("Person", "affiliation", "Consortium"),
                  row("Person", "publication", "Article"),
                  row("Article", "title", "Literal"), row("Other", "noise", "Literal")],
        collections=[CollectionProfile(subject_class="urn:Article", property_uri="urn:authors",
                                       member_types=["urn:Person"], member_kinds=["IRI"],
                                       graph_uri="urn:data", list_count=4)],
    )
    navigation = schema.discover_paths(max_hops=2, max_paths_per_length=30)
    paths = [p for p in navigation.paths if p.steps[0].property_uri == "urn:author"
             and p.steps[1].property_uri in {"urn:name", "urn:publication"}]
    paths[0].instance_support = "matched"
    paths[0].source_count, paths[0].matched_sources = 7, 4
    selected = schema.select(paths=paths, fields=[("urn:Person", "urn:affiliation"),
                                                ("urn:Article", "urn:authors")])
    assert len(selected.patterns) == 5, "Keep both affiliation ranges and the return path"
    assert {p.object_class for p in selected.patterns if p.property_uri == "urn:affiliation"} == {
        "urn:Organization", "urn:Consortium"}
    assert selected.collections == schema.collections, "Keep ordered author evidence"
    assert all(p.count == 4 and p.graphs == {"urn:data": 4} for p in selected.patterns)
    assert selected.source.about.class_entity_count_states == {"urn:Article": "partial"}
    assert selected.paths[0].matched_sources == 4
    restored = type(selected).model_validate_json(selected.model_dump_json())
    assert restored == selected, "Selected fields, paths and complete source survive saving"
    schema.patterns[0].count = 999
    assert selected.patterns[0].count == 4, "Selection retains its own evidence snapshot"
    with pytest.raises(ValueError, match="Unknown selected field"):
        schema.select(fields=[("urn:Article", "urn:missing")])
    foreign = paths[0].model_copy(deep=True)
    foreign.steps[0].property_uri = "urn:foreign"
    with pytest.raises(ValueError, match="Path is not retained"):
        schema.select(paths=[foreign])
