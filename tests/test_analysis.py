from rdfsolve.mappings import (
    ClassIndex,
    EntityClassInfo,
    MappingEdge,
    derive_class_mappings,
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
