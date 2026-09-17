"""Test model classes."""

import pytest

from rdfsolve.models import SchemaPattern
from rdfsolve.schema_models import AboutMetadata, MinedSchema
from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_NAMESPACES


def test_schema_pattern():
    pattern = SchemaPattern(
        subject_class="http://ex.org/Person",
        property_uri="http://ex.org/name",
        object_class="Literal",
        datatype="http://www.w3.org/2001/XMLSchema#string",
        count=100,
    )
    assert pattern.count == 100


def _schema(*patterns: SchemaPattern) -> MinedSchema:
    return MinedSchema(patterns=list(patterns), about=AboutMetadata(dataset_name="test"))


SERVICE = SchemaPattern(
    subject_class="http://www.w3.org/ns/shacl#SPARQLSelectExecutable",
    property_uri="https://schema.org/target",
    object_class="http://www.w3.org/ns/sparql-service-description#Service",
    count=105,
    graphs={"https://example.org/.well-known/sparql-examples": 105},
)
DATA = SchemaPattern(
    subject_class="http://ex.org/Person",
    property_uri="http://ex.org/name",
    object_class="Literal",
    count=7,
    graphs={"https://example.org/data": 7},
)


def test_clean_schema_removes_only_selected_namespaces():
    schema = _schema(SERVICE, DATA)
    cleaned = schema.clean_schema(namespaces=SUGGESTED_SERVICE_NAMESPACES)
    assert [p.property_uri for p in cleaned.patterns] == ["http://ex.org/name"]
    assert cleaned.about.cleaned["patterns_removed"] == 1
    assert cleaned.about.pattern_count == 1
    assert len(schema.patterns) == 2


def test_clean_schema_defaults_to_removing_nothing():
    schema = _schema(SERVICE, DATA)
    assert len(schema.clean_schema().patterns) == 2


def test_clean_schema_keeps_patterns_with_evidence_outside_the_listed_graphs():
    spanning = DATA.model_copy(
        update={"graphs": {"https://example.org/.well-known/void": 1, "https://example.org/data": 6}}
    )
    schema = _schema(spanning, SERVICE)
    cleaned = schema.clean_schema(graph_uris=["https://example.org/.well-known/"])
    assert cleaned.patterns == [spanning]


@pytest.mark.parametrize("drop,expected", [(False, 1), (True, 0)])
def test_clean_schema_decides_on_patterns_without_graph_evidence(drop, expected):
    schema = _schema(DATA.model_copy(update={"graphs": None}))
    cleaned = schema.clean_schema(
        graph_uris=["https://example.org/.well-known/"], drop_unattributed=drop
    )
    assert len(cleaned.patterns) == expected


def test_per_graph_counts_survive_the_canonical_round_trip():
    schema = _schema(DATA)
    restored = MinedSchema.from_dict(schema.to_dict())
    assert restored.patterns[0].graphs == {"https://example.org/data": 7}
