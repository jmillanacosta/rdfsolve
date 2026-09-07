"""Check canonical storage and keep RDF metadata out of patterns."""

import json

import pytest
from rdflib import Graph

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


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


def test_canonical_empty_schema_is_valid():
    schema = MinedSchema(patterns=[], about=AboutMetadata(dataset_name="empty"))
    assert MinedSchema.from_dict(schema.to_dict()) == schema


@pytest.mark.parametrize("version", [0, 2, "1", True])
def test_unknown_canonical_version_is_rejected(schema, version):
    raw = schema.to_dict()
    raw["version"] = version
    with pytest.raises(ValueError, match="version"):
        MinedSchema.from_dict(raw)


@pytest.mark.parametrize(
    "raw",
    [
        {},
        {"patterns": []},
        {"format": "unknown", "version": 1},
        {"@graph": [{"@id": "urn:A", "urn:p": {"@id": "urn:B"}}]},
    ],
)
def test_unsupported_documents_do_not_look_like_empty_success(raw):
    with pytest.raises(ValueError):
        MinedSchema.from_dict(raw)


def test_jsonld_round_trip_does_not_turn_metadata_into_patterns():
    original = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="urn:A", property_uri="urn:p", object_class="urn:B", count=7
            )
        ],
        about=AboutMetadata(
            dataset_name="test", description="Dataset metadata", class_count=2, property_count=1
        ),
    )
    raw = original.to_jsonld()
    restored = MinedSchema.from_dict(raw)
    assert len(restored.patterns) == 1
    assert restored.patterns[0].subject_class == "urn:A"
    assert restored.patterns[0].property_uri == "urn:p"
    assert restored.patterns[0].object_class == "urn:B"
    assert restored.patterns[0].count == 7
    assert restored.about.dataset_name == "test"
    assert restored.about.pattern_count == 1
    assert len(Graph().parse(data=json.dumps(raw), format="json-ld")) > 0


def test_legacy_adjacency_reader_keeps_its_own_context():
    raw = {
        "@context": {"ex": "https://example.org/"},
        "@about": {"dataset_name": "legacy"},
        "@graph": [{"@id": "ex:A", "ex:p": {"@id": "ex:B"}}],
    }
    result = MinedSchema.from_dict(raw)
    assert len(result.patterns) == 1
    assert result.patterns[0].subject_class == "https://example.org/A"
    assert result.about.dataset_name == "legacy"


@pytest.mark.parametrize("profile", ["canonical", "void"])
def test_linkml_uses_patterns_not_metadata(profile):
    from linkml_runtime.dumpers import json_dumper

    from rdfsolve.schema_models.linkml import to_linkml

    schema = MinedSchema(
        patterns=[SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")],
        about=AboutMetadata(dataset_name="test", description="Not a class"),
    )
    result = schema.to_linkml() if profile == "canonical" else to_linkml(schema.to_jsonld())
    document = json_dumper.to_dict(result)
    assert {item["class_uri"] for item in document["classes"].values()} == {"urn:A", "urn:B"}
    assert {item["slot_uri"] for item in document["slots"].values()} == {"urn:p"}


def test_canonical_reader_rejects_unknown_pattern_fields(schema):
    raw = schema.to_dict()
    raw["schema"]["patterns"][0]["typo_count"] = 1
    with pytest.raises(ValueError, match="Unknown canonical pattern"):
        MinedSchema.from_dict(raw)


@pytest.mark.parametrize(
    "context",
    [
        "https://example.org/context",
        ["file:///etc/passwd"],
        {"@import": "https://example.org/context"},
        {"term": {"@id": "urn:p", "@context": "https://example.org/context"}},
    ],
)
def test_reader_rejects_external_contexts_before_rdf_parsing(context, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("RDF parser must not run")

    monkeypatch.setattr(Graph, "parse", forbidden)
    with pytest.raises(ValueError, match="context"):
        MinedSchema.from_dict({"@context": context, "@graph": []})
