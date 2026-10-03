import json

import pytest
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
    raw["version"] = 999
    with pytest.raises(ValueError):
        MinedSchema.from_dict(raw)
