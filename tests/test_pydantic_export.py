"""Compile generated models and check labels, ranges, and metadata."""

from types import ModuleType
import sys

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


def generate(patterns, **metadata):
    schema = MinedSchema(patterns=patterns, about=AboutMetadata(**metadata))
    module = ModuleType("rdfsolve_generated_test")
    sys.modules[module.__name__] = module
    exec(compile(schema.to_pydantic(), "generated.py", "exec"), module.__dict__)
    return module, schema


def test_uses_labels_including_object_only_classes():
    module, _ = generate(
        [
            SchemaPattern(
                subject_class="urn:0001",
                subject_label="Adverse Outcome Pathway",
                property_uri="urn:has_event",
                object_class="urn:0002",
                object_label="Key Event",
            )
        ]
    )
    assert module.AdverseOutcomePathway.rdf_class_iri == "urn:0001"
    assert module.KeyEvent.rdf_class_iri == "urn:0002"
    event = module.KeyEvent(uri="urn:event")
    pathway = module.AdverseOutcomePathway(uri="urn:pathway", has_event=[event])
    assert pathway.has_event == [event]
    assert "KeyEvent" in module.AdverseOutcomePathway.model_json_schema()["$defs"]


def test_names_use_any_available_label_not_first_pattern():
    module, _ = generate(
        [
            SchemaPattern(
                subject_class="urn:0001",
                subject_label="urn:0001",
                property_uri="urn:p",
                object_class="Resource",
            ),
            SchemaPattern(
                subject_class="urn:0001",
                subject_label="Gene Product",
                property_uri="urn:q",
                object_class="Resource",
            ),
        ]
    )
    assert module.GeneProduct.rdf_class_iri == "urn:0001"


def test_keywords_quotes_and_collisions_compile():
    patterns = [
        SchemaPattern(
            subject_class=f"urn:{index}",
            subject_label='Class "one"\nunsafe triple """ quotes',
            property_uri=f"urn:{prop}",
            property_label='Text "quoted"\nnext line',
            object_class="Literal",
            datatype="http://www.w3.org/2001/XMLSchema#string",
        )
        for index in range(2)
        for prop in ("from", "model_dump", "a-b", "a_b", "date")
    ]
    module, schema = generate(patterns)
    models = [
        value
        for value in module.__dict__.values()
        if isinstance(value, type) and hasattr(value, "rdf_class_iri")
    ]
    assert len(models) == 2
    assert len({m.__name__ for m in models}) == 2
    for model in models:
        assert len([f for f in model.model_fields.values() if f.alias and f.alias.startswith("urn:")]) == 5
        assert {p.property_uri for p in patterns} <= set(model.model_json_schema()["properties"])
    assert (
        schema.to_pydantic()
        == schema.model_copy(update={"patterns": list(reversed(patterns))}).to_pydantic()
    )


def test_preserves_mixed_ranges_without_inventing_cardinality():
    module, _ = generate(
        [
            SchemaPattern(
                subject_class="urn:A",
                subject_label="Thing",
                property_uri="urn:value",
                object_class=kind,
                datatype=datatype,
            )
            for kind, datatype in [
                ("urn:B", None),
                ("Literal", "http://www.w3.org/2001/XMLSchema#integer"),
                ("Resource", None),
                ("BlankNode", None),
            ]
        ]
    )
    instance = module.Thing(uri="urn:test", value=[1, "urn:ref"])
    assert instance.value == [1, "urn:ref"]
    assert module.Thing(uri="urn:test").value is None


def test_source_version_is_in_generated_json_schema():
    module, _ = generate(
        [
            SchemaPattern(
                subject_class="urn:A",
                subject_label="Thing",
                property_uri="urn:p",
                object_class="Resource",
            )
        ],
        source_version_iri="urn:release:2026",
        schema_version="urn:release:2026",
    )
    assert module.Thing.model_json_schema()["source_version_iri"] == "urn:release:2026"
    assert module.Thing.model_json_schema()["schema_version"] == "urn:release:2026"
