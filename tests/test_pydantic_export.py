import sys
from types import ModuleType

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


def generate(patterns, **metadata):
    schema = MinedSchema(patterns=patterns, about=AboutMetadata(**metadata))
    module = ModuleType("rdfsolve_generated_test")
    sys.modules[module.__name__] = module
    exec(compile(schema.to_pydantic(), "generated.py", "exec"), module.__dict__)
    return (module, schema)


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
