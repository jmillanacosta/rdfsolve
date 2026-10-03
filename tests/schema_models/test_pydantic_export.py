"""rdfsolve.schema_models.exporters.pydantic: Pydantic models are generated from a schema, under a
contract, and registered."""

import json
import sys
from types import ModuleType

import pytest
from rdflib import RDF, XSD, Graph, Literal, Namespace

from rdfsolve.api import Client
from rdfsolve.client.registry import Registry
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclNodeShape, ShaclPropertyShape, ShaclShapesGraph
from tests.client.test_api import client


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


E = Namespace("https://example.org/")


def test_contract_records_enforce_reviewed_fields_and_constraints(tmp_path):
    schema = MinedSchema(
        about={"dataset_name": "approved", "schema_version": "1.0"},
        patterns=[
            SchemaPattern(
                subject_class=str(E.Item),
                property_uri=str(E.label),
                object_class="Literal",
                datatype=str(RDF.langString),
            )
        ],
        shapes=ShaclShapesGraph(
            node_shapes=[
                ShaclNodeShape(
                    uri=str(E.ItemShape),
                    target_class=str(E.Item),
                    property_shapes=[
                        ShaclPropertyShape(
                            path=str(E.label),
                            min_count=1,
                            max_count=1,
                            datatype=str(RDF.langString),
                        )
                    ],
                )
            ]
        ),
    )
    restored = MinedSchema.from_dict(schema.to_dict())
    with Client(restored, Graph(), contract=True) as client:
        item = client.create(str(E.Item), label=Literal("One", lang="en"))
        assert len(item.to_graph()) == 2
        assert type(item).model_config["json_schema_extra"]["schema_version"] == "1.0"
        with pytest.raises(ValueError, match="MinCountConstraintComponent"):
            client.create(str(E.Item))
        with pytest.raises(ValueError, match="MaxCountConstraintComponent"):
            client.create(str(E.Item), label=[Literal("One", lang="en"), Literal("Un", lang="fr")])
        with pytest.raises(ValueError, match="Extra inputs"):
            client.model(str(E.Item))(label=Literal("One", lang="en"), typo="bad")
        field = client.field_name(type(item), "label")
        item.rdf_terms[field][0].update(language=None, datatype=str(XSD.string))
        with pytest.raises(ValueError, match="label"):
            item.to_graph()
        assert not client.queries
    namespace = {"__name__": "generated_contract"}
    exec(compile(schema.to_pydantic(contract=True), "contract.py", "exec"), namespace)
    generated = next(
        value
        for value in namespace.values()
        if isinstance(value, type) and getattr(value, "rdf_class_iri", None) == str(E.Item)
    )
    assert len(generated(label=Literal("One", lang="en")).to_graph()) == 2
    with pytest.raises(ValueError, match="MinCountConstraintComponent"):
        generated()
    schema.shapes.node_shapes[0].deactivated = True
    relaxed = schema.to_pydantic_classes(contract=True)
    assert len(next(iter(relaxed.values()))().to_graph()) == 1, (
        "Deactivated shapes became mandatory"
    )
    schema.shapes = None
    model = next(iter(schema.to_pydantic_classes(contract=True).values()))
    assert len(model().to_graph()) == 1, "Mining invented a required field"


def test_registry_roundtrip_and_rejected_documents(tmp_path):
    with client() as data:
        registry = data.registry(source_id="aopwikirdf")
        assert not data.queries
        path = tmp_path / "registry.json"
        registry.write(path)
        assert Registry.read(path) == registry
        raw = json.loads(path.read_text())
        for bad in (
            {**raw, "format_version": 2},
            {**raw, "source_id": "changed"},
            {**raw, "load_code": "untrusted.py"},
        ):
            path.write_text(json.dumps(bad))
            with pytest.raises(ValueError):
                Registry.read(path)
        data.graph_uris = ["http://aopwiki.org/"]
        assert data.registry(source_id="aopwikirdf").revision != registry.revision
