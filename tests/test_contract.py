import pytest
from rdflib import RDF, XSD, Graph, Literal, Namespace

from rdfsolve import MinedSchema, SchemaPattern
from rdfsolve.api import Client
from rdfsolve.schema_models.shacl_model import ShaclNodeShape, ShaclPropertyShape, ShaclShapesGraph

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
