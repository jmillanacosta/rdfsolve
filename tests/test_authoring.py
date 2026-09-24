import pandas as pd
import pytest
from rdflib import RDF, XSD, BNode, Graph, Literal, Namespace
from rdflib.compare import isomorphic

from rdfsolve import MinedSchema, SchemaPattern
from rdfsolve.api import Client

E = Namespace("https://example.org/")


def test_records_and_tables_preserve_rdf_values(tmp_path):
    schema = MinedSchema(
        about={"dataset_name": "records"},
        patterns=[
            SchemaPattern(
                subject_class=str(E.Item),
                property_uri=str(prop),
                object_class=kind,
                datatype=datatype,
            )
            for prop, kind, datatype in [
                (E.label, "Literal", str(RDF.langString)),
                (E.date, "Literal", str(XSD.gYear)),
                (E.date, "Literal", str(XSD.date)),
                (E.part, str(E.Part), None),
                (E.part, "Literal", str(RDF.langString)),
            ]
        ]
        + [
            SchemaPattern(
                subject_class=str(E.Part),
                property_uri=str(E.label),
                object_class="Literal",
                datatype=str(RDF.langString),
            )
        ],
    )
    with Client(schema, Graph()) as client:
        part = client.create(str(E.Part), label=Literal("Teil", lang="de"))
        item = client.create(
            str(E.Item),
            uri=str(E.one),
            part=part,
            label=Literal("One", lang="en"),
            date=Literal("2026", datatype=XSD.gYear, normalize=False),
        )
        direct = client.model(str(E.Item))(
            uri=str(E.one),
            part=part,
            label=Literal("One", lang="en"),
            **{str(E.date): Literal("2026", datatype=XSD.gYear, normalize=False)},
        )
        expected = Graph().parse(
            data="""
            @prefix e: <https://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            e:one a e:Item; e:label "One"@en; e:date "2026"^^xsd:gYear;
                e:part [a e:Part; e:label "Teil"@de] .
        """,
            format="turtle",
        )
        assert isomorphic(item.to_graph(), expected), (
            "Direct records lost RDF terms or nested nodes"
        )
        assert isomorphic(direct.to_graph(), expected)
        rows = client.from_table(
            str(E.Item),
            pd.DataFrame([{"id": str(E.one), "text": "One", "date": "2026", "part": part}]),
            id_column="id",
            languages={"label": "en"},
            datatypes={"date": str(XSD.gYear)},
            label="text",
            date="date",
            part="part",
        )
        path = tmp_path / "records.ttl"
        client.save(path, rows)
        assert isomorphic(Graph().parse(path), expected), "Table import changed the authored graph"
        unnamed = client.from_table(
            str(E.Part),
            pd.DataFrame([{"text": "A"}, {"text": "B"}]),
            languages={"label": "en"},
            label="text",
        )
        assert len({r.uri for r in unnamed}) == 2
        assert all(isinstance(next(r.to_graph().subjects()), BNode) for r in unnamed)
        left = client.create(str(E.Part), uri=BNode("shared"), blank_node_scope="left")
        right = client.create(str(E.Part), uri=BNode("shared"), blank_node_scope="right")
        assert set(left.to_graph().subjects()).isdisjoint(right.to_graph().subjects())
        assert (None, RDF.type, E.Part) in left.to_graph()
        linked = client.create(str(E.Item), part=left).to_graph()
        assert next(linked.objects(None, E.part)) in set(linked.subjects(RDF.type, E.Part))

        with pytest.raises(ValueError, match="label"):
            client.create(str(E.Item), label=Literal(1, datatype=XSD.integer))
        with pytest.raises(ValueError, match="date"):
            client.create(str(E.Item), date=Literal("wrong", datatype=XSD.date))
        with pytest.raises(ValueError, match="Unknown"):
            client.create(str(E.Item), missing="value")
        with pytest.raises(ValueError, match="row"):
            client.from_table(str(E.Item), pd.DataFrame([{"id": "relative"}]), id_column="id")
        dated = client.create(
            str(E.Item), date=Literal("2026-09-24", datatype=XSD.date, normalize=False)
        )
        restored = type(dated).model_validate_json(dated.model_dump_json())
        assert isomorphic(dated.to_graph(), restored.to_graph()), (
            "A mixed date field lost its RDF value"
        )
        mixed = client.create(str(E.Item), part=[left, Literal("other", lang="en")]).to_graph()
        assert set(mixed.subjects(RDF.type, E.Part)) <= set(mixed.objects(None, E.part))
        assert Literal("other", lang="en") in set(mixed.objects(None, E.part))
        assert not client.queries
