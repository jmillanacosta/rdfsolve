import pandas as pd
import pyoxigraph as ox
import pytest
from rdflib import RDF, XSD, BNode, Graph, Literal, Namespace, URIRef
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
                (E.date, "Literal", str(XSD.gYearMonth)),
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
        quads = item.to_oxigraph()
        year = ox.Literal("2026", datatype=ox.NamedNode(str(XSD.gYear)))
        assert ox.Quad(ox.NamedNode(str(E.one)), ox.NamedNode(str(E.date)), year) in quads
        assert len(quads) == len(expected), "The same statements, also for Oxigraph"
        assert sum(isinstance(q.object, ox.BlankNode) for q in quads) == 1, "Nested nodes stay blank"
        by_iri = client.create(
            str(E.Item), uri=str(E.one), part=part, label=Literal("One", lang="en"),
            **{E.date: Literal("2026", datatype=XSD.gYear, normalize=False)},
        )
        assert isomorphic(by_iri.to_graph(), expected)
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

        inferred = client.create(
            str(E.Item), uri=str(E.one), language="en", extra_types=[str(E.Other)],
            label="One", date="2026-09", part=part,
        ).to_graph()
        assert (E.one, E.date, Literal("2026-09", datatype=XSD.gYearMonth, normalize=False)) in inferred
        assert (E.one, E.label, Literal("One", lang="en")) in inferred
        assert set(inferred.objects(E.one, RDF.type)) == {E.Item, E.Other}
        inferred_rows = client.from_table(
            str(E.Item), pd.DataFrame([{"label": "One", "date": "2026-09"}]),
            language="en", label="label", date="date",
        )
        assert set(inferred_rows[0].to_graph().objects(None, E.date)) == {
            Literal("2026-09", datatype=XSD.gYearMonth, normalize=False)
        }
        for bad in ("2026-99", "2026-02-30", "2026-09+14:01"):
            with pytest.raises(ValueError, match="date"):
                client.create(str(E.Item), date=bad)
        with pytest.raises(ValueError, match="date"):
            client.create(str(E.Item), date=Literal("2026-09", datatype=XSD.gYear, normalize=False))
        with pytest.raises(ValueError, match="part.*[Aa]mbiguous"):
            client.create(str(E.Item), language="en", part=str(E.Part))
        explicit = client.create(str(E.Item), part=URIRef(E.Part), language="en")
        assert (None, E.part, E.Part) in explicit.to_graph()
        with pytest.raises(ValueError, match="IRI"):
            client.create(str(E.Item), extra_types=["relative"])

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
        sparse = client.from_table(
            str(E.Part),
            pd.DataFrame([{"id": None, "text": pd.NA}, {"id": str(E.named), "text": "A"}]),
            id_column="id",
            languages={"label": "en"},
            label="text",
        )
        assert sparse[0].uri.startswith("_:") and sparse[0].label is None
        assert client.catalogue.fragments[sparse.references[0]].term.kind == "bnode"
        scoped_refs = []
        for scope in ("first", "second"):
            group = client.from_table(
                str(E.Part),
                pd.DataFrame([{"id": BNode("same")}]),
                id_column="id",
                blank_node_scope=scope,
            )
            scoped_refs.extend(group.references)
        assert len(set(scoped_refs)) == 2, "Blank-node scopes merged in the catalogue"
        direct_path = tmp_path / "direct.ttl"
        client.save(direct_path, item)
        assert isomorphic(Graph().parse(direct_path), expected)
        client.save(tmp_path / "sparse.ttl", sparse, left)
        assert not client.queries

    people = MinedSchema(about={"dataset_name": "api-check"}, patterns=[
        SchemaPattern(subject_class=iri, property_uri=str(E.knows), object_class=iri)
        for iri in ("https://schema.org/Person", "http://xmlns.com/foaf/0.1/Person")
    ])
    with Client(people, Graph()) as client:
        client.models["Person"] = client.model("https://schema.org/Person")
        assert client.model("Person").rdf_class_iri == "https://schema.org/Person"
        with pytest.raises(ValueError, match="ambiguous") as error:
            client.model("person")
        assert all(iri in str(error.value) for iri in people.get_classes())
        iri = "https://schema.org/Person"
        model = client.model(iri)
        assert not client.links(iri).empty
        assert client.field_name(iri, str(E.knows)) == client.field_name(model, str(E.knows))
        assert client.type_name(iri) == client.type_name(model)
        assert client.link_name(iri, "knows") == client.link_name(model, "knows")
        assert all(iri in client.diagram() for iri in people.get_classes())

        raw = client.diagram(fenced=False)
        assert client.diagram() == "```mermaid\n" + raw + "\n```"
        assert raw.startswith("flowchart LR") and "-->" in raw
        paths = pd.DataFrame([{
            "Path": 1, "Step": 1, "From class": "Person",
            "To class": "Person", "Link": "knows",
        }])
        paths.attrs.update(routes=[[(iri, str(E.knows), iri, False)]], truncated=True)
        raw_path = client.diagram(paths=paths, fenced=False)
        assert "```" not in raw_path and "-->" in raw_path
        assert "%% Partial view" in raw_path
