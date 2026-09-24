import json

import pytest
from rdflib import RDF, XSD, BNode, Dataset, Graph, Literal, Namespace
from rdflib.collection import Collection
from rdflib.compare import isomorphic

from rdfsolve import MinedSchema, SchemaMiner
from rdfsolve.api import Client, RDFList
from rdfsolve.mining.edge_graph_split import split_by_edge_graph

E = Namespace("https://example.org/")


def test_collection_profiles_and_ordered_records(tmp_path):
    source = Dataset()
    graph = source.graph(E.data)
    graph.parse(
        data="""
        @prefix e: <https://example.org/> .
        e:one a e:Record; e:members (e:a e:b e:a) .
        e:two a e:Record; e:members () .
        e:a a e:Agent; e:label "A"@en .
        e:b a e:Agent; e:label "B"@en .
    """,
        format="turtle",
    )
    source.graph(E.other).parse(
        data="""
        @prefix e: <https://example.org/> .
        e:one a e:Record; e:members ("not in scope") .
    """,
        format="turtle",
    )
    with SchemaMiner.from_graph(source, graph_uris=[str(E.data)], counts=False, delay=0) as miner:
        schema = miner.mine()
    assert len(schema.collections) == 1, "Lists must aggregate by owner class, predicate and graph"
    profile = schema.collections[0]
    assert profile.member_types == [str(E.Agent)] and profile.member_kinds == ["IRI"]
    assert (profile.list_count, profile.min_length, profile.max_length) == (2, 0, 3)
    assert profile.graph_uri == str(E.data) and profile.invalid_count == 0
    assert schema.clean_schema(graph_uris=[str(E.data)]).collections == []
    assert split_by_edge_graph(schema, str(E.other), "other").collections == []
    restored = MinedSchema.from_dict(schema.to_dict())
    with Client(restored, Graph(), contract=True) as client:
        a = client.create(str(E.Agent), uri=str(E.a), label=Literal("A", lang="en"))
        b = client.create(str(E.Agent), uri=str(E.b), label=Literal("B", lang="en"))
        record = client.create(str(E.Record), uri=str(E.one), members=RDFList(items=[a, b, a]))
        empty = client.create(str(E.Record), uri=str(E.two), members=RDFList(items=[]))
        output = record.to_graph() + empty.to_graph()
        assert isomorphic(output, graph), (
            "Writing a collection lost order, duplicates or member records"
        )
        head = output.value(E.one, E.members)
        assert list(Collection(output, head)) == [E.a, E.b, E.a]
        cloned = type(record).model_validate_json(record.model_dump_json())
        assert isomorphic(cloned.to_graph(), record.to_graph()), "Record JSON lost collection types"
        ordinary = client.create(str(E.Record), members=["_:one", "_:two"])
        assert len(list(ordinary.to_graph().objects(None, E.members))) == 2
        assert not list(ordinary.to_graph().triples((None, RDF.first, None)))
        with pytest.raises(ValueError, match="members"):
            client.create(
                str(E.Record), members=RDFList(items=[Literal("wrong member")])
            ).to_graph()
    graph = Graph().parse(
        data="""
        @prefix dcat: <http://www.w3.org/ns/dcat#> .
        @prefix prov: <http://www.w3.org/ns/prov#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix e: <https://example.org/> .
        e:dataset a dcat:Dataset; dct:title "Measurements"@en;
            prov:qualifiedAttribution [a prov:Attribution; prov:agent e:lab];
            e:readings ("1"^^xsd:integer "2"^^xsd:integer "1"^^xsd:integer) .
        e:lab a foaf:Organization; foaf:name "Laboratory"@en .
        """,
        format="turtle",
    )
    with SchemaMiner.from_graph(graph, counts=False, delay=0) as miner:
        observed = miner.mine()
    snapshot = tmp_path / "approved-schema.json"
    snapshot.write_text(json.dumps(observed.to_dict()))
    approved = MinedSchema.from_json(snapshot)
    namespace = {"__name__": "generated_authoring_workflow"}
    exec(compile(approved.to_pydantic(contract=True), "models.py", "exec"), namespace)
    models = {
        value.rdf_class_iri: value
        for value in namespace.values()
        if isinstance(value, type) and hasattr(value, "rdf_class_iri")
    }
    lab = models["http://xmlns.com/foaf/0.1/Organization"](
        uri=str(E.lab),
        name=Literal("Laboratory", lang="en"),
    )
    attribution = models["http://www.w3.org/ns/prov#Attribution"](agent=lab)
    record = models["http://www.w3.org/ns/dcat#Dataset"](
        uri=str(E.dataset),
        title=Literal("Measurements", lang="en"),
        qualifiedattribution=attribution,
        readings=RDFList(items=[Literal(1), Literal(2), Literal(1)]),
    )
    output = tmp_path / "authored.ttl"
    record.to_graph().serialize(output, format="turtle")
    assert isomorphic(Graph().parse(output), graph), "The approved model changed the input graph"
    assert len(approved.collections) == 1 and approved.collections[0].member_datatypes == [
        str(XSD.integer)
    ]
    assert approved.to_dict() == observed.to_dict(), "Authoring changed the approved snapshot"
    restored_record = type(record).model_validate_json(record.model_dump_json())
    assert isomorphic(restored_record.to_graph(), graph)
    record.readings = RDFList(items=[Literal(3)])
    assert len(list(record.to_graph().objects(None, RDF.first))) == 1, (
        "Observed lengths became constraints"
    )

    broken = Graph().parse(
        data="""
        @prefix e: <https://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        e:r a e:Record; e:members _:cycle .
        _:cycle rdf:first e:a; rdf:rest _:cycle .
    """,
        format="turtle",
    )
    profiles = schema.discover_collections(broken, graph_uris=[])
    assert profiles[0].invalid_count == 1 and profiles[0].list_count == 0
