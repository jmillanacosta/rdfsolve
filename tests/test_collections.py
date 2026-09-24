import pytest
from rdflib import RDF, BNode, Dataset, Graph, Literal, Namespace
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
    with Client(restored, Graph()) as client:
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
