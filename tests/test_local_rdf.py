import pyoxigraph as ox
import pytest
from rdflib import BNode, Dataset, Graph, Literal, Namespace, RDF, XSD
from rdflib.compare import isomorphic

from rdfsolve import MinedSchema, SchemaMiner
from rdfsolve.api import Client
from rdfsolve.api import to_oxigraph
from rdfsolve.local_rdf import LocalRdf

E = Namespace("https://example.org/")


def test_local_backends_preserve_graphs_terms_and_schema():
    data = Dataset()
    graph = data.graph(E.data)
    graph.parse(data="""
        @prefix e: <https://example.org/> .
        e:s a e:Record; e:link e:o; e:text "hello"@en; e:items ("one" "two") .
        e:o e:label "untyped" .
    """, format="turtle")
    data.graph(E.context).add((E.o, RDF.type, E.Target))
    data.graph(E.other).add((E.s, E.text, Literal("outside")))
    data.default_graph.add((E.default, E.text, Literal("default")))
    data.graph(E.empty)
    blank_graph = BNode("graph")
    data.graph(blank_graph).add((E.blank, E.text, Literal("blank graph")))
    inputs = set(data.quads((None, None, None, None)))
    converted = to_oxigraph(data)
    outside = ox.Quad(ox.NamedNode(str(E.s)), ox.NamedNode(str(E.text)), ox.Literal("outside"), ox.NamedNode(str(E.other)))
    assert len(converted) == len(inputs) and outside in converted, "Named graphs stay named"
    schemas = []
    for source, backend in ((data, "rdflib"), (data, "oxigraph"), (converted, "oxigraph")):
        engine = LocalRdf(source, backend=backend)
        assert engine.backend == backend
        assert {str(row.o) for row in engine.query("SELECT ?o WHERE { ?s ?p ?o }")} == {"default"}
        query = f"SELECT ?o FROM <{E.data}> WHERE {{ <{E.s}> <{E.text}> ?o }}"
        assert list(engine.query(query))[0].o == Literal("hello", lang="en")
        assert bool(engine.query(f"ASK {{ GRAPH <{E.other}> {{ ?s ?p ?o }} }}"))
        found = engine.query(f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{E.data}> {{ ?s ?p ?o }} }}")
        assert isomorphic(found.graph, graph)
        assert list(engine.query(f"SELECT ?o WHERE {{ GRAPH ?g {{ <{E.blank}> ?p ?o }} }}"))[0].o == Literal("blank graph")
        with SchemaMiner.from_graph(source, local_backend=backend,
                graph_uris=[str(E.data)], type_context_graph_uris=[str(E.context)],
                delay=0) as miner:
            schema = miner.mine("backends")
            assert miner.last_report.config["local_backend"]["engine"] == backend
            assert miner.last_report.completion_state == "complete"
            assert len(schema.collections) == 1
            assert any(p.object_class == str(E.Target) for p in schema.patterns)
            assert all(p.graph_uri == str(E.data) for p in schema.structural_patterns)
            schemas.append(schema)
        with Client(schema, source, local_backend=backend, graph_uris=[str(E.data)]) as client:
            record = client.get(client.model(str(E.Record)), str(E.s))
            assert str(record.text[0]) == "hello"
            assert client.session_metadata()["local_backend"]["engine"] == backend
    def observations(schema):
        return {
            field: sorted(
                (p.model_dump_json(exclude={"examples", "witness_query", "recount_query"})
                 for p in getattr(schema, field) or [])
            )
            for field in ("patterns", "structural_patterns", "collections")
        }
    assert observations(schemas[0]) == observations(schemas[1]), "Backend changed schema evidence"
    assert observations(schemas[2]) == observations(schemas[1]), "Oxigraph data needs no RDFLib graph"
    void = schemas[0].to_void_graph().serialize(format="turtle")
    assert observations(MinedSchema.from_void(void, local_backend="rdflib")) == (
        observations(MinedSchema.from_void(void, local_backend="oxigraph"))
    )
    assert set(data.quads((None, None, None, None))) == inputs, "Mining changed the caller's dataset"

    data.default_union = True
    engine = LocalRdf(data, backend="oxigraph")
    assert len(list(engine.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"))) == len(data)
    assert {str(row.o) for row in engine.query(query)} == {"hello"}, "FROM must override union scope"

    data.graph(E.other).add((E.s, E.text, Literal("hello", lang="en")))
    engine = LocalRdf(data, backend="oxigraph")
    assert engine.backend == "rdflib" and "merge" in engine.metadata()["fallback_reason"]
    merged = f"SELECT (COUNT(*) AS ?n) FROM <{E.data}> FROM <{E.other}> WHERE {{ <{E.s}> <{E.text}> ?o }}"
    assert int(list(engine.query(merged))[0].n) == 2, "Count overlapping triples once"

    lexical = Graph()
    lexical.add((E.s, E.value, Literal("01", datatype=XSD.integer, normalize=False)))
    lexical.add((E.s, E.value, Literal("1", datatype=XSD.integer, normalize=False)))
    engine = LocalRdf(lexical, backend="oxigraph")
    assert engine.backend == "rdflib" and engine.metadata()["fallback_reason"]
    assert {str(row.o) for row in engine.query("SELECT ?o WHERE { ?s ?p ?o }")} == {"01", "1"}
    assert len(lexical) == 2
    with pytest.raises(ValueError, match="backend"):
        LocalRdf(data, backend="unknown")
