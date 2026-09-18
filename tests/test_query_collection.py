"""Exercise query collections against RDF parsing and local AOPWiki data."""

from pathlib import Path
from unittest.mock import Mock

import pytest
from rdflib import RDF, RDFS, SH, XSD, Graph, Literal, URIRef
from rdflib.compare import isomorphic

from rdfsolve.query_collection import QueryCollection
from rdfsolve.sparql_helper import SparqlHelper

DATA = Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl"


def test_page_logging_compiles_only_new_examples(monkeypatch):
    from rdfsolve.schema_models.shacl_model import ShaclShapesGraph
    from rdfsolve.sparql_helper import QueryRecord

    compiled = []
    original = ShaclShapesGraph.compile_query

    def compile_query(self, query):
        compiled.append(query.text)
        return original(self, query)

    monkeypatch.setattr(ShaclShapesGraph, "compile_query", compile_query)
    with SparqlHelper("https://example.org/sparql") as helper:
        helper.enable_query_collection()
        for offset in range(20):
            query = f"SELECT ?x WHERE {{ VALUES ?x {{ 1 2 }} }} LIMIT 1 OFFSET {offset}"
            record = QueryRecord(query, "SELECT", helper.endpoint_url)
            helper._record_query(record)
            helper._record_query(record)
        assert len(compiled) == 20
        assert len(helper.get_collected_queries()) == 40
        exported = Graph().parse(data=helper.export_queries_as_ttl(), format="turtle")
        assert len(list(exported.subjects(SH.select))) == 20
        assert set(map(str, exported.objects(None, SH.select))) == set(compiled)


def test_named_queries_roundtrip_and_session_isolation(monkeypatch):
    data = Graph().parse(DATA, format="turtle")
    query = "SELECT ?dataset WHERE { ?dataset a <http://rdfs.org/ns/void#Dataset> }"
    with (
        SparqlHelper("https://example.org/sparql") as first,
        SparqlHelper("https://example.org/sparql") as second,
    ):
        first.add_query(
            'Datasets "source"\nlist', query, description='Quotes " and backslashes \\ survive.'
        )
        with pytest.raises(ValueError, match="unique"):
            first.add_query('Datasets "source"\nlist', query)
        first.queries.rename('Datasets "source"\nlist', "datasets")
        second.load_shacl(Graph().parse(data=first.export_queries_as_ttl(), format="turtle"))
        assert isomorphic(first.queries.graph, second.queries.graph)
        assert set(second.queries.queries) == {"datasets"}
        result = list(data.query(second.queries.queries["datasets"].query))
        assert result and URIRef("https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF") in {
            row[0] for row in result
        }
        execute = Mock(return_value={"boolean": False})
        monkeypatch.setattr(first, "_execute", execute)
        first.add_query("empty", "ASK { FILTER(false) }")
        assert first.run_query("empty") is False
        assert first.history[-1].success
        execute.return_value = {"boolean": "broken"}
        with pytest.raises(Exception, match="boolean"):
            first.run_query("empty")
        assert not first.history[-1].success
        assert first.history[-1].name == "empty"
        assert not second.history
        assert "empty" not in second.queries.queries
        first.enable_query_collection()
        monkeypatch.undo()
        monkeypatch.setattr(
            first,
            "_get_query",
            Mock(return_value=data.query(query).serialize(format="json").decode()),
        )
        first.select(query)
        assert len(first.get_collected_queries()) == 1
        assert not second.get_collected_queries()


def test_prefixes_paths_and_validation_context(monkeypatch):
    # Apply a SHACL path to retained AOPWiki RDF, not an invented domain graph.
    graph = Graph().parse(
        data="""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        <urn:query> a sh:SPARQLSelectExecutable; rdfs:label "datasets";
          sh:prefixes <urn:prefixes>; sh:select "SELECT ?s WHERE { ?s a void:Dataset }".
        <urn:prefixes> owl:imports <urn:shared> .
        <urn:shared> sh:declare [sh:prefix "void"; sh:namespace "http://rdfs.org/ns/void#"^^<http://www.w3.org/2001/XMLSchema#anyURI>] .
        <urn:downloads> sh:path (<http://rdfs.org/ns/void#subset> <http://purl.org/dc/elements/1.1/description>) .
        <urn:shape> sh:sparql <urn:validation> .
        <urn:validation> sh:select "SELECT $this WHERE { $this $PATH ?value }" .
    """,
        format="turtle",
    )
    with SparqlHelper("https://example.org/sparql") as helper:
        helper.load_shacl(graph)
        data = Graph().parse(DATA, format="turtle")
        assert list(data.query(helper.queries.queries["datasets"].query))
        query = helper.queries.path_query(
            "urn:downloads", "https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF"
        )
        # Compare generated traversal with direct RDF traversal.
        root = URIRef("https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF")
        expected = {
            value
            for subset in data.objects(root, URIRef("http://rdfs.org/ns/void#subset"))
            for value in data.objects(subset, URIRef("http://purl.org/dc/elements/1.1/description"))
        }
        assert expected
        assert {row[0] for row in data.query(query)} == expected
        execute = Mock()
        monkeypatch.setattr(helper, "_execute", execute)
        with pytest.raises(ValueError, match="context"):
            helper.run_query("urn:validation")
        execute.assert_not_called()
        assert isomorphic(
            graph, Graph().parse(data=helper.export_queries_as_ttl(), format="turtle")
        )
        with pytest.raises(ValueError):
            helper.queries.path_query("urn:downloads", "urn:x> } UNION { ?s ?p ?o")


def test_invalid_import_is_atomic():
    collection = QueryCollection()
    collection.add("kept", "ASK {}")
    original = collection.to_turtle()
    graph = Graph()
    node = URIRef("urn:bad")
    graph.add((node, RDF.type, SH.SPARQLSelectExecutable))
    graph.add((node, RDFS.label, Literal("bad")))
    graph.add((node, SH.select, Literal("DELETE WHERE { ?s ?p ?o }")))
    with pytest.raises(Exception):
        collection.load_shacl(graph)
    assert collection.to_turtle() == original
    with pytest.raises(Exception):
        collection.add("update", "INSERT DATA { <urn:s> <urn:p> <urn:o> }")
    graph.remove((node, SH.select, None))
    graph.add((node, SH.select, Literal("SELECT * WHERE { ?s missing:p ?o }")))
    with pytest.raises(Exception):
        collection.load_shacl(graph)
    assert collection.to_turtle() == original


def test_example_locations_and_repository_import(tmp_path, monkeypatch):
    from rdfsolve.models.source_model import SourceModel
    from rdfsolve.query_collection import SCHEMA

    (tmp_path / "one.rq").write_text(
        "# Title: Items\n# Description: Return item identities.\nSELECT ?s WHERE { ?s a ex:Item }"
    )
    (tmp_path / "template.rq").write_text("SELECT ?s WHERE { ?s ?p {{value}} }")
    collection = QueryCollection()
    report = collection.load_directory(
        tmp_path, endpoint="https://example.org/sparql", prefixes={"ex": "https://example.org/"}
    )
    assert [row["status"] for row in report] == ["loaded", "rejected"]
    assert len(collection.queries) == 1
    saved = collection.queries["Items"]
    assert str(collection.graph.value(saved.node, RDFS.comment)) == "Return item identities."
    assert collection.graph.value(saved.node, SCHEMA.url)
    from rdfsolve.schema_models.shacl_model import ShaclShapesGraph, ShaclSparqlExecutable

    assert isinstance(collection.shacl, ShaclShapesGraph)
    assert isinstance(collection.shacl.queries[0], ShaclSparqlExecutable)
    resource = collection.graph.value(saved.node, SH.prefixes)
    declaration = collection.graph.value(resource, SH.declare)
    assert collection.graph.value(declaration, SH.prefix) == Literal("ex")
    assert collection.graph.value(declaration, SH.namespace) == Literal(
        "https://example.org/", datatype=XSD.anyURI
    )
    assert not str(collection.graph.value(saved.node, SH.select)).startswith("PREFIX")
    data = Graph().parse(data="<urn:item> a <https://example.org/Item> .", format="turtle")
    assert list(data.query(saved.query)) == [(URIRef("urn:item"),)]
    dump = tmp_path / "examples.ttl"
    collection.to_turtle(dump)
    source = SourceModel(
        name="example",
        endpoint="https://example.org/sparql",
        sparql_examples={
            "shacl_dumps": "examples.ttl",
            "link_to_repository": "https://example.org/repo",
        },
    )
    loaded = QueryCollection.from_source(source, base_dir=tmp_path)
    assert loaded.queries["Items"].query == saved.query
    assert source.model_dump()["sparql_examples"]["shacl_dumps"] == ["examples.ttl"]
    source.sparql_examples.shacl_dumps = []
    source.sparql_examples.shacl_graph_in_endpoint = ["urn:examples"]
    called = []

    def construct(helper, query):
        called.append(query)
        return collection.graph

    monkeypatch.setattr(SparqlHelper, "construct_graph", construct)
    assert QueryCollection.from_source(source).queries["Items"].query == saved.query
    assert "GRAPH <urn:examples>" in called[0]
    with pytest.raises(ValueError):
        SourceModel(name="bad", sparql_examples={"shacl_graph_in_endpoint": "urn:x> }"})


def test_typed_shacl_prefix_roundtrip_and_conflict():
    from rdfsolve.schema_models.shacl_model import ShaclShapesGraph

    collection = QueryCollection()
    saved = collection.add(
        "Items", "SELECT ?s WHERE { ?s a ex:Item }", prefixes={"ex": "urn:example:"}
    )
    graph = collection.shacl.to_rdf()
    typed = ShaclShapesGraph.from_rdf(graph)
    assert typed.compile_query(typed.queries[0]) == saved.query
    assert isomorphic(graph, typed.to_rdf())
    declaration = next(graph.subjects(SH.prefix, Literal("ex")))
    graph.set((declaration, SH.namespace, Literal("urn:example:")))
    with pytest.raises(ValueError, match="anyURI"):
        QueryCollection().load_shacl(graph)
    graph.set((declaration, SH.namespace, Literal("urn:example:", datatype=XSD.anyURI)))
    resource = graph.value(saved.node, SH.prefixes)
    other = URIRef("urn:other-declaration")
    graph.add((resource, SH.declare, other))
    graph.add((other, SH.prefix, Literal("ex")))
    graph.add((other, SH.namespace, Literal("urn:other:", datatype=XSD.anyURI)))
    with pytest.raises(ValueError, match="Conflicting"):
        ShaclShapesGraph.from_rdf(graph)


def test_query_collection_uses_mined_prefixes():
    from rdfsolve.schema_models import MinedSchema

    schema = MinedSchema(about={}, prefixes={"ex": "urn:example:"})
    collection = QueryCollection()
    saved = collection.add("Items", "SELECT ?s WHERE { ?s a ex:Item }", schema=schema)
    data = Graph().parse(data="<urn:a> a <urn:example:Item> .", format="turtle")
    assert list(data.query(saved.query)) == [(URIRef("urn:a"),)]
    graph = collection.shacl.to_rdf()
    resource = graph.value(saved.node, SH.prefixes)
    declaration = next(graph.objects(resource, SH.declare))
    assert graph.value(declaration, SH.namespace) == Literal("urn:example:", datatype=XSD.anyURI)
    with pytest.raises(ValueError, match="Invalid prefix name"):
        MinedSchema(about={}, prefixes={"bad name": "urn:example:"})
