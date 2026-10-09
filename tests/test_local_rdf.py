"""rdfsolve.local_rdf: local RDF is loaded and queried, with restriction patterns."""

import pyoxigraph as ox
import pytest
from rdflib import RDF, XSD, BNode, Dataset, Graph, Literal, Namespace
from rdflib.compare import isomorphic

from rdfsolve import MinedSchema, SchemaMiner
from rdfsolve.api import Client, to_oxigraph
from rdfsolve.local_rdf import LocalRdf
from rdfsolve.mining.restrictions import mine_restriction_patterns
from rdfsolve.schema_models import AboutMetadata

E = Namespace("https://example.org/")


def test_local_backends_preserve_graphs_terms_and_schema():
    data = Dataset()
    graph = data.graph(E.data)
    graph.parse(
        data="""
        @prefix e: <https://example.org/> .
        e:s a e:Record; e:link e:o; e:text "hello"@en; e:items ("one" "two") .
        e:o e:label "untyped" .
    """,
        format="turtle",
    )
    data.graph(E.context).add((E.o, RDF.type, E.Target))
    data.graph(E.other).add((E.s, E.text, Literal("outside")))
    data.default_graph.add((E.default, E.text, Literal("default")))
    data.graph(E.empty)
    blank_graph = BNode("graph")
    data.graph(blank_graph).add((E.blank, E.text, Literal("blank graph")))
    inputs = set(data.quads((None, None, None, None)))
    converted = to_oxigraph(data)
    outside = ox.Quad(
        ox.NamedNode(str(E.s)),
        ox.NamedNode(str(E.text)),
        ox.Literal("outside"),
        ox.NamedNode(str(E.other)),
    )
    assert len(converted) == len(inputs) and outside in converted, "Named graphs stay named"
    schemas = []
    for source, backend in ((data, "rdflib"), (data, "oxigraph"), (converted, "oxigraph")):
        engine = LocalRdf(source, backend=backend)
        assert engine.backend == backend
        assert {str(row.o) for row in engine.query("SELECT ?o WHERE { ?s ?p ?o }")} == {"default"}
        query = f"SELECT ?o FROM <{E.data}> WHERE {{ <{E.s}> <{E.text}> ?o }}"
        assert list(engine.query(query))[0].o == Literal("hello", lang="en")
        assert bool(engine.query(f"ASK {{ GRAPH <{E.other}> {{ ?s ?p ?o }} }}"))
        found = engine.query(
            f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{E.data}> {{ ?s ?p ?o }} }}"
        )
        assert isomorphic(found.graph, graph)
        assert list(engine.query(f"SELECT ?o WHERE {{ GRAPH ?g {{ <{E.blank}> ?p ?o }} }}"))[
            0
        ].o == Literal("blank graph")
        with SchemaMiner.from_graph(
            source,
            local_backend=backend,
            graph_uris=[str(E.data)],
            type_context_graph_uris=[str(E.context)],
            delay=0,
        ) as miner:
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
                p.model_dump_json(exclude={"examples", "witness_query", "recount_query"})
                for p in getattr(schema, field) or []
            )
            for field in ("patterns", "structural_patterns", "collections")
        }

    assert observations(schemas[0]) == observations(schemas[1]), "Backend changed schema evidence"
    assert observations(schemas[2]) == observations(schemas[1]), (
        "Oxigraph data needs no RDFLib graph"
    )
    void = schemas[0].to_void_graph().serialize(format="turtle")
    assert observations(MinedSchema.from_void(void, local_backend="rdflib")) == (
        observations(MinedSchema.from_void(void, local_backend="oxigraph"))
    )
    assert set(data.quads((None, None, None, None))) == inputs, (
        "Mining changed the caller's dataset"
    )

    data.default_union = True
    engine = LocalRdf(data, backend="oxigraph")
    assert len(list(engine.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"))) == len(data)
    assert {str(row.o) for row in engine.query(query)} == {"hello"}, (
        "FROM must override union scope"
    )

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


def test_a_zip_archive_of_rdf_files_loads_every_file(tmp_path):
    import zipfile

    from rdfsolve.local_rdf import load_store

    archive = tmp_path / "dump.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("a/one.ttl", "<urn:a> <urn:p> <urn:b> .")
        z.writestr("two.nt", "<urn:c> <urn:p> <urn:d> .\n")
        z.writestr("README.txt", "not RDF")
    assert len(load_store(archive)) == 2


def test_the_dumps_of_one_release_load_into_one_store(tmp_path):
    """Two dumps of one release (a zip and a file) give one store with both layers."""
    import zipfile

    from rdfsolve.local_rdf import load_store

    archive = tmp_path / "layer-one.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("one.ttl", "<urn:a> <urn:p> <urn:b> .")
    (tmp_path / "layer-two.ttl").write_text("<urn:a> <urn:q> <urn:c> .")
    store = load_store([archive, tmp_path / "layer-two.ttl"])
    assert len(store) == 2
    assert len(load_store(archive)) == 1


OBO = "http://purl.obolibrary.org/obo/"
DATA = f"""
@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix obo: <{OBO}> .
obo:RO_0001025 a owl:ObjectProperty ; rdfs:label "located in" .
obo:RO_0002452 a owl:ObjectProperty ; rdfs:label "has symptom" .
obo:DOID_1 a owl:Class ; rdfs:label "liver disease" ;
  rdfs:subClassOf [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ; owl:someValuesFrom obo:UBERON_0002107 ] ,
                  [ a owl:Restriction ; owl:onProperty obo:RO_0002452 ; owl:someValuesFrom obo:SYMP_1 ] .
obo:DOID_2 a owl:Class ; rdfs:label "heart disease" ;
  rdfs:subClassOf [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ; owl:someValuesFrom obo:UBERON_0000948 ] ,
                  [ a owl:Restriction ; owl:onProperty obo:RO_0002452 ; owl:allValuesFrom obo:SYMP_2 ] .
obo:DOID_3 a owl:Class ; owl:equivalentClass [ owl:intersectionOf ( obo:DOID_2
    [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ; owl:someValuesFrom obo:UBERON_0000948 ] ) ] .
obo:DOID_4 a owl:Class ; rdfs:subClassOf [ a owl:Restriction ; owl:onProperty obo:RO_0001025 ;
    owl:someValuesFrom [ owl:unionOf ( obo:UBERON_1 obo:UBERON_2 ) ] ] .
obo:UBERON_0002107 rdfs:label "liver" .
"""


def _mine():
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        return mine_restriction_patterns(miner.helper)


def _key(p):
    return p.axiom, p.property_uri.rsplit("/", 1)[-1], p.form, p.filler_namespace.rsplit("/", 1)[-1]


def test_each_relation_of_the_ontology_is_a_pattern_with_its_counts():
    found = _mine()
    assert found.state == "complete" and found.query_count > 0
    by = {_key(p): p for p in found.patterns}
    assert set(by) == {
        ("SubClassOf", "RO_0001025", "some", "UBERON_"),
        ("SubClassOf", "RO_0001025", "some", "(class expression)"),
        ("SubClassOf", "RO_0002452", "some", "SYMP_"),
        ("SubClassOf", "RO_0002452", "only", "SYMP_"),
        ("EquivalentTo", "RO_0001025", "some", "UBERON_"),
    }
    located = by["SubClassOf", "RO_0001025", "some", "UBERON_"]
    assert (located.count, located.classes) == (2, 2) and located.subject_namespace == OBO + "DOID_"
    assert located.example_subject.startswith(OBO + "DOID_")
    assert located.example_filler.startswith(OBO + "UBERON_")


def test_the_label_is_in_manchester_syntax_with_the_labels_of_the_source():
    by = {_key(p): p for p in _mine().patterns}
    assert by["SubClassOf", "RO_0001025", "some", "UBERON_"].label == (
        "DOID SubClassOf 'located in' some UBERON"
    )
    assert by["SubClassOf", "RO_0002452", "only", "SYMP_"].label == (
        "DOID SubClassOf 'has symptom' only SYMP"
    )
    assert by["EquivalentTo", "RO_0001025", "some", "UBERON_"].label == (
        "DOID EquivalentTo (… and 'located in' some UBERON)"
    )


def test_the_patterns_are_kept_in_the_schema_file():
    schema = MinedSchema(about=AboutMetadata.build(dataset_name="x"), patterns=[])
    schema.restriction_patterns = _mine()
    read = MinedSchema.from_dict(schema.to_dict())
    assert read.restriction_patterns == schema.restriction_patterns
    assert (
        MinedSchema.from_dict(
            MinedSchema(about=schema.about, patterns=[]).to_dict()
        ).restriction_patterns
        is None
    )


def test_a_folder_of_rdf_files_loads_every_file(tmp_path):
    from rdfsolve.local_rdf import load_store

    (tmp_path / "a.ttl").write_text("<urn:a> <urn:p> <urn:b> .\n")
    (tmp_path / "b.nt").write_text("<urn:b> <urn:p> <urn:c> .\n")
    (tmp_path / "notes.txt").write_text("not RDF")
    assert len(load_store(tmp_path)) == 2


class _Entry:
    name = "fixture.source"
    download_ttl = ["http://unreachable.invalid/data.ttl"]
    model_extra: dict = {}


def test_an_unreachable_download_fails_with_its_url(tmp_path, monkeypatch):
    import urllib.request

    from rdfsolve import local_rdf

    monkeypatch.setenv("RDFSOLVE_DOWNLOADS", str(tmp_path))
    monkeypatch.setattr("rdfsolve.sources.load_sources", lambda: [_Entry()])

    def refuse(url, timeout):
        assert timeout == local_rdf.DOWNLOAD_TIMEOUT
        raise TimeoutError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)
    with pytest.raises(ConnectionError, match="unreachable.invalid/data.ttl"):
        local_rdf.registry_files("fixture.source")
    assert not list((tmp_path / "fixture.source").iterdir()), "no partial file is left"


def test_a_registry_name_is_the_entry_even_beside_a_folder_of_that_name(tmp_path, monkeypatch):
    from rdfsolve import local_rdf

    (tmp_path / "fixture.source").mkdir()  # e.g. the downloads folder in the working directory
    monkeypatch.chdir(tmp_path)
    data = tmp_path / "downloaded.ttl"
    data.write_text("<urn:a> <urn:p> <urn:b> .\n")
    monkeypatch.setattr("rdfsolve.sources.load_sources", lambda: [_Entry()])
    asked = []
    monkeypatch.setattr(local_rdf, "registry_files", lambda name: asked.append(name) or [data])
    from rdfsolve.schema_models import SchemaPattern

    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="fixture"),
        patterns=[SchemaPattern(subject_class="urn:C", property_uri="urn:p", object_class="urn:D")],
    )
    Client.open(schema, data_file="fixture.source")
    assert asked == ["fixture.source"]
