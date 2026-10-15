"""rdfsolve.client.sql_tables: the records of a graph as tables for SQL, following a profile,
written to SQLite and read back as the same graph."""

from __future__ import annotations

import sqlite3

import pandas as pd
from rdflib import Graph
from rdflib.compare import isomorphic

from rdfsolve.client.sql_tables import from_tables, layout, tables

PROFILE = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix schema: <https://schema.org/> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ex: <https://example.org/shapes/> .
ex:Tool a sh:NodeShape ; sh:targetClass schema:SoftwareApplication ;
    sh:property [ sh:path schema:name ; sh:maxCount 1 ] ,
        [ sh:path schema:identifier ; sh:maxCount 1 ] ,
        [ sh:path dct:conformsTo ; sh:maxCount 1 ; sh:nodeKind sh:IRI ] ,
        [ sh:path schema:about ; sh:nodeKind sh:IRI ] ,
        [ sh:path schema:dateCreated ; sh:maxCount 1 ] .
ex:Person a sh:NodeShape ; sh:targetClass schema:Person ;
    sh:property [ sh:path schema:name ; sh:maxCount 1 ] .
"""

DATA = """
@prefix schema: <https://schema.org/> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<https://example.org/tool/1> a schema:SoftwareApplication, <https://example.org/Extra> ;
    schema:name "Tool"@en ; schema:identifier "T1"^^xsd:string ;
    dct:conformsTo <https://example.org/profile> ;
    schema:about <https://example.org/term/1>, "Other" ;
    schema:dateCreated "2026-10-15"^^xsd:date ;
    schema:termCode "outside the profile" ;
    schema:author _:someone .
<https://example.org/tool/2> a schema:SoftwareApplication ;
    schema:name "One", "Two" ;
    dct:conformsTo "a text where a link is expected" .
_:someone a schema:Person ; schema:name "Someone" .
<https://example.org/untyped> schema:name "No class" .
"""


def test_names_come_from_the_profile_curies():
    shapes = layout(PROFILE)
    tool = shapes.classes["https://schema.org/SoftwareApplication"]
    assert tool.name == "schema_software_application"
    assert tool.single["http://purl.org/dc/terms/conformsTo"] == "dct_conforms_to"
    assert tool.several == {"https://schema.org/about": "schema_about"}
    columns = shapes.columns()["schema_software_application"]
    assert columns[:2] == ["iri", "type"] and "schema_identifier__term" in columns


def test_tables_go_to_sql_and_come_back_as_the_same_graph():
    data = Graph().parse(data=DATA, format="turtle")
    found = tables(data, PROFILE)
    tool = found["schema_software_application"].set_index("iri")
    assert tool.loc["https://example.org/tool/1", "schema_name__term"] == "@en"
    assert pd.isna(tool.loc["https://example.org/tool/2", "schema_name"]), "two values: long table"
    assert tool.loc["https://example.org/tool/2", "dct_conforms_to__term"] == ""
    long = found["schema_software_application__values"]
    assert set(long[long["iri"] == "https://example.org/tool/2"]["value"]) == {"One", "Two"}
    outside = set(found["statements"]["property"])
    assert {"https://schema.org/termCode", "https://schema.org/author"} <= outside
    assert "https://example.org/Extra" in set(found["statements"]["value"])
    connection = sqlite3.connect(":memory:")
    for name, frame in found.items():
        frame.to_sql(name, connection, index=False)
    read = {
        # The table names are the test's own.
        name: pd.read_sql(f'SELECT * FROM "{name}"', connection, dtype="string")  # noqa: S608
        for name in found
    }
    assert isomorphic(from_tables(read, PROFILE), data)


def test_without_data_the_tables_are_empty_with_their_columns():
    empty = tables(Graph(), PROFILE)
    assert set(empty) == {
        "schema_software_application",
        "schema_software_application__values",
        "schema_person",
        "schema_person__values",
        "statements",
    }
    assert all(frame.empty for frame in empty.values())
    assert list(empty["schema_person"].columns) == [
        "iri",
        "type",
        "schema_name",
        "schema_name__term",
    ]


def test_a_client_gives_the_tables_of_its_local_rdf():
    import pyoxigraph as ox

    from rdfsolve.client.api import Client
    from rdfsolve.schema_models import MinedSchema

    data = Graph().parse(data=DATA, format="turtle")
    store = ox.Store()
    store.load(DATA.encode(), format=ox.RdfFormat.TURTLE)
    schema = MinedSchema(about={"dataset_name": "fixture"}, patterns=[], shape_profile=PROFILE)
    for source in (data, store):
        client = Client(schema, source, graph_uris=[])
        # Oxigraph keeps "T1"^^xsd:string as "T1" (the same literal in RDF 1.1): the graph read
        # back is the one the client holds.
        assert isomorphic(client.from_tables(client.tables()), client._local_graph())
    assert isomorphic(Client(schema, data, graph_uris=[])._local_graph(), data)
