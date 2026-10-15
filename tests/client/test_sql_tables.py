"""rdfsolve.client.sql_tables: the records of a graph as tables for SQL, following a profile:
the SQL of the tables, rows that know their record, and the same graph read back."""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest
from rdflib import BNode, Graph, Literal, URIRef
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
        [ sh:path schema:provider ] ,
        [ sh:path schema:dateCreated ; sh:maxCount 1 ] .
ex:ToolName a sh:NodeShape ; sh:targetClass schema:SoftwareApplication ;
    sh:property [ sh:path schema:description ; sh:minCount 1 ] ,
        [ sh:path schema:description ; sh:maxCount 1 ; sh:message "One description." ] .
ex:Person a sh:NodeShape ; sh:targetClass schema:Person ;
    sh:property [ sh:path schema:name ; sh:maxCount 1 ] .
ex:Organization a sh:NodeShape ; sh:targetClass schema:Organization ;
    sh:property [ sh:path schema:name ; sh:maxCount 1 ] , [ sh:path schema:address ] .
"""

DATA = """
@prefix schema: <https://schema.org/> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<https://example.org/tool/1> a schema:SoftwareApplication, <https://example.org/Extra> ;
    schema:name "Tool"@en ; schema:identifier "T1"^^xsd:string ; schema:description "A tool." ;
    dct:conformsTo <https://example.org/profile> ;
    schema:about <https://example.org/term/1>, "Other" ;
    schema:dateCreated "2026-10-15"^^xsd:date ;
    schema:termCode "outside the profile" ;
    schema:author _:someone ;
    schema:provider [ a schema:Organization ; schema:name "Nested Org" ;
                      schema:address [ schema:streetAddress "Deep Street 1" ] ] .
<https://example.org/tool/2> a schema:SoftwareApplication ;
    schema:name "One", "Two" ;
    dct:conformsTo "a text where a link is expected" .
_:someone a schema:Person ; schema:name "Someone" .
<https://example.org/untyped> schema:name "No class" .
"""
TOOL = "https://example.org/tool/1"


def _database(found: dict[str, pd.DataFrame]) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript(layout(PROFILE).ddl())
    for name, frame in sorted(found.items()):
        frame.to_sql(name, connection, index=False, if_exists="append")
    return connection


def _read(connection: sqlite3.Connection, where: str = "", *args: str) -> dict[str, pd.DataFrame]:
    names = [n for (n,) in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {
        # The table names are the database's own.
        name: pd.read_sql(
            f'SELECT * FROM "{name}" {where}', connection, params=args, dtype="string"
        )  # noqa: S608
        for name in names
    }


def test_names_come_from_the_profile_and_a_field_is_single_when_any_shape_says_so():
    shapes = layout(PROFILE)
    tool = shapes.classes["https://schema.org/SoftwareApplication"]
    assert tool.name == "schema_software_application"
    assert tool.single["http://purl.org/dc/terms/conformsTo"] == "dct_conforms_to"
    assert "https://schema.org/description" in tool.single, "the stricter of two shapes counts"
    assert tool.several["https://schema.org/about"] == "schema_about"
    columns = shapes.columns()
    assert columns["schema_software_application"][:3] == ["record", "iri", "type"]
    assert columns["schema_software_application__schema_about"] == [
        "record",
        "iri",
        "value",
        "term",
    ]


def test_the_ddl_keys_the_tables_and_the_rows_come_back_as_the_same_graph():
    data = Graph().parse(data=DATA, format="turtle")
    found = tables(data, PROFILE)
    connection = _database(found)
    with pytest.raises(sqlite3.IntegrityError):
        found["schema_software_application"].head(1).to_sql(
            "schema_software_application", connection, index=False, if_exists="append"
        )
    assert isomorphic(from_tables(_read(connection), PROFILE), data)


def test_a_record_holds_its_blank_nodes_at_any_depth():
    data = Graph().parse(data=DATA, format="turtle")
    found = tables(data, PROFILE)
    organization = found["schema_organization"]
    assert list(organization["record"]) == [TOOL]
    nested = found["statements"]
    assert set(nested[nested["value"] == "Deep Street 1"]["record"]) == {TOOL}
    tool = from_tables(_read(_database(found), "WHERE record = ?", TOOL), PROFILE)
    assert (None, None, Literal("Nested Org")) in tool
    assert (None, None, Literal("Deep Street 1")) in tool
    assert (None, None, Literal("Someone")) in tool
    assert not list(tool.subjects(None, Literal("One"))), "only the rows of that record"


def test_a_blank_node_of_two_records_is_refused():
    data = Graph().parse(data=DATA, format="turtle")
    shared = BNode("shared")
    data.add((URIRef(TOOL), URIRef("https://schema.org/author"), shared))
    data.add((URIRef("https://example.org/tool/2"), URIRef("https://schema.org/author"), shared))
    data.add((shared, URIRef("https://schema.org/name"), Literal("Shared")))
    with pytest.raises(ValueError, match="_:shared"):
        tables(data, PROFILE)


def test_a_single_field_with_two_values_goes_to_the_statements():
    data = Graph().parse(data=DATA, format="turtle")
    found = tables(data, PROFILE)
    tool = found["schema_software_application"].set_index("iri")
    assert pd.isna(tool.loc["https://example.org/tool/2", "schema_name"])
    statements = found["statements"]
    assert set(statements[statements["iri"] == "https://example.org/tool/2"]["value"]) >= {
        "One",
        "Two",
    }
    assert tool.loc["https://example.org/tool/2", "dct_conforms_to__term"] == ""


def test_the_sql_does_not_depend_on_how_the_profile_was_parsed():
    with_defaults = Graph().parse(data=PROFILE, format="turtle")
    declared_only = Graph(bind_namespaces="none").parse(data=PROFILE, format="turtle")
    assert layout(with_defaults).ddl() == layout(declared_only).ddl() == layout(PROFILE).ddl()
    assert "dct_conforms_to" in layout(with_defaults).ddl()


def test_without_data_the_tables_are_empty_with_their_columns():
    empty = tables(Graph(), PROFILE)
    assert set(empty) == set(layout(PROFILE).columns())
    assert all(frame.empty for frame in empty.values())


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
