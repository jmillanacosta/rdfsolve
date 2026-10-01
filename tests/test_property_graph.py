"""Property graphs from RDF: lossless, typed, named, folded, and checked writers."""

import csv
import json
from datetime import date

import networkx as nx
import pyoxigraph as ox
import pytest

from rdfsolve.property_graph import Conversion, Fold, PropertyGraph, suggest_folds
from rdfsolve.schema_models import MinedSchema, SchemaPattern

DATA = """
@prefix e: <https://pg-test.invalid/> . @prefix o: <https://other-test.invalid/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
e:asah1 a e:Protein, e:GeneProduct ; rdfs:label "ASAH1", "acid ceramidase"@en ; e:name "ASAH1" ;
    o:name "Acid ceramidase" ; e:mass "44.6"^^xsd:decimal ; e:length 395 ;
    e:seen "2026-09-03"^^xsd:date ; e:score "12a"^^xsd:integer ; e:code "x"^^xsd:string .
e:cer a e:Metabolite ; rdfs:label "ceramide" ; e:shape [ e:rings 0 ] .
e:sph a e:Metabolite ; rdfs:label "sphingosine" .
e:c1 a e:Catalysis ; e:source e:asah1 ; e:target e:cer ; e:partOf e:wp ; e:note "acid" .
e:c2 a e:Catalysis ; e:source e:asah1 ; e:target e:sph .
e:about e:refersTo e:c2 .
"""
E = "https://pg-test.invalid/"
PREFIXES = {"e": E, "o": "https://other-test.invalid/"}


XSD = "http://www.w3.org/2001/XMLSchema#"


def graph():
    """Return the test data as an Oxigraph dataset."""
    return ox.Dataset(ox.parse(DATA.encode(), ox.RdfFormat.TURTLE))


def test_plain_export_is_lossless_typed_and_named():
    pg = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES)
    report = pg.report()
    assert report["lossless"]["passed"], report["lossless"]
    assert report["names"]["passed"], "Clashing local names fall back to CURIEs"
    keys = pg.names()["keys"]
    assert {"e:name", "o:name", "label", "mass"} <= set(keys), keys
    node = pg.to_networkx().nodes[E + "asah1"]
    assert sorted(node["labels"]) == ["GeneProduct", "Protein"]
    assert node["mass"] == 44.6 and node["length"] == 395 and node["seen"] == date(2026, 9, 3)
    assert node["score"] == "12a" and node["score__datatype"] == XSD + "integer", "No native form"
    assert node["label"] == ["ASAH1", "acid ceramidase"] and node["label__lang"] == ["", "en"]
    assert report["identity"]["blank_nodes"] == 1


def test_types_and_names_can_be_set():
    as_text = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, types={XSD + "decimal": None})
    assert as_text.to_networkx().nodes[E + "asah1"]["mass"] == "44.6"
    no_native = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, native=False)
    assert no_native.to_networkx().nodes[E + "asah1"]["length"] == "395"
    suffix = Conversion(lambda t: int(t.rstrip("a")), lambda v: f"{v}a", "long")
    custom_type = PropertyGraph.from_rdf(
        graph(), prefixes=PREFIXES, types={XSD + "integer": suffix}
    )
    assert custom_type.to_networkx().nodes[E + "asah1"]["score"] == 12
    curies = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, names="curie")
    assert "e:mass" in curies.names()["keys"]
    custom = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, names={E + "mass": "massDa"})
    assert "massDa" in custom.names()["keys"] and custom.report()["lossless"]["passed"]
    clash = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, names=lambda iri: "same")
    assert not clash.report()["names"]["passed"], "A naming that clashes fails the names gate"
    with pytest.raises(ValueError, match="names="):
        PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, names="labels")


def test_fold_applies_where_it_holds_and_is_undone():
    fold = Fold(E + "Catalysis", E + "source", E + "target", name="CATALYSES")
    pg = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, folds=[fold])
    report = pg.report()
    assert report["lossless"]["passed"], report["lossless"]
    assert report["folds"][0] == {
        "fold": "Catalysis: source -> target",
        "applied": 1,
        "kept: linked to": 1,
    }
    edges = [(s, t, d) for s, t, d in pg.to_networkx().edges(data=True) if d["type"] == "CATALYSES"]
    assert len(edges) == 1
    source, target, data = edges[0]
    assert (source, target, data["via"], data["note"]) == (E + "asah1", E + "cer", E + "c1", "acid")
    assert data["partOf"] == E + "wp" and data["partOf__datatype"] == "@id"
    assert E + "c2" in pg.nodes, "A catalysis that something links to stays a node"


def test_writers_check_what_they_write(tmp_path):
    pg = PropertyGraph.from_rdf(
        graph(), prefixes=PREFIXES, folds=[Fold("e:Catalysis", "e:source", "e:target")]
    )
    pg.to_graphml(tmp_path / "pg.graphml")
    assert pg.checks["graphml"]["passed"]
    back = nx.read_graphml(tmp_path / "pg.graphml", force_multigraph=True)
    assert json.loads(back.nodes[E + "asah1"]["labels"]) == ["GeneProduct", "Protein"]
    folder = pg.to_neo4j(tmp_path / "neo4j")
    with (folder / "nodes.csv").open() as handle:
        header = next(csv.reader(handle))
    assert header[:2] == ["id:ID", ":LABEL"]
    assert {"mass:double", "length:long", "seen:date", "label:string[]"} <= set(header), header
    with (folder / "relationships.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert {r[":TYPE"] for r in rows} >= {"Catalysis", "source", "target"}
    assert "--array-delimiter=';'" in (folder / "import.sh").read_text()
    data = pg.to_json()
    assert data["report"]["lossless"]["passed"]
    mass = next(n for n in data["nodes"] if n["id"] == E + "asah1")["properties"]["mass"]
    assert mass == [{"value": 44.6, "datatype": XSD + "decimal"}]


def test_schema_sets_multiplicity_and_suggests_folds():
    cls = E + "Catalysis"
    patterns = [
        SchemaPattern(
            subject_class=cls,
            property_uri=E + "source",
            object_class=E + "Protein",
            count=2,
            distinct_subjects=2,
            distinct_objects=1,
        ),
        SchemaPattern(
            subject_class=cls,
            property_uri=E + "target",
            object_class=E + "Metabolite",
            count=2,
            distinct_subjects=2,
            distinct_objects=2,
        ),
        SchemaPattern(
            subject_class=E + "Protein",
            property_uri=E + "mass",
            object_class="http://www.w3.org/2000/01/rdf-schema#Literal",
            count=1,
            distinct_subjects=1,
            distinct_objects=1,
        ),
    ]
    schema = MinedSchema(
        about={"dataset_name": "pg-test", "class_entity_counts": {cls: 2}}, patterns=patterns
    )
    folds = suggest_folds(schema)
    assert [(f.source, f.target) for f in folds] == [(E + "source", E + "target")]
    pg = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES, schema=schema, folds=folds)
    report = pg.report()
    assert report["multiplicity"]["passed"] and report["lossless"]["passed"]
    assert pg.to_networkx().nodes[E + "asah1"]["mass"] == 44.6, "Single-valued: not a list"


def test_oxigraph_sources_and_named_graphs():
    store = ox.Store()
    store.load(DATA.encode(), ox.RdfFormat.TURTLE)
    from_store = PropertyGraph.from_rdf(store)
    assert from_store.report()["lossless"]["passed"]
    assert len(from_store.nodes) == len(PropertyGraph.from_rdf(graph(), prefixes=PREFIXES).nodes)
    assert len(from_store.to_oxigraph()) == len(graph())
    quads = [
        ox.Quad(
            ox.NamedNode(E + "a"), ox.NamedNode(E + "p"), ox.Literal("v"), ox.NamedNode(E + "g")
        )
    ]
    named = PropertyGraph.from_rdf(quads).report()["lossless"]
    assert not named["passed"] and named["named_graphs"] == [f"<{E}g>"], "Graph names are not kept"


def test_client_records_become_a_checked_property_graph():
    from pathlib import Path

    from rdfsolve.api import Client

    folder = Path(__file__).parents[1] / "notebooks/mcp/schemas"
    client = Client.open(
        folder / "aopwikirdf-small.schema.json", data_file=folder / "aopwikirdf-small.ttl"
    )
    assert isinstance(client.source, ox.Store), "A data file is loaded by Oxigraph"
    found = client.search(["decreased"])
    pg = client.property_graph(found)
    report = pg.report()
    assert report["lossless"]["passed"] and report["names"]["passed"]
    assert report["nodes"] == len(found)
    assert len(client.to_oxigraph(found)) == len(pg.to_oxigraph())
