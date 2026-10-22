"""rdfsolve.property_graph: property graphs from RDF are lossless, typed and named, with folds that
apply where they hold, writers that check what they write, schemas that set multiplicity, sources in
named graphs, records of a client, the hierarchy as a node attribute, and statements about
themselves as values."""

import csv
import json
from datetime import date

import networkx as nx
import pyoxigraph as ox
import pytest

from rdfsolve.property_graph import Conversion, Fold, Identity, PropertyGraph, suggest_folds
from rdfsolve.schema_models import MinedSchema, SchemaPattern
from tests.property_graph.data import (
    DATA,
    IDS,
    MAPPINGS,
    PREFIXES,
    RDF_TYPE,
    WPV,
    XSD,
    E,
    _pathway,
    graph,
)


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


def test_an_edge_type_and_a_key_may_share_a_name():
    data = f"<{E}c1> <{E}source> <{E}asah1> . <{E}asah1> <https://other-test.invalid/source> 'x' ."
    pg = PropertyGraph.from_rdf(ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)))
    assert pg.names()["types"] == {"source": E + "source"}
    assert "source" in pg.names()["keys"] and pg.report()["names"]["passed"]


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

    folder = Path(__file__).parents[2] / "notebooks/mcp/schemas"
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


def test_the_hierarchy_is_a_node_attribute():
    """A superclass is a property of the node (a reference), not an edge; a blank-node superclass
    (an OWL restriction) stays an edge. The RDF is given back."""
    data = ox.Dataset(
        ox.parse(
            b"""
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . @prefix owl: <http://www.w3.org/2002/07/owl#> .
        <urn:serine> a owl:Class ; rdfs:label "serine" ; rdfs:subClassOf <urn:amino-acid>, [ a owl:Restriction ] .
        <urn:amino-acid> a owl:Class ; rdfs:label "amino acid" .""",
            ox.RdfFormat.TURTLE,
        )
    )
    pg = PropertyGraph.from_rdf(data)
    assert pg.report()["lossless"]["passed"]
    assert pg.to_networkx().nodes["urn:serine"]["subClassOf"] == "urn:amino-acid"
    assert [e.target for e in pg.edges if e.source == "urn:serine"] == [
        next(i for i in pg.nodes if i.startswith("_:"))
    ]
    edges = PropertyGraph.from_rdf(data, as_attributes=())
    assert {e.target for e in edges.edges if e.source == "urn:serine"} >= {"urn:amino-acid"}


def test_a_merged_node_takes_the_data_class_not_the_majority():
    """WP4726 had 70 ChEBI classes and 67 metabolites, so a majority made merged metabolites
    Class and left unmerged ones Metabolite. The role rule keeps the data's own class."""

    extra = "".join(
        f"<http://purl.obolibrary.org/obo/CHEBI_{n}> a <http://www.w3.org/2002/07/owl#Class> .\n"
        for n in range(1, 6)
    )
    data = ox.Dataset(ox.parse((IDS + extra).encode(), ox.RdfFormat.TURTLE))
    chemical = ["http://www.w3.org/2002/07/owl#Class"]
    # WikiPathways issues its own DataNode kind; that must not count against a metabolite's role.
    kinds = {
        "chebi": chemical,
        "cas": chemical,
        "wikipathways": [
            "http://vocabularies.wikipathways.org/wp#DataNode",
            "http://vocabularies.wikipathways.org/wp#Metabolite",
        ],
    }
    metabolite = "http://vocabularies.wikipathways.org/wp#Metabolite"
    for policy, expected in (("role", metabolite), ("majority", chemical[0])):
        pg = PropertyGraph.from_rdf(
            data, identity=Identity(kinds=kinds, mappings=MAPPINGS, exact=True, labels=policy)
        )
        water = next(
            n for n in pg.nodes.values() if "https://identifiers.org/cas/7732-18-5" in n.members
        )
        assert water.labels == [expected], policy
        assert pg.report()["lossless"]["passed"]


def test_networkx_keeps_the_edge_type_when_a_fold_has_other_classes():
    """A folded catalysis that is also a DirectedInteraction keeps its edge type; its classes
    are kept as rdf_type."""
    wp = "http://vocabularies.wikipathways.org/wp#"
    data = ox.Dataset(
        ox.parse(
            f"""
        @prefix wp: <{wp}> .
        <urn:c> a wp:Catalysis, wp:DirectedInteraction ; wp:source <urn:e> ; wp:target <urn:r> .
        <urn:e> a wp:Protein . <urn:r> a wp:Conversion .""".encode(),
            ox.RdfFormat.TURTLE,
        )
    )
    pg = PropertyGraph.from_rdf(
        data, folds=[Fold(wp + "Catalysis", wp + "source", wp + "target", "CATALYSES")]
    )
    ((_, _, attrs),) = pg.to_networkx().edges(data=True)
    assert attrs["type"] == "CATALYSES" and attrs["rdf_type"] == wp + "DirectedInteraction"


def test_a_statement_about_itself_is_a_value_not_a_loop():
    data = ox.Dataset(
        ox.parse(
            b"""
        <https://identifiers.org/chebi/CHEBI:15377> a <urn:Metabolite> ;
            <urn:bdbChEBI> <https://identifiers.org/chebi/CHEBI:15377> .""",
            ox.RdfFormat.TURTLE,
        )
    )
    pg = PropertyGraph.from_rdf(data)
    assert not pg.edges and pg.report()["lossless"]["passed"]


def test_folds_are_found_in_the_records():
    """Each catalysis links one enzyme and one reaction (and a shared pathway): it folds, as
    the most specific class; a reaction that a catalysis points to stays a node."""
    from rdfsolve.property_graph import suggested_folds

    wp = "http://vocabularies.wikipathways.org/wp#"
    part = "http://purl.org/dc/terms/isPartOf"
    turtle = f"@prefix wp: <{wp}> .\n" + "".join(
        f"<urn:c{i}> a wp:Catalysis, wp:Interaction ; wp:source <urn:e{i}> ; wp:target <urn:r{i}> ; <{part}> <urn:p> .\n"
        f"<urn:r{i}> a wp:Conversion, wp:Interaction ; wp:source <urn:m{i}> ; wp:target <urn:n{i}> ; <{part}> <urn:p> .\n"
        f"<urn:e{i}> a wp:Protein . <urn:m{i}> a wp:Metabolite . <urn:n{i}> a wp:Metabolite .\n"
        for i in range(4)
    )
    data = ox.Dataset(ox.parse(turtle.encode(), ox.RdfFormat.TURTLE))
    folds = suggested_folds([], data)
    assert {(f.cls, f.source, f.target) for f in folds} == {
        (wp + "Catalysis", wp + "source", wp + "target"),
        (wp + "Conversion", wp + "source", wp + "target"),
    }
    pg = PropertyGraph.from_rdf(data, folds=folds)
    assert pg.report()["lossless"]["passed"]
    assert all(n.startswith("urn:r") or not n.startswith("urn:c") for n in pg.nodes)
    assert sum(1 for n in pg.nodes if n.startswith("urn:r")) == 4, (
        "a reaction a catalysis targets stays"
    )
