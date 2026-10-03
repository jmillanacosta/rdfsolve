"""rdfsolve.property_graph folds: a fold of pairs links the members of each instance, each fold is
a CONSTRUCT and a SHACL rule giving the same edges, a conversion becomes edges with its enzyme
attached, and networkx views keep chosen edge and node types."""

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


def test_a_fold_of_pairs_links_the_proteins_of_each_complex_and_keeps_the_complex():
    """Each pair of proteins of a complex gets a derived edge (the metabolite is not paired); the
    complex stays a node; derived edges are not given back as RDF, so the round trip holds."""
    wp, data = _pathway()
    ppi = Fold.pairs(
        wp + "Complex", wp + "participants", among=wp + "Protein", name="IN_COMPLEX_WITH"
    )
    pg = PropertyGraph.from_rdf(data, folds=[ppi])
    assert pg.fold_edges(0) == {
        ("urn:p1", "urn:p2"),
        ("urn:p1", "urn:p3"),
        ("urn:p2", "urn:p3"),
        ("urn:p2", "urn:p4"),
    }
    assert "urn:c1" in pg.nodes and pg.report()["lossless"]["passed"]
    assert pg.report()["folds"][0]["pairs"] == 4
    edge = next(
        d for _, _, d in pg.to_networkx().edges(data=True) if d["type"] == "IN_COMPLEX_WITH"
    )
    assert edge["derived"] is True and edge["via"] in {"urn:c1", "urn:c2"}


def test_two_complexes_of_one_pair_give_two_edges_in_networkx():
    wp, data = _pathway()
    data.add(
        ox.Quad(ox.NamedNode("urn:c3"), ox.NamedNode(wp + "participants"), ox.NamedNode("urn:p1"))
    )
    data.add(
        ox.Quad(ox.NamedNode("urn:c3"), ox.NamedNode(wp + "participants"), ox.NamedNode("urn:p2"))
    )
    data.add(ox.Quad(ox.NamedNode("urn:c3"), ox.NamedNode(RDF_TYPE), ox.NamedNode(wp + "Complex")))
    ppi = Fold.pairs(
        wp + "Complex", wp + "participants", among=wp + "Protein", name="IN_COMPLEX_WITH"
    )
    pg = PropertyGraph.from_rdf(data, folds=[ppi])
    g = pg.to_networkx()
    assert g.number_of_edges() == len(pg.edges)
    assert {d["via"] for d in g.get_edge_data("urn:p1", "urn:p2").values()} == {"urn:c1", "urn:c3"}


def test_each_fold_is_a_construct_and_a_shacl_rule_that_give_the_same_edges():
    """The CONSTRUCT compiled from each fold, run on the RDF, gives the edges the graph has (the
    log can rebuild the product); the SHACL rule parses as a node shape with a TripleRule."""
    import rdflib

    wp, data = _pathway()
    catalysis = Fold(wp + "Catalysis", wp + "source", wp + "target", "CATALYSES")
    ppi = Fold.pairs(
        wp + "Complex", wp + "participants", among=wp + "Protein", name="IN_COMPLEX_WITH"
    )
    pg = PropertyGraph.from_rdf(data, folds=[catalysis, ppi])
    store = ox.Store()
    for quad in data:
        store.add(quad)
    for index, fold in enumerate([catalysis, ppi]):
        rebuilt = {(t.subject.value, t.object.value) for t in store.query(fold.to_construct())}
        assert rebuilt == pg.fold_edges(index), fold.name
        shapes = rdflib.Graph().parse(data=fold.to_shacl(), format="turtle")
        sh = rdflib.Namespace("http://www.w3.org/ns/shacl#")
        assert (None, sh.targetClass, rdflib.URIRef(fold.cls)) in shapes
        assert len(list(shapes.subjects(rdflib.RDF.type, sh.TripleRule))) == 1
    scoped = {
        (t.subject.value, t.object.value) for t in store.query(ppi.to_construct(focus=["urn:c2"]))
    }
    assert scoped == {("urn:p2", "urn:p4")}


def test_the_graph_types_are_written_in_pg_schema():
    wp, data = _pathway()
    catalysis = Fold(wp + "Catalysis", wp + "source", wp + "target", "CATALYSES")
    text = PropertyGraph.from_rdf(data, folds=[catalysis]).to_pg_schema("wp")
    assert text.startswith("CREATE GRAPH TYPE wpType LOOSE {")
    assert "(proteinType: Protein)" in text
    assert "(:proteinType)-[catalysesType: CATALYSES" in text


def test_a_conversion_becomes_metabolite_edges_with_its_enzyme_attached():
    """A conversion of one substrate into two products becomes two edges (each keeps the
    conversion); its catalysis puts the enzyme on them as catalyzed_by and disappears; the RDF is
    given back; each fold's CONSTRUCT gives what the graph holds. A catalysis of an interaction
    that is not folded stays a node."""
    wp = "http://vocabularies.wikipathways.org/wp#"
    data = ox.Dataset(
        ox.parse(
            f"""@prefix wp: <{wp}> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    <urn:r1> a wp:Conversion ; wp:source <urn:m1> ; wp:target <urn:m2>, <urn:m3> .
    <urn:k1> a wp:Catalysis ; wp:source <urn:p1> ; wp:target <urn:r1> ; rdfs:label "k1" .
    <urn:k2> a wp:Catalysis ; wp:source <urn:p1> ; wp:target <urn:i1> .
    <urn:i1> a wp:Binding ; wp:source <urn:p1> .
    <urn:p1> a wp:Protein . <urn:m1> a wp:Metabolite . <urn:m2> a wp:Metabolite . <urn:m3> a wp:Metabolite .""".encode(),
            ox.RdfFormat.TURTLE,
        )
    )
    conversion = Fold(wp + "Conversion", wp + "source", wp + "target", "CONVERTED_TO", each=True)
    catalysis = Fold.attach(
        wp + "Catalysis",
        wp + "source",
        wp + "target",
        name="catalyzed_by",
        target_class=wp + "Conversion",
    )
    pg = PropertyGraph.from_rdf(data, folds=[conversion, catalysis])
    report = pg.report()
    assert report["lossless"]["passed"], report["lossless"]
    assert "urn:r1" not in pg.nodes and "urn:k1" not in pg.nodes and "urn:k2" in pg.nodes
    g = pg.to_networkx()
    edges = {(u, v): d for u, v, d in g.edges(data=True) if d["type"] == "CONVERTED_TO"}
    assert set(edges) == {("urn:m1", "urn:m2"), ("urn:m1", "urn:m3")}
    assert all(d["catalyzed_by"] == ["urn:p1"] and d["via"] == "urn:r1" for d in edges.values())
    store = ox.Store()
    for quad in data:
        store.add(quad)
    for index, fold in enumerate(pg.folds):
        rebuilt = {(t.subject.value, t.object.value) for t in store.query(fold.to_construct())}
        assert rebuilt == pg.fold_edges(index), fold.name


def test_a_protein_is_a_protein_however_a_source_drew_it():
    """WikiPathways draws UniProt proteins as GeneProduct or Protein; the issuer's kind is the
    node type and the drawing is kept as type, so the RDF is given back."""
    wp, up = "http://vocabularies.wikipathways.org/wp#", "http://purl.uniprot.org/core/"
    data = f"""<https://identifiers.org/uniprot/P04035> a <{wp}GeneProduct> .
    <http://purl.uniprot.org/uniprot/P04035> a <{up}Protein> .
    <https://identifiers.org/uniprot/Q14534> a <{wp}GeneProduct>, <{wp}Protein> .
    <http://purl.uniprot.org/uniprot/Q14534> a <{up}Protein> ."""
    rdf = ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE))
    pg = PropertyGraph.from_rdf(rdf, identity=Identity(kinds={"uniprot": [up + "Protein"]}))
    assert pg.report()["node_types"] == {"Protein": 2} and pg.report()["lossless"]["passed"]
    q14534 = next(
        n for n in pg.nodes.values() if "http://purl.uniprot.org/uniprot/Q14534" in n.members
    )
    drawn = {
        v.lexical for v in q14534.properties["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
    }
    assert {wp + "GeneProduct", wp + "Protein"} <= drawn
    role = PropertyGraph.from_rdf(
        rdf, identity=Identity(kinds={"uniprot": [up + "Protein"]}, labels="role")
    )
    assert role.report()["node_types"] == {"GeneProduct": 1, "GeneProduct + Protein": 1}


def test_two_drawings_of_one_metabolite_give_one_edge_and_both_statements():
    wp = "http://vocabularies.wikipathways.org/wp#"
    data = f"""<https://identifiers.org/chebi/CHEBI:16113> a <{wp}Metabolite> ; <{wp}isPartOf> <urn:wp1> ;
        <{wp}bdbHmdb> <https://identifiers.org/hmdb/HMDB0000067> .
    <http://purl.obolibrary.org/obo/CHEBI_16113> a <{wp}Metabolite> ; <{wp}isPartOf> <urn:wp1> ;
        <{wp}bdbHmdb> <https://identifiers.org/hmdb/HMDB0000067> .
    <urn:wp1> a <{wp}Pathway> ."""
    rdf = ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE))
    pg = PropertyGraph.from_rdf(rdf, identity=Identity())
    assert [e.type for e in pg.edges] == [wp + "isPartOf"] and pg.report()["lossless"]["passed"]
    # Both drawings cite one HMDB id: one edge, then a value of the node for each statement.
    assert pg.report()["identity"]["edges_made_one"] == 2


def test_networkx_keeps_chosen_edge_types_and_gives_each_node_a_category_and_title():
    pg = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES)
    g = pg.to_networkx(edge_types=["source"])
    assert {d["type"] for *_, d in g.edges(data=True)} == {"source"}
    assert set(g.nodes) == {E + "c1", E + "c2", E + "asah1"}
    assert g.nodes[E + "asah1"]["category"] == "GeneProduct + Protein"
    assert g.nodes[E + "asah1"]["title"] == "ASAH1"
    assert g.nodes[E + "c1"]["category"] == "Catalysis" and g.nodes[E + "c1"]["title"] == "c1"


def test_networkx_can_leave_out_node_types_with_their_edges():
    pg = PropertyGraph.from_rdf(graph(), prefixes=PREFIXES)
    g = pg.to_networkx(without=["Catalysis"])
    assert E + "c1" not in g and E + "asah1" in g
    assert not [d for *_, d in g.edges(data=True) if d["type"] in ("source", "target")]
