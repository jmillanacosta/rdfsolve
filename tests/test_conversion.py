"""Conversion profiles: path rules written as SHACL and compiled to one-endpoint CONSTRUCTs."""

import pyoxigraph as ox
import rdflib

from rdfsolve.conversion import Profile, Rule, rules_of
from rdfsolve.property_graph import Fold, PropertyGraph

WP = "http://vocabularies.wikipathways.org/wp#"
BL = "https://w3id.org/biolink/vocab/"
DATA = f"""@prefix wp: <{WP}> .
<urn:c1> a wp:Complex ; wp:participants <urn:p1>, <urn:p2>, <urn:m1> .
<urn:k1> a wp:Catalysis ; wp:source <urn:p1> ; wp:target <urn:r1> .
<urn:k2> a wp:Catalysis ; wp:source <urn:p2> ; wp:target <urn:r2> .
<urn:r1> a wp:Conversion ; wp:source <urn:m1> ; wp:target <urn:m2> .
<urn:r2> a wp:Conversion ; wp:source <urn:m2> ; wp:target <urn:m3> .
<urn:p1> a wp:Protein . <urn:p2> a wp:Protein .
<urn:m1> a wp:Metabolite . <urn:m2> a wp:Metabolite . <urn:m3> a wp:Metabolite ."""


def store():
    found = ox.Store()
    found.load(DATA.encode(), ox.RdfFormat.TURTLE)
    return found


def pairs(rule):
    return {(t.subject.value, t.object.value) for t in store().query(rule.to_construct())}


def test_rules_give_biolink_edges_and_categories():
    assert pairs(Rule(WP + "Catalysis", BL + "catalyzes", subject=WP + "source", object=WP + "target")) == {
        ("urn:p1", "urn:r1"), ("urn:p2", "urn:r2")}
    assert pairs(Rule(WP + "Conversion", BL + "has_input", object=WP + "source")) == {("urn:r1", "urn:m1"), ("urn:r2", "urn:m2")}
    assert pairs(Rule(WP + "Protein", BL + "category", value=BL + "Protein")) == {
        ("urn:p1", BL + "Protein"), ("urn:p2", BL + "Protein")}
    assert pairs(Rule(WP + "Complex", BL + "in_complex_with", subject=WP + "participants",
                      subject_class=WP + "Protein", pairs=True)) == {("urn:p1", "urn:p2")}


def test_a_longer_path_gives_a_sif_projection():
    """catalysis-precedes: the enzyme of a catalysis, to the enzyme of a catalysis of a reaction
    whose input is the first reaction's output."""
    precedes = Rule(WP + "Catalysis", "urn:sif:catalysis-precedes", subject=WP + "source",
                    object=f"<{WP}target>/<{WP}target>/^<{WP}source>/^<{WP}target>/<{WP}source>")
    assert pairs(precedes) == {("urn:p1", "urn:p2")}
    production = Rule(WP + "Catalysis", "urn:sif:controls-production-of", subject=WP + "source",
                      object=f"<{WP}target>/<{WP}target>", object_class=WP + "Metabolite")
    assert pairs(production) == {("urn:p1", "urn:m2"), ("urn:p2", "urn:m3")}


def test_a_profile_is_shacl_with_its_versions_and_runs_its_rules():
    profile = Profile("wikipathways-biolink", [
        Rule(WP + "Catalysis", BL + "catalyzes", subject=WP + "source", object=WP + "target"),
        Rule(WP + "Complex", BL + "in_complex_with", subject=WP + "participants", subject_class=WP + "Protein", pairs=True),
    ], target="Biolink Model", target_version="4.4.5", provenance={"source": "wikipathways", "source release": "20260610"})
    text = profile.to_shacl()
    assert "# source release: 20260610" in text and "# target version: 4.4.5" in text
    graph = rdflib.Graph().parse(data=text, format="turtle")
    sh = rdflib.Namespace("http://www.w3.org/ns/shacl#")
    assert len(list(graph.subjects(rdflib.RDF.type, sh.TripleRule))) == 2
    assert (None, rdflib.URIRef("http://purl.org/pav/version"), rdflib.Literal("4.4.5")) in graph
    client = type("C", (), {"construct": lambda self, q: ox.Dataset(ox.Quad(t.subject, t.predicate, t.object) for t in store().query(q))})()
    assert profile.counts(profile.run(client)) == {BL + "catalyzes": 2, BL + "in_complex_with": 1}


def test_the_rules_of_folds_give_the_graphs_folded_edges():
    folds = [Fold(WP + "Catalysis", WP + "source", WP + "target", "CATALYSES"),
             Fold.pairs(WP + "Complex", WP + "participants", among=WP + "Protein", name="IN_COMPLEX_WITH")]
    data = ox.Dataset(ox.parse(DATA.encode(), ox.RdfFormat.TURTLE))
    pg = PropertyGraph.from_rdf(data, folds=folds)
    for index, rule in enumerate(rules_of(pg.folds)):
        assert pairs(rule) == pg.fold_edges(index), rule.name


def test_a_rule_can_skip_focus_nodes_of_a_more_specific_class():
    data = f"""@prefix wp: <{WP}> . <urn:g> a wp:GeneProduct . <urn:p> a wp:GeneProduct, wp:Protein ."""
    found = ox.Store()
    found.load(data.encode(), ox.RdfFormat.TURTLE)
    rule = Rule(WP + "GeneProduct", BL + "category", value=BL + "Gene", unless_class=WP + "Protein")
    assert {t.subject.value for t in found.query(rule.to_construct())} == {"urn:g"}
    graph = rdflib.Graph()
    rule.add_shacl(graph, rdflib.URIRef("urn:shape"))
    assert (None, rdflib.URIRef("http://www.w3.org/ns/shacl#not"), None) in graph
