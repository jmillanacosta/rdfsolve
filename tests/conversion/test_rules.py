"""rdfsolve.conversion rules: path rules give Biolink edges and categories, are written as SHACL
profiles with their versions, fold edges, and can leave out or require classes and links."""

import pyoxigraph as ox
import rdflib

from rdfsolve.conversion import Profile, Query, Rule, rules_of
from rdfsolve.property_graph import Fold, Identity, PropertyGraph
from tests.conversion.data import BIOLINK_YAML, BL, DATA, WP, Local, biolink_client, pairs, store


def test_rules_give_biolink_edges_and_categories():
    assert pairs(
        Rule(WP + "Catalysis", BL + "catalyzes", subject=WP + "source", object=WP + "target")
    ) == {("urn:p1", "urn:r1"), ("urn:p2", "urn:r2")}
    assert pairs(Rule(WP + "Conversion", BL + "has_input", object=WP + "source")) == {
        ("urn:r1", "urn:m1"),
        ("urn:r2", "urn:m2"),
    }
    assert pairs(Rule(WP + "Protein", BL + "category", value=BL + "Protein")) == {
        ("urn:p1", BL + "Protein"),
        ("urn:p2", BL + "Protein"),
    }
    assert pairs(
        Rule(
            WP + "Complex",
            BL + "in_complex_with",
            subject=WP + "participants",
            subject_class=WP + "Protein",
            pairs=True,
        )
    ) == {("urn:p1", "urn:p2")}


def test_a_longer_path_gives_a_sif_projection():
    """catalysis-precedes: the enzyme of a catalysis, to the enzyme of a catalysis of a reaction
    whose input is the first reaction's output."""
    precedes = Rule(
        WP + "Catalysis",
        "urn:sif:catalysis-precedes",
        subject=WP + "source",
        object=f"<{WP}target>/<{WP}target>/^<{WP}source>/^<{WP}target>/<{WP}source>",
    )
    assert pairs(precedes) == {("urn:p1", "urn:p2")}
    production = Rule(
        WP + "Catalysis",
        "urn:sif:controls-production-of",
        subject=WP + "source",
        object=f"<{WP}target>/<{WP}target>",
        object_class=WP + "Metabolite",
    )
    assert pairs(production) == {("urn:p1", "urn:m2"), ("urn:p2", "urn:m3")}


def test_a_profile_is_shacl_with_its_versions_and_runs_its_rules():
    profile = Profile(
        "wikipathways-biolink",
        [
            Rule(WP + "Catalysis", BL + "catalyzes", subject=WP + "source", object=WP + "target"),
            Rule(
                WP + "Complex",
                BL + "in_complex_with",
                subject=WP + "participants",
                subject_class=WP + "Protein",
                pairs=True,
            ),
        ],
        target="Biolink Model",
        target_version="4.4.5",
        provenance={"source": "wikipathways", "source release": "20260610"},
    )
    text = profile.to_shacl()
    assert "# source release: 20260610" in text and "# target version: 4.4.5" in text
    graph = rdflib.Graph().parse(data=text, format="turtle")
    sh = rdflib.Namespace("http://www.w3.org/ns/shacl#")
    assert len(list(graph.subjects(rdflib.RDF.type, sh.TripleRule))) == 2
    assert (None, rdflib.URIRef("http://purl.org/pav/version"), rdflib.Literal("4.4.5")) in graph
    client = type(
        "C",
        (),
        {
            "construct": lambda self, q: ox.Dataset(
                ox.Quad(t.subject, t.predicate, t.object) for t in store().query(q)
            )
        },
    )()
    assert profile.counts(profile.run(client)) == {BL + "catalyzes": 2, BL + "in_complex_with": 1}


def test_the_rules_of_folds_give_the_graphs_folded_edges():
    folds = [
        Fold(WP + "Catalysis", WP + "source", WP + "target", "CATALYSES"),
        Fold.pairs(
            WP + "Complex", WP + "participants", among=WP + "Protein", name="IN_COMPLEX_WITH"
        ),
    ]
    data = ox.Dataset(ox.parse(DATA.encode(), ox.RdfFormat.TURTLE))
    pg = PropertyGraph.from_rdf(data, folds=folds)
    for index, rule in enumerate(rules_of(pg.folds)):
        assert pairs(rule) == pg.fold_edges(index), rule.name


def test_a_rule_can_skip_focus_nodes_of_a_more_specific_class():
    data = f"""@prefix wp: <{WP}> . <urn:g> a wp:GeneProduct . <urn:p> a wp:GeneProduct, wp:Protein ."""
    found = ox.Store()
    found.load(data.encode(), ox.RdfFormat.TURTLE)
    rule = Rule(
        WP + "GeneProduct", BL + "category", value=BL + "Gene", unless_classes=(WP + "Protein",)
    )
    assert {t.subject.value for t in found.query(rule.to_construct())} == {"urn:g"}
    graph = rdflib.Graph()
    rule.add_shacl(graph, rdflib.URIRef("urn:shape"))
    assert (None, rdflib.URIRef("http://www.w3.org/ns/shacl#not"), None) in graph


def test_a_class_left_out_with_filter_not_exists(tmp_path):
    (tmp_path / "wp-biolink-genes.rq").write_text("""# source: wp
# target: biolink 4.4.5
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?node a biolink:Gene . }
WHERE { ?node a wp:Metabolite . FILTER NOT EXISTS { ?node a wp:Protein } FILTER NOT EXISTS { ?node a wp:Rna } }
""")
    rules = Query.read(tmp_path / "wp-biolink-genes.rq").rules()
    assert [r.unless_classes for r in rules] == [(WP + "Protein", WP + "Rna")]
    from rdfsolve.conversion import write_query

    written = write_query(rules, title="genes", prefixes={"wp": WP, "biolink": BL})
    assert "FILTER NOT EXISTS { ?metabolite a wp:Protein }" in written and "wp:Rna" in written
    assert (
        len(
            list(
                rdflib.Graph()
                .parse(data=Profile(name="p", rules=rules).to_shacl(), format="turtle")
                .objects(None, rdflib.SH["not"])
            )
        )
        == 2
    )
    rows = Profile.from_queries([tmp_path / "wp-biolink-genes.rq"]).rebuilds(Local())
    assert rows[0]["same"] and rows[0]["as written"] == 3


def test_exact_kind_leaves_out_the_kinds_beside_and_below():
    """A rule for exactly one kind leaves out the kinds at its level and below, from the mined
    schema's class extensions (the whole source), not from the data at hand."""
    from types import SimpleNamespace

    from rdfsolve.conversion import _other_kinds

    contained_in = {
        WP + "GeneProduct": [WP + "DataNode"],
        WP + "Protein": [WP + "DataNode"],
        WP + "Rna": [WP + "DataNode"],
        WP + "Mrna": [WP + "Rna", WP + "GeneProduct"],
        WP + "Conversion": [WP + "Interaction"],
    }
    client = SimpleNamespace(
        schema=SimpleNamespace(class_extensions=SimpleNamespace(contained_in=contained_in))
    )
    assert _other_kinds(client, WP + "GeneProduct") == (WP + "Mrna", WP + "Protein", WP + "Rna")


def test_an_end_of_a_rule_can_leave_out_a_class(tmp_path):
    """Entities that take part in a pathway, other than pathways drawn in it: the subject's
    exclusion goes through the query, the rules, SPARQL, SHACL and a written query."""
    from rdfsolve.conversion import write_query

    text = """# source: wp
# target: biolink 4.4.5
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?entity biolink:participates_in ?pathway . }
WHERE { ?pathway a wp:Pathway . ?entity a wp:DataNode ; wp:partOf ?pathway . FILTER NOT EXISTS { ?entity a wp:Pathway } }
"""
    (tmp_path / "wp-biolink-p.rq").write_text(text)
    rules = Query.read(tmp_path / "wp-biolink-p.rq").rules()
    assert [r.subject_unless_classes for r in rules] == [(WP + "Pathway",)]
    data = ox.Store()
    data.load(
        f"""@prefix wp: <{WP}> .
    <urn:wp1> a wp:Pathway . <urn:wp2> a wp:Pathway, wp:DataNode ; wp:partOf <urn:wp1> .
    <urn:g1> a wp:DataNode ; wp:partOf <urn:wp1> .""".encode(),
        ox.RdfFormat.TURTLE,
    )
    found = {(t.subject.value, t.object.value) for t in data.query(rules[0].to_construct())}
    assert found == {("urn:g1", "urn:wp1")}, "the drawn pathway is left out"
    written = write_query(rules, title="p", prefixes={"wp": WP, "biolink": BL})
    assert "FILTER NOT EXISTS { ?partof a wp:Pathway }" in written
    shapes = rdflib.Graph().parse(data=Profile(name="p", rules=rules).to_shacl(), format="turtle")
    assert len(list(shapes.objects(None, rdflib.SH["not"]))) == 1


def test_a_rule_can_require_a_link_of_its_focus(tmp_path):
    """Pairs of proteins only from a complex that is part of a pathway: the condition is in the
    compiled rule and in the query written from it, and the query gives back the same rule."""
    from rdfsolve.conversion import write_query

    rule = Rule(
        WP + "Complex",
        BL + "in_complex_with",
        subject=f"<{WP}participants>",
        subject_class=WP + "Protein",
        pairs=True,
        requires=((f"<{WP}partOf>", WP + "Pathway"),),
        subject_as="protein",
    )
    data = ox.Store()
    data.load(
        f"""@prefix wp: <{WP}> .
    <urn:c1> a wp:Complex ; wp:participants <urn:p1>, <urn:p2> ; wp:partOf <urn:wp1> .
    <urn:c2> a wp:Complex ; wp:participants <urn:p3>, <urn:p4> ; wp:partOf <urn:i1> .
    <urn:wp1> a wp:Pathway . <urn:i1> a wp:Interaction .
    <urn:p1> a wp:Protein . <urn:p2> a wp:Protein . <urn:p3> a wp:Protein . <urn:p4> a wp:Protein .""".encode(),
        ox.RdfFormat.TURTLE,
    )
    assert {(t.subject.value, t.object.value) for t in data.query(rule.to_construct())} == {
        ("urn:p1", "urn:p2")
    }
    text = write_query([rule], title="pairs", prefixes={"wp": WP, "biolink": BL})
    assert "?complex wp:partOf ?pathway ." in text and "?pathway a wp:Pathway ." in text
    (tmp_path / "wp-biolink-pairs.rq").write_text(text)
    back = Query.read(tmp_path / "wp-biolink-pairs.rq").rules()
    assert [r.requires for r in back] == [((f"<{WP}partOf>", WP + "Pathway"),)]
