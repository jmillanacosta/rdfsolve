"""Conversion profiles: path rules written as SHACL and compiled to one-endpoint CONSTRUCTs."""

import pyoxigraph as ox
import rdflib

from rdfsolve.conversion import Profile, Query, Rule, rules_of
from rdfsolve.property_graph import Fold, Identity, PropertyGraph

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


QUERIES = {
    "wp-biolink-catalysis.rq": """# title: An enzyme catalyzes a reaction
# source: wp
# target: biolink 4.4.5
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?enzyme biolink:catalyzes ?reaction . }
WHERE { ?catalysis a wp:Catalysis ; wp:source ?enzyme ; wp:target ?reaction . }
""",
    "wp-biolink-reaction.rq": """# title: A reaction has inputs and outputs
# source: wp
# target: biolink 4.4.5
# match: exact
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?reaction a biolink:MolecularActivity ; biolink:has_input ?in ; biolink:has_output ?out . }
WHERE { ?reaction a wp:Conversion ; wp:source ?in ; wp:target ?out . }
""",
    "wp-biolink-pairs.rq": """# title: Proteins of one complex
# source: wp
# target: biolink 4.4.5
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?a biolink:in_complex_with ?b . }
WHERE { ?complex a wp:Complex ; wp:participants ?a, ?b . ?a a wp:Protein . ?b a wp:Protein . FILTER(?a != ?b) }
""",
    "wp-biolink-union.rq": """# title: Not plain
# source: wp
# target: biolink 4.4.5
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?x a biolink:NamedThing . }
WHERE { { ?x a wp:Protein } UNION { ?x a wp:Metabolite } }
""",
}


class Local:
    """A client stand-in that runs CONSTRUCTs on the test store."""

    def construct(self, query):
        out = ox.Dataset()
        for t in store().query(query):
            out.add(ox.Quad(t.subject, t.predicate, t.object))
        return out


def profile(tmp_path):
    for name, text in QUERIES.items():
        (tmp_path / name).write_text(text)
    return Profile.from_queries(sorted(tmp_path.glob("*.rq")))


def test_queries_become_rules_that_give_what_the_queries_give(tmp_path):
    """A plain query becomes rules whose CONSTRUCTs give the query's statements; a query outside
    the plain subset is kept and run whole."""
    p = profile(tmp_path)
    assert (p.dataset, p.target, p.target_version) == ("wp", "biolink", "4.4.5")
    assert [q.name for q in p.whole()] == ["wp-biolink-union"] and "Union" in p.whole()[0].problem
    rows = {r["query"]: r for r in p.rebuilds(Local())}
    assert all(
        rows[n]["same"] for n in ("wp-biolink-catalysis", "wp-biolink-reaction", "wp-biolink-pairs")
    )
    assert rows["wp-biolink-pairs"]["compiled"] == 1, "a pair once"
    scoped = {r["query"]: r for r in p.rebuilds(Local(), scope="VALUES ?x { <urn:k1> <urn:r1> }")}
    assert (
        scoped["wp-biolink-catalysis"]["as written"] == 1 and scoped["wp-biolink-catalysis"]["same"]
    )
    shapes = rdflib.Graph().parse(data=p.to_shacl(), format="turtle")
    assert len(list(shapes.subjects(rdflib.RDF.type, rdflib.SH.TripleRule))) == 5
    assert "conversion/wp/wp-biolink-catalysis" in p.to_shacl(), "each rule names its query"
    assert (
        "sh:construct" in p.queries[0].executable()
        and "SPARQLConstructExecutable" in p.queries[0].executable()
    )


def test_the_sssom_rows_say_what_the_queries_state(tmp_path):
    """An edge-like class maps to the predicate and its links to biolink:subject and object; a
    link of the focus to the predicate on its property shape; broadMatch unless the header says
    otherwise; pairs map no term."""
    from rdfsolve.schema_models.exporters.shacl import shape_iri

    table = profile(tmp_path).to_sssom().df
    rows = {(r.subject_id, r.object_id): r.predicate_id for r in table.itertuples()}
    assert rows[("wp:Catalysis", "biolink:catalyzes")] == "skos:broadMatch"
    shape = shape_iri("wp", WP + "Catalysis", WP + "source").rsplit("/", 1)[-1]
    assert rows[(f"shape:{shape}", "biolink:subject")] == "skos:broadMatch"
    assert rows[("wp:Conversion", "biolink:MolecularActivity")] == "skos:exactMatch"
    assert not [k for k in rows if k[1] == "biolink:in_complex_with"]


def test_a_category_from_the_identifiers_of_a_node(tmp_path):
    """A node with no category takes the one its identifiers and those decided for it share."""
    from rdfsolve.conversion import Biolink, categorize

    yaml = tmp_path / "biolink.yaml"
    yaml.write_text("""version: 9.9
classes:
  named thing: {}
  gene: {is_a: named thing, id_prefixes: [NCBIGene, ENSEMBL]}
  protein: {is_a: named thing, id_prefixes: [UniProtKB, ENSEMBL]}
  protein isoform: {is_a: protein, id_prefixes: [UniProtKB]}
""")
    biolink = Biolink.read(yaml)
    assert biolink.categories("ensembl") == [BL + "Gene", BL + "Protein"]
    assert biolink.categories("uniprot") == [BL + "Protein"], "the broadest class that lists it"
    data = "<https://identifiers.org/ensembl/ENSG00000113161> <urn:x> 'HMGCR' ."
    pg = PropertyGraph.from_rdf(ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)))
    report = categorize(
        pg,
        biolink,
        [
            (
                "https://identifiers.org/ensembl/ENSG00000113161",
                "https://identifiers.org/uniprot/P04035",
            )
        ],
    )
    assert report["given"] == {BL + "Protein": 1} and pg.report()["lossless"]["passed"]


def test_the_rules_hang_from_the_first_typed_variable_written(tmp_path):
    """The author chooses the root by writing it first; a scope on the root (in a pathway, or
    the pathway itself) limits the query as written and its rules alike."""
    text = """# source: wp
# target: biolink 4.4.5
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?entity biolink:participates_in ?pathway . }
WHERE { ?pathway a wp:Pathway . ?entity a wp:Protein ; wp:partOf ?pathway . }
"""
    (tmp_path / "wp-biolink-pathways.rq").write_text(text)
    query = Query.read(tmp_path / "wp-biolink-pathways.rq")
    assert query.focus_variable() == "pathway"
    data = store()
    for s in (
        "<urn:p1> <http://vocabularies.wikipathways.org/wp#partOf> <urn:wp1>, <urn:wp2> .",
        "<urn:wp1> a <http://vocabularies.wikipathways.org/wp#Pathway> .",
        "<urn:wp2> a <http://vocabularies.wikipathways.org/wp#Pathway> .",
    ):
        data.load(s.encode(), ox.RdfFormat.TURTLE)

    class Client:
        def construct(self, q):
            out = ox.Dataset()
            for t in data.query(q):
                out.add(ox.Quad(t.subject, t.predicate, t.object))
            return out

    scope = "{ ?x <http://vocabularies.wikipathways.org/wp#partOf> <urn:wp1> } UNION { VALUES ?x { <urn:wp1> } }"
    rows = Profile.from_queries([tmp_path / "wp-biolink-pathways.rq"]).rebuilds(Client(), scope)
    assert rows == [{"query": "wp-biolink-pathways", "as written": 1, "compiled": 1, "same": True}]


def test_rules_written_as_a_query_are_read_back_the_same(tmp_path):
    """write_query gives a plain query that Query reads into the same rules, with the ends
    named as asked; the query gives what the rules give."""
    from rdfsolve.conversion import write_query

    rules = [
        Rule(
            WP + "Conversion",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            value=BL + "MolecularActivity",
        ),
        Rule(WP + "Conversion", BL + "has_input", object=f"<{WP}source>", object_as="substrate"),
        Rule(WP + "Conversion", BL + "has_output", object=f"<{WP}target>", object_as="product"),
    ]
    pairs_rule = [
        Rule(
            WP + "Complex",
            BL + "in_complex_with",
            subject=f"<{WP}participants>",
            subject_class=WP + "Protein",
            pairs=True,
            subject_as="protein",
        )
    ]
    for name, rs in (("reaction", rules), ("pairs", pairs_rule)):
        text = write_query(
            rs,
            title=name,
            source="wp",
            target="biolink 4.4.5",
            prefixes={"wp": WP, "biolink": BL},
            focus_as=name,
        )
        (tmp_path / f"wp-biolink-{name}.rq").write_text(text)
        back = Query.read(tmp_path / f"wp-biolink-{name}.rq")
        assert back.rules() == [Rule(**{**vars(r), "name": back.name}) for r in rs], text
    assert (
        "?reaction biolink:has_input ?substrate ."
        in (tmp_path / "wp-biolink-reaction.rq").read_text()
    )
    rows = Profile.from_queries(sorted(tmp_path.glob("*.rq"))).rebuilds(Local())
    assert all(r["same"] for r in rows) and rows[0]["as written"] == 1


def test_a_rule_keeps_the_rest_of_its_query_as_a_condition(tmp_path):
    """A template statement is made only where the whole WHERE matches: the category of a
    pathway needs an entity in it, as in the query."""
    (tmp_path / "wp-biolink-pathways.rq").write_text("""# source: wp
# target: biolink 4.4.5
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
CONSTRUCT { ?pathway a biolink:Pathway . ?entity biolink:participates_in ?pathway . }
WHERE { ?pathway a wp:Pathway . ?entity a wp:Protein ; wp:partOf ?pathway . }
""")
    data = store()
    data.load(
        b"<urn:wp1> a <http://vocabularies.wikipathways.org/wp#Pathway> . <urn:wp2> a <http://vocabularies.wikipathways.org/wp#Pathway> . <urn:p1> <http://vocabularies.wikipathways.org/wp#partOf> <urn:wp1> .",
        ox.RdfFormat.TURTLE,
    )

    class Client:
        def construct(self, q):
            out = ox.Dataset()
            for t in data.query(q):
                out.add(ox.Quad(t.subject, t.predicate, t.object))
            return out

    profile = Profile.from_queries([tmp_path / "wp-biolink-pathways.rq"])
    assert profile.rules[0].requires, "the category rule needs the entity"
    assert profile.rebuilds(Client()) == [
        {"query": "wp-biolink-pathways", "as written": 2, "compiled": 2, "same": True}
    ]
    assert "sh:condition" in profile.to_shacl()


def test_rules_take_curies_and_a_and_within_names_the_scope():
    """Terms are written as CURIEs (the Biolink Model's own prefix) or "a"; within() builds the
    scope from records and a link name."""
    from types import SimpleNamespace

    from rdfsolve.conversion import _expand, within

    assert _expand("a") == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    assert _expand("biolink:catalyzes") == BL + "catalyzes"
    record = type("Pathway", (), {})()
    vars(record)["uri"] = "urn:wp1"
    client = SimpleNamespace(type_name=lambda model: "Pathway")
    import rdfsolve.conversion as conversion

    original = conversion._link
    conversion._link = lambda client, name, focus: WP + "partOf"
    try:
        scope = within(client, SimpleNamespace(records=[record]), via="Is part of")
    finally:
        conversion._link = original
    assert (
        scope
        == f"{{ ?x <{WP}partOf> ?within . VALUES ?within {{ <urn:wp1> }} }} UNION {{ VALUES ?x {{ <urn:wp1> }} }}"
    )


BIOLINK_YAML = """version: 9.9
classes:
  named thing: {}
  gene: {is_a: named thing, mixins: [gene or gene product], id_prefixes: [NCBIGene, ENSEMBL]}
  protein: {is_a: named thing, mixins: [gene product mixin], id_prefixes: [UniProtKB]}
  macromolecular machine mixin: {mixin: true}
  gene or gene product: {is_a: macromolecular machine mixin, mixin: true}
  gene product mixin: {is_a: gene or gene product, mixin: true}
  molecular activity: {is_a: named thing, id_prefixes: [RHEA]}
  association: {is_a: named thing}
slots:
  catalyzes: {is_a: related to}
  has input: {is_a: has participant, domain: molecular activity, range: named thing}
"""


def test_a_biolink_graph_is_written_as_kgx_and_its_terms_drawn(tmp_path):
    """KGX nodes and edges: Biolink CURIEs, categories, names, provenance; an association node is
    one edge with its qualifier. The model draws a class with its parents and a slot as an edge."""
    import csv

    from rdfsolve.conversion import Biolink, to_kgx

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<https://identifiers.org/ncbigene/3156> a <{BL}Gene> ; <{BL}name> "HMGCR" .
    <http://identifiers.org/ncbigene/3156> a <{BL}Gene> .
    <https://identifiers.org/ncbigene/3156>
        <{BL}catalyzes> <urn:r1> .
    <urn:r1> a <{BL}MolecularActivity> ; <{BL}has_input> <https://source-test.invalid/data/Complex/a2d92> .
    <https://source-test.invalid/data/Complex/a2d92> a <{BL}MacromolecularComplex> .
    <urn:i1> a <{BL}Association> ; <{BL}subject> <https://identifiers.org/ncbigene/3156> ;
        <{BL}predicate> <{BL}regulates> ; <{BL}object> <urn:r1> ; <{BL}object_direction_qualifier> "decreased" ."""
    graph = PropertyGraph.from_rdf(
        ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)), identity=Identity()
    )
    nodes, edges = to_kgx(graph, biolink, tmp_path / "kgx", "infores:test", local_prefix="src")
    node_rows = {r["id"]: r for r in csv.DictReader(nodes.open(), delimiter="\t")}
    assert node_rows["NCBIGene:3156"] == {
        "id": "NCBIGene:3156",
        "category": "biolink:Gene",
        "name": "HMGCR",
    }
    assert "urn:i1" not in node_rows, "an association is an edge"
    edge_rows = list(csv.DictReader(edges.open(), delimiter="\t"))
    assert {
        (r["subject"], r["predicate"], r["object"], r["object_direction_qualifier"])
        for r in edge_rows
    } == {
        ("NCBIGene:3156", "biolink:catalyzes", "urn:r1", ""),
        ("NCBIGene:3156", "biolink:regulates", "urn:r1", "decreased"),
        ("urn:r1", "biolink:has_input", "src_complex:a2d92", ""),
    }
    assert all(r["primary_knowledge_source"] == "infores:test" for r in edge_rows)
    import json

    local = next(r["object"] for r in edge_rows if r["predicate"] == "biolink:has_input")
    prefix, _, name = local.partition(":")
    assert "/" not in name and json.loads((tmp_path / "kgx" / "prefixes.json").read_text())[
        prefix
    ] + name == ("https://source-test.invalid/data/Complex/a2d92"), (
        "an IRI no registry knows: a CURIE of a named namespace, expanded by prefixes.json"
    )
    drawn = biolink.diagram("biolink:Gene", "biolink:has_input")
    assert "**gene**" in drawn and '-->|"is a"|' in drawn and "ids: NCBIGene, ENSEMBL" in drawn
    assert "has input (is a has participant)" in drawn


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


def test_reaction_nodes_imply_derivation_edges_with_their_catalysts(tmp_path):
    """Biolink's rule: input derives_into output, with the reaction's catalysts as
    catalyst_qualifier; derived edges keep the round trip and are written to KGX."""
    import csv

    from rdfsolve.conversion import Biolink, derive_conversions, to_kgx

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<urn:r1> a <{BL}MolecularActivity> ; <{BL}has_input> <https://identifiers.org/chebi/CHEBI:1> ;
        <{BL}has_output> <https://identifiers.org/chebi/CHEBI:2>, <https://identifiers.org/chebi/CHEBI:3> .
    <https://identifiers.org/ncbigene/3156> a <{BL}Gene> ; <{BL}catalyzes> <urn:r1> .
    <https://identifiers.org/uniprot/P04035> a <{BL}Protein> ; <{BL}catalyzes> <urn:r1> .
    <https://identifiers.org/chebi/CHEBI:1> a <{BL}ChemicalEntity> . <https://identifiers.org/chebi/CHEBI:2> a <{BL}ChemicalEntity> .
    <https://identifiers.org/chebi/CHEBI:3> a <{BL}ChemicalEntity> ."""
    graph = PropertyGraph.from_rdf(ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)))
    assert derive_conversions(graph, biolink) == {
        "reactions": 1,
        "derivations": 2,
        "unknown processes": 0,
        "related": 0,
        "with catalyst": 2,
    }
    assert (
        graph.report()["lossless"]["passed"] and graph.report()["edge_types"]["derives_into"] == 2
    )
    # every catalyst of a reaction stays on its derived edges, also in networkx
    network = graph.to_networkx(edge_types=["derives_into"])
    assert [sorted(d["catalyst_qualifier"]) for *_, d in network.edges(data=True)] == [
        ["https://identifiers.org/ncbigene/3156", "https://identifiers.org/uniprot/P04035"]
    ] * 2
    _, edges = to_kgx(graph, biolink, tmp_path / "kgx", "infores:test")
    rows = [
        r
        for r in csv.DictReader(edges.open(), delimiter="\t")
        if r["predicate"] == "biolink:derives_into"
    ]
    assert {(r["subject"], r["object"], r["catalyst_qualifier"]) for r in rows} == {
        ("chebi:1", "chebi:2", "NCBIGene:3156|UniProtKB:P04035"),
        ("chebi:1", "chebi:3", "NCBIGene:3156|UniProtKB:P04035"),
    }


def test_biolink_ancestors_with_mixins_join_genes_and_proteins(tmp_path):
    """Along is_a only, a gene and a protein meet at named thing; with their mixins, both are a
    gene or gene product (and a macromolecular machine mixin, catalyst_qualifier's range)."""
    from rdfsolve.conversion import Biolink

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    assert biolink.ancestors("protein") == ["named thing"]
    assert biolink.ancestors("protein", mixins=True) == [
        "named thing",
        "gene product mixin",
        "gene or gene product",
        "macromolecular machine mixin",
    ]
    shared = set(biolink.ancestors("gene", mixins=True)) & set(
        biolink.ancestors("protein", mixins=True)
    )
    assert shared == {"named thing", "gene or gene product", "macromolecular machine mixin"}


def test_a_process_of_unknown_kind_gives_only_related_to(tmp_path):
    """A process that is no molecular activity (the source does not say what it is) links its
    input to its output with related_to, Biolink's root predicate, also when both are the same
    chemical; its catalyst stays attached. No derives_into is claimed."""
    from rdfsolve.conversion import Biolink, derive_conversions

    (tmp_path / "b.yaml").write_text(
        BIOLINK_YAML.replace(
            "  molecular activity: {is_a: named thing, id_prefixes: [RHEA]}",
            "  biological process or activity: {is_a: named thing}\n"
            "  molecular activity: {is_a: biological process or activity, id_prefixes: [RHEA]}",
        )
    )
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<urn:p1> a <{BL}BiologicalProcessOrActivity> ;
        <{BL}has_input> <https://identifiers.org/chebi/CHEBI:16113> ;
        <{BL}has_output> <https://identifiers.org/chebi/CHEBI:16113> .
    <urn:complex> <{BL}catalyzes> <urn:p1> .
    <https://identifiers.org/chebi/CHEBI:16113> a <{BL}ChemicalEntity> ."""
    graph = PropertyGraph.from_rdf(ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)))
    assert derive_conversions(graph, biolink) == {
        "reactions": 0,
        "derivations": 0,
        "unknown processes": 1,
        "related": 1,
        "with catalyst": 1,
    }
    network = graph.to_networkx(edge_types=["related_to"])
    assert [(u, v, d["catalyst_qualifier"]) for u, v, d in network.edges(data=True)] == [
        (
            "https://identifiers.org/chebi/CHEBI:16113",
            "https://identifiers.org/chebi/CHEBI:16113",
            ["urn:complex"],
        )
    ]


def test_an_association_gives_one_direct_edge_and_one_kgx_row(tmp_path):
    """An association node (regulates, decreased) gives one edge from its subject to its object
    with the qualifier attached; KGX still writes it as one row."""
    import csv

    from rdfsolve.conversion import Biolink, derive_associations, to_kgx

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<https://identifiers.org/ncbigene/335> a <{BL}Gene> .
    <https://identifiers.org/ncbigene/19> a <{BL}Gene> .
    <urn:i1> a <{BL}Association> ; <{BL}subject> <https://identifiers.org/ncbigene/335> ;
        <{BL}predicate> <{BL}regulates> ; <{BL}object> <https://identifiers.org/ncbigene/19> ;
        <{BL}object_direction_qualifier> "increased" ."""
    graph = PropertyGraph.from_rdf(
        ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)), identity=Identity()
    )
    assert derive_associations(graph, biolink) == 1
    assert graph.report()["lossless"]["passed"]
    network = graph.to_networkx(edge_types=["regulates"])
    assert [(u, v, d["object_direction_qualifier"]) for u, v, d in network.edges(data=True)] == [
        (
            "https://identifiers.org/ncbigene/335",
            "https://identifiers.org/ncbigene/19",
            "increased",
        )
    ]
    _, edges = to_kgx(graph, biolink, tmp_path / "kgx", "infores:test")
    rows = list(csv.DictReader(edges.open(), delimiter="\t"))
    assert [(r["subject"], r["predicate"], r["object"]) for r in rows] == [
        ("NCBIGene:335", "biolink:regulates", "NCBIGene:19")
    ]


def test_one_query_of_several_rules_keeps_each_rule_to_its_own_ends(tmp_path):
    """A complex drawn without members (a Complex Portal node) is still a complex: the members
    that the has_part rule needs are OPTIONAL in the query file, so the type rule does not need
    them, both when the file runs whole and when it is read back into rules."""
    from rdfsolve.conversion import Profile, Query, Rule, write_query

    rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    requires = ((f"<{WP}partOf>", WP + "Pathway"),)
    rules = [
        Rule(WP + "Complex", rdf_type, value=BL + "MacromolecularComplex", requires=requires),
        Rule(WP + "Complex", BL + "has_part", object=f"<{WP}participants>", requires=requires),
    ]
    text = write_query(rules, title="complex", prefixes={"wp": WP, "biolink": BL})
    assert "OPTIONAL { ?complex wp:participants ?participants . }" in text
    data = ox.Store()
    data.load(
        f"""@prefix wp: <{WP}> .
    <urn:wp1> a wp:Pathway .
    <urn:c1> a wp:Complex ; wp:partOf <urn:wp1> ; wp:participants <urn:p1> .
    <urn:c2> a wp:Complex ; wp:partOf <urn:wp1> .""".encode(),
        ox.RdfFormat.TURTLE,
    )
    typed = {
        t.subject.value for t in data.query(text) if t.object.value == BL + "MacromolecularComplex"
    }
    assert typed == {"urn:c1", "urn:c2"}
    (tmp_path / "wp-biolink-complex.rq").write_text(text)
    back = Query.read(tmp_path / "wp-biolink-complex.rq").rules()
    assert [r.requires for r in back] == [requires, requires]
    profile = Profile("complex", back)
    rebuilt = {
        t.subject.value
        for query in profile.constructs("")
        for t in data.query(query)
        if t.object.value == BL + "MacromolecularComplex"
    }
    assert rebuilt == {"urn:c1", "urn:c2"}
