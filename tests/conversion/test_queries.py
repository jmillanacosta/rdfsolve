"""rdfsolve.conversion queries: SPARQL queries become rules that give what the queries give, are
written back the same, keep the rest of a query as a condition, and keep each rule to its own ends
when several share one query."""

import pyoxigraph as ox
import rdflib

from rdfsolve.conversion import Profile, Query, Rule, rules_of
from rdfsolve.property_graph import Fold, Identity, PropertyGraph
from tests.conversion.data import BIOLINK_YAML, BL, DATA, WP, Local, biolink_client, pairs, store

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
    from rdfsolve import conversion

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
