"""Mine the neighbourhood of chosen resources: seeds, class samples and one hop."""

from rdflib import Dataset

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.scope_strategy import ScopeStrategy

S, EX = "https://schema.org/", "https://example.org/"
ARTICLE, PERSON, LITERAL = S + "ScholarlyArticle", S + "Person", "Literal"
DATA = """
@prefix s: <https://schema.org/> . @prefix ex: <https://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
s:author rdfs:label "author"@en, "auteur"@fr . s:Person rdfs:label "Person"@en .
ex:paper a s:ScholarlyArticle; s:name "P"; s:author ex:alice; s:citation ex:other .
ex:alice a s:Person; s:name "Alice"; s:knows ex:bob .
ex:bob a s:Person; s:email "bob@example.org" .
ex:other a s:ScholarlyArticle; s:name "O"; s:about ex:topic .
ex:loose s:name "No type" .
"""


def mine(strategy, **options):
    data = Dataset().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(data, strategy=strategy, delay=0, **options) as miner:
        return miner.mine("scope"), miner.last_report


def test_every_statement_of_the_seeds_and_of_one_hop_is_a_row():
    strategy = ScopeStrategy([EX + "paper", EX + "loose"], follow=[S + "author"])
    schema, report = mine(strategy, counts=True)
    rows = {(p.subject_class, p.property_uri, p.object_class) for p in schema.patterns}
    assert rows == {
        (ARTICLE, S + "name", LITERAL),
        (ARTICLE, S + "author", PERSON),
        (ARTICLE, S + "citation", ARTICLE),
        (PERSON, S + "name", LITERAL),
        (PERSON, S + "knows", PERSON),
    }, "Only the scope: no email of bob, no subject of the cited article, no rdf:type rows"
    labels = {a.term_iri: a.text.value for a in schema.enrichment.labels}
    assert labels == {S + "author": "author", PERSON: "Person"}
    author = next(p for p in schema.patterns if p.property_uri == S + "author")
    assert (author.property_label, author.object_label) == ("author", "Person"), "No label queries"
    examples = {(e.property_uri, e.value.value) for e in schema.enrichment.examples}
    assert (S + "knows", EX + "bob") in examples
    scope = report.config["scope"]
    assert scope["followed"] == 1 and scope["unclassified_subjects"] == 1, "Counted, not guessed"
    phases = {phase.name for phase in report.phases}
    whole = {
        "counts",
        "class-entity-counts",
        "graph-census",
        "typed-coverage",
        "enrichment",
        "labels",
    }
    assert not phases & whole, "Phases that read all the data do not run"
    assert report.completion_state == "complete"


def test_a_prefix_implies_a_class_and_a_full_class_sample_keeps_the_run_partial():
    strategy = ScopeStrategy(
        [EX + "loose", EX + "paper"],
        classes=[PERSON],
        window=1,
        prefix_classes={EX + "lo": S + "Thing"},
        ignore_classes=[S + "Scholarly"],
    )
    schema, report = mine(strategy)
    rows = {(p.subject_class, p.property_uri, p.object_class) for p in schema.patterns}
    assert (S + "Thing", S + "name", LITERAL) in rows
    assert not {c for row in rows for c in row} & {ARTICLE}, "Ignored types are no classes"
    assert report.config["scope"]["classes"] == {PERSON: {"members": 1, "state": "sampled"}}
    assert report.completion_state == "partial", "Other members may add rows"
