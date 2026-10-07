"""rdfsolve.plan: proposals from the records' identifiers and the links' names, choices with what
they would do, the conversion run as generated queries, and models with and without LinkML."""

import json

import pytest
import rdflib

EX = "http://example.org/s#"
UNIPROT = "http://purl.uniprot.org/uniprot/"
CHEBI = "http://purl.obolibrary.org/obo/CHEBI_"
LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

DATA = f"""@prefix ex: <{EX}> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
ex:pw1 a ex:Pathway ; rdfs:label "first pathway" .
<{UNIPROT}P00001> a ex:Protein ; rdfs:label "protein one" ; ex:isPartOf ex:pw1 .
<{UNIPROT}P00002> a ex:Protein ; rdfs:label "protein two" ; ex:isPartOf ex:pw1 .
<{CHEBI}15422> a ex:Metabolite ; rdfs:label "small molecule one" ; ex:isPartOf ex:pw1 .
<{CHEBI}16761> a ex:Metabolite ; rdfs:label "small molecule two" ; ex:isPartOf ex:pw1 .
ex:r1 a ex:Reaction ; ex:source <{CHEBI}15422> ; ex:target <{CHEBI}16761> ; ex:isPartOf ex:pw1 .
ex:c1 a ex:Catalysis ; ex:source <{UNIPROT}P00001> ; ex:target ex:r1 ; ex:isPartOf ex:pw1 .
"""

MODEL = """id: http://example.org/target/
name: target
version: "1.0"
default_prefix: t
prefixes:
  t: http://example.org/target/
classes:
  thing: {}
  pathway: {is_a: thing}
  protein: {is_a: thing, id_prefixes: [UniProtKB]}
  small molecule: {is_a: thing, id_prefixes: [CHEBI]}
slots:
  name: {slot_uri: "rdfs:label"}
  related to: {domain: thing, range: thing}
  part of: {is_a: related to}
  catalyzes: {is_a: related to, domain: protein}
"""


def _client():
    """A client over the records, with the schema their statements give."""
    from rdfsolve import MinedSchema, SchemaPattern
    from rdfsolve.client.api import Client

    graph = rdflib.Graph().parse(data=DATA, format="turtle")
    patterns = {
        (str(s_type), str(p), str(o_type) if o_type is not None else "Literal")
        for s, p, o in graph
        if str(p) != str(rdflib.RDF.type)
        for s_type in graph.objects(s, rdflib.RDF.type)
        for o_type in ([*graph.objects(o, rdflib.RDF.type)] or [None])
    }
    schema = MinedSchema(
        about={"dataset_name": "s"},
        patterns=[
            SchemaPattern(subject_class=s, property_uri=p, object_class=o)
            for s, p, o in sorted(patterns)
        ],
    )
    return Client(schema, graph, graph_uris=[])


@pytest.fixture()
def plan(tmp_path):
    from rdfsolve.plan import Plan, Target

    path = tmp_path / "target.yaml"
    path.write_text(MODEL)
    client = _client()
    scope = client.from_table("Pathway", [EX + "pw1"])
    return Plan(scope, into=Target(path), contents=client.kinds.Protein.IsPartOf)


def test_kinds_are_proposed_from_the_records_identifiers(plan):
    assert plan.Protein.target.name == "protein" and plan.Protein.status == "proposed"
    assert "uniprot" in plan.Protein.chosen.why
    assert plan.Metabolite.target.name == "small molecule" and "chebi" in plan.Metabolite.chosen.why


def test_a_relation_is_proposed_by_its_name_and_its_ends(plan):
    assert plan.Catalysis.role == "edge" and plan.Catalysis.target.name == "catalyzes"
    assert plan.IsPartOf.role == "container" and plan.IsPartOf.target.name == "part of"


def test_a_name_alone_does_not_decide_where_the_target_names_kinds_by_identifiers(plan):
    assert plan.Pathway.status == "choose" and plan.Pathway.options[0].term.name == "pathway"
    assert "no identifiers of a registered namespace" in plan.Pathway.note


def test_a_process_without_process_kinds_in_the_target_is_a_choice(plan):
    assert plan.Reaction.role == "process" and plan.Reaction.status == "choose"
    assert plan.Reaction in plan.open
    assert "as_edge()" in repr(plan.Reaction)


def test_choices_print_as_tables_with_what_they_would_do(plan):
    text = repr(plan)
    assert "Decided" in text and "To choose" in text and "plan.Reaction" in repr(plan.open)
    plan.Reaction.as_edge()
    assert plan.Reaction.role == "edge"
    plan.Reaction.use(plan.target.predicates.related_to)
    assert plan.Reaction.status == "chosen" and plan.open == [plan.Pathway]
    plan.Pathway.use(plan.target.classes.pathway)
    assert not plan.open
    assert "Nothing open" in repr(plan.open)


def test_a_term_that_does_not_fit_is_refused(plan):
    with pytest.raises(ValueError, match="choose a class"):
        plan.Protein.use(plan.target.predicates.catalyzes)


def test_the_plan_runs_as_generated_queries(plan, tmp_path):
    plan.Reaction.leave_out()
    plan.Pathway.use(plan.target.classes.pathway)
    network = plan.run(tmp_path / "queries")
    written = {p.name for p in network.rules}
    assert {"protein.rq", "metabolite.rq", "catalysis.rq", "is_part_of.rq"} <= written
    assert "reaction.rq" not in written
    counts = network.counts()
    assert counts["kinds"]["protein"] == 2 and counts["kinds"]["small molecule"] == 2
    assert counts["relations"]["catalyzes"] == 1 and counts["relations"]["part of"] >= 4
    assert counts["relations"]["name"] >= 4
    saved = network.save(tmp_path / "out")
    assert (
        saved["statements"].stat().st_size > 0
        and (tmp_path / "out" / "queries" / "catalysis.rq").exists()
    )


def test_a_metagraph_target_states_less_and_the_plan_says_so(tmp_path):
    from rdfsolve.plan import Plan, Target
    from rdfsolve.targets.metagraph import Metagraph

    path = tmp_path / "metagraph.json"
    path.write_text(
        json.dumps(
            {
                "metanode_kinds": ["Protein", "Compound", "Pathway"],
                "metaedge_tuples": [
                    ["Protein", "Pathway", "participates", "both"],
                    ["Protein", "Compound", "binds", "both"],
                ],
            }
        )
    )
    client = _client()
    target = Target(Metagraph.read(path, name="graph"))
    assert "hierarchy" in target.lacks and "identifiers" in target.lacks
    plan = Plan(
        client.from_table("Pathway", [EX + "pw1"]),
        into=target,
        contents=client.kinds.Protein.IsPartOf,
    )
    assert "does not state" in repr(plan)
    assert plan.Protein.target.name == "Protein" and plan.Pathway.target.name == "Pathway"
    assert plan.Metabolite.status == "choose"


def test_names_keep_registered_mixed_case_words():
    from rdfsolve.naming import label, words

    assert words("hasValue") == ["has", "Value"]
    assert "ChEBI" in words("BridgeDbChEBILink")
    assert label("hasInChIKey").endswith("InChIKey")


def test_a_diagram_is_its_mermaid_text_and_prints_drawn():
    from rdfsolve.client.diagram import Diagram

    source = 'flowchart LR\n  A["Kind"] -->|"link"| B["Other"]'
    drawn = Diagram(source)
    assert str(drawn) == source and isinstance(drawn, str)
    assert "Kind" in repr(drawn) and "flowchart" not in repr(drawn)
    both = Diagram('flowchart LR\n  A["Kind"] -->|"to"| B["Other"]\n  B -->|"back"| A')
    assert "▸ Other" in repr(both) and "└◂" in repr(both)
