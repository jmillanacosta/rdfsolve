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
ex:g1 a ex:Group ; ex:member <{UNIPROT}P00001>, <{UNIPROT}P00002>, <{CHEBI}15422> ; ex:isPartOf ex:pw1 .
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
  catalysis event: {is_a: thing}
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
    return Plan(scope, into=Target(path), contents=client.kinds.Protein.is_part_of)


def test_kinds_are_proposed_from_the_records_identifiers(plan):
    assert plan.Protein.target.name == "protein" and plan.Protein.status == "proposed"
    assert "uniprot" in plan.Protein.chosen.why
    assert plan.Metabolite.target.name == "small molecule" and "chebi" in plan.Metabolite.chosen.why


def test_a_relation_is_proposed_by_its_name_and_its_ends(plan):
    assert plan.Catalysis.reading == "edge" and plan.Catalysis.target.name == "catalyzes"
    assert plan.is_part_of.reading == "container" and plan.is_part_of.target.name == "part of"


def test_a_name_alone_does_not_decide_where_the_target_names_kinds_by_identifiers(plan):
    assert plan.Pathway.status == "choose" and plan.Pathway.options[0].term.name == "pathway"
    assert "no identifiers of a registered namespace" in plan.Pathway.note


def test_a_process_without_process_kinds_in_the_target_is_a_choice(plan):
    assert plan.Reaction.reading == "process" and plan.Reaction.status == "choose"
    assert plan.Reaction in plan.open
    assert "as_edge()" in repr(plan.Reaction)


def test_choices_print_as_tables_with_what_they_would_do(plan):
    text = repr(plan)
    assert "Decided" in text and "To choose" in text and "plan.Reaction" in repr(plan.open)
    plan.Reaction.as_edge()
    assert plan.Reaction.reading == "edge"
    plan.Reaction.use(plan.target.relations.related_to)
    assert plan.Reaction.status == "chosen" and plan.Pathway in plan.open
    plan.Pathway.use(plan.target.kinds.pathway)
    plan.Group.leave_out()
    assert not plan.open
    assert "Nothing open" in repr(plan.open)


def test_contents_are_written_as_in_related_from_either_side(tmp_path):
    from rdfsolve.plan import Plan, Target

    path = tmp_path / "target.yaml"
    path.write_text(MODEL)
    client = _client()
    scope = client.from_table("Pathway", [EX + "pw1"])
    by_text = Plan(scope, into=Target(path), contents="^Is part of")
    assert by_text.contents_incoming and len(by_text.records) == len(scope) + 7
    by_link = Plan(scope, into=Target(path), contents=client.kinds.Protein.is_part_of)
    assert by_link.contents_incoming and len(by_link.records) == len(by_text.records)
    with pytest.raises(ValueError, match="choices: \\^Is part of"):
        Plan(scope, into=Target(path), contents="Has part")


def test_following_links_reads_large_sets_in_batches():
    from rdfsolve.client.hydration import HydrationLimitError

    client = _client()
    client.max_subjects, client.batch_size = 1, 1  # every request as small as it can be
    scope = client.from_table("Pathway", [EX + "pw1"])
    members = scope.related(via="Is part of", incoming=True)
    assert len(members) == 7
    assert len(scope.related(via="^Is part of")) == 7, '"^link" reads the link backwards'
    client.max_rows = 1
    with pytest.raises(HydrationLimitError, match="max_rows=1"):
        scope.related(via="Is part of", incoming=True)


def test_a_node_kind_can_be_converted_as_edges_between_named_links(plan, tmp_path):
    plan.Reaction.leave_out()
    plan.Pathway.use(plan.target.kinds.pathway)
    plan.Catalysis.as_edge("source", "TARGET").use(plan.target.relations.related_to)
    assert plan.Catalysis.reading == "edge" and plan.Catalysis.target.name == "related to"
    with pytest.raises(ValueError, match="has no link 'Nowhere'"):
        plan.Catalysis.as_edge("Nowhere", "Target")
    network = plan.run(tmp_path / "queries")
    assert network.counts()["relations"]["related to"] == 1
    # a record converted as an edge (or left out) is no node, so it is part of nothing
    typed = {q.subject.value for q in network.statements if q.predicate.value.endswith("#type")}
    part_of = [q for q in network.statements if q.predicate.value.endswith("part_of")]
    assert part_of and all(q.subject.value in typed for q in part_of)


def test_the_printouts_show_the_ends_and_how_to_convert_a_kind_as_edges(plan):
    assert "Source → Target" in repr(plan) or "source → target" in repr(plan).lower()
    line = plan.Pathway.edge_line()
    assert line == "" or line.startswith("plan.Pathway.as_edge(")
    plan.Catalysis.as_node()
    assert plan.Catalysis.edge_line().startswith("plan.Catalysis.as_edge('")
    assert "as edges Source → Target" in repr(plan.Catalysis)
    table = repr(plan.open)
    assert "as edges" in table and "why it is open" in table and "Source → Target" in table


def test_a_kind_switches_reading_with_the_term_chosen(plan):
    plan.Catalysis.use(plan.target.kinds.thing)  # a class: its records become nodes
    assert plan.Catalysis.reading == "node" and plan.Catalysis.ends == (None, None)
    assert any(o.reading == "edge" for o in plan.Catalysis.options), (
        "the edge reading stays offered"
    )
    plan.Catalysis.use(plan.target.relations.catalyzes)  # a relation: edges between its links
    assert plan.Catalysis.reading == "edge" and plan.Catalysis.target.name == "catalyzes"
    assert [e.name for e in plan.Catalysis.ends] == ["Source", "Target"]
    plan.Catalysis.as_node()
    assert plan.Catalysis.reading == "node"


def test_options_show_every_reading_of_a_kind(plan):
    readings = {o.reading or plan.Catalysis.reading for o in plan.Catalysis.options}
    assert readings == {"edge", "node"}
    table = plan.Catalysis.options_table()
    assert set(table["as"]) == {"edge", "node"}


def test_a_term_that_does_not_fit_is_refused(plan):
    with pytest.raises(ValueError, match="no start and end link"):
        plan.Protein.use(plan.target.relations.catalyzes)


def test_the_plan_runs_as_generated_queries(plan, tmp_path):
    plan.Reaction.leave_out()
    plan.Pathway.use(plan.target.kinds.pathway)
    network = plan.run(tmp_path / "queries")
    written = {p.name for p in network.rules}
    assert {"protein.rq", "metabolite.rq", "catalysis.rq", "is_part_of.rq"} <= written
    assert "reaction.rq" not in written
    counts = network.counts()
    assert counts["kinds"]["protein"] == 2 and counts["kinds"]["small molecule"] == 2
    assert counts["relations"]["catalyzes"] == 1 and counts["relations"]["part of"] >= 4
    assert counts["relations"]["name"] >= 4
    assert "CONSTRUCT" in network.queries.catalysis and "catalysis" in repr(network.queries)
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
        contents=client.kinds.Protein.is_part_of,
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


def test_tested_paths_are_read_for_the_plans_kinds_only_and_give_evidence(plan, tmp_path):
    from rdfsolve.schema_models.navigation import read_tested_paths, tested_step_support

    step = lambda s, p, o: {"subject_class": EX + s, "property_uri": EX + p, "object_class": EX + o}  # noqa: E731
    packed = {
        "format": "edges-1",
        "edges": [
            step("Catalysis", "source", "Protein"),
            step("Protein", "isPartOf", "Pathway"),
            step("Catalysis", "target", "Reaction"),
            step("Other", "x", "Pathway"),
        ],
        "rows": [
            {"edges": [0, 1], "matched": 8, "sources": 10},
            {"edges": [2, 1], "matched": 9, "sources": 10},
            {"edges": [3, 1], "matched": 1, "sources": 1},
        ],
    }
    path = tmp_path / "schema.json"
    path.write_text(json.dumps({"schema": {"navigation": {"strategy": "tested", "paths": packed}}}))
    assert len(read_tested_paths(path)) == 3
    assert len(read_tested_paths(path, start_classes=[EX + "Catalysis"])) == 2
    support = tested_step_support(path, start_classes=[EX + "Catalysis"])
    assert support == {
        (EX + "Catalysis", EX + "source"): (8, 10),
        (EX + "Catalysis", EX + "target"): (9, 10),
    }
    with_paths = plan.add_paths(path)  # into the client; the plan proposes again
    catalysis = with_paths.kinds.Catalysis
    assert with_paths.client.link_support(catalysis.iri, catalysis.source.iri) == 0.8
    assert with_paths.path_support(catalysis, catalysis.target) == 0.9
    assert list(catalysis.paths.frame["follow"]) == [9, 8]
    assert "80%" in repr(catalysis.links), "the links show the share of records that follow them"


def test_a_group_kind_can_be_edges_between_each_pair_of_its_members(plan, tmp_path):
    assert "pairs" in {o.reading for o in plan.Group.options}, (
        "a link to several records offers pairs"
    )
    assert "pairs over Member" in repr(plan.open)
    plan.Group.use(plan.target.relations.related_to)  # a relation: pairs of its members
    assert plan.Group.reading == "pairs" and plan.Group.ends[0].name == "Member"
    assert "3 edges, each pair" in plan.consequence(plan.Group, plan.target.relations.related_to)
    plan.Reaction.leave_out()
    plan.Pathway.use(plan.target.kinds.pathway)
    for p in list(plan.open):
        p.leave_out()
    network = plan.run(tmp_path / "queries")
    assert (tmp_path / "queries" / "group.rq").exists()
    assert network.counts()["relations"]["related to"] >= 3


def test_terms_can_be_named_as_text(plan):
    plan.Pathway.use("Pathway")
    assert plan.Pathway.target.name == "pathway"
    plan.Catalysis.use("RelatedTo")
    assert plan.Catalysis.target.name == "related to"
    assert "CONSTRUCT" in plan.Catalysis.query and "related_to" in repr(plan.Catalysis.query)
    assert "no query until it is decided" in plan.Reaction.query
    with pytest.raises(ValueError, match="no term named 'relatd'"):
        plan.Catalysis.use("relatd")


def test_run_says_where_it_cannot_write(plan, tmp_path):
    blocked = tmp_path / "file"
    blocked.write_text("")
    with pytest.raises(ValueError, match="cannot be written to"):
        plan.run(blocked / "queries")


def test_everything_the_plan_shows_is_also_data(plan, tmp_path):
    import json

    from rdfsolve.plan import NotFoundError, PlanError

    data = plan.to_dict()
    json.dumps(data)  # JSON-safe, for tools and interfaces
    assert data["open"] and data["next"] and data["proposals"][0]["options"]
    opened = next(p for p in data["proposals"] if p["status"] == "choose")
    assert opened["why_open"] and opened["decide"][-1].endswith(".leave_out()")
    json.dumps(plan.target.kinds.to_dict())
    json.dumps(plan.kinds.Catalysis.to_dict())
    with pytest.raises(NotFoundError) as caught:
        plan.Nowhere  # noqa: B018
    assert isinstance(caught.value, AttributeError) and isinstance(caught.value, KeyError)
    assert (
        caught.value.to_dict()["error"]["code"] == "unknown_kind"
        and "Catalysis" in caught.value.choices
    )
    with pytest.raises(PlanError) as wrong:
        plan.Catalysis.as_edge("Nowhere", "Target")
    assert wrong.value.to_dict()["error"]["choices"]
    plan.Reaction.leave_out()
    plan.Pathway.use("pathway")
    plan.Group.leave_out()
    network = plan.run(tmp_path / "queries")
    json.dumps(network.to_dict())
