"""Ontology terms grouped under one representative are counted once per edge: a record typed
with two member terms of a group gives its edges once, in the count queries of the grouped
terms (before mining) and in the recount of the rows that grouping after mining merges. The
queries of plain classes are unchanged."""

from __future__ import annotations

import pytest
from rdflib import Graph

from rdfsolve.evidence.observed import (
    build_literal_profile_query,
    build_node_kind_query,
    build_property_usage_query,
)
from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining import query_builders as qb
from rdfsolve.mining.grouped_recount import group_members
from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_as_data import build_term_object_pair_query
from rdfsolve.mining.query_builders import Representative, Window
from rdfsolve.sparql_helper import EndpointError

EX = "urn:ex:"
DATA = """@prefix ex: <urn:ex:> .
ex:s1 a ex:A , ex:B ; ex:p ex:o1 ; ex:name "one" ; ex:link ex:untyped ; ex:node [ ex:x "y" ] .
ex:s2 a ex:A ; ex:p ex:o1 ; ex:name "two" .
ex:o1 a ex:C , ex:D .
ex:k a ex:K ; ex:p ex:o1 .
"""
GROUP = Representative(EX + "G", [EX + "A", EX + "B"])
COUNT_BUILDERS = [
    qb._build_batched_typed_count_query,
    qb._build_batched_literal_count_query,
    qb._build_batched_untyped_count_query,
    qb._build_batched_blank_node_count_query,
]


def _select(backend, query):
    helper = LocalGraphHelper("urn:test", Graph().parse(data=DATA, format="turtle"), backend=backend)
    return helper.select(query)["results"]["bindings"]


def _numbers(rows, *names):
    return [tuple(int(row[n]["value"]) for n in names) for row in rows]


@pytest.mark.parametrize("backend", ["oxigraph", "rdflib"])
def test_a_record_typed_with_two_members_counts_once(backend):
    p = EX + "p"
    typed = _select(backend, qb._build_batched_typed_count_query([GROUP], None, property_uri=p))
    assert _numbers(typed, "cnt", "subjects", "objects") == [(2, 2, 1)] * 2  # oc C and D
    lighter = qb._build_batched_typed_count_query([GROUP], None, property_uri=p, subjects=False)
    assert _numbers(_select(backend, lighter), "cnt", "objects") == [(2, 1)] * 2
    literal = qb._build_batched_literal_count_query([GROUP], None, property_uri=EX + "name")
    assert _numbers(_select(backend, literal), "cnt", "subjects") == [(2, 2)]
    literal = qb._build_batched_literal_count_query(
        [GROUP], None, property_uri=EX + "name", subjects=False
    )
    assert _numbers(_select(backend, literal), "cnt") == [(2,)]
    untyped = qb._build_batched_untyped_count_query([GROUP], None, property_uri=EX + "link")
    assert _numbers(_select(backend, untyped), "cnt", "subjects", "objects") == [(1, 1, 1)]
    blank = qb._build_batched_blank_node_count_query([GROUP], None, property_uri=EX + "node")
    assert _numbers(_select(backend, blank), "cnt", "subjects", "objects") == [(1, 1, 1)]
    total = qb._build_class_property_total_query([GROUP], p, None)
    assert _numbers(_select(backend, total), "cnt", "subjects") == [(2, 2)]
    # every property at once (?p unbound), paged as the miner pages it
    paged = qb._build_batched_literal_count_query([GROUP], None, paginated=True)
    rows = _select(backend, paged.format(offset=0, limit=100))
    assert _numbers(rows, "cnt") == [(2,)] and rows[0]["p"]["value"] == EX + "name"


@pytest.mark.parametrize("backend", ["oxigraph", "rdflib"])
def test_a_sample_of_a_group_gives_each_edge_once(backend):
    query = qb._build_batched_literal_count_query(
        [GROUP], None, property_uri=EX + "name", sample=10
    )
    assert _numbers(_select(backend, query), "cnt", "subjects") == [(2, 2)]


@pytest.mark.parametrize("backend", ["oxigraph", "rdflib"])
def test_property_evidence_of_a_group_counts_each_edge_once(backend):
    usage = _select(backend, build_property_usage_query([GROUP], None, property_uri=EX + "p"))
    assert _numbers(usage, "triples", "subjects", "objects") == [(2, 2, 1)]
    kinds = _select(backend, build_node_kind_query([GROUP], None, property_uri=EX + "name"))
    assert _numbers(kinds, "values") == [(2,)]
    profile = build_literal_profile_query([GROUP], None, property_uri=EX + "name")
    assert _numbers(_select(backend, profile), "values") == [(2,)]
    every = {r["p"]["value"]: r for r in _select(backend, build_property_usage_query([GROUP], None))}
    assert int(every[EX + "p"]["triples"]["value"]) == 2


def test_the_term_values_of_a_group_count_each_edge_once():
    data = DATA + (
        "ex:s1 ex:about ex:T1 . ex:s2 ex:about ex:T1 . "
        "ex:T1 a <http://www.w3.org/2002/07/owl#Class> .\n"
    )
    graph = Graph().parse(data=data, format="turtle")
    helper = LocalGraphHelper("urn:test", graph)
    query = build_term_object_pair_query(GROUP, EX + "about", None)
    rows = helper.select(query)["results"]["bindings"]
    assert _numbers(rows, "n") == [(2,)]


@pytest.mark.parametrize("backend", ["oxigraph", "rdflib"])
def test_objects_typed_with_two_members_of_an_object_group_count_once(backend):
    groups = {EX + "H": [EX + "C", EX + "D"]}
    for subject, expected in ((GROUP, (2, 2, 1)), (EX + "K", (1, 1, 1))):
        query = qb._build_grouped_object_count_query([subject], None, EX + "p", groups)
        rows = _select(backend, query)
        assert _numbers(rows, "cnt", "subjects", "objects") == [expected]
        assert rows[0]["oc"]["value"] == EX + "H" and rows[0]["class"]["value"] == str(subject)


def test_queries_of_plain_classes_are_unchanged():
    """Only a group's queries make its edges distinct; a plain class or batch keeps its query."""
    for builder in COUNT_BUILDERS:
        for classes in ([EX + "A"], [EX + "A", EX + "K"]):
            for options in ({}, {"property_uri": EX + "p"}, {"paginated": True}):
                query = builder(classes, ["urn:g"], **options)
                assert "SELECT DISTINCT ?class ?s" not in query
                assert "SELECT DISTINCT ?class ?s" in builder([GROUP], ["urn:g"], **options)
    query = qb._build_class_property_total_query([EX + "A"], EX + "p", None)
    assert "SELECT DISTINCT" not in query
    plain = qb._build_batched_literal_count_query([EX + "A"], None, property_uri=EX + "p", sample=5)
    assert "SELECT ?s ?o" in plain and "SELECT DISTINCT ?s ?o" in (
        qb._build_batched_literal_count_query([GROUP], None, property_uri=EX + "p", sample=5)
    )


def test_a_window_of_a_group_holds_its_member_terms():
    query = qb._build_batched_literal_query([GROUP], None, window=Window(10, 20))
    assert "SELECT ?s ?_member ?class WHERE { VALUES (?_member ?class)" in query
    assert "LIMIT 10 OFFSET 20" in query
    rows = _select("oxigraph", qb._build_batched_literal_query([GROUP], None, window=Window(10)))
    assert {r["class"]["value"] for r in rows} == {EX + "G"}
    plain = qb._build_batched_literal_query([EX + "A"], None, window=Window(10))
    assert "{ SELECT ?s WHERE { ?s a <urn:ex:A> . } LIMIT 10 OFFSET 0 }" in plain


def test_group_members_follow_groups_before_and_after_mining():
    before = {"urn:G": ["urn:a", "urn:b"]}
    after = {"urn:G": "urn:R", "urn:c": "urn:R", "urn:d": "urn:d"}
    assert group_members(before, after) == {
        "urn:G": frozenset({"urn:G", "urn:a", "urn:b"}),
        "urn:R": frozenset({"urn:R", "urn:G", "urn:a", "urn:b", "urn:c"}),
    }
    assert group_members({}, None) == {}


HIERARCHY = """@prefix ex: <urn:ex:> . @prefix t: <urn:term:> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
t:parent a owl:Class . t:a a owl:Class ; rdfs:subClassOf t:parent .
t:b a owl:Class ; rdfs:subClassOf t:parent .
ex:s1 a t:a , t:b ; ex:mass "1" ; ex:ref ex:r1 .
ex:s2 a t:a ; ex:mass "2" ; ex:ref ex:r1 .
ex:r1 a ex:Record .
"""
PARENT = "urn:term:parent"


def _mine(**options):
    graph = Graph().parse(data=HIERARCHY, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        result = mine_with_ontology(
            miner, dataset_name="grouped", ontology_as_data=True, ontology_term_budget=1, **options
        )
        return result.data_schema, miner.last_report


def _rows(patterns):
    return {(p.subject_class, p.property_uri, p.object_class): p for p in patterns}


@pytest.mark.parametrize("before", [True, False], ids=["grouped-before", "merged-after"])
def test_a_grouped_row_is_exact(before):
    """Grouped before mining, the group's queries count each edge once; merged after mining,
    the summed rows (3 edges for 2) are counted again over the member terms."""
    schema, report = _mine(ontology_group_before_mining=1 if before else None)
    rows = _rows(schema.patterns)
    for key, objects in (((PARENT, EX + "mass", "Literal"), 2), ((PARENT, EX + "ref", EX + "Record"), 1)):
        row = rows[key]
        assert (row.count, row.count_semantics, row.distinct_subjects, row.distinct_objects) == (
            2,
            "endpoint_default",
            2,
            objects,
        ), key
        assert row.evidence_source == "inferred"
    recount = report.config["ontology_term_subsumption"]["recount"]
    if before:
        assert recount["merged_rows"] == 0
    else:
        assert recount["recounted"] == recount["merged_rows"] >= 2
        raw = _rows(schema.raw_patterns)
        assert raw["urn:term:a", EX + "mass", "Literal"].count == 2
        assert raw["urn:term:b", EX + "mass", "Literal"].count == 1


def test_a_refused_recount_keeps_the_upper_bound(monkeypatch):
    select = LocalGraphHelper.select

    def refuse(helper, query, purpose="", **options):
        if purpose.startswith("ontology-terms/recount"):
            raise EndpointError("HTTP 500: refused")
        return select(helper, query, purpose=purpose, **options)

    monkeypatch.setattr(LocalGraphHelper, "select", refuse)
    schema, report = _mine()
    row = _rows(schema.patterns)[PARENT, EX + "mass", "Literal"]
    assert (row.count, row.count_semantics, row.distinct_subjects) == (3, "upper_bound", None)
    recount = report.config["ontology_term_subsumption"]["recount"]
    assert recount["recounted"] == 0 and recount["kept_upper_bound"] == recount["merged_rows"]
    assert any(g.purpose.startswith("ontology-terms/recount") for g in report.measurement_gaps)
