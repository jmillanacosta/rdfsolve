"""rdfsolve.mappings.routes: a route across two datasets is a path in the first, a link, and a path
in the second, tested on the data. The start instances and their link values are read, the values
are looked up in the second dataset with its path, and the start instances that reach the end are
counted. Every value read gives a confirmed route; a sample gives a tested one."""

import json

import pytest
from rdflib import Dataset

from rdfsolve.analysis import best_route, build_connectivity
from rdfsolve.api import Client
from rdfsolve.mappings import routes, signatures
from rdfsolve.mappings.routes import (
    Route,
    RouteEvidence,
    check_routes,
    federated_query,
    propose_segments,
    write_routes,
)
from rdfsolve.mappings.signatures import Link, LinkEvidence
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from tests.mappings.data import JOIN, UP
from tests.mappings.test_signatures import FLAG_GENES, FLAG_PROTEINS, IN, STATED, flagged

HAS_GENE = SchemaPattern(
    subject_class="urn:Pathway", property_uri="urn:has", object_class="urn:Gene"
)
IN_TAXON = SchemaPattern(
    subject_class="urn:Protein", property_uri="urn:in", object_class="urn:Taxon"
)
GENES = MinedSchema(
    about=AboutMetadata.build(dataset_name="genes"),
    patterns=[
        HAS_GENE,
        SchemaPattern(subject_class="urn:Gene", property_uri="urn:xref", object_class="Resource"),
    ],
)
PROTEINS = MinedSchema(
    about=AboutMetadata.build(dataset_name="proteins"),
    patterns=[
        IN_TAXON,
        SchemaPattern(
            subject_class="urn:Protein", property_uri="urn:binds", object_class="urn:Drug"
        ),
        SchemaPattern(subject_class="urn:Protein", property_uri="urn:name", object_class="Literal"),
    ],
)
SOURCE = f"""
<urn:pw/1> a <urn:Pathway> ; <urn:has> <urn:gene/1>, <urn:gene/2> .
<urn:pw/2> a <urn:Pathway> ; <urn:has> <urn:gene/3> .
<urn:pw/3> a <urn:Pathway> ; <urn:has> <urn:gene/2> .
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P38398> .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}P99999> .
"""
TARGET = f"""
<{UP}P04637> a <urn:Protein> ; <urn:in> <urn:taxon/9606> ; <urn:name> "p53" .
<{UP}P38398> a <urn:Protein> ; <urn:name> "BRCA1" .
<urn:taxon/9606> a <urn:Taxon> .
"""
ENDPOINTS = {"genes": "https://genes.example/sparql", "proteins": "https://proteins.example/sparql"}
SAME = LinkEvidence(JOIN, 3, 2, {UP + "{id}": 2}, [(UP + "P04637", UP + "P04637")], complete=True)
ROUTE = RouteEvidence(Route(JOIN, (HAS_GENE,), (IN_TAXON,)), starts=3, matched=1, complete=True)


def run(link=JOIN, schemas=(GENES, PROTEINS), data=(SOURCE, TARGET), **options):
    befores, afters = propose_segments(link, *schemas)
    with (
        Client(schemas[0], Dataset().parse(format="turtle", data=data[0])) as s,
        Client(schemas[1], Dataset().parse(format="turtle", data=data[1])) as t,
    ):
        return check_routes(link, befores, afters, s, t, **options)


def test_segments_are_the_schema_steps_at_each_end_of_the_link():
    befores, afters = propose_segments(JOIN, GENES, PROTEINS)
    assert [[p.property_uri for p in b] for b in befores] == [[], ["urn:has"]]
    assert [[p.property_uri for p in a] for a in afters] == [[], ["urn:in"], ["urn:binds"]]
    route = Route(JOIN, (HAS_GENE,), ())
    assert (route.start_class, route.end_class) == ("urn:Pathway", "urn:Protein")


def test_a_route_is_counted_by_the_start_instances_that_reach_the_end():
    result = run(sample=None, read_target=True)
    found = {
        (
            tuple(p.property_uri for p in e.route.before),
            tuple(p.property_uri for p in e.route.after),
        ): (e.starts, e.matched)
        for e in result.routes
    }
    assert found == {
        (("urn:has",), ()): (3, 2),  # pathways 1 and 3 have a gene whose protein is found
        ((), ("urn:in",)): (3, 1),  # one gene has a protein with a taxon
        (("urn:has",), ("urn:in",)): (3, 1),  # pathway 1 only
    }, "The link alone is not a route; the route to a drug has no match and is left out"
    assert result.tested == 5 and all(e.complete and e.level == "confirmed" for e in result.routes)
    whole = next(e for e in result.routes if e.route.before and e.route.after)
    assert (whole.route.start_class, whole.route.end_class) == ("urn:Pathway", "urn:Taxon")
    sampled = run(sample=2, read_target=False)
    assert sampled.routes and all(not e.complete and e.level == "tested" for e in sampled.routes)
    assert all(e.starts <= 2 for e in sampled.routes)


def test_the_budget_stops_the_test_and_is_reported(monkeypatch):
    ticks = iter(range(0, 10_000, 50))
    result = run(sample=None, budget_s=60, clock=lambda: next(ticks))
    assert result.stop_reason == "budget" and result.tested < 5
    # A lookup is stopped before its next query, and a stopped lookup ends the link's test.
    with Client(PROTEINS, Dataset().parse(format="turtle", data=TARGET)) as client:
        with pytest.raises(signatures.LookupStoppedError):
            signatures._lookup(JOIN, client, ["uniprot:P04637"], {}, stop=lambda: True)
        assert list(
            signatures._lookup(JOIN, client, ["uniprot:P04637"], {}, stop=lambda: False)
        ) == ["uniprot:P04637"]
    calls = []

    def lookup(*args, stop=None, **options):
        calls.append(stop)
        if len(calls) > 2:
            raise signatures.LookupStoppedError
        return signatures._lookup(*args, stop=stop, **options)

    monkeypatch.setattr(routes, "_lookup", lookup)
    assert run(sample=10).stop_reason == "budget"
    assert all(callable(stop) for stop in calls), "Each lookup can be stopped"


OWL, RDFS = "http://www.w3.org/2002/07/owl#", "http://www.w3.org/2000/01/rdf-schema#"
UBERON = "http://purl.obolibrary.org/obo/UBERON_"
EVENTS = MinedSchema(
    about=AboutMetadata.build(dataset_name="events"),
    patterns=[
        SchemaPattern(
            subject_class="urn:KeyEvent", property_uri="urn:organ", object_class="Resource"
        )
    ],
)
DISEASES = MinedSchema(
    about=AboutMetadata.build(dataset_name="diseases"),
    patterns=[
        SchemaPattern(
            subject_class=OWL + "Class", property_uri=RDFS + "label", object_class="Literal"
        ),
        SchemaPattern(
            subject_class=OWL + "Class",
            property_uri=RDFS + "subClassOf",
            object_class=OWL + "Class",
        ),
        SchemaPattern(
            subject_class=OWL + "Class",
            property_uri="http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            object_class=OWL + "Class",
        ),
    ],
)
EVENT_DATA = f"<urn:ke/1> a <urn:KeyEvent> ; <urn:organ> <{UBERON}0002107> . <urn:ke/2> a <urn:KeyEvent> ; <urn:organ> <{UBERON}0000948> ."
DISEASE_DATA = f"""
@prefix owl: <{OWL}> . @prefix rdfs: <{RDFS}> .
<urn:doid/liver-disease> a owl:Class ; rdfs:label "liver disease" ;
  rdfs:subClassOf [ a owl:Restriction ; owl:onProperty <urn:located_in> ; owl:someValuesFrom <{UBERON}0002107> ] .
[] a owl:Axiom ; owl:annotatedSource <urn:doid/heart-disease> ; owl:annotatedTarget <{UBERON}0000948> .
<urn:doid/heart-disease> a owl:Class ; rdfs:label "heart disease" .
"""


def construct(name, prop):
    return Link(
        "shared",
        "events",
        "urn:KeyEvent",
        "urn:organ",
        "uberon",
        "diseases",
        OWL + name,
        OWL + prop,
    )


def test_a_route_to_an_owl_construct_ends_at_the_term_it_describes():
    """A Restriction or an Axiom holds the identifier as a value; the route ends at the class it
    describes. A path of rdf:type steps is not a relation and is not proposed."""
    _, afters = propose_segments(construct("Restriction", "someValuesFrom"), EVENTS, DISEASES)
    assert [[p.property_uri for p in a] for a in afters] == [[], [RDFS + "subClassOf"]]
    for name, prop in (("Restriction", "someValuesFrom"), ("Axiom", "annotatedTarget")):
        result = run(
            construct(name, prop), (EVENTS, DISEASES), (EVENT_DATA, DISEASE_DATA), sample=None
        )
        (resolved,) = [e for e in result.routes if not e.route.after]
        assert (resolved.starts, resolved.matched) == (2, 1)
        assert resolved.route.end_class == OWL + "Class" and resolved.route.resolved == name
    link = construct("Restriction", "someValuesFrom")
    evidence = LinkEvidence(
        link, 2, 1, {UBERON + "{id}": 1}, [(UBERON + "0002107", UBERON + "0002107")]
    )
    query = federated_query(
        Route(link), evidence, {"events": "https://e/sparql", "diseases": "https://d/sparql"}
    )
    assert f"?e <{RDFS}subClassOf> ?x ." in query and f"?x <{OWL}someValuesFrom> ?t ." in query


def test_the_federated_query_has_both_endpoints_and_the_rewrite():
    same = federated_query(ROUTE.route, SAME, ENDPOINTS)
    assert (
        "SERVICE <https://genes.example/sparql>" in same
        and "SERVICE <https://proteins.example/sparql>" in same
    )
    assert "?n1 <urn:xref> ?t ." in same and "BIND" not in same, "The same IRI on both sides"
    assert "?t <urn:in> ?m1 . ?m1 a <urn:Taxon>" in same and "not executed" in same
    rewritten = LinkEvidence(
        JOIN, 3, 2, {UP + "{id}": 2}, [("https://identifiers.org/uniprot:P04637", UP + "P04637")]
    )
    assert (
        'BIND(IRI(CONCAT("http://purl.uniprot.org/uniprot/", '
        'STRAFTER(STR(?v), "https://identifiers.org/uniprot:"))) AS ?t)'
        in federated_query(ROUTE.route, rewritten, ENDPOINTS)
    )
    assert federated_query(ROUTE.route, SAME, {"genes": ENDPOINTS["genes"]}) is None


def test_the_file_holds_the_matched_routes_and_the_counts_of_the_test(tmp_path):
    path = tmp_path / "routes.json"
    write_routes(
        path, [ROUTE], links=[SAME], endpoints=ENDPOINTS, tested=5, stop_reason=None, failed=[]
    )
    written = json.loads(path.read_text())
    assert written["format"] == "routes-1" and (written["tested"], written["matched"]) == (5, 1)
    (row,) = written["routes"]
    assert (row["source"], row["start_class"], row["target"], row["end_class"]) == (
        "genes",
        "urn:Pathway",
        "proteins",
        "urn:Taxon",
    )
    assert row["before"] == [["urn:Pathway", "urn:has", "urn:Gene"]] and row["after"] == [
        ["urn:Protein", "urn:in", "urn:Taxon"]
    ]
    assert row["link"]["property"] == "urn:xref" and row["link"]["identifier_type"] == "uniprot"
    assert (row["start_instances"], row["matched_instances"]) == (3, 1)
    assert row["evidence"] == "confirmed" and "SERVICE" in row["federated_query"]


def test_a_tested_route_is_one_confirmed_edge_and_one_without_flags_is_taken_first():
    graph = build_connectivity({"genes": GENES, "proteins": PROTEINS}, links=[SAME], routes=[ROUTE])
    found = best_route(graph, ("genes", "urn:Pathway"), ("proteins", "urn:Taxon"))
    assert [e["kind"] for e in found["edges"]] == ["route"] and not found["composed"]
    assert found["evidence"] == "confirmed" and found["edges"][0]["matched"] == 1
    composed = best_route(
        build_connectivity({"genes": GENES, "proteins": PROTEINS}, links=[SAME]),
        ("genes", "urn:Pathway"),
        ("proteins", "urn:Taxon"),
    )
    assert composed["composed"] and composed["evidence"] == "plausible"
    # A route over an identity link with flagged identifiers carries the flags; one without is preferred.
    stated, referred = (
        flagged(STATED),
        flagged(
            Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")
        ),
    )
    schemas = {"genes": FLAG_GENES, "proteins": FLAG_PROTEINS}
    flagged_route = RouteEvidence(Route(STATED, (), (IN,)), 2, 1, True, flags=stated.flags)
    clean_route = RouteEvidence(Route(referred.link, (), (IN,)), 2, 1, True)
    ends = ("genes", "urn:Gene"), ("proteins", "urn:Taxon")
    only = build_connectivity(schemas, links=[stated], routes=[flagged_route])
    found = best_route(only, *ends)
    assert found["flagged"] and found["edges"][0]["flags"] == {"namespace": 2}
    assert found["evidence"] == "confirmed", "The join is on the data; the flag is about the claim"
    link_edge = best_route(only, ("genes", "urn:Gene"), ("proteins", "urn:Protein"))
    assert link_edge["flagged"] and link_edge["edges"][0]["kind"] == "verified_link"
    taken = best_route(
        build_connectivity(schemas, links=[stated, referred], routes=[flagged_route, clean_route]),
        *ends,
    )
    assert not taken["flagged"] and taken["edges"][0]["predicate"] == "urn:xref"
