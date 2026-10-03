"""The tested routes across datasets are written as one file; a tested route is one edge of the
connectivity graph, so a route search takes it before a composition of plausible edges. For each
route a federated query is generated from the record; it is a view and is not executed."""

import json

from rdfsolve.analysis import best_route, build_connectivity
from rdfsolve.mappings.routes import Route, RouteEvidence, federated_query, write_routes
from rdfsolve.mappings.signatures import LinkEvidence
from tests.test_cross_dataset_routes import GENES, HAS_GENE, IN_TAXON, JOIN, PROTEINS, UP

ROUTE = RouteEvidence(Route(JOIN, (HAS_GENE,), (IN_TAXON,)), starts=3, matched=1, complete=True)
SAME = LinkEvidence(JOIN, 3, 2, {UP + "{id}": 2}, [(UP + "P04637", UP + "P04637")], complete=True)
REWRITTEN = LinkEvidence(
    JOIN, 3, 2, {UP + "{id}": 2}, [("https://identifiers.org/uniprot:P04637", UP + "P04637")]
)
ENDPOINTS = {"genes": "https://genes.example/sparql", "proteins": "https://proteins.example/sparql"}


def test_a_tested_route_is_one_confirmed_edge():
    graph = build_connectivity({"genes": GENES, "proteins": PROTEINS}, links=[SAME], routes=[ROUTE])
    found = best_route(graph, ("genes", "urn:Pathway"), ("proteins", "urn:Taxon"))
    assert [e["kind"] for e in found["edges"]] == ["route"] and not found["composed"]
    assert found["evidence"] == "confirmed" and found["edges"][0]["matched"] == 1
    without = build_connectivity({"genes": GENES, "proteins": PROTEINS}, links=[SAME])
    composed = best_route(without, ("genes", "urn:Pathway"), ("proteins", "urn:Taxon"))
    assert composed["composed"] and composed["evidence"] == "plausible"


def test_the_federated_query_has_both_endpoints_and_the_rewrite():
    same = federated_query(ROUTE.route, SAME, ENDPOINTS)
    assert "SERVICE <https://genes.example/sparql>" in same
    assert "SERVICE <https://proteins.example/sparql>" in same
    assert "?n1 <urn:xref> ?t ." in same and "BIND" not in same, "The same IRI on both sides"
    assert "?t <urn:in> ?m1 . ?m1 a <urn:Taxon>" in same and "not executed" in same
    rewritten = federated_query(ROUTE.route, REWRITTEN, ENDPOINTS)
    assert (
        'BIND(IRI(CONCAT("http://purl.uniprot.org/uniprot/", '
        'STRAFTER(STR(?v), "https://identifiers.org/uniprot:"))) AS ?t)' in rewritten
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
    assert (row["source"], row["start_class"]) == ("genes", "urn:Pathway")
    assert (row["target"], row["end_class"]) == ("proteins", "urn:Taxon")
    assert row["before"] == [["urn:Pathway", "urn:has", "urn:Gene"]]
    assert row["link"]["property"] == "urn:xref" and row["link"]["identifier_type"] == "uniprot"
    assert row["after"] == [["urn:Protein", "urn:in", "urn:Taxon"]]
    assert (row["start_instances"], row["matched_instances"]) == (3, 1)
    assert row["evidence"] == "confirmed" and "SERVICE" in row["federated_query"]
