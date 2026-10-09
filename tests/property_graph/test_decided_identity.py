"""rdfsolve.property_graph identity follows decided mappings to identifiers that no node has, keeps
two names of one namespace apart unless a pair states them directly, and never merges on a name;
to_kgx names a merged node by the identifier its Biolink category puts first."""

import csv
import json

import pyoxigraph as ox

from rdfsolve.conversion import Biolink, to_kgx
from rdfsolve.mappings.claims import Claims, claim
from rdfsolve.property_graph import Identity, PropertyGraph, identity_violations

BL = "https://w3id.org/biolink/vocab/"
IDO = "https://identifiers.org/"
OBO = "http://purl.obolibrary.org/obo/"
P1 = "http://example.org/pathway/1"
P2 = "http://example.org/pathway/2"
HMDB = IDO + "hmdb/HMDB0001206"  # acetyl-CoA, drawn with HMDB in one pathway
CAS = IDO + "cas/72-89-9"  # and with CAS in another
OTHER = IDO + "hmdb/HMDB0001423"  # coenzyme A, drawn with the same name by mistake
CHEBI = OBO + "CHEBI_15351"  # the decided ChEBI class; no pathway draws it
WP78 = IDO + "wikipathways/WP78"
WP78_R = IDO + "wikipathways/WP78_r141553"
XREF = "http://www.geneontology.org/formats/oboInOwl#hasDbXref"
BRIDGEDB = "http://vocabularies.wikipathways.org/wp#bdbChEBI"


def statements() -> ox.Dataset:
    data = f"""
    @prefix bl: <{BL}> .
    <{HMDB}> a bl:ChemicalEntity ; bl:name "Acetyl-CoA" ; bl:participates_in <{P1}> .
    <{CAS}> a bl:ChemicalEntity ; bl:name "Acetyl CoA" ; bl:participates_in <{P2}> .
    <{OTHER}> a bl:ChemicalEntity ; bl:name "Acetyl-CoA" ; bl:participates_in <{P2}> .
    <{WP78}> a bl:Pathway ; bl:name "Citrate cycle" .
    <{WP78_R}> a bl:Pathway ; bl:name "TCA cycle" .
    <{P1}> a bl:Pathway . <{P2}> a bl:Pathway .
    """
    return ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE))


def decision():
    """ChEBI states the HMDB id; BridgeDb states the CAS number; both to ChEBI 15351."""
    return Claims(
        [
            claim(HMDB, CHEBI, "chebi", XREF),
            claim(CAS, IDO + "chebi/CHEBI:15351", "wikipathways", BRIDGEDB),
        ]
    ).decide(namespaces=["chebi"], exact=["chebi"])


def test_nodes_decided_to_one_absent_identifier_are_one_node():
    pg = PropertyGraph.from_rdf(statements(), identity=Identity(same=decision().pairs()))
    report = pg.report()
    assert report["lossless"]["passed"], report["lossless"]
    merged = [n for n in pg.nodes.values() if HMDB in (n.members or [n.id])]
    assert len(merged) == 1 and CAS in merged[0].members
    assert merged[0].identifiers == ["chebi:15351"]
    assert report["identity"]["joined_through_absent"] == 1
    assert OTHER in pg.nodes, "a shared name alone merges nothing"
    edges = {(e.source, e.target) for e in pg.edges if e.type == BL + "participates_in"}
    assert {(merged[0].id, P1), (merged[0].id, P2)} <= edges, "one node keeps both pathways"
    assert pg.to_networkx().nodes[merged[0].id]["decided_ids"] == ["chebi:15351"]


def test_a_direct_pair_of_one_namespace_merges_and_a_chain_does_not():
    """A pathway and its versioned record, paired directly, are one node; two ChEBI ids that
    only a chain joins stay apart, and the refusal says why."""
    direct = PropertyGraph.from_rdf(statements(), identity=Identity(same=[(WP78, WP78_R)]))
    assert direct.report()["lossless"]["passed"]
    assert [n for n in direct.nodes.values() if {WP78, WP78_R} <= set(n.members)]
    assert direct.report()["identity"]["same_namespace_pairs"] == 1

    chained = [(HMDB, CHEBI), (CAS, CHEBI), (CAS, OBO + "CHEBI_57288")]
    pg = PropertyGraph.from_rdf(statements(), identity=Identity(same=chained))
    identity = pg.report()["identity"]
    assert HMDB in pg.nodes and CAS in pg.nodes, "two ChEBI ids in one cluster: not merged"
    assert identity["refused"] == [
        {
            "nodes": sorted([CAS, HMDB]),
            "identifiers": ["chebi:15351", "chebi:57288"],
            "reason": "two identifiers of one namespace, not declared variants",
        }
    ]
    shacl = identity_violations(identity["refused"])
    focus = {q.object.value for q in shacl if q.predicate.value.endswith("#focusNode")}
    assert focus == {CAS, HMDB}
    assert {q.object.value for q in shacl if q.predicate.value.endswith("#conforms")} == {"false"}


def test_a_supporting_direct_claim_makes_the_decided_mapping_exact():
    """ChEBI decides through another identifier (not exact by itself); BridgeDb states the same
    target directly, into a namespace taken as exact: the pair is exact."""
    kegg = IDO + "kegg.compound/C00002"
    found = Claims(
        [
            claim(kegg, OBO + "CHEBI_15422", "chebi", XREF, through=IDO + "hmdb/HMDB0000538"),
            claim(kegg, IDO + "chebi/CHEBI:15422", "wikipathways", BRIDGEDB),
        ]
    ).decide(namespaces=["chebi"], exact=["chebi"])
    assert found.table()["decided by"].tolist() == ["chebi"]
    assert (kegg, IDO + "chebi/CHEBI:15422") in found.pairs()
    alone = Claims(
        [claim(kegg, OBO + "CHEBI_15422", "chebi", XREF, through=IDO + "hmdb/HMDB0000538")]
    ).decide(namespaces=["chebi"], exact=["chebi"])
    assert alone.pairs() == [], "a chain through a cross-reference is not exact"


def test_kgx_names_a_merged_node_by_its_category_first_prefix(tmp_path):
    biolink = Biolink(
        "test",
        {
            "named thing": {"id_prefixes": []},
            "chemical entity": {"is_a": "named thing", "id_prefixes": ["CHEBI", "CAS", "HMDB"]},
            "pathway": {"is_a": "named thing", "id_prefixes": ["WIKIPATHWAYS"]},
            "association": {"is_a": "entity"},
            "entity": {},
        },
        {"name": {}, "participates in": {}},
    )
    pg = PropertyGraph.from_rdf(statements(), identity=Identity(same=decision().pairs()))
    nodes, edges = to_kgx(pg, biolink, tmp_path / "kgx", "infores:test", local_prefix="src")
    rows = {r["id"]: r for r in csv.DictReader(nodes.open(), delimiter="\t")}
    assert rows["CHEBI:15351"]["xref"] == "CAS:72-89-9|HMDB:HMDB0001206"
    assert "HMDB:HMDB0001423" in rows, "the other compound keeps its own node"
    subjects = [r["subject"] for r in csv.DictReader(edges.open(), delimiter="\t")]
    assert subjects.count("CHEBI:15351") == 2
    assert json.loads((tmp_path / "kgx" / "prefixes.json").read_text()) is not None


def test_what_the_issuer_states_directly_comes_before_a_chain():
    """ChEBI lists the HMDB id on one class, and the KEGG id that BridgeDb links to it on
    another form: the direct statement decides; the chain is used only when nothing is direct."""
    kegg = IDO + "kegg.compound/C00031"
    found = Claims(
        [
            claim(HMDB, OBO + "CHEBI_4167", "chebi", XREF),
            claim(HMDB, OBO + "CHEBI_15903", "chebi", XREF, through=kegg),
        ]
    ).decide(namespaces=["chebi"], exact=["chebi"])
    assert found.table()["targets"].tolist() == ["chebi:4167"]
    assert found.table()["outcome"].tolist() == ["accepted"]
    chained = Claims([claim(HMDB, OBO + "CHEBI_15903", "chebi", XREF, through=kegg)]).decide(
        namespaces=["chebi"], exact=["chebi"]
    )
    assert chained.table()["targets"].tolist() == ["chebi:15903"]


def test_two_forms_join_their_nodes_only_when_the_pair_is_given():
    """Citrate is drawn with an HMDB id (decided: citric acid) and with a ChemSpider id (decided:
    citrate(3-)). They stay two nodes; a caller that takes the two forms as one entity gives
    the pair, and they are one node."""
    citrate = IDO + "chemspider/29081"
    data = statements()
    data.add(ox.Quad(ox.NamedNode(citrate), ox.NamedNode(BL + "name"), ox.Literal("Citrate")))
    decided = [(HMDB, OBO + "CHEBI_30769"), (citrate, OBO + "CHEBI_16947")]
    apart = PropertyGraph.from_rdf(data, identity=Identity(same=decided))
    assert HMDB in apart.nodes and citrate in apart.nodes
    forms = [(OBO + "CHEBI_30769", OBO + "CHEBI_16947")]
    joined = PropertyGraph.from_rdf(data, identity=Identity(same=decided + forms))
    assert joined.report()["lossless"]["passed"]
    node = next(n for n in joined.nodes.values() if HMDB in (n.members or [n.id]))
    assert citrate in node.members and node.identifiers == ["chebi:16947", "chebi:30769"]
