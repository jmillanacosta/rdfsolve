"""rdfsolve.reconciliation.checks: the inventory, coverage, cluster checks, name audit,
regression cases and run report of a conversion, on a small graph of any source."""

import pyoxigraph as ox

from rdfsolve.mappings.claims import Claims, claim
from rdfsolve.property_graph import Identity, PropertyGraph
from rdfsolve.reconciliation.checks import (
    IdentityPolicy,
    audit,
    cluster_checks,
    coverage,
    inventory,
    regressions,
    run_report,
)

BL = "https://w3id.org/biolink/vocab/"
IDO = "https://identifiers.org/"
OBO = "http://purl.obolibrary.org/obo/"
CHEM, GENE, PATHWAY, COMPLEX = (BL + k for k in ("ChemicalEntity", "Gene", "Pathway", "Complex"))
HMDB, CAS = IDO + "hmdb/HMDB0001206", IDO + "cas/72-89-9"  # acetyl-CoA, two ways
CITRIC, CITRATE = IDO + "hmdb/HMDB0000094", IDO + "chemspider/29081"  # two forms
ATP = IDO + "kegg.compound/C00002"  # no claim
G1, G2 = IDO + "ncbigene/1351", IDO + "ncbigene/9377"  # two genes, both labelled COX
WP, WP_R = IDO + "wikipathways/WP78", IDO + "wikipathways/WP78_r141553"
LOCAL = "http://example.org/pathway/WP111/Complex/db040"
CPX = IDO + "complexportal/CPX-577"
XREF = "http://www.geneontology.org/formats/oboInOwl#hasDbXref"
POLICY = IdentityPolicy(
    authority={CHEM: "chebi", GENE: "ncbigene", PATHWAY: "wikipathways", COMPLEX: "complexportal"}
)


def statements() -> ox.Dataset:
    rows = [
        (HMDB, CHEM, "Acetyl-CoA"),
        (CAS, CHEM, "Acetyl CoA"),
        (CITRIC, CHEM, "Citrate"),
        (CITRATE, CHEM, "citrate"),
        (ATP, CHEM, "ATP"),
        (IDO + "hmdb/HMDB0000538", CHEM, "ATP"),
        (G1, GENE, "COX"),
        (G2, GENE, "COX"),
        (WP, PATHWAY, "TCA cycle"),
        (WP_R, PATHWAY, "TCA cycle (Krebs)"),
        (LOCAL, COMPLEX, "Complex 1"),
        (CPX, COMPLEX, "Complex 1"),
    ]
    text = "\n".join(f'<{s}> a <{c}> ; <{BL}name> "{n}" .' for s, c, n in rows)
    return ox.Dataset(ox.parse(text.encode(), ox.RdfFormat.TURTLE))


def resolution():
    return Claims(
        [
            claim(HMDB, OBO + "CHEBI_15351", "chebi", XREF),
            claim(CAS, OBO + "CHEBI_15351", "chebi", XREF),
            claim(CITRIC, OBO + "CHEBI_30769", "chebi", XREF),
            claim(CITRATE, OBO + "CHEBI_16947", "chebi", XREF),
            claim(IDO + "hmdb/HMDB0000538", OBO + "CHEBI_15422", "chebi", XREF),
        ]
    ).decide(namespaces=["chebi"], exact=["chebi"])


FORMS = [(OBO + "CHEBI_30769", OBO + "CHEBI_16947")]


def graphs():
    before = PropertyGraph.from_rdf(statements(), identity=Identity())
    after = PropertyGraph.from_rdf(
        statements(), identity=Identity(same=[(WP, WP_R), *resolution().pairs()])
    )
    return before, after


def test_the_inventory_lists_namespaces_versions_and_contextual_nodes():
    table = inventory(statements())
    row = table[(table["class"] == PATHWAY) & (table["namespace"] == "wikipathways")].iloc[0]
    assert row["nodes"] == 2 and row["versioned"] == 1
    chemicals = table[table["class"] == CHEM].set_index("namespace")["nodes"].to_dict()
    assert chemicals == {"cas": 1, "chemspider": 1, "hmdb": 3, "kegg.compound": 1}
    assert "-" in set(table[table["class"] == COMPLEX]["namespace"]), "a contextual node"


def test_coverage_lists_what_reaches_the_authority():
    table = coverage(statements(), POLICY, resolution())
    kegg = table[table["namespace"] == "kegg.compound"].iloc[0]
    assert (kegg["mapped"], kegg["missing"]) == (0, "kegg.compound:C00002")
    hmdb = table[table["namespace"] == "hmdb"].iloc[0]
    assert hmdb["share"] == 1.0
    assert table[table["namespace"] == "-"].iloc[0]["treatment"].startswith("contextual")


def test_cluster_checks_report_merges_and_refusals():
    _, after = graphs()
    table = cluster_checks(after, POLICY)
    assert set(table["status"]) == {"merged"}
    refused = PropertyGraph.from_rdf(
        statements(),
        identity=Identity(same=[(HMDB, OBO + "CHEBI_15351"), (HMDB, OBO + "CHEBI_57288")]),
    )
    # one node with two ChEBI ids decided for it: a conflict
    table = cluster_checks(refused, POLICY)
    assert table[table["status"] == "conflict"]["reason"].str.contains("2 chebi").all()


def test_the_audit_explains_every_shared_name():
    _, after = graphs()
    found = audit(after, POLICY, resolution(), variants=FORMS)
    reasons = found["duplicates"].set_index("name")["explanation"].to_dict()
    assert reasons == {
        "atp": "missing claim",
        "citrate": "variants kept apart",
        "complex 1": "contextual",
        "cox": "different entities",
    }
    assert "acetyl coa" not in reasons, "one node now"
    assert bool(found["passed"]["passed"].iloc[0])
    missing = found["missing_authority"]
    assert set(missing["namespace"]) >= {"kegg.compound", "-"}


def test_regressions_and_the_run_report():
    before, after = graphs()
    cases = {
        "acetyl-CoA": [HMDB, CAS, "chebi:15351"],
        "WP78": [WP, WP_R],
        "citrate": [CITRIC, CITRATE],
    }
    table = regressions(after, cases).set_index("case")["passed"].to_dict()
    assert table == {"acetyl-CoA": True, "WP78": True, "citrate": False}
    joined = PropertyGraph.from_rdf(
        statements(), identity=Identity(same=[*resolution().pairs(), *FORMS], variants=FORMS)
    )
    assert regressions(joined, {"citrate": [CITRIC, CITRATE]})["passed"].all()
    report = run_report(
        before,
        after,
        POLICY,
        covered=coverage(statements(), POLICY, resolution()),
        clusters=cluster_checks(after, POLICY),
        audited=audit(after, POLICY, resolution(), variants=FORMS),
    ).set_index("kind")
    assert report.loc[CHEM, "nodes before"] == 6 and report.loc[CHEM, "nodes after"] == 5
    assert report.loc[PATHWAY, "nodes after"] == 1
    assert report["unexplained duplicates"].sum() == 0
