"""Grouping before mining at GO-CAM scale: ontology terms are grouped under their ancestors; the
terms without a parent (gene-product IRIs used as types) are grouped by namespace when there are
too many to read their shapes through an endpoint; every term stays on record as a member, and a
namespace group too large to name in a query is mined over a spread of its members, marked
sampled."""

from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import Mock

from rdfsolve.mining import ontology_as_data
from rdfsolve.mining.ontology_as_data import (
    choose_representatives,
    group_by_namespace,
    namespace_group_iri,
    spread,
)
from rdfsolve.mining.query_builders import Representative
from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

GO = "http://purl.obolibrary.org/obo/GO_"
UNIPROT = "http://identifiers.org/uniprot/"
MGI = "http://identifiers.org/mgi/MGI:"
ROOT = GO + "0008150"


def _classes() -> list[str]:
    """GO terms under one root, and gene products of two namespaces with no parent."""
    go = [f"{GO}{i:07d}" for i in range(1, 41)]
    uniprot = [f"{UNIPROT}P{i:05d}" for i in range(300)]
    mgi = [f"{MGI}{i}" for i in range(150)]
    return go + uniprot + mgi


class _Endpoint:
    """Answers rdfs:subClassOf: each GO term has the root as parent; nothing else is asked."""

    sparql_engine = "blazegraph"

    def __init__(self):
        self.purposes = []

    def select(self, query, purpose=""):
        self.purposes.append(purpose)
        assert purpose == "ontology-terms/superclasses", "No shape is read"
        terms = re.findall(r"<([^>]+)>", query.split("VALUES", 1)[1].split("}", 1)[0])
        rows = [
            {"c": {"value": t}, "parent": {"value": ROOT}}
            for t in terms
            if t.startswith(GO) and t != ROOT
        ]
        return {"results": {"bindings": rows}}


def _context(endpoint):
    report = Mock()
    report.report.config = {}
    return SimpleNamespace(
        ontology_term_budget=5,
        group_before_mining=10,
        report=report,
        helper=endpoint,
        ontology_graph_uris=None,
        ontology_hierarchy_files=[],
        graph_uris=None,
        grouped_members={},
    )


def test_terms_without_a_parent_are_grouped_by_namespace_when_too_many(monkeypatch):
    monkeypatch.setattr(ontology_as_data, "SHAPE_READ_MAX_TERMS", 100)
    monkeypatch.setattr(ontology_as_data, "NAMESPACE_GROUP_MINED_MEMBERS", 200)
    endpoint = _Endpoint()
    context = _context(endpoint)
    classes = _classes()
    grouped = TwoPhaseStrategy()._group_terms(classes, context)
    record = context.report.report.config["ontology_term_grouping"]
    assert record["grouping_of_terms_without_parent"] == "namespace"
    groups = record["namespace_groups"]
    assert {g["bioregistry_prefix"] for g in groups.values()} == {"uniprot", "mgi"}
    uniprot = namespace_group_iri("bioregistry:uniprot")
    assert groups[uniprot]["terms"] == 300 and groups[uniprot]["mined_members"] == 200
    # Every class is on record as a member of its group or as a class of its own.
    members = record["representative_members"]
    on_record = {t for terms in members.values() for t in terms} | {str(c) for c in grouped}
    assert set(classes) <= on_record
    assert sorted(members[uniprot]) == sorted(c for c in classes if c.startswith(UNIPROT))
    # The GO terms are lifted to their root; the root stands for them.
    assert ROOT in members and len(members[ROOT]) == 40
    mined = {str(c): c for c in grouped}
    assert len(mined[uniprot].members) == 200, "A spread of the members is named in queries"
    assert len(mined[namespace_group_iri("bioregistry:mgi")].members) == 150
    (outcome,) = [call.args[0] for call in context.report.record_outcome.call_args_list]
    (failure,) = outcome.failures
    assert failure.category == "sampled" and failure.classes == [uniprot]
    assert set(endpoint.purposes) == {"ontology-terms/superclasses"}


def test_few_terms_without_a_parent_are_still_grouped_by_shape(monkeypatch):
    calls = []

    def shapes(helper, terms, **kwargs):
        calls.append(list(terms))
        return {}, []

    monkeypatch.setattr(ontology_as_data, "fetch_shapes", shapes)
    context = _context(_Endpoint())
    TwoPhaseStrategy()._group_terms(_classes(), context)
    record = context.report.report.config["ontology_term_grouping"]
    assert record["grouping_of_terms_without_parent"] == "shape" and calls
    assert "namespace_groups" not in record


def test_namespaces_with_one_bioregistry_prefix_are_one_group():
    terms = [f"{UNIPROT}P{i}" for i in range(120)] + [
        f"http://purl.uniprot.org/uniprot/Q{i}" for i in range(120)
    ]
    chosen = choose_representatives(terms, {}, 1)
    groups = group_by_namespace(chosen, {})
    (group,) = groups.values()
    assert group["bioregistry_prefix"] == "uniprot" and group["terms"] == 240
    assert len(group["namespaces"]) == 2


def test_a_namespace_unknown_to_bioregistry_is_grouped_by_its_namespace():
    terms = [f"JaponicusDB:SPAC{i}.01" for i in range(150)]
    chosen = choose_representatives(terms, {}, 1)
    (group,) = group_by_namespace(chosen, {}).values()
    assert group["key"] == "JaponicusDB:" and group["bioregistry_prefix"] is None


def test_a_spread_is_even_and_stable():
    terms = [f"urn:t{i:04d}" for i in range(1000)]
    picked = spread(list(reversed(terms)), 10)
    assert picked == [f"urn:t{i:04d}" for i in range(0, 1000, 100)]
    assert spread(terms[:3], 10) == terms[:3]
    assert isinstance(Representative("urn:g", picked), str)
