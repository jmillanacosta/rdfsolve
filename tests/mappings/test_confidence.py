"""The confidence a mapping is given, the edge it makes, and how it was made."""

import pytest

from rdfsolve.mappings import enrichment
from rdfsolve.mappings.confidence import (
    DEFAULTS,
    ConfidencePolicy,
    agent_type,
    combined,
    edge_type,
    knowledge_level,
)


def test_a_row_keeps_its_confidence_else_its_set_or_justification_default():
    policy = ConfidencePolicy()
    assert policy.of(0.4, "semapv:LexicalMatching") == 0.4
    assert policy.of(None, "semapv:LexicalMatching") == DEFAULTS["LexicalMatching"] == 0.7
    assert policy.of(None, "semapv:ManualMappingCuration") == 0.95
    assert policy.of(None, "semapv:UnspecifiedMatching") == 0.6
    assert policy.of(None, "semapv:UnspecifiedMatching", set_confidence=0.95) == 0.95
    assert policy.of(None, "semapv:MappingChaining", set_confidence=0.95) == 0.85
    assert policy.of(1.0, "semapv:MappingChaining", ignore_confidence=True) == 0.85


def test_defaults_can_be_set_and_are_checked():
    policy = ConfidencePolicy(by_justification={"LexicalMatching": 0.5}, unspecified=0.3)
    assert policy.of(None, "semapv:LexicalMatching") == 0.5
    assert policy.of(None, "semapv:ManualMappingCuration") == 0.3
    with pytest.raises(ValueError):
        ConfidencePolicy(unspecified=1.5)


def test_predicates_and_justifications_type_the_edge():
    assert edge_type("skos:exactMatch") == edge_type("owl:equivalentClass") == "EQUIVALENT"
    assert edge_type("semapv:crossSpeciesExactMatch") == "CROSS_SPECIES"
    assert edge_type("oboInOwl:hasDbXref") == "XREF"
    assert edge_type("RO:0100001") == "RELATED"
    assert edge_type("rdfs:subClassOf") is None
    assert knowledge_level("semapv:ManualMappingCuration") == "knowledge_assertion"
    assert knowledge_level("semapv:LexicalMatching") == "prediction"
    assert knowledge_level("semapv:MappingChaining") == "logical_entailment"
    assert agent_type("semapv:ManualMappingCuration") == "manual_agent"
    assert knowledge_level("semapv:UnspecifiedMatching") == "not_provided"


def test_independent_sets_combine_as_a_noisy_or():
    assert combined([0.5, 0.5]) == pytest.approx(0.75)
    assert combined([]) == 0.0


def test_enrichment_writes_effective_confidence_and_leaves_out_hierarchy(tmp_path):
    from collections import Counter

    tsv = tmp_path / "set.sssom.tsv"
    tsv.write_text(
        "#curie_map:\n#  A: http://example.org/a/\n#mapping_set_id: https://example.org/set\n"
        "subject_id\tpredicate_id\tobject_id\tmapping_justification\tconfidence\n"
        "A:1\tskos:exactMatch\tA:9\tsemapv:LexicalMatching\t\n"
        "A:2\tskos:exactMatch\tA:9\tsemapv:UnspecifiedMatching\t1.0\n"
        "A:3\trdfs:subClassOf\tA:9\tsemapv:UnspecifiedMatching\t\n"
        "A:4\tskos:exactMatch\tA:9\t\t\n"
    )
    observed = {f"http://example.org/a/{n}": "one" for n in (1, 2, 3, 4)}
    skipped: Counter[str] = Counter()
    msdf = enrichment.enrich_sssom_file(
        tsv, observed, {}, set_confidence=0.95, ignore_confidence=True, skipped=skipped
    )
    assert msdf is not None
    rows = msdf.df.set_index("subject_id")
    assert rows.loc["A:1", "confidence"] == pytest.approx(0.7)
    assert rows.loc["A:2", "confidence"] == pytest.approx(0.95)
    assert "source_confidence=1.0" in rows.loc["A:2", "other"]
    assert skipped == {"hierarchy predicate": 1, "no justification": 1}
