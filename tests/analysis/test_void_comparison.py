"""rdfsolve.analysis.void_comparison: a schema read from a VoID beside a schema mined from the
same data: what both and each state, how shared counts agree, and what each cost."""

from rdfsolve.analysis.void_comparison import compare_void_with_mined
from rdfsolve.schema_models.pattern import SchemaPattern

XSD = "http://www.w3.org/2001/XMLSchema#"


def _p(s, p, o, count=None, datatype=None):
    return SchemaPattern(
        subject_class=s, property_uri=p, object_class=o, count=count, datatype=datatype
    )


def test_both_sides_are_reported_without_a_ground_truth():
    void = [
        _p("urn:A", "urn:link", "urn:B", 100),
        _p("urn:A", "urn:name", "Literal", 50, XSD + "string"),
        _p("urn:A", "urn:only", "urn:C", 5),
    ]
    mined = [
        _p("urn:A", "urn:link", "urn:B", 95),
        _p("urn:A", "urn:name", "Literal", 50, XSD + "string"),
        _p("urn:A", "urn:link", "Resource", 3),
    ]
    result = compare_void_with_mined(
        void,
        mined,
        void_report={"strategy": "void", "total_queries_sent": 40, "phases": [{"duration_s": 2}]},
    )
    assert (result.patterns.both, result.patterns.void_only, result.patterns.mined_only) == (
        2,
        1,
        1,
    )
    assert (result.classes.both, result.classes.void_only) == (2, 1)
    assert result.counts.compared == 2 and result.counts.within_10_percent == 2
    assert result.counts.within_1_percent == 1
    assert result.cost["void"] == {
        "strategy": "void",
        "queries_sent": 40,
        "queries_failed": None,
        "seconds": 2,
        "measurement_gaps": 0,
    }


def test_counts_of_a_sampled_graph_are_not_compared_and_absences_are_not_evidence():
    void = [
        _p("urn:A", "urn:link", "urn:B", 1000),
        _p("urn:A", "urn:name", "Literal", 50, XSD + "string"),
        _p("urn:A", "urn:rare", "urn:C", 1),
    ]
    sampled = _p("urn:A", "urn:link", "urn:B", 10)
    sampled.graphs = {"urn:big": 10}
    complete = _p("urn:A", "urn:name", "Literal", 50, XSD + "string")
    complete.graphs = {"urn:small": 50}
    how = {"urn:big": "Sample: 1 of 100 parts.", "urn:elsewhere": "Sample: not in scope."}
    result = compare_void_with_mined(void, [sampled, complete], sampled_graphs=how)
    assert result.counts.compared == 1 and result.counts.within_1_percent == 1
    assert result.counts.sampled_not_compared == 1
    assert result.sampled_graphs == {"urn:big": "Sample: 1 of 100 parts."}
    assert result.mined_count_basis == "sample"
    assert result.void_only_absence_supported is False
    assert result.patterns.void_only == 1, "stated, but not evidence that the data lacks it"

    unscoped = _p("urn:A", "urn:link", "urn:B", 10)  # no per-graph counts
    result = compare_void_with_mined(
        void, [unscoped], sampled_graphs=how, mined_report={"graph_uris": ["urn:big"]}
    )
    assert result.counts.compared == 0 and result.counts.sampled_not_compared == 1

    full = compare_void_with_mined(void, [sampled, complete])
    assert full.counts.compared == 2 and full.mined_count_basis == "full_data"
    assert full.void_only_absence_supported is True
    outside = compare_void_with_mined(
        void, [complete], sampled_graphs=how, mined_report={"graph_uris": ["urn:small"]}
    )
    assert outside.mined_count_basis == "full_data", "the sampled graph is not in its scope"


def test_the_script_finds_the_sampled_graphs_of_a_source_and_of_its_graph_schemas():
    from pathlib import Path

    from rdfsolve.sources import load_sources
    from scripts.compare_void_first import sampled_graphs_by_name

    registry = Path(__file__).resolve().parents[2] / "data" / "sources.yaml"
    sampled = sampled_graphs_by_name(load_sources(registry))
    assert set(sampled["uniprot"]) == {
        "http://sparql.uniprot.org/uniprot",
        "http://sparql.uniprot.org/uniparc",
        "http://sparql.uniprot.org/uniref",
    }
    assert set(sampled["uniprot.uniparc"]) == {"http://sparql.uniprot.org/uniparc"}
    assert sampled["uniprot.journal"] == {}, "a complete graph of a sampled source"
