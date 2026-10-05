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
