"""On QLever, the property usage of a class alone is counted per object first, and its distinct
subjects are taken from the class and property. The property usage query names its count
?triples, not ?cnt (Bgee run 11 stopped its outputs with KeyError 'cnt', job 114278)."""

from rdfsolve._outcomes import QueryOutcome
from rdfsolve.evidence.observed import build_property_usage_query
from rdfsolve.mining import property_queries

C, P = "urn:ex:Gene", "urn:ex:expressedIn"


def test_property_usage_takes_distinct_subjects_from_the_class_and_property(monkeypatch):
    answers = iter(
        [
            QueryOutcome([{"class": {"value": C}, "p": {"value": P}, "triples": {"value": "7"}}]),
            QueryOutcome([{"cnt": {"value": "7"}, "subjects": {"value": "3"}}]),
        ]
    )
    monkeypatch.setattr(
        "rdfsolve.mining.query_fallbacks.select_outcome", lambda *args, **kwargs: next(answers)
    )
    outcome = property_queries._subjects_from_total(
        C, P, None, None, build_property_usage_query, "property-usage", helper=None
    )
    assert outcome is not None and outcome.state == "complete"
    assert outcome.rows == [
        {
            "class": {"value": C},
            "p": {"value": P},
            "triples": {"value": "7"},
            "subjects": {"value": "3"},
        }
    ]
