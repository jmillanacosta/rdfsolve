"""When the structural discovery of one property is refused on QLever, the property is recorded
as not discovered and the source is partial; the other properties are still discovered
(UberGraph: the discovery of IAO_0000115 reached the time limit, its query cannot be paged, and
the whole source failed after 2 h 13 min, 2026-09-30)."""

from types import SimpleNamespace

from rdfsolve.mining import structural_strategy
from rdfsolve.sparql_helper import EndpointTimeoutError


def test_a_refused_property_does_not_fail_the_source(monkeypatch):
    outcomes = []
    context = SimpleNamespace(report=SimpleNamespace(record_outcome=outcomes.append))

    def select(context, query, purpose, **options):
        if "<urn:p:slow>" in query:
            raise EndpointTimeoutError("Operation timed out")
        return [{"p": {"value": "urn:p:fast"}}]

    monkeypatch.setattr(structural_strategy, "_select", select)
    entry = {
        "census_properties": {
            "urn:p:slow": {"uncoveredTriples": 5},
            "urn:p:fast": {"uncoveredTriples": 2},
            "urn:p:covered": {"uncoveredTriples": 0},
        }
    }
    rows = structural_strategy._patterns_discovery(context, None, [], entry)
    assert rows == [{"p": {"value": "urn:p:fast"}}]
    assert "discovery_refused" in entry["census_properties"]["urn:p:slow"]
    assert len(outcomes) == 1 and outcomes[0].state == "partial"
    assert "urn:p:slow" in outcomes[0].failures[0].message


def test_triples_of_refused_properties_are_counted_as_not_discovered():
    entry = {
        "census_properties": {
            "urn:p:slow": {"uncoveredTriples": 5, "discovery_refused": "timed out"},
            "urn:p:fast": {"uncoveredTriples": 2},
        }
    }
    assert structural_strategy._undiscovered_triples(entry) == 5
    assert structural_strategy._undiscovered_triples({}) == 0
