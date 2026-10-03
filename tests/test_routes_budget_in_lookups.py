"""The time budget of a link also stops the lookups of a route: a route test waited more than
two hours on the lookups of one link at a remote endpoint (Rhea, job 114413, 2026-09-30),
because the budget was read only between routes. A stopped lookup ends the test of the link
with the stop reason budget, and the routes tested before are kept."""

import pytest
from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings import routes, signatures
from rdfsolve.mappings.routes import check_routes, propose_segments
from tests.test_cross_dataset_routes import GENES, JOIN, PROTEINS, SOURCE, TARGET


def test_a_lookup_stops_before_its_next_query_when_asked():
    target = Dataset().parse(format="turtle", data=TARGET)
    with Client(PROTEINS, target) as client:
        with pytest.raises(signatures.LookupStoppedError):
            signatures._lookup(JOIN, client, ["uniprot:P04637"], {}, stop=lambda: True)
        found = signatures._lookup(JOIN, client, ["uniprot:P04637"], {}, stop=lambda: False)
    assert list(found) == ["uniprot:P04637"]


def test_a_stopped_lookup_ends_the_link_with_the_budget_as_reason(monkeypatch):
    calls = []

    def lookup(*args, stop=None, **options):
        calls.append(stop)
        if len(calls) > 2:
            raise signatures.LookupStoppedError
        return signatures._lookup(*args, stop=stop, **options)

    monkeypatch.setattr(routes, "_lookup", lookup)
    befores, afters = propose_segments(JOIN, GENES, PROTEINS)
    source = Dataset().parse(format="turtle", data=SOURCE)
    target = Dataset().parse(format="turtle", data=TARGET)
    with Client(GENES, source) as s, Client(PROTEINS, target) as t:
        result = check_routes(JOIN, befores, afters, s, t, sample=10)
    assert result.stop_reason == "budget"
    assert all(callable(stop) for stop in calls), "Each lookup can be stopped"
