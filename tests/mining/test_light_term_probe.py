"""The ontology-term probe of a VoID-first source is a light step: each query has the light
limit, and a page that does not answer in time is not asked again in smaller pages (UniProt, job
115330: four paged attempts of ontology-terms/object at the 330 s deadline, 1337 s, nothing
found); the source keeps its patterns and records the probe as not probed."""

from unittest.mock import Mock

import pytest
from rdflib import Dataset

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper

DATA = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<urn:T1> a owl:Class . <urn:T2> a owl:Class .
<urn:a> a <urn:A> ; <urn:about> <urn:T1> .
<urn:b> a <urn:A> ; <urn:about> <urn:T2> .
"""


def test_a_page_of_a_probe_is_not_asked_again_in_smaller_pages(monkeypatch):
    monkeypatch.setattr("rdfsolve.sparql_helper.time.sleep", lambda seconds: None)
    with SparqlHelper("https://example.org/sparql", max_retries=1) as helper:
        select = Mock(side_effect=EndpointTimeoutError("deadline of 60 s"))
        monkeypatch.setattr(helper, "select", select)
        template = SparqlHelper.prepare_paginated_query("SELECT ?s WHERE { ?s ?p ?o }")
        with helper.budget(60), pytest.raises(EndpointTimeoutError):
            list(
                helper.select_chunked(template, chunk_size=10_000, purpose="ontology-terms/object")
            )
        assert select.call_count == 1
        select.reset_mock()
        with pytest.raises(EndpointTimeoutError):
            list(helper.select_chunked(template, chunk_size=8, purpose="ontology-terms/object"))
        assert select.call_count > 1, "Outside a probe, pages are made smaller"


def test_the_term_probe_of_a_void_first_source_gives_up_at_the_first_timeout(monkeypatch):
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        miner.light_probe_seconds = 60
        seen = []
        budget = Mock()
        budget.return_value.__enter__ = Mock(side_effect=lambda: seen.append("light"))
        budget.return_value.__exit__ = Mock(return_value=False)
        monkeypatch.setattr(miner.helper, "budget", budget, raising=False)

        def refuse(*args, **kwargs):
            raise EndpointTimeoutError("deadline of 60 s")

        monkeypatch.setattr("rdfsolve.mining.ontology_as_data.probe_term_patterns", refuse)
        result = mine_with_ontology(
            miner, dataset_name="light", ontology_as_data=True, ontology_term_budget=100
        )
        patterns = result.data_schema.patterns
        probe = miner.last_report.config.get("ontology_term_probe")
    assert budget.call_args.args == (60,) and seen == ["light"]
    assert probe["state"] == "not_probed" and probe["seconds_per_query"] == 60
    assert patterns
