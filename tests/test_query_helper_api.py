from unittest.mock import Mock

import pytest
from rdfsolve.sparql_helper import EndpointError, SparqlHelper


def test_http_500_recovery_depends_on_error_body(monkeypatch):
    body, category, calls = (
        "Virtuoso 37000 Error SP030: SPARQL compiler: syntax error",
        EndpointError,
        1,
    )
    import requests

    with SparqlHelper("https://example.org/sparql", max_retries=2, initial_backoff=0) as helper:
        helper.enable_query_collection()

        def fail(*args, **kwargs):
            helper._last_error_body = body
            response = requests.Response()
            response.status_code = 500
            raise requests.HTTPError("HTTP 500", response=response)

        request = Mock(side_effect=fail)
        monkeypatch.setattr(helper, "_get_query", request)
        with pytest.raises(category):
            helper.select("SELECT ?s WHERE { ?s ?p ?o }")
        assert request.call_count == calls
        record = helper.get_collected_queries()[-1]
        assert not record.success and record.status_code == 500
        assert record.response_excerpt == body and record.error_message
