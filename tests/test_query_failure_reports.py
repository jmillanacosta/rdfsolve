import pytest
from rdfsolve.sparql_helper import PaginationTruncatedError


def test_paging_recovers_timeout_and_short_server_pages(monkeypatch):
    import json
    import re

    from rdflib import Graph
    from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper

    from tests.test_client_api import DATA

    graph = Graph().parse(DATA, format="turtle")
    query = "SELECT ?s ?title WHERE { ?s <http://purl.org/dc/elements/1.1/title> ?title } ORDER BY ?s ?title"
    expected = json.loads(graph.query(query).serialize(format="json"))["results"]["bindings"]
    calls = []

    def select(query, **kwargs):
        offset = int(re.search("OFFSET (\\d+)", query)[1])
        size = int(re.search("LIMIT (\\d+)", query)[1])
        calls.append((offset, size))
        if len(calls) == 1:
            raise EndpointTimeoutError("The first page timed out")
        return {"results": {"bindings": expected[offset : offset + min(size, 2)]}}

    monkeypatch.setattr("rdfsolve.sparql_helper.time.sleep", lambda _: None)
    with SparqlHelper("https://example.org/sparql") as helper:
        monkeypatch.setattr(helper, "select", select)
        actual = [
            row
            for page in helper.select_chunked(
                helper.prepare_paginated_query(query),
                chunk_size=8,
                max_pages=None,
                until_empty=True,
                stable_terms=True,
            )
            for row in page
        ]
        assert actual == expected and calls[:2] == [(0, 8), (0, 4)]
        assert calls[-1][0] == len(expected)
        monkeypatch.setattr(
            helper, "select", lambda *a, **k: {"results": {"bindings": expected[:2]}}
        )
        with pytest.raises(PaginationTruncatedError, match="repeated a page"):
            list(
                helper.select_chunked(
                    helper.prepare_paginated_query(query),
                    chunk_size=8,
                    max_pages=None,
                    until_empty=True,
                )
            )
