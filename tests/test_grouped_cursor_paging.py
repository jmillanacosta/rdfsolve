"""Virtuoso refuses to sort more than 10,000 rows for a page (error SR353), so offset paging stops
there. A grouped query (GROUP BY on projected variables) has one row per group, so the pages
continue with a cursor on the group variables (SIBiLS: the objects of one property)."""

import json
import re

from rdflib import Graph, URIRef

from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper

GRAPH = Graph()
for i in range(7):
    for j in range(i + 1):
        GRAPH.add((URIRef(f"urn:s{j}"), URIRef("urn:p"), URIRef(f"urn:o{i}")))


class SortLimitedEndpoint(SparqlHelper):
    """Answer from GRAPH; refuse the unpaged query and any page past offset 2, as Virtuoso does."""

    def select(self, query, purpose=""):
        offset = re.search(r"OFFSET\s+(\d+)", query)
        if offset is None and "__rdfsolve_cursor" not in query:
            raise EndpointTimeoutError("Query cost/time limit: Virtuoso S1T00 Error SR171")
        if offset and int(offset[1]) + 2 > 2:
            raise EndpointTimeoutError(
                "Query cost/time limit: Virtuoso 22023 Error SR353: Sorted TOP clause specifies"
                " more then 10001 rows to sort. Only 10000 are allowed."
            )
        return json.loads(GRAPH.query(query).serialize(format="json"))


def test_a_grouped_query_is_paged_by_cursor_after_the_sort_limit(monkeypatch, tmp_path):
    monkeypatch.setenv("RDFSOLVE_HTTP_LOCK_DIR", str(tmp_path))
    helper = SortLimitedEndpoint("https://example.org/sparql")
    helper.inter_request_delay = helper.select_page_cooldown = 0
    helper.select_page_size = 2
    helper.select_page_retries = 0
    query = "SELECT ?o (COUNT(*) AS ?n) WHERE { ?s <urn:p> ?o } GROUP BY ?o"
    rows = helper.select_with_fallback(query, purpose="structural/objects")["results"]["bindings"]
    counts = {r["o"]["value"]: int(r["n"]["value"]) for r in rows}
    assert counts == {f"urn:o{i}": i + 1 for i in range(7)}, "Every group, once"
    assert helper.last_select_execution["strategy"] == "cursor_recovery"
