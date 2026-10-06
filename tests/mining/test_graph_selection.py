"""rdfsolve.mining.graph_selection: mining is scoped to the selected data graphs, and subjects
whose types come from companion (context) graphs are mined with them."""

import json
from unittest.mock import Mock

from rdflib import Dataset, URIRef

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.pattern_enrichment import enrich_patterns_with_counts
from rdfsolve.schema_models import SchemaPattern


def _helper(data: Dataset) -> Mock:
    helper = Mock()
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        data.query(query).serialize(format="json")
    )
    return helper


def test_pattern_distinct_counts_are_not_summed_across_named_graphs():
    data = Dataset()
    for graph_iri in ("urn:g:one", "urn:g:two"):
        graph = data.graph(URIRef(graph_iri))
        graph.parse(
            data="\n            <urn:s> a <urn:C>; <urn:p> <urn:o> .\n            <urn:o> a <urn:D> .\n            ",
            format="turtle",
        )
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-21T00:00:00+00:00")
    helper = _helper(data)
    patterns = enrich_patterns_with_counts(
        [SchemaPattern(subject_class="urn:C", property_uri="urn:p", object_class="urn:D")],
        helper,
        ["urn:g:one", "urn:g:two"],
        miner._report,
        lambda query, purpose, chunk=None: helper.select(query)["results"]["bindings"],
        class_batch_size=5,
        class_chunk_size=None,
        unsafe_paging=False,
        delay=0,
    )
    assert patterns[0].count == 2
    assert patterns[0].graphs == {"urn:g:one": 1, "urn:g:two": 1}
    assert patterns[0].distinct_subjects is None
    assert patterns[0].distinct_objects is None

    from rdfsolve.mining.enrichment import example_query
    from rdfsolve.release.scientific_validation import pattern_existence_query
    from rdfsolve.sparql_helper import EndpointTimeoutError

    scoped = Dataset(default_union=False)
    scoped.graph(URIRef("urn:data")).parse(
        data="""
        <urn:s> a <urn:C>; <urn:p> <urn:o>; <urn:other> <urn:x> .
        <urn:s2> a <urn:C>; <urn:p> <urn:o> .
        <urn:o> a <urn:D> .
        <urn:contextSubject> <urn:p> <urn:o> .
    """,
        format="turtle",
    )
    for graph in ["urn:types", "urn:types:duplicate"]:
        scoped.graph(URIRef(graph)).parse(
            data="""
            <urn:o> a <urn:D>, <urn:E>; <urn:leak> "context edge" .
            <urn:contextSubject> a <urn:Outside> .
            <urn:noise> a <urn:C>; <urn:p> <urn:o> .
        """,
            format="turtle",
        )
    expected = None
    for strategy in ["two-phase", "single-pass", "one-shot", "fallback"]:
        with SchemaMiner.from_graph(
            scoped,
            graph_uris=["urn:data"],
            type_context_graph_uris=["urn:types", "urn:types:duplicate"],
            strategy="two-phase" if strategy == "fallback" else strategy,
            delay=0,
        ) as miner:
            if strategy == "fallback":
                select = miner.helper.select

                def timeout_batch(query, select=select, **options):
                    if options.get("purpose") == "two-phase/typed-object":
                        raise EndpointTimeoutError("Exercise property fallback")
                    return select(query, **options)

                miner.helper.select = timeout_batch
                collect = miner._collect_bindings

                def timeout_page(query, purpose="", chunk_size=None, collect=collect):
                    if purpose == "two-phase/typed-object":
                        raise EndpointTimeoutError("Exercise property fallback without waiting")
                    return collect(query, purpose, chunk_size)

                miner._collect_bindings = timeout_page
            schema = miner.mine()
            actual = {
                (p.subject_class, p.property_uri, p.object_class): (
                    p.count,
                    p.graphs,
                    p.distinct_subjects,
                    p.distinct_objects,
                )
                for p in schema.patterns
            }
            assert actual["urn:C", "urn:p", "urn:D"] == (2, {"urn:data": 2}, 2, 1), strategy
            assert actual["urn:C", "urn:p", "urn:E"] == (2, {"urn:data": 2}, 2, 1), strategy
            assert ("urn:C", "urn:p", "Resource") not in actual, strategy
            assert actual["urn:C", "urn:other", "Resource"][0] == 1, strategy
            assert actual["urn:Outside", "urn:p", "urn:E"] == (1, {"urn:data": 1}, 1, 1), strategy
            assert all(p != "urn:leak" for sc, p, oc in actual), strategy
            assert schema.about.class_entity_counts["urn:C"] == 2, (
                "Context cannot enlarge populations"
            )
            assert schema.about.type_context_graph_uris == ["urn:types", "urn:types:duplicate"]
            assert miner.last_report.config["type_context"]["state"] == "nonempty"
            assert miner.last_report.completion_state == "complete", strategy
            linked = next(
                p
                for p in schema.patterns
                if p.property_uri == "urn:p" and p.object_class == "urn:E"
            )
            for query in [
                example_query(linked, ["urn:data"], 2, miner.type_context_graph_uris),
                pattern_existence_query(
                    linked, ["urn:data"], type_context_graph_scope=miner.type_context_graph_uris
                ),
            ]:
                assert miner.helper.select(query)["results"]["bindings"], (
                    "Saved witnesses must use companion types"
                )
            if expected is None:
                expected = actual
            assert actual == expected, f"{strategy} changed the scope or counts"


def test_classifications_with_companion_subject_types():
    data = Dataset(default_union=False)
    data.parse(
        data="""
        @prefix e: <urn:chemical:> .
        e:data {
            e:c1 a e:Chemical; e:group e:g1 .
            e:c2 a e:Chemical; e:group e:g2 .
        }
        e:labels { e:g1 e:label "PFAS"@en, "PFAS group"@en . e:g2 e:label "Other"@en . }
        e:types {
            e:g1 a e:Group . e:g2 a e:Group .
            e:ghost a e:Group; e:label "Ghost"@en .
            e:foreign a e:Outside; e:noise "Exclude" .
            e:g1 e:label "Context-only label"@en .
        }
        e:outside { e:g2 e:label "Outside scope"@en . }
    """,
        format="trig",
    )
    results = []
    for strategy in ("two-phase", "single-pass", "one-shot"):
        with SchemaMiner.from_graph(
            data,
            graph_uris=["urn:chemical:data", "urn:chemical:labels"],
            type_context_graph_uris=["urn:chemical:types"],
            strategy=strategy,
            delay=0,
            enrich=True,
            examples_per_pattern=2,
        ) as miner:
            schema = miner.mine("chemical-classifications")
            assert miner.last_report.config["local_backend"]["engine"] == "oxigraph"
            assert miner.last_report.completion_state == "complete"
            labels = [
                p
                for p in schema.patterns
                if p.subject_class == "urn:chemical:Group"
                and p.property_uri == "urn:chemical:label"
            ]
            assert len(labels) == 1, "Discover fields on data subjects typed in companion context"
            assert labels[0].count == 3 and labels[0].graphs == {"urn:chemical:labels": 3}
            assert schema.about.class_entity_counts["urn:chemical:Group"] == 2
            assert "urn:chemical:Outside" not in schema.get_classes()
            assert not schema.structural_patterns, (
                "Recovered typed labels need no structural fallback"
            )
            examples = [
                e for e in schema.enrichment.examples if e.subject_class == "urn:chemical:Group"
            ]
            assert examples and all(
                e.value.value in {"PFAS", "PFAS group", "Other"} for e in examples
            )
            nav = schema.discover_paths(max_hops=2)
            route = next(
                p
                for p in nav.paths
                if p.steps[0].property_uri == "urn:chemical:group"
                and p.steps[-1].property_uri == "urn:chemical:label"
            )
            schema.probe_paths([route], helper=miner.helper)
            assert (route.source_count, route.matched_sources) == (2, 2)
            rows = miner.helper.select(schema.select(paths=[route]).path_query(route))
            assert len(rows["results"]["bindings"]) == 3, "Keep all real group labels"
            results.append(
                {
                    (p.subject_class, p.property_uri, p.object_class, p.datatype): p.count
                    for p in schema.patterns
                }
            )
    assert results[0] == results[1] == results[2], "Strategies must agree on measured evidence"


def _fake_endpoint(monkeypatch, helper, refuse_define=False):
    """Answer every request of *helper* without a network and keep the queries sent."""
    from rdfsolve.sparql_helper import QueryError

    sent = []

    def execute(query, accept, query_type="SELECT", *args, **kwargs):
        sent.append(query)
        if refuse_define and query.lstrip().startswith("DEFINE"):
            raise QueryError("HTTP 400: Invalid SPARQL query: Token DEFINE")
        if query_type == "ASK":
            return {"boolean": True}
        # One row: the self-check of the exclusion needs a non-empty answer.
        row = {"s": {"type": "uri", "value": "urn:s"}}
        return {"head": {"vars": ["s"]}, "results": {"bindings": [row]}}

    monkeypatch.setattr(helper, "_execute_request", execute)
    return sent


ENGINE_GRAPHS = [
    "http://data.example/",
    "http://www.openlinksw.com/schemas/virtrdf#",
    "http://localhost:8890/DAV/",
    "servicedescription",
]


def test_engine_graphs_are_left_out_of_every_query_without_a_dataset(monkeypatch):
    """Virtuoso reads its system graphs in a query without FROM (WikiPathways: virtrdf#
    properties in the census). They are excluded with its pragmas in every query that has no
    dataset clause, and only there: Virtuoso drops a FROM that names an excluded graph."""
    from rdfsolve import void_retrieval
    from rdfsolve.mining.miner import SchemaMiner
    from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_GRAPHS

    monkeypatch.setattr(void_retrieval, "discover_graph_names", lambda *a, **k: ENGINE_GRAPHS)
    miner = SchemaMiner(
        endpoint_url="https://endpoint.example/sparql",
        excluded_graph_prefixes=SUGGESTED_SERVICE_GRAPHS,
        delay=0,
    )
    sent = _fake_endpoint(monkeypatch, miner.helper)
    with miner._session("engine"):
        excluded = sorted(ENGINE_GRAPHS[1:])
        assert miner.helper.excluded_graphs == excluded
        record = miner.last_report.config["excluded_graphs"]
        assert record["state"] == "excluded" and record["graph_uris"] == excluded
        miner.helper.select("SELECT DISTINCT ?p WHERE { ?s ?p ?o }")
        miner.helper.select("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }")
        miner.helper.select("SELECT ?p FROM <http://data.example/> WHERE { ?s ?p ?o }")
        miner.helper.ask("ASK FROM NAMED <http://data.example/> { GRAPH ?g { ?s ?p ?o } }")
    unscoped, graph_pattern, scoped, named = sent[-4:]
    for graph in excluded:
        assert f"DEFINE input:default-graph-exclude <{graph}>" in unscoped
        assert f"DEFINE input:default-graph-exclude <{graph}>" in graph_pattern
    assert "input:named-graph-exclude" not in "".join(sent), "It empties dbpedia's answers"
    assert "DEFINE" not in scoped and "DEFINE" not in named
    assert miner.helper.excluded_graphs == [], "The exclusion ends with the session"


def test_an_engine_without_the_pragmas_is_recorded_and_nothing_is_excluded(monkeypatch):
    from rdfsolve import void_retrieval
    from rdfsolve.mining.miner import SchemaMiner
    from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_GRAPHS

    monkeypatch.setattr(void_retrieval, "discover_graph_names", lambda *a, **k: ENGINE_GRAPHS)
    miner = SchemaMiner(
        endpoint_url="https://endpoint.example/sparql",
        excluded_graph_prefixes=SUGGESTED_SERVICE_GRAPHS,
        delay=0,
    )
    sent = _fake_endpoint(monkeypatch, miner.helper, refuse_define=True)
    with miner._session("engine"):
        assert miner.helper.excluded_graphs == []
        record = miner.last_report.config["excluded_graphs"]
        assert record["state"] == "not_supported" and "DEFINE" in record["error"]
        miner.helper.select("SELECT DISTINCT ?p WHERE { ?s ?p ?o }")
    assert not sent[-1].startswith("DEFINE")


def test_no_engine_graph_and_a_scoped_source_send_no_pragma(monkeypatch):
    from rdfsolve import void_retrieval
    from rdfsolve.mining.miner import SchemaMiner
    from rdfsolve.schema_models._constants import SUGGESTED_SERVICE_GRAPHS

    monkeypatch.setattr(
        void_retrieval, "discover_graph_names", lambda *a, **k: ["http://data.example/"]
    )
    miner = SchemaMiner(
        endpoint_url="https://endpoint.example/sparql",
        excluded_graph_prefixes=SUGGESTED_SERVICE_GRAPHS,
        delay=0,
    )
    _fake_endpoint(monkeypatch, miner.helper)
    with miner._session("data only"):
        assert miner.helper.excluded_graphs == []
        assert miner.last_report.config["excluded_graphs"]["state"] == "none_found"
    scoped = SchemaMiner(
        endpoint_url="https://endpoint.example/sparql",
        graph_uris=["http://data.example/"],
        excluded_graph_prefixes=SUGGESTED_SERVICE_GRAPHS,
        delay=0,
    )
    sent = _fake_endpoint(monkeypatch, scoped.helper)
    with scoped._session("scoped"):
        assert "excluded_graphs" not in scoped.last_report.config
    assert not any("DEFINE" in q for q in sent)
