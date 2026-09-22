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

    from rdfsolve.sparql_helper import EndpointTimeoutError
    from rdfsolve.mining.enrichment import example_query
    from rdfsolve.release.scientific_validation import pattern_existence_query

    scoped = Dataset(default_union=False)
    scoped.graph(URIRef("urn:data")).parse(data='''
        <urn:s> a <urn:C>; <urn:p> <urn:o>; <urn:other> <urn:x> .
        <urn:s2> a <urn:C>; <urn:p> <urn:o> .
        <urn:o> a <urn:D> .
        <urn:contextSubject> <urn:p> <urn:o> .
    ''', format="turtle")
    for graph in ["urn:types", "urn:types:duplicate"]:
        scoped.graph(URIRef(graph)).parse(data='''
            <urn:o> a <urn:D>, <urn:E>; <urn:leak> "context edge" .
            <urn:contextSubject> a <urn:Outside> .
            <urn:noise> a <urn:C>; <urn:p> <urn:o> .
        ''', format="turtle")
    expected = None
    for strategy in ["two-phase", "single-pass", "one-shot", "fallback"]:
        with SchemaMiner.from_graph(scoped, graph_uris=["urn:data"],
                type_context_graph_uris=["urn:types", "urn:types:duplicate"],
                strategy="two-phase" if strategy == "fallback" else strategy,
                delay=0) as miner:
            if strategy == "fallback":
                select = miner.helper.select

                def timeout_batch(query, **options):
                    if options.get("purpose") == "two-phase/typed-object":
                        raise EndpointTimeoutError("Exercise property fallback")
                    return select(query, **options)

                miner.helper.select = timeout_batch
                collect = miner._collect_bindings

                def timeout_page(query, purpose="", chunk_size=None):
                    if purpose == "two-phase/typed-object":
                        raise EndpointTimeoutError("Exercise property fallback without waiting")
                    return collect(query, purpose, chunk_size)

                miner._collect_bindings = timeout_page
            schema = miner.mine()
            actual = {(p.subject_class, p.property_uri, p.object_class):
                      (p.count, p.graphs, p.distinct_subjects, p.distinct_objects)
                      for p in schema.patterns}
            assert actual["urn:C", "urn:p", "urn:D"] == (2, {"urn:data": 2}, 2, 1), strategy
            assert actual["urn:C", "urn:p", "urn:E"] == (2, {"urn:data": 2}, 2, 1), strategy
            assert ("urn:C", "urn:p", "Resource") not in actual, strategy
            assert actual["urn:C", "urn:other", "Resource"][0] == 1, strategy
            assert all(sc != "urn:Outside" and p != "urn:leak" for sc, p, oc in actual), strategy
            assert schema.about.class_entity_counts["urn:C"] == 2, "Context cannot enlarge populations"
            assert schema.about.type_context_graph_uris == ["urn:types", "urn:types:duplicate"]
            assert miner.last_report.config["type_context"]["state"] == "nonempty"
            assert miner.last_report.completion_state == "complete", strategy
            linked = next(p for p in schema.patterns if p.property_uri == "urn:p" and p.object_class == "urn:E")
            for query in [
                example_query(linked, ["urn:data"], 2, miner.type_context_graph_uris),
                pattern_existence_query(linked, ["urn:data"], type_context_graph_scope=miner.type_context_graph_uris),
            ]:
                assert miner.helper.select(query)["results"]["bindings"], "Saved witnesses must use companion types"
            if expected is None:
                expected = actual
            assert actual == expected, f"{strategy} changed the scope or counts"
