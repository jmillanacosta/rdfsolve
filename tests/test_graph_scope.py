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
