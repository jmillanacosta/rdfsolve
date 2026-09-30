"""The discovery by the property sets of QLever answers one row for each structural pattern, with
its triples, distinct subjects and distinct objects, not one row for each edge: the rows of
WikiPathways gpml:hasDataNode (137,699 edges, each with the property sets of both nodes) exceeded
the response budget of 64 MiB, and a query with GROUP_CONCAT is not paged. The witness of each
pattern is one edge of its group, sampled in the same answer. RDFLib stands in for QLever here."""

from tests.test_qlever_patterns import DATA, mine, shapes

MORE = DATA + """
<urn:d> <urn:p> <urn:b> ; <urn:q> "z" .
<urn:e> <urn:p> <urn:c> .
"""


def test_discovery_answers_one_row_for_each_pattern(monkeypatch):
    exact_entry, exact, _ = mine(monkeypatch, MORE, patterns=False)
    entry, result, sent = mine(monkeypatch, MORE)
    assert entry["census"] == "qlever_patterns" and shapes(result) == shapes(exact)
    discovery = [q for q in sent if "ql:has-predicate ?sp" in q]
    assert discovery and all("COUNT(DISTINCT ?s)" in q and "GROUP BY ?p ?ss" in q for q in discovery)
    witness = [q for q in sent if q.rstrip().endswith("LIMIT 1") and "ql:has-predicate" not in q]
    assert not witness, "No witness queries"
    exact_examples = {(p.property_uri, tuple(p.subject_properties)): p for p in exact.structural_patterns}
    for pattern in result.structural_patterns:
        (example,) = pattern.examples
        assert {"s", "o"} <= set(example), "A witness edge of the pattern"
        other = exact_examples[pattern.property_uri, tuple(pattern.subject_properties)]
        assert example["o"]["type"] == other.examples[0]["o"]["type"]
