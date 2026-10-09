"""rdfsolve.mining.structural_strategy discovery: untyped subjects and their edges, node kinds, a
refused property during discovery, and recounts of language-tagged literals."""

import re
from types import SimpleNamespace

from rdflib import Dataset

from rdfsolve.evidence.observed import build_node_kind_query
from rdfsolve.mining import structural_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.structural_strategy import _discovery_query, structural_queries
from rdfsolve.schema_models.structural import StructuralPattern
from rdfsolve.sparql_helper import EndpointError, EndpointTimeoutError, QueryError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> ; <urn:q> "x" .
<urn:b> a <urn:B> .
<urn:u> <urn:p> <urn:b> ; <urn:q> "y" .
<urn:w> <urn:q> "z" .
"""


def test_uncovered_edges_of_untyped_subjects_are_found_without_the_typed_keys(monkeypatch):
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        sent = []

        def refuse(run):
            def call(query, *args, purpose="", **kwargs):
                sent.append(query)
                if purpose != "structural/coverage" and "?_subjectType" in query:
                    raise EndpointError(
                        "HTTP 500: Virtuoso 42000 Error SQ200: Stack Overflow in cost model"
                    )
                return run(query, *args, purpose=purpose, **kwargs)

            return call

        monkeypatch.setattr(miner.helper, "select", refuse(miner.helper.select))
        monkeypatch.setattr(
            miner.helper, "select_with_fallback", refuse(miner.helper.select_with_fallback)
        )
        result = miner.mine("untyped")
        (entry,) = miner.last_report.config["structural_coverage"]
    assert entry["state"] == "complete" and entry["uncovered_triples"] == 3
    found = sorted(
        (p.property_uri, p.subject_selection, p.count) for p in result.structural_patterns
    )
    assert found == [("urn:p", "untyped", 1), ("urn:q", "untyped", 1), ("urn:q", "untyped", 1)]
    for pattern in result.structural_patterns:
        assert f"FILTER NOT EXISTS {{ ?s <{pattern.property_uri}> ?o ." in pattern.recount_query
    assert not any("NOT EXISTS { ?s a ?_type" in q for q in sent), (
        "The group repeats the edge (QLever)"
    )


def kind(query: str, variable: str) -> str:
    """Return the expression that a query binds to *variable*."""
    return re.search(rf"BIND\((.*?) AS \?{variable}\)", query)[1]


def test_node_kinds_are_read_with_isblank_first():
    discovery = _discovery_query(None, [], "")
    observed = build_node_kind_query(["urn:A"], None)
    for expression in (kind(discovery, "sk"), kind(discovery, "ok"), kind(observed, "kind")):
        assert expression.startswith("IF(isBlank("), expression
        assert "isIRI" not in expression, "Virtuoso: isIRI is true for blank nodes in a BIND"


def test_discovery_groups_only_the_nodes_of_uncovered_edges():
    """The property sets are grouped for the subjects and objects of the uncovered edges only,
    with the same answer as grouping every node (UberGraph: 47 properties refused at 10 min each)."""
    import json

    from rdflib import Graph

    data = Graph().parse(
        format="turtle",
        data="""<urn:a> <urn:p> <urn:b> ; <urn:q> "x" . <urn:b> <urn:r> <urn:c> .
        <urn:c> <urn:q> "y" . _:n <urn:p> "z" ; <urn:r> <urn:a> .""",
    )
    residual = "VALUES ?p { <urn:p> }"
    own = _discovery_query(None, [], residual)
    assert "WHERE { { SELECT DISTINCT ?s WHERE { ?s ?p ?o . VALUES ?p { <urn:p> } } }" in own
    # The earlier form, which grouped every node of the graph.
    whole = own.replace(
        "{ SELECT DISTINCT ?s WHERE { ?s ?p ?o . VALUES ?p { <urn:p> } } } ", ""
    ).replace("{ SELECT DISTINCT ?o WHERE { ?s ?p ?o . VALUES ?p { <urn:p> } } } ", "")
    assert "SELECT DISTINCT ?s WHERE" not in whole

    def rows(query):
        """Return the answer rows, with the property sets in order (GROUP_CONCAT has none)."""
        found = json.loads(data.query(query).serialize(format="json"))["results"]["bindings"]
        for row in found:
            for key in ("ss", "os"):
                if key in row:
                    row[key]["value"] = ">".join(
                        sorted(structural_strategy._properties(row[key]["value"]))
                    )
        return sorted(json.dumps(r, sort_keys=True) for r in found)

    assert rows(own) == rows(whole) and len(rows(own)) == 2


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


def split_select(refuse):
    """Answer the split query with the triples of the subjects that have each property, refuse
    the discovery queries that *refuse* accepts and answer the others with one row."""

    def select(context, query, purpose, **options):
        if purpose == "structural/discovery-split":
            sizes = {"urn:p:slow": 5, "urn:x": 3, "urn:y": 5}
            if "MINUS { ?s ql:has-predicate <urn:x> }" in query:
                sizes = {"urn:p:slow": 2, "urn:y": 2}
            return [{"sp": {"value": p}, "n": {"value": str(n)}} for p, n in sizes.items()]
        if refuse(query):
            raise EndpointTimeoutError("Operation timed out. Last operation: GroupBy")
        return [{"p": {"value": "urn:p:slow"}, "q": {"value": query}}]

    return select


def test_a_refused_property_is_split_by_a_property_of_its_subjects(monkeypatch):
    """A refused discovery is split by a property that holds about half of its triples: the
    subject's property set is a key of the groups, so both parts give the rows of the whole
    (OMA dcterms:identifier: 49,916,389 edges timed out in the final GROUP BY)."""
    outcomes = []
    context = SimpleNamespace(report=SimpleNamespace(record_outcome=outcomes.append))
    select = split_select(lambda q: "ql:has-predicate <urn:x>" not in q)
    monkeypatch.setattr(structural_strategy, "_select", select)
    entry = {"census_properties": {"urn:p:slow": {"uncoveredTriples": 5, "untypedTriples": 5}}}
    rows = structural_strategy._patterns_discovery(context, None, [], entry)
    parts = [row["q"]["value"] for row in rows]
    assert len(parts) == 2 and not outcomes
    assert all(q.count("ql:has-predicate <urn:x>") == 3 for q in parts), "In all three patterns"
    assert "MINUS { ?s ql:has-predicate <urn:x> }" in parts[1]
    assert "discovery_refused" not in entry["census_properties"]["urn:p:slow"]


def test_a_part_that_cannot_be_split_is_recorded_with_its_triples(monkeypatch):
    outcomes = []
    context = SimpleNamespace(report=SimpleNamespace(record_outcome=outcomes.append))
    select = split_select(lambda q: "?s ql:has-predicate <urn:x> ." not in q)
    monkeypatch.setattr(structural_strategy, "_select", select)
    entry = {"census_properties": {"urn:p:slow": {"uncoveredTriples": 5, "untypedTriples": 5}}}
    rows = structural_strategy._patterns_discovery(context, None, [], entry)
    census = entry["census_properties"]["urn:p:slow"]
    assert len(rows) == 1 and census["undiscoveredTriples"] == 2
    assert "discovery_refused" in census and structural_strategy._undiscovered_triples(entry) == 2
    assert len(outcomes) == 1 and outcomes[0].state == "partial"


def test_a_refused_property_is_recorded_when_each_property_is_discovered_alone(monkeypatch):
    """The discovery with one query for each property has the same rule: a refused query is
    recorded and does not fail the source."""
    outcomes = []
    context = SimpleNamespace(
        report=SimpleNamespace(record_outcome=outcomes.append),
        graph_uris=None,
        type_context_graph_uris=None,
    )

    def select(context, query, purpose, **options):
        if "<urn:p:slow>" in query:
            raise QueryError("Cannot paginate volatile expressions without changing their meaning")
        return [{"p": {"value": "urn:p:fast"}}]

    monkeypatch.setattr(structural_strategy, "_select", select)
    entry = {
        "census_properties": {
            "urn:p:slow": {"uncoveredTriples": 5, "untypedTriples": 5},
            "urn:p:fast": {"uncoveredTriples": 2, "untypedTriples": 2},
        }
    }
    rows, untyped = structural_strategy._property_discovery(context, None, [], [], entry)
    assert rows == [{"p": {"value": "urn:p:fast"}}] and untyped == {"urn:p:slow", "urn:p:fast"}
    assert "discovery_refused" in entry["census_properties"]["urn:p:slow"]
    assert len(outcomes) == 1 and outcomes[0].state == "partial"


XSD = "http://www.w3.org/2001/XMLSchema#"
LANG_STRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"


def pattern(datatype, language):
    return StructuralPattern(
        subject_properties=["urn:p"],
        object_properties=[],
        subject_kind="IRI",
        object_kind="Literal",
        property_uri="urn:p",
        datatype=datatype,
        language=language,
        graph_uri=None,
        type_graph_uris=[],
        object_type_graph_uris=[],
        covered_types=[],
        subject_selection="untyped",
        count=1,
        distinct_subjects=1,
        distinct_objects=1,
        witness_query="",
        recount_query="",
    )


def test_the_language_is_tested_only_for_a_language_string():
    for datatype in (XSD + "integer", XSD + "dateTime", XSD + "string"):
        witness, recount = structural_queries(pattern(datatype, ""))
        assert f"DATATYPE(?o) = <{datatype}>" in recount and "LANG(?o)" not in recount + witness
    witness, recount = structural_queries(pattern(LANG_STRING, "en"))
    assert 'FILTER(LANG(?o) = "en")' in recount and 'FILTER(LANG(?o) = "en")' in witness


def test_the_discovery_queries_hold_no_string_escapes():
    """Virtuoso returns the escape of a newline in a SPARQL string undecoded, so the separators
    of property sets and witnesses are plain characters that no IRI holds."""
    import inspect

    from rdfsolve.mining import structural_strategy

    source = inspect.getsource(structural_strategy)
    assert 'SEPARATOR=">"' in source and "\\\\n" not in source
    assert structural_strategy._properties("urn:a b>urn:c") == ["urn:a b", "urn:c"]


def test_the_property_set_of_each_subject_is_read_once_for_the_subject():
    """The property set of a subject is grouped from its ql:has-predicate rows, not from each of
    its edges: MedGen dcterms:references (95,419,320 edges of 125,005 subjects) took 338 s
    to group one row per edge and property, and 3.5 s to group one row per subject and property;
    the discovery then timed out after 600 s (corpus-local-4a3affdf-2)."""
    query = structural_strategy._patterns_query(
        None, [], "urn:p", ["?s ql:has-predicate <urn:x> ."]
    )
    subjects = re.search(r"\{ SELECT \?s \(GROUP_CONCAT.*?GROUP BY \?s \}", query, re.DOTALL)[0]
    assert "?s ql:has-predicate <urn:p> ." in subjects and "?_o" not in subjects
    assert "?s ql:has-predicate <urn:x> ." in subjects, "The part's clauses apply to the subject"


def test_a_pattern_that_its_recount_does_not_confirm_is_a_gap_not_a_failure(monkeypatch):
    """Virtuoso can group one subject twice, so that a discovered property set has no edge in its
    recount (AOP-Wiki virtrdf:item, WikiPathways pav:hasVersion: "Structural discovery and
    witnesses disagree"). The pattern is left out and recorded with its numbers; the other
    patterns and the typed schema stand, and the coverage is partial."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        select = miner.helper.select

        def refuse_p(query, *args, purpose="", **kwargs):
            if purpose in ("structural/count", "structural/witness") and "{ <urn:p> }" in query:
                if purpose == "structural/witness":
                    return {"head": {"vars": ["s", "o"]}, "results": {"bindings": []}}
                zero = {"type": "literal", "value": "0"}
                return {
                    "head": {"vars": ["n", "subjects", "objects"]},
                    "results": {"bindings": [{"n": zero, "subjects": zero, "objects": zero}]},
                }
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", refuse_p)
        result = miner.mine("inconsistent")
        report = miner.last_report
        (entry,) = report.config["structural_coverage"]
    assert entry["state"] == "partial" and entry["uncovered_triples"] == 3
    (left_out,) = entry["inconsistent_patterns"]
    assert left_out["property"] == "urn:p" and left_out["recount"] == 0
    assert left_out["witnesses"] == 0 and left_out["subject_properties"] == ["urn:p", "urn:q"]
    assert entry["unaccounted_triples"] == 1
    assert sorted(p.property_uri for p in result.structural_patterns) == ["urn:q", "urn:q"]
    messages = [gap.message for gap in report.measurement_gaps]
    assert any("urn:p" in m for m in messages)
    assert any("account for 2 of 3 uncovered triples" in m for m in messages)
    assert result.patterns, "The typed schema stands"


def _mine_refusing(monkeypatch, refuse):
    """Mine DATA through a fake remote endpoint that refuses the discovery queries *refuse*
    selects, as Virtuoso refuses one over its estimated-time limit."""
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    sent = []
    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        select = miner.helper.select

        def call(query, *args, purpose="", **kwargs):
            sent.append((purpose, query))
            if purpose == "structural/discovery" and refuse(query):
                raise QueryError(
                    "Virtuoso 42000 Error The estimated execution time 1115 (sec) exceeds the "
                    "limit of 400 (sec)."
                )
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", call)
        result = miner.mine("refused")
        (entry,) = miner.last_report.config["structural_coverage"]
        return result, entry, miner.last_report, sent


def test_a_refused_discovery_is_read_in_batches_of_subjects(monkeypatch):
    """The discovery query groups with GROUP_CONCAT and is not paged ("Cannot paginate volatile
    expressions"); a refused property is discovered for batches of its uncovered subjects, with
    the same patterns (WikiPathways dc:creator)."""
    result, entry, report, sent = _mine_refusing(monkeypatch, lambda q: "FILTER(?s IN" not in q)
    assert entry["state"] == "complete" and entry["undiscovered_triples"] == 0
    found = sorted((p.property_uri, p.count) for p in result.structural_patterns)
    assert found == [("urn:p", 1), ("urn:q", 1), ("urn:q", 1)]
    assert any(p == "structural/discovery-subjects" for p, _ in sent)
    assert not report.query_failures


def test_a_subject_refused_alone_is_discovered_from_pairs(monkeypatch):
    """A subject whose grouped discovery is refused even alone is discovered from its edges and
    the (node, property) pairs, queries without GROUP_CONCAT that can be paged in a fixed order
    (IDEAL author, WikiPathways gpml:hasDataNode: "Cannot paginate volatile expressions")."""
    plain, *_ = _mine_refusing(monkeypatch, lambda q: False)
    result, entry, report, sent = _mine_refusing(
        monkeypatch, lambda q: "FILTER(?s IN" not in q or "<urn:w>" in q
    )
    assert any(p == "structural/discovery-pairs" for p, _ in sent)
    assert entry["state"] == "complete" and entry["undiscovered_triples"] == 0
    assert not report.query_failures
    dump = sorted(p.model_dump_json() for p in result.structural_patterns)
    assert dump == sorted(p.model_dump_json() for p in plain.structural_patterns), (
        "The same patterns as the grouped discovery of the whole property"
    )


def test_subjects_still_refused_are_counted_as_not_discovered(monkeypatch):
    original = structural_strategy._select

    def pairs_refused(context, query, purpose, **options):
        if purpose == "structural/discovery-pairs" and "<urn:w>" in query:
            raise QueryError("Virtuoso 42000 Error SR171: Transaction timed out")
        return original(context, query, purpose, **options)

    monkeypatch.setattr(structural_strategy, "_select", pairs_refused)
    result, entry, report, _ = _mine_refusing(
        monkeypatch, lambda q: "FILTER(?s IN" not in q or "<urn:w>" in q
    )
    assert "SR171" in entry["census_properties"]["urn:q"]["discovery_refused"], (
        "The reason is the last refusal, not the refusal to page the whole property"
    )
    q = entry["census_properties"]["urn:q"]
    assert q["undiscoveredTriples"] == 1 and "discovery_refused" in q
    assert entry["state"] == "partial" and entry["undiscovered_triples"] == 1
    found = sorted((p.property_uri, p.count) for p in result.structural_patterns)
    assert found == [("urn:p", 1), ("urn:q", 1)], "urn:u keeps its patterns"
    assert report.query_failures, "The refused part is recorded"


def test_blank_node_edges_of_a_subject_refused_alone_keep_their_grouped_query(monkeypatch):
    """Blank-node labels do not hold between requests, so the pairs of a blank node cannot be
    joined to its edges: those edges are discovered with the grouped query of the subject."""
    data = DATA + '<urn:w> <urn:r> [ <urn:k> "k" ] .\n'

    def mine_with(refuse):
        monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
        with SchemaMiner.from_graph(Dataset().parse(data=data, format="turtle"), delay=0) as miner:
            select = miner.helper.select

            def call(query, *args, purpose="", **kwargs):
                if purpose == "structural/discovery" and refuse(query):
                    raise QueryError("Virtuoso 42000 Error SR171: Transaction timed out")
                return select(query, *args, purpose=purpose, **kwargs)

            monkeypatch.setattr(miner.helper, "select", call)
            result = miner.mine("blank")
            (entry,) = miner.last_report.config["structural_coverage"]
        return result, entry

    plain, _ = mine_with(lambda q: False)
    result, entry = mine_with(
        lambda q: (
            ("FILTER(?s IN" not in q and "FILTER(isBlank(?s))" not in q)
            or ("<urn:w>" in q and "isBlank(?s) || isBlank(?o)" not in q)
        )
    )
    assert entry["undiscovered_triples"] == 0
    assert sorted(p.model_dump_json(exclude={"examples"}) for p in result.structural_patterns) == (
        sorted(p.model_dump_json(exclude={"examples"}) for p in plain.structural_patterns)
    )
    assert any(p.object_kind == "BlankNode" for p in result.structural_patterns)


SAME_SHAPE = """
<urn:a> a <urn:A> ; <urn:q> "x" .
<urn:u> <urn:q> "y" .
<urn:w> <urn:q> "z" .
"""


def _mine(monkeypatch, data, select_hook):
    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(Dataset().parse(data=data, format="turtle"), delay=0) as miner:
        select = miner.helper.select

        def call(query, *args, purpose="", **kwargs):
            return select_hook(select, query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", call)
        result = miner.mine("hook")
        (entry,) = miner.last_report.config["structural_coverage"]
        return result, entry, miner.last_report


def test_a_refused_subject_with_a_discovered_pattern_is_not_a_gap(monkeypatch):
    """The recount of a pattern counts the edges of every subject with its property set, also
    of a subject whose discovery was refused (PlantMetWiki: 1,855,293 recounted of 1,855,296
    uncovered, 354 not discovered, was reported as more than the 1,854,942 expected)."""

    def hook(select, query, *args, purpose="", **kwargs):
        refused = ("FILTER(?s IN" not in query and purpose == "structural/discovery") or (
            "<urn:w>" in query and purpose in ("structural/discovery", "structural/discovery-pairs")
        )
        if refused:
            raise QueryError("Virtuoso 42000 Error SR171: Transaction timed out")
        return select(query, *args, purpose=purpose, **kwargs)

    result, entry, report = _mine(monkeypatch, SAME_SHAPE, hook)
    assert entry["undiscovered_triples"] == 1 and entry["undiscovered_triples_in_patterns"] == 1
    assert "unaccounted_triples" not in entry
    assert not any("account for" in gap.message for gap in report.measurement_gaps)
    assert [p.count for p in result.structural_patterns] == [2]


def test_a_wrong_property_set_of_a_grouped_discovery_is_discovered_again(monkeypatch):
    """Virtuoso gave PlantMetWiki pathways property sets with properties missing in the grouped
    discovery of 2,478 subjects; the patterns recounted to 0. The property is discovered again
    from pairs, and its patterns are those of a correct discovery."""
    plain, *_ = _mine(monkeypatch, DATA, lambda select, q, *a, **k: select(q, *a, **k))

    def hook(select, query, *args, purpose="", **kwargs):
        answer = select(query, *args, purpose=purpose, **kwargs)
        if purpose == "structural/discovery":
            for row in answer["results"]["bindings"]:
                if "ss" in row and row["ss"]["value"].count(">"):
                    row["ss"] = {"type": "literal", "value": row["ss"]["value"].split(">")[0]}
        return answer

    result, entry, report = _mine(monkeypatch, DATA, hook)
    assert entry["discovered_again_from_pairs"]
    assert all(n["unconfirmed_after"] == 0 for n in entry["discovered_again_from_pairs"].values())
    assert "inconsistent_patterns" not in entry
    assert sorted(p.model_dump_json(exclude={"examples"}) for p in result.structural_patterns) == (
        sorted(p.model_dump_json(exclude={"examples"}) for p in plain.structural_patterns)
    )
