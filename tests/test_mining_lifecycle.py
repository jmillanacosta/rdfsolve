"""Check optional phases and final report state without network requests."""

import json
from unittest.mock import Mock

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import DCTERMS

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining import mine_with_ontology
from rdfsolve.schema_models import SchemaPattern
from rdfsolve.schema_models.metadata import MetadataDocument
from rdfsolve.schema_models.ontology import OntologyStructure


@pytest.fixture
def miner(tmp_path, monkeypatch):
    result = SchemaMiner(
        "https://example.org/sparql", counts=False, report_path=tmp_path / "report.json"
    )
    monkeypatch.setattr(result, "_run_patterns_phase", lambda: ([], None))
    monkeypatch.setattr(result, "_run_labels_phase", lambda patterns: (patterns, set()))
    monkeypatch.setattr(result, "_query_declared_classes", lambda: {"urn:A"})
    monkeypatch.setattr(result, "query_dataset_metadata", lambda: {})
    return result


@pytest.mark.parametrize("ontology", [False, True])
@pytest.mark.parametrize("metadata", [False, True])
@pytest.mark.parametrize("probed", [False, True])
def test_optional_phase_matrix(miner, monkeypatch, ontology, metadata, probed):
    reports = []

    def optional(value):
        def run(self):
            report = miner._report.report
            assert report.finished_at is None
            reports.append(report)
            return value

        return run

    monkeypatch.setattr("rdfsolve.mining.OntologyMiner.mine", optional(OntologyStructure()))
    monkeypatch.setattr("rdfsolve.mining.MetadataMiner.mine", optional(MetadataDocument(graph=Graph())))
    pattern = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
    monkeypatch.setattr(
        "rdfsolve.mining.ontology_as_data.probe_term_patterns",
        lambda *a, **kw: [pattern] if probed else [],
    )
    result = mine_with_ontology(
        miner, ontology, metadata, dataset_name="test", ontology_as_data=True
    )
    report = miner.last_report
    assert report.finished_at
    assert all(item is report for item in reports)
    assert (result.ontology is not None) is ontology
    assert (result.metadata is not None) is metadata
    assert report.pattern_count == len(result.data_schema.patterns) == int(probed)
    assert report.class_count == result.data_schema.about.class_count == (2 if probed else 0)
    assert report.property_count == result.data_schema.about.property_count == int(probed)
    assert result.data_schema.about.finished_at == report.finished_at
    if ontology:
        assert report.ontology_extraction is not None
    saved = json.loads(miner._report_path.read_text())
    assert saved["pattern_count"] == report.pattern_count
    assert saved["finished_at"] == report.finished_at


def test_failed_metadata_query_does_not_drop_schema(miner, monkeypatch):
    monkeypatch.setattr(
        miner, "query_dataset_metadata", Mock(side_effect=RuntimeError("metadata failed"))
    )
    schema = miner.mine("test")
    assert schema.patterns == []
    assert miner.last_report.finished_at
    assert any(phase.error == "metadata failed" for phase in miner.last_report.phases)


def test_report_counts_final_filtered_schema(miner, monkeypatch):
    monkeypatch.setattr(
        "rdfsolve.schema_models.core.SERVICE_NAMESPACE_PREFIXES",
        ("http://www.openlinksw.com/schemas/virtrdf#",),
    )
    pattern = SchemaPattern(
        subject_class="http://www.openlinksw.com/schemas/virtrdf#QuadMap",
        property_uri="urn:p",
        object_class="urn:B",
    )
    monkeypatch.setattr(miner, "_run_patterns_phase", lambda: ([pattern], None))
    schema = miner.mine("test")
    assert schema.patterns == []
    assert schema.about.pattern_count == miner.last_report.pattern_count == 0
    assert schema.about.class_count == miner.last_report.class_count == 0
    assert schema.about.property_count == miner.last_report.property_count == 0


def test_failed_run_replaces_previous_report(miner, monkeypatch):
    miner.mine("first")
    first = miner.last_report
    monkeypatch.setattr(
        miner, "_run_patterns_phase", Mock(side_effect=RuntimeError("mining failed"))
    )
    with pytest.raises(RuntimeError, match="mining failed"):
        miner.mine("second")
    assert miner.last_report is not first
    assert miner.last_report.dataset_name == "second"
    assert miner.last_report.finished_at
    assert "mining failed" in miner.last_report.abort_reason


def test_new_run_clears_injected_ontology_classes(miner):
    miner._ontology_classes = ["urn:stale"]
    miner.mine("test")
    assert not miner._ontology_classes


def test_failed_optional_phase_keeps_its_report(miner, monkeypatch):
    monkeypatch.setattr(
        "rdfsolve.mining.OntologyMiner.mine", Mock(side_effect=RuntimeError("ontology failed"))
    )
    with pytest.raises(RuntimeError, match="ontology failed"):
        mine_with_ontology(miner, extract_ontology=True, dataset_name="test")
    assert miner.last_report.finished_at
    phase = next(p for p in miner.last_report.phases if p.name == "ontology-extraction")
    assert phase.finished_at
    assert "ontology failed" in phase.error


def test_metadata_export_uses_literal_nodes():
    source = Graph()
    source.add((URIRef("urn:dataset"), DCTERMS.title, Literal("Title")))
    source.add((URIRef("urn:dataset"), DCTERMS.description, Literal("Text")))
    metadata = MetadataDocument(graph=source)
    graph = metadata.to_rdf_graph()
    assert (URIRef("urn:dataset"), DCTERMS.title, Literal("Title")) in graph
    assert (URIRef("urn:dataset"), DCTERMS.description, Literal("Text")) in graph


def test_bounded_graph_uses_real_mining_queries():
    graph = Graph().parse(data="@prefix e: <urn:mine:> . e:a a e:A; e:link e:b. e:b a e:B.", format="turtle")
    with SchemaMiner.from_graph(graph, counts=False, delay=0, strategy="one-shot") as miner:
        schema = miner.mine("local-fixture")
        assert ("urn:mine:A", "urn:mine:link", "urn:mine:B") in {
            (p.subject_class, p.property_uri, p.object_class) for p in schema.patterns}
        assert schema.prefixes["e"] == "urn:mine:"
        assert miner.last_report.finished_at


def test_an_anonymous_object_class_becomes_one_blank_node_pattern(monkeypatch):
    """Virtuoso returns nodeID:// for anonymous classes; aggregate, do not drop."""
    from rdfsolve.mining.strategy import MiningContext
    from rdfsolve.mining.two_phase_strategy import TwoPhaseStrategy

    rows = [
        {
            "class": {"type": "uri", "value": "urn:C"},
            "p": {"type": "uri", "value": "urn:p"},
            "oc": {"type": "bnode", "value": "nodeID://b215908"},
        },
        {
            "class": {"type": "uri", "value": "urn:C"},
            "p": {"type": "uri", "value": "urn:p"},
            "oc": {"type": "bnode", "value": "nodeID://b215909"},
        },
        {
            "class": {"type": "uri", "value": "urn:C"},
            "p": {"type": "uri", "value": "urn:p"},
            "oc": {"type": "uri", "value": "urn:D"},
        },
    ]
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-17T00:00:00+00:00")
    helper = Mock()
    helper.select.return_value = {
        "results": {"bindings": [{"class": {"type": "uri", "value": "urn:C"}}]}
    }
    context = MiningContext(
        helper=helper,
        graph_uris=None,
        report=miner._report,
        collect_bindings=lambda *a, **kw: [],
    )
    monkeypatch.setattr(
        "rdfsolve.mining.two_phase_strategy.query_with_bisect",
        lambda classes, graphs, build_fn, purpose, *a, **kw: __import__(
            "rdfsolve._outcomes", fromlist=["QueryOutcome"]
        ).QueryOutcome(rows if "typed-object" in purpose else [], "complete", []),
    )
    patterns = TwoPhaseStrategy().mine(context)
    assert sorted(p.object_class for p in patterns) == ["BlankNode", "urn:D"]
    blank = next(p for p in patterns if p.object_class == "BlankNode")
    assert (blank.subject_class, blank.property_uri) == ("urn:C", "urn:p")
    assert miner._report.report.dropped_invalid_uris == 0


def test_all_strategies_exclude_shacl_executable_classes_from_empirical_patterns():
    from rdflib import Literal, Namespace, RDF

    SH = Namespace("http://www.w3.org/ns/shacl#")
    EX = Namespace("urn:ex:")
    graph = Graph()
    graph.add((EX.item, RDF.type, EX.DomainClass))
    graph.add((EX.item, EX.p, Literal("domain")))
    graph.add((EX.query, RDF.type, SH.SPARQLSelectExecutable))
    graph.add((EX.query, SH.select, Literal("SELECT * WHERE { ?s ?p ?o }")))

    for strategy in ("one-shot", "single-pass", "two-phase"):
        with SchemaMiner.from_graph(graph, counts=False, delay=0, strategy=strategy) as miner:
            schema = miner.mine(dataset_name=f"test-{strategy}")
        assert all(
            pattern.subject_class != str(SH.SPARQLSelectExecutable)
            for pattern in schema.patterns
        )
        assert any(pattern.subject_class == str(EX.DomainClass) for pattern in schema.patterns)


@pytest.mark.parametrize("strategy", ["one-shot", "single-pass", "two-phase"])
def test_mining_sets_pattern_types(strategy):
    from rdfsolve.schema_models.pattern import PatternType

    graph = Graph().parse(data='''
        @prefix e: <urn:kind:> .
        e:a a e:A; e:link e:b; e:text "value"; e:unknown e:c; e:node [e:p "x"] .
        e:b a e:B .
    ''', format="turtle")
    with SchemaMiner.from_graph(graph, counts=False, delay=0, strategy=strategy) as miner:
        schema = miner.mine("kinds")
    kinds = {p.property_uri: p.pattern_type for p in schema.patterns}
    assert kinds["urn:kind:link"] == PatternType.OBJECT_PROPERTY
    assert kinds["urn:kind:unknown"] == PatternType.OBJECT_PROPERTY
    assert kinds["urn:kind:text"] == PatternType.DATATYPE_PROPERTY
    assert kinds["urn:kind:node"] == PatternType.BLANK_NODE_PROPERTY
    assert PatternType.UNKNOWN not in kinds.values()


def test_failed_counts_keep_patterns_and_report_partial(tmp_path, monkeypatch):
    from rdfsolve.sparql_helper import EndpointError

    graph = Graph().parse(data='@prefix e: <urn:count:> . e:a a e:A; e:p "x" .', format="turtle")
    path = tmp_path / "report.json"
    with SchemaMiner.from_graph(graph, delay=0, report_path=path) as miner:
        select = miner.helper.select

        def fail_counts(query, **kwargs):
            if kwargs.get("purpose") == "counts/literal":
                assert json.loads(path.read_text())["completion_state"] == "unfinished"
                raise EndpointError("Count query failed")
            return select(query, **kwargs)

        monkeypatch.setattr(miner.helper, "select", fail_counts)
        schema = miner.mine("count")
    assert schema.patterns
    assert next(p for p in schema.patterns if p.property_uri == "urn:count:p").count is None
    report = json.loads(path.read_text())
    assert report["completion_state"] == "partial"
    assert report["finished_at"]
    assert report["pattern_count"] == len(schema.patterns)
    assert any(f["purpose"] == "counts/literal" for f in report["query_failures"])


def test_error_after_pattern_discovery_keeps_report_count(miner, monkeypatch):
    pattern = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="Literal")
    monkeypatch.setattr(miner, "_run_patterns_phase", lambda: ([pattern], None))
    monkeypatch.setattr(miner, "_run_labels_phase", Mock(side_effect=RuntimeError("labels failed")))
    with pytest.raises(RuntimeError, match="labels failed"):
        miner.mine("test")
    assert miner.last_report.pattern_count == 1
    assert miner.last_report.completion_state == "partial"
