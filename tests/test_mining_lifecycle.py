"""Check optional phases and final report state without network requests."""

import json
from unittest.mock import Mock

import pytest
from rdflib import Literal, URIRef
from rdflib.namespace import DCTERMS

from rdfsolve.miner import SchemaMiner
from rdfsolve.mining import mine_with_ontology
from rdfsolve.schema_models import SchemaPattern
from rdfsolve.schema_models.metadata import DatasetDescription, MetadataPatterns
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
@pytest.mark.parametrize("detected", [False, True])
def test_optional_phase_matrix(miner, monkeypatch, ontology, metadata, detected):
    reports = []

    def optional(value):
        def run(self):
            report = miner._report.report
            assert report.finished_at is None
            reports.append(report)
            return value

        return run

    monkeypatch.setattr("rdfsolve.mining.detect_ontology_as_data", lambda *a, **kw: detected)
    monkeypatch.setattr("rdfsolve.mining._query_owl_class_superclasses", lambda *a: ["urn:A"])
    monkeypatch.setattr("rdfsolve.mining.OntologyMiner.mine", optional(OntologyStructure()))
    monkeypatch.setattr("rdfsolve.mining.MetadataMiner.mine", optional(MetadataPatterns()))
    pattern = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
    monkeypatch.setattr(
        "rdfsolve.mining.mine_ontology_as_data_patterns", lambda *a, **kw: [pattern]
    )
    monkeypatch.setattr(
        "rdfsolve.mining.mine_ontology_as_data_subject_patterns", lambda *a, **kw: []
    )
    result = mine_with_ontology(
        miner, ontology, metadata, dataset_name="test", ontology_as_data=True
    )
    report = miner.last_report
    assert report.finished_at
    assert all(item is report for item in reports)
    assert (result.ontology is not None) is ontology
    assert (result.metadata is not None) is metadata
    assert report.pattern_count == len(result.data_schema.patterns) == int(detected)
    assert report.class_count == result.data_schema.about.class_count == (2 if detected else 0)
    assert report.property_count == result.data_schema.about.property_count == int(detected)
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
    monkeypatch.setattr("rdfsolve.mining.detect_ontology_as_data", lambda *a, **kw: False)
    monkeypatch.setattr(
        "rdfsolve.mining.OntologyMiner.mine", Mock(side_effect=RuntimeError("ontology failed"))
    )
    with pytest.raises(RuntimeError, match="ontology failed"):
        mine_with_ontology(miner, extract_ontology=True, dataset_name="test")
    assert miner.last_report.finished_at
    phase = miner.last_report.phases[0]
    assert phase.name == "ontology-extraction"
    assert phase.finished_at
    assert "ontology failed" in phase.error


def test_metadata_export_uses_literal_nodes():
    metadata = MetadataPatterns(
        datasets=[DatasetDescription(uri="urn:dataset", title="Title", description="Text")]
    )
    graph = metadata.to_rdf_graph()
    assert (URIRef("urn:dataset"), DCTERMS.title, Literal("Title")) in graph
    assert (URIRef("urn:dataset"), DCTERMS.description, Literal("Text")) in graph
