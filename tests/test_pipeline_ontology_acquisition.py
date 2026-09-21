import json
from pathlib import Path
from types import SimpleNamespace

from pydantic import BaseModel

from scripts.pipeline_stages.base import Stage
from scripts.pipeline_stages.config import PipelineConfig
from rdfsolve.evidence.ontology import OntologyGraphCandidate
from rdfsolve.mining.ontology_discovery import OntologyDiscoverySummary


class Pattern(BaseModel):
    subject_class: str
    property_uri: str
    object_class: str | None = None


class About:
    ontology_graph_uris: list[str] | None = None


class Schema:
    def __init__(self):
        self.patterns = [
            Pattern(
                subject_class="http://purl.obolibrary.org/obo/CHEBI_15377",
                property_uri="http://example.org/p",
            )
        ]
        self.about = About()

    def get_classes(self):
        return ["http://purl.obolibrary.org/obo/CHEBI_15377"]

    def get_properties(self):
        return ["http://example.org/p"]


def test_pipeline_writes_discovery_and_usage_scoped_acquisition(monkeypatch, tmp_path: Path):
    config = PipelineConfig()
    config.discover_ontology_graphs = True
    stage = Stage(config)
    candidate = OntologyGraphCandidate(
        graph_uri="http://example.org/ontology",
        explicit_ontology_iris=["http://purl.obolibrary.org/obo/chebi.owl"],
        candidate_reasons=["ontology_structure_probe"],
        observed_class_overlap=["http://purl.obolibrary.org/obo/CHEBI_15377"],
    )
    summary = OntologyDiscoverySummary(
        endpoint="https://example.org/sparql",
        observed_at="2026-09-18T00:00:00Z",
        discovered_named_graphs=1,
        scanned_named_graphs=1,
        candidates=[candidate],
    )
    monkeypatch.setattr(
        "rdfsolve.mining.ontology_discovery.discover_remote_ontology_graphs",
        lambda *args, **kwargs: summary,
    )
    helper = SimpleNamespace(endpoint_url="https://example.org/sparql")
    schema = Schema()
    stage._save_ontology_discovery(
        schema,
        tmp_path,
        "demo",
        "_remote",
        helper=helper,
        mining_context="remote_endpoint",
    )

    assert (tmp_path / "demo_remote_ontology_discovery.json").exists()
    acquisition = json.loads((tmp_path / "demo_remote_ontology_acquisition.json").read_text())
    assert acquisition["dataset_id"] == "demo"
    chebi = next(x for x in acquisition["candidates"] if x["namespace"].endswith("CHEBI_"))
    assert chebi["ontology_id"] == "chebi"
    assert chebi["graph_evidence"][0]["graph_uri"] == "http://example.org/ontology"
    assert chebi["graph_evidence"][0]["access_context"] == "remote_endpoint"
    assert chebi["reference_sources"][0]["source_url"].endswith("/chebi.owl")
    assert schema.about.ontology_graph_uris == ["http://example.org/ontology"]
