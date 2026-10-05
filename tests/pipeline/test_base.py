"""scripts.pipeline_stages.base: stages keep restriction patterns, clean service data, and acquire
ontologies."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import BaseModel
from rdflib import Graph

from rdfsolve import SchemaMiner
from rdfsolve.ontology.discovery import OntologyDiscoverySummary, OntologyGraphCandidate
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from scripts.pipeline_stages import cli
from scripts.pipeline_stages.base import Stage, restriction_scope
from scripts.pipeline_stages.config import PipelineConfig
from tests.test_local_rdf import DATA


class AnyStage(Stage):
    name = "any"

    def _execute(self):
        return {}


def test_the_stage_writes_the_restriction_patterns(tmp_path):
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path,
        repo_dir=tmp_path,
        sources_file=registry,
        output_dir=tmp_path / "run",
        restriction_patterns=True,
        navigation_hops=0,
        output_formats=["json"],
    )
    schema = MinedSchema(about=AboutMetadata.build(dataset_name="x"), patterns=[])
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        AnyStage(config)._save_schema_outputs(schema, tmp_path, "x", "_local", helper=miner.helper)
    written = json.loads((tmp_path / "x_local_schema.json").read_text())
    written = written.get("schema", written)
    assert written["restriction_patterns"]["state"] == "complete"
    assert len(written["restriction_patterns"]["patterns"]) == 5


def test_the_scope_has_the_data_graphs_and_the_ontology_graphs():
    about = AboutMetadata.build(dataset_name="x").model_copy(
        update={"graph_uris": ["urn:g:data"], "ontology_graph_uris": ["urn:g:ontology"]}
    )
    assert restriction_scope(about) == ["urn:g:data", "urn:g:ontology"]
    assert restriction_scope(AboutMetadata.build(dataset_name="x")) is None, "The whole dataset"


def test_the_restriction_patterns_option_is_read_from_the_command_line(monkeypatch):
    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--restriction-patterns"])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].restriction_patterns is True


V = "http://www.openlinksw.com/schemas/virtrdf#"
SCHEMA = MinedSchema(
    about=AboutMetadata.build(dataset_name="x"),
    patterns=[
        SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="Literal"),
        SchemaPattern(subject_class=V + "QuadMap", property_uri=V + "item", object_class="Literal"),
    ],
)


def _stage(tmp_path, clean):
    registry = tmp_path / "sources.yaml"
    registry.write_text("[]\n")
    config = PipelineConfig(
        base_dir=tmp_path,
        repo_dir=tmp_path,
        sources_file=registry,
        output_dir=tmp_path / "run",
        clean_service_data=clean,
    )
    return AnyStage(config)


def test_the_stage_removes_service_data_when_asked(tmp_path):
    cleaned = _stage(tmp_path, True)._without_service_data(SCHEMA)
    assert [p.property_uri for p in cleaned.patterns] == ["urn:p"]
    assert cleaned.about.cleaned["patterns_removed"] == 1
    kept = _stage(tmp_path, False)._without_service_data(SCHEMA)
    assert kept is SCHEMA and kept.about.cleaned is None


def test_the_clean_service_data_option_is_read_from_the_command_line(monkeypatch):
    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--clean-service-data"])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].clean_service_data is True


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
        "rdfsolve.ontology.discovery.discover_remote_ontology_graphs",
        lambda *args, **kwargs: summary,
    )
    helper = SimpleNamespace(endpoint_url="https://example.org/sparql")
    schema = Schema()
    stage._save_ontology_discovery(
        schema, tmp_path, "demo", "_remote", helper=helper, mining_context="remote_endpoint"
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


def test_ontology_discovery_takes_the_graphs_from_a_published_service_description(
    monkeypatch, tmp_path: Path
):
    """An endpoint whose published VoID lists its named graphs is not scanned for them, and the
    discovery file says where the graph names came from."""
    from rdfsolve.mining.void_strategy import PublishedVoid
    from rdfsolve.ontology import discovery

    void = Graph().parse(
        data="""
        @prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
        <https://example.org/sparql#service> sd:defaultDataset <https://example.org/sparql#d> .
        <https://example.org/sparql#d> sd:namedGraph [ sd:name <http://example.org/core> ] ,
            [ sd:name <http://example.org/data> ] .
        """,
        format="turtle",
    )
    published = PublishedVoid("https://example.org/.well-known/void", void, "2026-09-02")

    def no_scan(*args, **kwargs):
        raise AssertionError("the endpoint was scanned for graph names")

    monkeypatch.setattr(discovery, "discover_graph_names", no_scan)
    monkeypatch.setattr(discovery, "inspect_remote_ontology_graph", lambda *a, **k: None)
    config = PipelineConfig()
    config.discover_ontology_graphs = True
    helper = SimpleNamespace(endpoint_url="https://example.org/sparql")
    Stage(config)._save_ontology_discovery(
        Schema(),
        tmp_path,
        "demo",
        "_remote",
        helper=helper,
        mining_context="remote_endpoint",
        published_void=published,
    )
    saved = json.loads((tmp_path / "demo_remote_ontology_discovery.json").read_text())
    assert saved["discovered_named_graphs"] == 2
    assert saved["graph_names_source"] == "service_description"
    assert saved["graph_names_evidence"] == (
        "https://example.org/.well-known/void (issued 2026-09-02)"
    )


def test_an_exported_graph_leaves_out_terms_that_are_not_rdf_iris(caplog):
    """A dataset's metadata can name a namespace with a leading space; the graph is written
    without that triple, not refused."""
    from rdflib import Graph, Literal, URIRef

    from scripts.pipeline_stages.base import rdf_only

    graph = Graph()
    graph.add(
        (
            URIRef("urn:dataset"),
            URIRef("http://rdfs.org/ns/void#uriSpace"),
            URIRef(" http://identifiers.org/obo.aeo/"),
        )
    )
    graph.add((URIRef("urn:dataset"), URIRef("http://purl.org/dc/terms/title"), Literal("x")))
    report = SimpleNamespace(config={"iri_findings": {"terms": []}})
    with caplog.at_level("WARNING"):
        written = rdf_only(graph, "metadata", report).serialize(format="turtle")
    assert "obo.aeo" not in written and '"x"' in written
    assert "1 triples with 1 terms that are not RDF IRIs left out" in caplog.text
    # The triples left out are data-quality findings of the report, not only a log line.
    assert report.config["iri_findings"]["graphs"]["metadata"] == {
        "triples_left_out": 1,
        "terms": [{"iri": " http://identifiers.org/obo.aeo/", "triples": 1}],
    }
    assert report.config["iri_findings"]["terms"] == []
