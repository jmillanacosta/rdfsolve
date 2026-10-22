"""rdfsolve.registry_rdf: the source registry and its identity decisions as RDF."""

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from rdflib import OWL, RDF, RDFS, Graph, Literal, URIRef
from rdflib.compare import isomorphic
from rdflib.namespace import DCTERMS, FOAF, PROV

from rdfsolve.cli import main
from rdfsolve.dataset_identity import read_overrides, read_registry
from rdfsolve.models.source_model import SourceModel
from rdfsolve.registry_rdf import (
    DCAT,
    FIELDS,
    RDFSOLVE,
    SCHEMA,
    SD,
    SPDX,
    TERMS,
    VOID,
    load_registry,
    registry_to_rdf,
    write_registry_rdf,
)
from rdfsolve.vocab import unregistered_terms

BASE = "https://w3id.org/rdfsolve/"
EP = "https://example.org/sparql"
REGISTRY = Path(__file__).parents[1] / "data" / "sources.yaml"
# Properties whose objects must be IRIs, and those whose objects must be literals.
IRI_VALUED = {
    VOID.sparqlEndpoint,
    VOID.dataDump,
    SD.endpoint,
    SD.name,
    SD.namedGraph,
    SD.graph,
    SD.defaultDataset,
    DCAT.endpointURL,
    DCAT.downloadURL,
    DCAT.distribution,
    DCAT.landingPage,
    DCAT.mediaType,
    DCAT.compressFormat,
    DCAT.packageFormat,
    DCAT.dataset,
    DCAT.service,
    DCAT.record,
    DCTERMS.license,
    DCTERMS.isReferencedBy,
    DCTERMS.isPartOf,
    OWL.differentFrom,
    DCAT.servesDataset,
    DCTERMS.requires,
    DCTERMS.type,
    SCHEMA.subjectOf,
}
LITERAL_VALUED = {
    DCTERMS.identifier,
    DCTERMS.title,
    DCAT.keyword,
    VOID.uriSpace,
    VOID.uriRegexPattern,
    SCHEMA.alternateName,
}
OBO = "http://purl.obolibrary.org/obo/"
AUTOMATIC_ASSERTION = URIRef(OBO + "ECO_0000203")
MANUAL_ASSERTION = URIRef(OBO + "ECO_0000218")
HAS_CURATION_STATUS = URIRef(OBO + "IAO_0000114")
PENDING_FINAL_VETTING = URIRef(OBO + "IAO_0000125")
SAMPLE = URIRef("http://semanticscience.org/resource/SIO_001050")
ONTOLOGY = URIRef("http://publications.europa.eu/resource/authority/dataset-type/ONTOLOGY")
# Terms this writer no longer writes or declares: each has a standard replacement.
REPLACED = {
    "basis",
    "decidedBy",
    "identityReviewComplete",
    "identityCandidateCount",
    "datasetKind",
    "bioregistryRecord",
    "alias",
    "typeContextGraph",
    "ontologyGraph",
    "sampledInputs",
    "queryExamples",
}


def _registry(tmp_path: Path) -> tuple[Path, Path]:
    """Write a registry that uses most fields, with overrides deciding each relation kind."""
    entries = [
        {
            "name": "demo",
            "aliases": ["demo.old"],
            "catalogs": ["rdfportal"],
            "catalog_local_name": "demo",
            "dataset_kind": "instance",
            "endpoint": EP,
            "graph_uris": ["https://example.org/graph/a", "https://example.org/graph/b"],
            "ontology_graph_uris": ["https://example.org/graph/onto"],
            "graph_sources": {
                "https://example.org/graph/a": {"download_ttl": ["https://example.org/a.ttl.gz"]},
                "https://example.org/graph/b": {"download_nt": ["https://example.org/b.nt"]},
                "https://example.org/graph/onto": {"download_owl": ["https://example.org/o.owl"]},
            },
            "sampled_graphs": {"https://example.org/graph/b": "first file only"},
            "uri_formats": ["https://example.org/thing/$1"],
            "keywords": ["chemistry"],
            "notes": "A curated note.",
            "sparql_examples": {
                "shacl_graph_in_endpoint": ["https://example.org/graph/examples"],
                "shacl_dumps": ["../local/examples.ttl"],
                "link_to_repository": "https://github.com/example/queries",
            },
            "bioregistry_prefix": "demo",
            "bioregistry_name": "Demo Resource",
            "bioregistry_description": "A demo.",
            "bioregistry_homepage": "https://example.org/",
            "bioregistry_license": "CC-BY-4.0",
            "bioregistry_uri_prefix": "https://example.org/thing/",
            "bioregistry_publications": [{"doi": "10.1000/xyz"}, {"pubmed": "123"}],
            "kg_registry_id": "demo-kg",
            "chunk_size": 100,
            "endpoint_status": "up",
        },
        {
            "name": "demo.mirror",
            "endpoint": "https://mirror.example.org/sparql",
            "download_nt": ["https://mirror.example.org/all.nt"],
            "download_tgz": ["https://mirror.example.org/all.tgz"],
            "checksums": {"https://mirror.example.org/all.nt": {"md5": "0" * 32}},
            "bioregistry_license": "hpo",
        },
        {"name": "demo.part", "endpoint": EP, "graph_uris": ["https://example.org/graph/a"]},
        {"name": "demo.dump", "download_nt": ["https://dump.example.org/all.nt"]},
        {
            "name": "onto",
            "dataset_kind": "ontology",
            "type_context_graph_uris": ["https://example.org/graph/types"],
            "bioregistry_prefix": "demo",
        },
        {"name": "lone", "endpoint": "https://lone.example.org/sparql"},
        {"name": "lone.copy", "download_nt": ["https://lone-copy.example.org/all.nt"]},
        {"name": "other", "endpoint": EP, "graph_uris": ["https://example.org/graph/z"]},
        {"name": "old.release", "download_ttl": ["https://example.org/old.ttl"]},
        {"name": "conversion", "endpoint": "https://conv.example.org/sparql"},
        {"name": "twin", "endpoint": "https://twin.example.org/sparql"},
        {"name": "twin.copy", "endpoint": "https://twin.example.org/sparql/"},
        {
            "name": "svc",
            "source_role": "service",
            "endpoint": EP,
            "graph_uris": ["https://example.org/graph/a"],
            "catalogs": ["rdfportal"],
            "download_ttl": ["https://example.org/void.ttl"],
        },
    ]
    sources = tmp_path / "sources.yaml"
    sources.write_text(yaml.safe_dump(entries), encoding="utf-8")
    overrides = tmp_path / "identity_overrides.yaml"
    overrides.write_text(
        yaml.safe_dump(
            [
                {
                    "left": "demo",
                    "right": "demo.mirror",
                    "relation": "distribution_of",
                    "original": "demo",
                    "note": "m",
                },
                {
                    "left": "demo.dump",
                    "right": "demo",
                    "relation": "distribution_of",
                    "original": "demo",
                },
                {"left": "lone", "right": "lone.copy", "relation": "distribution_of"},
                {
                    "left": "demo",
                    "right": "old.release",
                    "relation": "version_of",
                    "newer": "demo",
                },
                {"left": "conversion", "right": "demo", "relation": "same_upstream"},
                {"left": "other", "right": "twin", "relation": "same_dataset"},
            ]
        ),
        encoding="utf-8",
    )
    return sources, overrides


def _node(name: str, kind: str = "dataset") -> URIRef:
    return URIRef(f"{BASE}{kind}/{name}")


def _graph(tmp_path: Path) -> Graph:
    sources, overrides = _registry(tmp_path)
    entries, resolution = load_registry(sources, overrides)
    return registry_to_rdf(entries, resolution, base_uri=BASE)


def test_the_registry_round_trips_through_turtle_and_json_ld(tmp_path):
    sources, overrides = _registry(tmp_path)
    ttl, jsonld = tmp_path / "out" / "registry.ttl", tmp_path / "out" / "registry.jsonld"
    graph = write_registry_rdf(sources, [ttl, jsonld], overrides, base_uri=BASE)
    assert isomorphic(graph, Graph().parse(ttl, format="turtle"))
    assert isomorphic(graph, Graph().parse(jsonld, format="json-ld"))
    again = tmp_path / "again.ttl"
    write_registry_rdf(sources, [again], overrides, base_uri=BASE)
    assert again.read_bytes() == ttl.read_bytes(), "The same registry gives the same file"


def test_each_entry_is_one_node_and_services_are_services(tmp_path):
    graph = _graph(tmp_path)
    names = [row["name"] for row in read_registry(tmp_path / "sources.yaml")]
    datasets = set(graph.subjects(RDF.type, DCAT.Dataset))
    services = set(graph.subjects(RDF.type, SD.Service))
    for name in names:
        nodes = set(graph.subjects(DCTERMS.identifier, Literal(name))) & (datasets | services)
        assert len(nodes) == 1, name
    assert services == {_node("svc", "service")}
    assert not datasets & services
    assert len(datasets | services) == len(names)
    service = _node("svc", "service")
    assert graph.value(service, SD.endpoint) == URIRef(EP)
    assert (service, RDF.type, DCAT.DataService) in graph
    assert (service, VOID.sparqlEndpoint, None) not in graph
    assert (service, VOID.dataDump, None) not in graph, "A service is not a dataset"
    default = graph.value(service, SD.defaultDataset)
    assert {graph.value(ng, SD.name) for ng in graph.objects(default, SD.namedGraph)} == {
        URIRef("https://example.org/graph/a")
    }


def test_graph_names_and_their_dumps_are_attached_to_the_entry(tmp_path):
    graph = _graph(tmp_path)
    demo = _node("demo")
    data = {graph.value(ng, SD.name): ng for ng in graph.objects(demo, SD.namedGraph)}
    assert set(data) == {
        URIRef("https://example.org/graph/a"),
        URIRef("https://example.org/graph/b"),
    }
    onto = graph.value(demo, DCTERMS.requires)
    assert graph.value(onto, SD.name) == URIRef("https://example.org/graph/onto")
    assert (onto, RDF.type, SD.NamedGraph) in graph
    assert onto not in data.values(), "An ontology graph is not a data graph"
    types = graph.value(_node("onto"), DCTERMS.requires)
    assert graph.value(types, SD.name) == URIRef("https://example.org/graph/types")
    content = graph.value(data[URIRef("https://example.org/graph/b")], SD.graph)
    assert (content, RDF.type, SD.Graph) in graph
    assert graph.value(content, VOID.dataDump) == URIRef("https://example.org/b.nt")
    assert (content, RDF.type, SAMPLE) in graph, "Sampled inputs: the content is a sample"
    assert graph.value(content, RDFS.comment) == Literal("first file only")
    whole = graph.value(data[URIRef("https://example.org/graph/a")], SD.graph)
    assert (whole, RDF.type, SAMPLE) not in graph
    dumps = set(graph.objects(demo, VOID.dataDump))
    assert dumps == {URIRef("https://example.org/a.ttl.gz"), URIRef("https://example.org/b.nt")}
    assert URIRef("https://example.org/o.owl") not in set(graph.objects(None, DCAT.downloadURL)), (
        "Ontology inputs are not distributions of the data"
    )
    assert not list(graph.triples((None, VOID.inDataset, None)))
    assert graph.value(demo, VOID.uriSpace) == Literal("https://example.org/thing/")
    assert set(graph.objects(demo, SCHEMA.subjectOf)) == {
        URIRef("https://example.org/graph/examples"),
        URIRef("https://github.com/example/queries"),
    }, "A local file path is not an IRI"


def test_distributions_formats_checksums_and_licenses(tmp_path):
    graph = _graph(tmp_path)
    mirror = _node("demo.mirror")
    by_url = {graph.value(d, DCAT.downloadURL): d for d in graph.objects(mirror, DCAT.distribution)}
    nt = by_url[URIRef("https://mirror.example.org/all.nt")]
    assert str(graph.value(nt, DCAT.mediaType)).endswith("application/n-triples")
    checksum = graph.value(nt, SPDX.checksum)
    assert graph.value(checksum, SPDX.algorithm) == SPDX.checksumAlgorithm_md5
    tgz = by_url[URIRef("https://mirror.example.org/all.tgz")]
    assert str(graph.value(tgz, DCAT.packageFormat)).endswith("application/x-tar")
    assert set(graph.objects(mirror, VOID.dataDump)) == {
        URIRef("https://mirror.example.org/all.nt")
    }, "An archive is a distribution, not an RDF dump"
    gz = graph.value(_node("demo"), DCAT.distribution)
    assert graph.value(gz, DCAT.compressFormat) is not None
    assert graph.value(_node("demo"), DCTERMS.license) == URIRef(
        "https://spdx.org/licenses/CC-BY-4.0"
    )
    document = graph.value(mirror, DCTERMS.license)
    assert (document, RDF.type, DCTERMS.LicenseDocument) in graph, "A license that is not SPDX"


def _statements(graph: Graph) -> dict[tuple[URIRef, URIRef], URIRef]:
    return {
        (graph.value(s, RDF.subject), graph.value(s, RDF.object)): s
        for s in graph.subjects(RDF.type, RDF.Statement)
    }


def test_identity_relations_are_asserted_only_when_decided(tmp_path):
    graph = _graph(tmp_path)
    assert (_node("demo"), DCAT.previousVersion, _node("old.release")) in graph, "newer, older"
    assert (_node("old.release"), DCAT.previousVersion, _node("demo")) not in graph
    assert (_node("conversion"), PROV.alternateOf, _node("demo")) in graph
    assert (_node("other"), RDFSOLVE.sameDataset, _node("twin")) in graph
    assert (_node("demo.part"), DCTERMS.isPartOf, _node("demo")) in graph, "graph_scope_of rule"
    assert (_node("demo"), OWL.differentFrom, _node("other")) in graph, "distinct rule"
    statements = _statements(graph)
    version = statements[(_node("demo"), _node("old.release"))]
    assert graph.value(version, RDF.predicate) == DCAT.previousVersion, "Statement as asserted"
    candidate = statements[(_node("twin"), _node("twin.copy"))]
    predicate = graph.value(candidate, RDF.predicate)
    assert (_node("twin"), predicate, _node("twin.copy")) not in graph, "Candidates not asserted"


def test_identity_statements_carry_their_assertion_method_and_curation_status(tmp_path):
    graph = _graph(tmp_path)
    statements = _statements(graph)
    override = statements[(_node("demo.mirror"), _node("demo"))]
    assert graph.value(override, DCTERMS.type) == MANUAL_ASSERTION
    assert set(graph.objects(override, RDFS.comment)) == {
        Literal("curated override"),
        Literal("m"),
    }, "The basis and the note are comments"
    rule = statements[(_node("demo.part"), _node("demo"))]
    assert graph.value(rule, DCTERMS.type) == AUTOMATIC_ASSERTION
    assert graph.value(rule, HAS_CURATION_STATUS) is None
    candidate = statements[(_node("twin"), _node("twin.copy"))]
    assert graph.value(candidate, HAS_CURATION_STATUS) == PENDING_FINAL_VETTING
    assert graph.value(candidate, DCTERMS.type) is None, "A candidate is not asserted"
    _, resolution = load_registry(tmp_path / "sources.yaml", tmp_path / "identity_overrides.yaml")
    pending = set(graph.subjects(HAS_CURATION_STATUS, PENDING_FINAL_VETTING))
    assert len(pending) == len(resolution.candidates), "One pending statement per candidate"
    registry = next(s for s in graph.subjects(RDF.type, DCAT.Catalog) if "/registry/" in str(s))
    assert set(graph.predicates(registry)) == {RDF.type, DCTERMS.identifier, DCTERMS.title} | {
        DCAT.dataset,
        DCAT.service,
    }, "Review state and candidate count are derived from the statements, not stored"


def test_a_distribution_with_a_recorded_original_is_written_from_the_original(tmp_path):
    graph = _graph(tmp_path)
    demo, mirror, dump = _node("demo"), _node("demo.mirror"), _node("demo.dump")
    assert (mirror, DCAT.servesDataset, demo) in graph, "An endpoint mirror serves the original"
    assert (mirror, RDF.type, DCAT.DataService) in graph
    assert (mirror, RDF.type, DCAT.Dataset) in graph
    assert (demo, DCAT.distribution, mirror) not in graph
    assert (demo, DCAT.distribution, dump) in graph, "A dump mirror is a distribution"
    assert {(dump, RDF.type, DCAT.Distribution), (dump, RDF.type, DCAT.Dataset)} <= set(graph)
    assert (demo, RDF.type, DCAT.Distribution) not in graph, "The original stays a dataset"
    assert not set(graph.triples((demo, RDFSOLVE.distributionOf, None)))
    statements = _statements(graph)
    assert graph.value(statements[(demo, dump)], RDF.predicate) == DCAT.distribution
    lone, copy = _node("lone"), _node("lone.copy")
    assert (lone, RDFSOLVE.distributionOf, copy) in graph, "No original: the rdfsolve term"
    assert (copy, RDF.type, DCAT.Distribution) not in graph


def test_an_original_must_name_one_entry_of_a_distribution_pair(tmp_path):
    overrides = tmp_path / "overrides.yaml"
    for item, message in [
        ({"relation": "distribution_of", "original": "c"}, "must name one of"),
        ({"relation": "same_dataset", "original": "a"}, "distribution_of only"),
    ]:
        overrides.write_text(yaml.safe_dump([{"left": "a", "right": "b", **item}]))
        with pytest.raises(ValueError, match=message):
            read_overrides(overrides)
    overrides.write_text(
        yaml.safe_dump([{"left": "a", "right": "b", "relation": "distribution_of", "original": "b"}])
    )
    (relation,) = read_overrides(overrides)
    assert (relation.left, relation.right, relation.original) == ("a", "b", "b")


def test_dataset_kinds_with_a_standard_value_are_typed(tmp_path):
    graph = _graph(tmp_path)
    assert graph.value(_node("onto"), DCTERMS.type) == ONTOLOGY
    assert graph.value(_node("demo"), DCTERMS.type) is None, "No standard value for instance"
    assert graph.value(_node("other"), DCTERMS.type) is None, "unknown is not written"


def test_only_the_rdfsolve_terms_still_used_are_declared(tmp_path):
    graph = _graph(tmp_path)
    declared = {str(s) for s in graph.subjects(RDF.type, RDF.Property)}
    assert declared == {str(RDFSOLVE.sameDataset), str(RDFSOLVE.distributionOf)}
    assert not set(TERMS) & REPLACED
    used = {str(p) for p in graph.predicates()} | {str(o) for o in graph.objects()}
    assert not {str(RDFSOLVE[name]) for name in REPLACED} & used


def test_catalog_records_name_the_entry_in_each_catalog(tmp_path):
    graph = _graph(tmp_path)
    rdfportal = URIRef(f"{BASE}catalog/rdfportal")
    assert (rdfportal, DCAT.dataset, _node("demo")) in graph
    assert (rdfportal, DCAT.service, _node("svc", "service")) in graph
    record = URIRef(f"{BASE}catalog/kg-registry/record/demo")
    assert graph.value(record, DCTERMS.identifier) == Literal("demo-kg")
    assert (_node("demo"), SCHEMA.alternateName, Literal("demo.old")) in graph


def test_the_bioregistry_record_is_a_catalog_record(tmp_path):
    graph = _graph(tmp_path)
    bioregistry = URIRef(f"{BASE}catalog/bioregistry")
    assert (bioregistry, RDF.type, DCAT.Catalog) in graph
    assert (bioregistry, DCAT.dataset, _node("demo")) in graph
    record = URIRef("https://bioregistry.io/registry/demo")
    assert (bioregistry, DCAT.record, record) in graph
    assert (record, RDF.type, DCAT.CatalogRecord) in graph
    assert graph.value(record, DCTERMS.identifier) == Literal("demo")
    assert set(graph.objects(record, FOAF.primaryTopic)) == {_node("demo"), _node("onto")}, (
        "Entries that share a prefix share its record"
    )


def test_an_access_field_that_is_not_an_iri_is_refused(tmp_path):
    sources = tmp_path / "sources.yaml"
    sources.write_text(yaml.safe_dump([{"name": "bad", "endpoint": "not an iri"}]))
    with pytest.raises(ValueError, match="bad.endpoint"):
        write_registry_rdf(sources, [tmp_path / "r.ttl"])


def test_every_registry_field_is_mapped_or_left_out_with_a_reason():
    declared = set(FIELDS)
    fields = set(SourceModel.model_fields)
    keys = {key for row in read_registry(REGISTRY) for key in row}
    sidecar = json.loads(REGISTRY.with_suffix(".metadata.json").read_text(encoding="utf-8"))
    keys |= {key for row in sidecar["sources"].values() for key in row}
    unlisted = {
        key for key in fields | keys if key not in declared and not key.startswith("download_")
    }
    assert not unlisted


def _check_node_kinds(graph: Graph) -> None:
    for predicate in IRI_VALUED:
        assert all(isinstance(o, URIRef) for o in graph.objects(None, predicate)), predicate
    for predicate in LITERAL_VALUED:
        assert all(isinstance(o, Literal) for o in graph.objects(None, predicate)), predicate


def test_the_project_registry_serializes_and_parses(tmp_path):
    entries, resolution = load_registry(REGISTRY)
    graph = registry_to_rdf(entries, resolution, base_uri=BASE)
    parsed = Graph().parse(data=graph.serialize(format="turtle"), format="turtle")
    assert len(parsed) == len(graph)
    datasets = set(parsed.subjects(RDF.type, DCAT.Dataset))
    services = set(parsed.subjects(RDF.type, SD.Service))
    assert len(entries) == len(datasets) + len(services)
    assert len(services) == sum(entry.source_role == "service" for entry in entries)
    for entry in entries:
        nodes = set(parsed.subjects(DCTERMS.identifier, Literal(entry.name)))
        assert len(nodes & (datasets | services)) == 1, entry.name
    _check_node_kinds(parsed)
    own = {str(RDFSOLVE[name]) for name in TERMS}
    assert unregistered_terms(parsed, own) == set()
    pending = set(parsed.subjects(HAS_CURATION_STATUS, PENDING_FINAL_VETTING))
    assert len(pending) == len(resolution.candidates)
    assert (not pending) is resolution.review_complete


def test_registry_cli_and_release_build_write_the_registry_rdf(tmp_path):
    sources, overrides = _registry(tmp_path)
    runner = CliRunner()
    out = tmp_path / "cli.ttl"
    result = runner.invoke(
        main, ["registry", "rdf", "--sources", str(sources), "--output", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert len(Graph().parse(out)) > 0
    built = runner.invoke(main, ["release", "build", str(tmp_path), "--release-id", "test"])
    assert built.exit_code == 0, built.output
    manifest = json.loads((tmp_path / "release.json").read_text(encoding="utf-8"))
    roles = {a["path"]: a["role"] for a in manifest["artifacts"]}
    assert roles["registry.ttl"] == "source_registry_rdf"
