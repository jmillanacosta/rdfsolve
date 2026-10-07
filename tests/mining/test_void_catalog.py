"""rdfsolve.mining.void_catalog: a source whose data are VoID descriptions of other datasets
(okn-void) is not mined; its VoID is split by described dataset, matched to the registry, and
given to the matched entries as their published VoID, which VoID-first mining and the agreement
with mined schemas read."""

import gzip

from rdflib import Graph

from rdfsolve.analysis.void_comparison import compare_void_with_mined, restrict_to_graphs
from rdfsolve.mining.void_catalog import (
    catalog_void_of,
    describe_catalog,
    load_catalogs,
    write_catalog,
)
from rdfsolve.mining.void_strategy import VoidStrategy, void_gaps
from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.schema_models.readers.void import void_datasets_of_graphs, void_graph_to_minedschema
from scripts.pipeline_stages.config import PipelineConfig, Source
from scripts.pipeline_stages.remote import RemoteMiningStage

K = "https://kg.example/"
CATALOG = K + "catalog"

# FRINK's VoID of two graphs and of the catalog itself: object class partitions
# (void-ext:objectClassPartition) give the class of the objects; the one without a class holds
# the literals and the objects without a class.
FRINK = """
@prefix void: <http://rdfs.org/ns/void#> .
@prefix void-ext: <http://ldf.fi/void-ext#> .
@prefix pav: <http://purl.org/pav/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <urn:ex:> .

<https://kg.example/alpha> a void:Dataset ; pav:version "v0.0.2" ;
    pav:lastUpdatedOn "2026-09-28T18:32:18Z"^^xsd:dateTime ; void:triples 10 ;
    void:classPartition <https://kg.example/alpha/class/a> ;
    void:propertyPartition <https://kg.example/alpha/property/link> .
<https://kg.example/alpha/property/link> void:property ex:link ; void:triples 7 .
<https://kg.example/alpha/class/a> a void:Dataset ; void:class ex:A ; void:entities 4 ;
    void:propertyPartition <https://kg.example/alpha/class/a/property/link> ,
        <https://kg.example/alpha/class/a/property/type> .
<https://kg.example/alpha/class/a/property/link> void:property ex:link ; void:triples 7 ;
    void-ext:objectClassPartition <https://kg.example/alpha/class/a/property/link/target/b> ,
        <https://kg.example/alpha/class/a/property/link/target/none> ;
    void-ext:datatypePartition <https://kg.example/alpha/class/a/property/link/datatype/s> .
<https://kg.example/alpha/class/a/property/link/target/b> void:class ex:B ; void:triples 3 .
<https://kg.example/alpha/class/a/property/link/target/none> void:triples 4 .
<https://kg.example/alpha/class/a/property/link/datatype/s> void-ext:datatype xsd:string ;
    void:triples 2 .
<https://kg.example/alpha/class/a/property/type> void:property rdf:type ; void:triples 4 ;
    void-ext:objectClassPartition <https://kg.example/alpha/class/a/property/type/target/none> .
<https://kg.example/alpha/class/a/property/type/target/none> void:triples 4 .

<https://kg.example/beta> a void:Dataset ; pav:version "v0.0.1" ;
    void:classPartition <https://kg.example/beta/class/c> .
<https://kg.example/beta/class/c> void:class ex:C ; void:entities 2 ;
    void:propertyPartition <https://kg.example/beta/class/c/property/name> .
<https://kg.example/beta/class/c/property/name> void:property ex:name ; void:triples 2 ;
    void-ext:datatypePartition <https://kg.example/beta/class/c/property/name/datatype/s> .
<https://kg.example/beta/class/c/property/name/datatype/s> void-ext:datatype xsd:string ;
    void:triples 2 .

<https://kg.example/gamma> a void:Dataset ;
    void:classPartition <https://kg.example/gamma/class/d> .
<https://kg.example/gamma/class/d> void:class ex:D ; void:entities 1 ;
    void:propertyPartition <https://kg.example/gamma/class/d/property/name> .
<https://kg.example/gamma/class/d/property/name> void:property ex:name ; void:triples 1 .

<https://kg.example/catalog> a void:Dataset ;
    void:classPartition <https://kg.example/catalog/class/ds> .
<https://kg.example/catalog/class/ds> void:class void:Dataset ; void:entities 9 .
"""


def _registry():
    return [
        Source.from_dict(
            {
                "name": "catalog",
                "dataset_kind": "catalog",
                "endpoint": "https://kg.example/catalog/sparql",
                "graph_uris": [CATALOG],
            }
        ),
        # Matched by its graph, beside its own VoID graph that the catalog does not describe.
        Source.from_dict({"name": "alpha", "graph_uris": [K + "alpha", K + "alpha#void"]}),
        Source.from_dict({"name": "alpha.data", "graph_uris": [K + "alpha"]}),
        # Matched by the IRI the registry gives of its dataset (no graphs: the whole endpoint).
        Source.from_dict(
            {
                "name": "beta",
                "endpoint": "https://kg.example/beta/sparql",
                "void_iri": K + "beta",
            }
        ),
    ]


def _catalog():
    void = Graph().parse(data=FRINK, format="turtle")
    return describe_catalog("catalog", void, _registry(), own_graphs=[CATALOG])


def test_a_catalog_is_read_not_mined():
    (catalog, *_) = _registry()
    assert not catalog.mining_enabled
    assert SourceModel(name="x", dataset_kind="instance").mining_enabled


def test_the_described_datasets_are_matched_to_the_registry():
    catalog = _catalog()
    found = {d.iri: [(m.name, m.basis) for m in d.entries] for d in catalog.datasets}
    assert found == {
        K + "alpha": [("alpha", ["graph"]), ("alpha.data", ["graph"])],
        K + "beta": [("beta", ["void_iri"])],
        K + "catalog": [],
        K + "gamma": [],
    }
    record = catalog.record()
    assert record["datasets_described"] == 3, "The catalog's own description is apart"
    assert record["datasets_matched"] == 2
    (alpha,) = [d for d in catalog.datasets if d.iri == K + "alpha"]
    assert (alpha.version, alpha.updated, alpha.class_partitions) == (
        "v0.0.2",
        "2026-09-28T18:32:18+00:00",
        1,
    )


def test_an_entry_gets_the_void_of_its_datasets_only_when_all_its_graphs_are_described():
    catalog = _catalog()
    alpha, data, beta = (s for s in _registry()[1:])
    assert catalog.for_source(alpha) is None, "alpha#void is not described by the catalog"
    void, datasets = catalog.for_source(data)
    assert [d.iri for d in datasets] == [K + "alpha"]
    assert K + "beta" not in {str(s) for s in void.subjects()}, "Scoped to alpha"
    void, datasets = catalog.for_source(beta)
    assert [d.iri for d in datasets] == [K + "beta"]


def test_object_class_partitions_give_the_links_and_the_objects_without_a_class():
    catalog = _catalog()
    void, _ = catalog.for_source(_registry()[2])
    schema = void_graph_to_minedschema(void, report_untyped=False)
    found = {(p.subject_class, p.property_uri, p.object_class, p.count) for p in schema.patterns}
    assert found == {
        ("urn:ex:A", "urn:ex:link", "urn:ex:B", 3),
        ("urn:ex:A", "urn:ex:link", "Literal", 2),
        ("urn:ex:A", "urn:ex:link", "Resource", 2),
    }, "4 objects without a class, 2 of them literals; rdf:type is membership"
    assert schema.about.class_entity_counts == {"urn:ex:A": 4}
    gaps = {(g.kind, g.subject_class, g.triples) for g in void_gaps(void)}
    assert gaps == {("objects", "urn:ex:A", 2)}, "The typed objects are no gap"


def test_a_graph_without_a_service_description_is_its_own_dataset():
    void = Graph().parse(data=FRINK, format="turtle")
    assert [str(d) for d in void_datasets_of_graphs(void, [K + "alpha"])] == [K + "alpha"]
    assert void_datasets_of_graphs(void, [K + "alpha#void"]) == []


def test_the_catalog_is_read_from_its_local_files_and_written_per_entry(tmp_path):
    nt = Graph().parse(data=FRINK, format="turtle").serialize(format="nt")
    dump = tmp_path / "g0001.nt.gz"
    with gzip.open(dump, "wt", encoding="utf-8") as f:
        f.write(nt)
    registry = _registry()
    registry[0] = Source.from_dict(
        {
            "name": "catalog",
            "dataset_kind": "catalog",
            "graph_uris": [CATALOG],
            "graph_sources": {CATALOG: {"download_nt": [f"file://{dump}"]}},
        }
    )
    (catalog,) = load_catalogs(registry)
    assert catalog.read_from.startswith("local files")
    record = write_catalog(catalog, registry, tmp_path / "out")
    alpha = record["entries"]["alpha"]
    assert (alpha["void_first"], alpha["graphs_not_described"]) == (False, [K + "alpha#void"])
    assert catalog_void_of(tmp_path / "out", "alpha") is None, (
        "A VoID of part of the entry's graphs is for the agreement only, not for VoID-first"
    )
    assert record["entries"]["alpha.data"]["void_first"] is True
    assert record["entries"]["beta"]["void"] == "catalog/beta.void.nt"
    published = catalog_void_of(tmp_path / "out", "beta")
    assert published.read_by == "catalog catalog"
    assert published.graph == f"catalog: {K}beta"
    assert len(published.void) == record["entries"]["beta"]["triples"]
    assert catalog_void_of(tmp_path / "out", "gamma") is None


def test_the_remote_stage_takes_the_catalog_void_when_the_endpoint_publishes_none(tmp_path):
    registry = _registry()
    catalog = describe_catalog(
        "catalog", Graph().parse(data=FRINK, format="turtle"), registry, own_graphs=[CATALOG]
    )
    write_catalog(catalog, registry, tmp_path / "catalogs")
    config = PipelineConfig(base_dir=tmp_path, output_dir=tmp_path / "run", enrich=False)
    beta = registry[3]
    stage = RemoteMiningStage(config)
    stage._published_void = {beta.endpoint: None}
    assert stage._void_strategy(beta, None) is None, "Without --void-catalogs it is mined"
    config.void_catalogs = tmp_path / "catalogs"
    strategy = stage._void_strategy(beta, None)
    assert isinstance(strategy, VoidStrategy)
    assert strategy.read_by == "catalog catalog"
    assert strategy.void_graph == f"catalog: {K}beta"


def test_a_mined_schema_is_cut_to_the_described_graphs_before_the_agreement():
    mined = [
        SchemaPattern(
            subject_class="urn:ex:A",
            property_uri="urn:ex:link",
            object_class="urn:ex:B",
            count=5,
            graphs={K + "alpha": 3, K + "alpha#void": 2},
        ),
        SchemaPattern(
            subject_class="urn:void:Dataset",
            property_uri="urn:void:triples",
            object_class="Literal",
            count=9,
            graphs={K + "alpha#void": 9},
        ),
    ]
    cut = restrict_to_graphs(mined, {K + "alpha"})
    assert [(p.property_uri, p.count) for p in cut] == [("urn:ex:link", 3)]
    void = [
        SchemaPattern(
            subject_class="urn:ex:A", property_uri="urn:ex:link", object_class="urn:ex:B", count=3
        )
    ]
    result = compare_void_with_mined(
        void, cut, void_class_counts={"urn:ex:A": 4}, mined_class_counts={"urn:ex:A": 5}
    )
    assert result.counts.within_1_percent == 1
    assert result.class_counts is not None
    assert (result.class_counts.compared, result.class_counts.median_ratio) == (1, 0.8)


def test_an_endpoint_void_of_one_dataset_without_service_description_is_the_endpoint_s():
    from rdfsolve.mining.void_strategy import PublishedVoid, void_for_source

    void = Graph().parse(data=FRINK, format="turtle")
    catalog = describe_catalog("catalog", void, _registry(), own_graphs=[CATALOG])
    own, _ = catalog.for_source(_registry()[3])
    scoped = void_for_source(PublishedVoid("urn:void", own, None), None)
    assert scoped is not None, "FRINK publishes each graph's VoID in its endpoint, without SD"
    assert void_for_source(PublishedVoid("urn:void", void, None), None) is None, (
        "A VoID of several datasets without a service description names none of them"
    )


def test_a_partition_whose_class_is_not_an_iri_is_left_out_and_the_rest_is_read(caplog):
    data = FRINK.replace(
        "<https://kg.example/alpha/class/a/property/link/target/b> void:class ex:B",
        "<https://kg.example/alpha/class/a/property/link/target/b> void:class <schema:Geo>",
    )
    void = Graph().parse(data=data, format="turtle")
    schema = void_graph_to_minedschema(void, report_untyped=False)
    objects = {p.object_class for p in schema.patterns if p.subject_class == "urn:ex:A"}
    assert objects == {"Literal", "Resource"}
    assert "schema:Geo" in caplog.text


def test_each_partition_left_out_is_counted_with_its_reason():
    data = FRINK.replace(
        "<https://kg.example/alpha/class/a/property/link/target/b> void:class ex:B",
        "<https://kg.example/alpha/class/a/property/link/target/b> void:class <schema:Geo>",
    )
    left_out = []
    void_graph_to_minedschema(Graph().parse(data=data, format="turtle"), left_out=left_out)
    assert left_out == [
        {
            "reason": "object_class: IRI with an unregistered scheme",
            "term": "schema:Geo",
            "subject_class": "urn:ex:A",
            "property": "urn:ex:link",
            "object": "schema:Geo",
            "triples": 3,
        }
    ]
