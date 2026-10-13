"""rdfsolve.mappings.instances and rdfsolve.mappings.keys: external mapping rows whose ends are
instances of the datasets (records named by an identifier, or values of a property) give
instance mappings, linksets and class mappings derived from the instances, counted in local
stores and asserted by namespace elsewhere."""

import csv
import json

import polars as pl
import pytest
from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from rdfsolve.analysis.connectivity import build_connectivity
from rdfsolve.identifiers import parse
from rdfsolve.mappings.instances import (
    DerivedClassMapping,
    InstanceSettings,
    derive_instance_mappings,
    derived_predicate,
    lower_share,
    read_derived_mappings,
    read_instance_links,
)
from rdfsolve.mappings.keys import IdentifierReader
from rdfsolve.mining.scan import store_from_graph
from rdfsolve.schema_models.enrichment import RdfTerm
from tests.mappings.data import schema

ENSEMBL = "https://identifiers.org/ensembl/"
GENE = "https://identifiers.org/ncbigene/"
VOID = "http://rdfs.org/ns/void#"

# Records of one dataset that cite genes: by Ensembl IRI, by a bare Ensembl literal, and one by
# the gene IRI that the other dataset also writes (a shared entity).
NODES = f"""
<http://example.org/dn/1> a <http://example.org/DataNode> ; <http://example.org/ensembl> <{ENSEMBL}ENSG00000141510> .
<http://example.org/dn/2> a <http://example.org/DataNode> ; <http://example.org/ensembl> <{ENSEMBL}ENSG00000012048> .
<http://example.org/dn/3> a <http://example.org/DataNode> ; <http://example.org/ensemblId> "ENSG00000139618" .
<http://example.org/dn/4> a <http://example.org/DataNode> ; <http://example.org/entrez> <{GENE}7157> ; <http://example.org/ensembl> <{ENSEMBL}ENSG00000141510> .
<http://example.org/dn/5> a <http://example.org/DataNode> .
"""
# Records named by their gene IRI.
GENES = f"""
<{GENE}7157> a <http://example.org/Gene> ; <http://example.org/self> <{GENE}7157> .
<{GENE}672> a <http://example.org/Gene> .
<{GENE}675> a <http://example.org/Gene> .
<{GENE}9999> a <http://example.org/Gene> .
"""
MAPPINGS = """#mapping_set_id: https://example.org/genes.sssom.tsv
#curie_map:
#  NCBIGene: http://identifiers.org/ncbigene/
#  ensembl: http://identifiers.org/ensembl/
#  skos: http://www.w3.org/2004/02/skos/core#
#  semapv: https://w3id.org/semapv/vocab/
subject_id\tpredicate_id\tobject_id\tmapping_justification\tconfidence
ensembl:ENSG00000141510\tskos:exactMatch\tNCBIGene:7157\tsemapv:ManualMappingCuration\t
ensembl:ENSG00000012048\tskos:exactMatch\tNCBIGene:672\tsemapv:ManualMappingCuration\t0.8
NCBIGene:675\tskos:exactMatch\tensembl:ENSG00000139618\tsemapv:UnspecifiedMatching\t
NCBIGene:675\trdfs:subClassOf\tensembl:ENSG00000139618\tsemapv:UnspecifiedMatching\t
NCBIGene:1\tskos:exactMatch\tensembl:ENSG00000000001\tsemapv:UnspecifiedMatching\t
"""


@pytest.fixture
def corpus(tmp_path):
    """Two local datasets with stores, one dataset known by its schema alone, and a mapping set."""
    stores = {}
    for name, data in (("nodes", NODES), ("genes", GENES)):
        stores[name] = store_from_graph(
            Graph().parse(format="turtle", data=data), tmp_path / name / "scan-store"
        ).path
    schemas = {
        "nodes": schema(
            "nodes",
            examples=[
                (
                    "http://example.org/DataNode",
                    "http://example.org/ensembl",
                    "http://example.org/dn/1",
                    ENSEMBL + "ENSG00000141510",
                ),
                (
                    "http://example.org/DataNode",
                    "http://example.org/ensemblId",
                    "http://example.org/dn/3",
                    "ensembl:ENSG00000139618",
                ),
            ],
        ),
        "genes": schema(
            "genes",
            examples=[
                ("http://example.org/Gene", "http://example.org/label", GENE + "7157", "TP53")
            ],
        ),
        "remote": schema(
            "remote",
            examples=[
                (
                    "http://example.org/Locus",
                    "http://example.org/gene",
                    "http://example.org/l/1",
                    GENE + "1",
                )
            ],
        ),
    }
    downloads = tmp_path / "downloads"
    (downloads / "genes").mkdir(parents=True)
    (downloads / "genes" / "genes.sssom.tsv").write_text(MAPPINGS)
    registry = [
        {
            "name": "genes",
            "url": "https://example.org/genes.sssom.tsv",
            "confidence": 0.9,
            "oxo_mapping_set_id": "https://example.org/oxo/genes",
        },
        {
            "name": "relations",
            "url": "https://example.org/r.sssom.tsv",
            "type": "property_mappings",
        },
    ]
    return registry, schemas, stores, downloads, tmp_path / "out"


def run(corpus, **options):
    registry, schemas, stores, downloads, out = corpus
    summary = derive_instance_mappings(
        registry, schemas, stores, out, downloads=downloads, **options
    )
    return summary, out / "mappings"


def test_identifier_keys_read_iris_and_curies_as_parse_does():
    iris = [
        GENE + "7157",
        "http://identifiers.org/ncbigene:7157",
        "http://purl.obolibrary.org/obo/CHEBI_15377",
        "https://identifiers.org/kegg.compound/C00031",
        "https://identifiers.org/kegg.compound/D00014",  # not a compound: read by Bioregistry
        "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:15377",
        "http://identifiers.org/ensembl/ENSG00000141510",
        "http://example.org/not/an/identifier",
        GENE + "not-a-number",
    ]
    reader = IdentifierReader()
    found = reader.iri_keys(pl.LazyFrame({"iri": iris}), "iri").collect()
    expected = [(p.curie if (p := parse(i)) and p.valid is not False else None) for i in iris]
    assert found["key"].to_list() == expected
    curies = ["NCBIGene:7157", "chebi:CHEBI:15377", "MESH:D000001", "nope:1", GENE + "1"]
    keyed = reader.curie_keys(pl.LazyFrame({"c": curies}), "c").collect()
    assert keyed["key"].to_list() == [
        "ncbigene:7157",
        "chebi:15377",
        "mesh:D000001",
        None,
        "ncbigene:1",
    ]
    bare = reader.local_keys(pl.LazyFrame({"v": ["7157", "x"]}), "v", "ncbigene").collect()
    assert bare["key"].to_list() == ["ncbigene:7157", None], "A local id must be valid"
    only = IdentifierReader({"ncbigene"})
    assert only.iri_keys(pl.LazyFrame({"iri": iris[2:4]}), "iri").collect()["key"].to_list() == [
        None,
        None,
    ], "A reader limited to some prefixes reads no other"


def test_identifier_keys_match_parse_on_random_registered_iris():
    import bioregistry

    iris = []
    for prefix in sorted(bioregistry.read_registry())[::7]:
        example = bioregistry.get_example(prefix)
        iri = bioregistry.get_iri(prefix, example) if example else None
        if iri:
            iris.append(iri)
    found = IdentifierReader().iri_keys(pl.LazyFrame({"iri": iris}), "iri").collect()
    expected = [(p.curie if (p := parse(i)) and p.valid is not False else None) for i in iris]
    agree = sum(a == b for a, b in zip(found["key"].to_list(), expected, strict=True))
    assert agree / len(iris) > 0.97


def test_lower_share_weighs_small_classes_down():
    assert lower_share(1, 1) == pytest.approx(0.2065, abs=1e-4)
    assert lower_share(2134, 2741) == pytest.approx(0.763, abs=1e-3)
    assert lower_share(0, 10) == 0.0
    assert lower_share(3, 0) is None


def test_predicate_follows_the_coverage_of_both_classes():
    assert derived_predicate(1.0, 0.96) == "skos:exactMatch"
    assert derived_predicate(1.0, 0.5) == "skos:broadMatch", "The covered source is narrower"
    assert derived_predicate(0.5, 1.0) == "skos:narrowMatch"
    assert derived_predicate(0.5, 0.5) == "skos:relatedMatch"
    assert derived_predicate(None, None) == "skos:relatedMatch", "No counts: related only"
    assert derived_predicate(0.9, 0.9, exact=0.8) == "skos:exactMatch"
    with pytest.raises(ValueError, match="between 0 and 1"):
        InstanceSettings(exact=1.5)


def test_rows_classified_and_instance_mappings_between_datasets(corpus):
    from sssom.parsers import parse_sssom_table

    summary, mappings = run(corpus)
    source = summary["sources"]["genes"]
    assert "relations" not in summary["sources"], "Property mapping sets are left out"
    assert source["rows"] == 5
    assert source["keyed"] == 4, "The hierarchy row is not a mapping"
    assert source["classified"]["subject"] == {"instance": 3, "namespace": 1}
    assert summary["datasets"]["remote"]["membership"] == "namespace"
    table = parse_sssom_table(mappings / "instance" / "genes.sssom.tsv")
    rows = {
        (r.subject_id, r.object_id, r.subject_source, r.object_source, r.confidence)
        for r in table.df.itertuples()
    }
    assert rows == {
        (
            "ensembl:ENSG00000141510",
            "ncbigene:7157",
            "rdfsolve:dataset/nodes",
            "rdfsolve:dataset/genes",
            0.95,
        ),
        (
            "ensembl:ENSG00000012048",
            "ncbigene:672",
            "rdfsolve:dataset/nodes",
            "rdfsolve:dataset/genes",
            0.8,
        ),
        (
            "ncbigene:675",
            "ensembl:ENSG00000139618",
            "rdfsolve:dataset/genes",
            "rdfsolve:dataset/nodes",
            0.9,
        ),
    }, "Each row once per dataset pair; the set's default for an unspecified justification"
    assert table.metadata["mapping_set_source"] == ["https://example.org/genes.sssom.tsv"]
    assert table.metadata["see_also"] == ["https://example.org/oxo/genes"]
    other = json.loads(table.df["other"].iloc[0])
    assert other["validation"] == "validated"


def test_linksets_count_bridges_and_keep_shared_entities_apart(corpus):
    summary, mappings = run(corpus)
    graph = Graph().parse(mappings / "linksets" / "instance-linksets.ttl")
    linksets = list(graph.subjects(RDF.type, URIRef(VOID + "Linkset")))
    triples = {
        (
            str(graph.value(node, URIRef(VOID + "subjectsTarget"))).rsplit("/", 1)[-1],
            str(graph.value(node, URIRef(VOID + "objectsTarget"))).rsplit("/", 1)[-1],
            graph.value(node, URIRef(VOID + "triples")),
        )
        for node in linksets
    }
    # genes sorts first: gene 7157 to dn/1 and dn/4, 672 to dn/2, 675 to dn/3; dn/4 also writes
    # gene 7157 as the genes dataset does, so that pair is a shared entity, not a bridge. The
    # dataset without a store is linked by namespace, without a count.
    assert {(a, b, n if n is None else int(n)) for a, b, n in triples} == {
        ("genes", "nodes", 3),
        ("nodes", "remote", None),
    }
    assert summary["sources"]["genes"]["shared_pairs"] == 1
    shared = (mappings / "linksets" / "shared-entities.tsv").read_text().splitlines()
    assert shared[1].split("\t")[:5] == ["genes", "nodes", "skos:exactMatch", "genes", "1"]
    sources = {
        str(o)
        for node in linksets
        for o in graph.objects(node, URIRef("http://purl.org/dc/terms/source"))
    }
    assert "https://example.org/genes.sssom.tsv" in sources


def test_record_links_derived_from_instances(corpus):
    _, mappings = run(corpus)
    assert not read_derived_mappings(mappings / "derived" / "instance-derived.sssom.tsv"), (
        "Data nodes cite genes through a property: no class mapping"
    )
    links = read_instance_links(mappings / "derived" / "instance-links.tsv")
    counted = {
        (d.source_class, d.target_class, d.target_property): d
        for d in links
        if d.validation != "asserted"
    }
    by_ensembl = counted[
        "http://example.org/Gene", "http://example.org/DataNode", "http://example.org/ensembl"
    ]
    # Genes 7157 and 672 of four take part; dn/1, dn/2, dn/4 of five data nodes.
    assert by_ensembl.source_property is None, "The gene record itself takes part"
    assert by_ensembl.source_coverage == pytest.approx(2 / 4)
    assert by_ensembl.target_coverage == pytest.approx(3 / 5)
    assert by_ensembl.pairs == 3
    assert by_ensembl.predicate is None
    assert by_ensembl.similarity == pytest.approx(2 / (4 + 5 - 2), abs=1e-4), "Written to 4 places"
    # The mean confidence of the rows times the larger share, at its lower bound: too few
    # records to validate.
    assert by_ensembl.confidence == pytest.approx((0.95 + 0.8) / 2 * lower_share(3, 5), abs=1e-4)
    assert by_ensembl.validation == "candidate"
    assert by_ensembl.mapping_sets == ("genes",)
    assert not [d for d in links if d.source_property == "http://example.org/self"], (
        "A value that names its own record is the record"
    )
    literal = counted[
        "http://example.org/Gene", "http://example.org/DataNode", "http://example.org/ensemblId"
    ]
    assert literal.pairs == 1, "A bare literal of a property that carries the type is read"
    assert literal.validation == "candidate", "Below the support threshold"
    same = counted[
        "http://example.org/Gene", "http://example.org/DataNode", "http://example.org/entrez"
    ]
    assert same.mapping_sets == ("same_identifier",), "The same identifier, without a mapping set"
    assert same.confidence == pytest.approx(max(lower_share(1, 4), lower_share(1, 5)), abs=1e-4)
    asserted = [d for d in links if d.validation == "asserted"]
    assert {(d.source_dataset, d.target_dataset, d.pairs) for d in asserted} == {
        ("nodes", "remote", None)
    }, "A dataset without a store is matched by namespace, without counts"
    assert "same_identifier" not in {s for d in asserted for s in d.mapping_sets}, (
        "An identifier two stores hold says nothing of a dataset known by namespace"
    )
    edges = {d.source_class: d.edge() for d in asserted}
    assert edges["http://example.org/DataNode"]["validation"] == "asserted"
    assert edges["http://example.org/DataNode"]["edge_type"] == "INSTANCE_LINK"


LOCI = f"""
<{GENE}7157> a <http://example.org/Locus> .
<{GENE}672> a <http://example.org/Locus> .
<{GENE}9999> a <http://www.w3.org/2002/07/owl#Class> .
<{ENSEMBL}ENSG00000139618> a <http://example.org/Transcript> .
<http://example.org/rec/1> a <http://example.org/Record> ;
    <http://purl.org/dc/terms/identifier> <{GENE}7157> .
"""


def test_class_mappings_derived_from_the_records(corpus):
    registry, schemas, stores, downloads, out = corpus
    stores = {
        "genes": stores["genes"],
        "loci": store_from_graph(
            Graph().parse(format="turtle", data=LOCI), out.parent / "loci" / "scan-store"
        ).path,
    }
    schemas = {"genes": schemas["genes"], "loci": schema("loci")}
    summary = derive_instance_mappings(registry, schemas, stores, out, downloads=downloads)
    derived = {
        d.target_class: d
        for d in read_derived_mappings(out / "mappings" / "derived" / "instance-derived.sssom.tsv")
    }
    assert set(derived) == {
        "http://example.org/Locus",
        "http://example.org/Transcript",
        "http://example.org/Record",
    }, "Classes of the modelling languages are left out"
    locus = derived["http://example.org/Locus"]
    # Genes 7157 and 672 of four are loci; every locus is a gene: the gene is broader.
    assert (locus.source_coverage, locus.target_coverage) == (0.5, 1.0)
    assert locus.predicate == "skos:narrowMatch"
    assert locus.mapping_sets == ("same_identifier",)
    assert locus.confidence == pytest.approx(lower_share(2, 2), abs=1e-4), "Two loci weigh as two"
    assert locus.validation == "candidate"
    transcript = derived["http://example.org/Transcript"]
    assert transcript.mapping_sets == ("genes",)
    assert transcript.confidence == pytest.approx(0.9 * lower_share(1, 1), abs=1e-4), "The set's default"
    record = derived["http://example.org/Record"]
    assert record.source_property is None and record.target_property is None, (
        "An identity property names its record: one mapping per class pair"
    )
    assert record.pairs == 1
    assert summary["derived_by_predicate"] == {"skos:narrowMatch": 3}
    assert not (out / "mappings" / "instance" / "same_identifier.sssom.tsv").exists(), (
        "An identifier joined with itself is not published as a mapping"
    )

    lenient = derive_instance_mappings(
        registry,
        schemas,
        stores,
        out.parent / "lenient",
        downloads=downloads,
        settings=InstanceSettings(exact=0.5),
    )
    assert lenient["derived_by_predicate"]["skos:exactMatch"] >= 1


def test_derived_mappings_are_network_edges(corpus):
    _, mappings = run(corpus)
    derived = read_instance_links(mappings / "derived" / "instance-links.tsv")
    schemas = corpus[1]
    graph = build_connectivity(schemas, derived=derived)
    edges = [d for _, _, d in graph.edges(data=True) if d["kind"] == "instance_derived"]
    assert edges
    assert all(e["edge_type"] == "INSTANCE_LINK" for e in edges)
    assert all(e["knowledge_level"] == "observation" for e in edges)
    assert all(e["agent_type"] == "data_analysis_pipeline" for e in edges)
    evidence = {e["validation"]: e["evidence"] for e in edges}
    assert evidence == {"candidate": "tested", "asserted": "plausible"}
    assert all(e["predicate_edge_type"] is None for e in edges)


def test_a_derived_mapping_without_both_classes_in_the_network_is_left_out():
    mapping = DerivedClassMapping(
        "a",
        "http://example.org/X",
        "b",
        "http://example.org/Y",
        "skos:exactMatch",
        "validated",
        0.9,
    )
    graph = build_connectivity(
        {
            "a": schema(
                "a",
                examples=[
                    ("http://example.org/X", "http://example.org/p", "http://example.org/x", "v")
                ],
            )
        },
        derived=[mapping],
    )
    assert not [d for _, _, d in graph.edges(data=True) if d["kind"] == "instance_derived"]


def test_remote_namespaces_come_from_class_examples(corpus):
    registry, schemas, stores, downloads, out = corpus
    remote = schema("remote")
    remote.enrichment.class_examples["http://example.org/Locus"] = [
        RdfTerm(kind="uri", value=GENE + "1")
    ]
    summary = derive_instance_mappings(
        registry, {**schemas, "remote": remote}, stores, out, downloads=downloads
    )
    assert summary["record_links_by_validation"]["asserted"] == 2


def test_inputs_take_the_local_schema_and_its_store(tmp_path):
    from types import SimpleNamespace

    from rdfsolve.mappings.instances import instance_inputs

    store = store_from_graph(Graph().parse(format="turtle", data=GENES), tmp_path / "s").path
    (tmp_path / "genes").mkdir()
    report = {"config": {"scan_store": {"path": str(store)}}}
    (tmp_path / "genes/genes_local_report.json").write_text(json.dumps(report))
    data = tmp_path / "data"
    store_from_graph(
        Graph().parse(format="turtle", data=NODES), data / "qlever_workdirs/nodes/scan-store"
    )
    local, remote = schema("genes"), schema("genes")
    extractions = [
        ("genes", SimpleNamespace(mode="remote", report_path=None), remote),
        (
            "genes",
            SimpleNamespace(mode="local", report_path="genes/genes_local_report.json"),
            local,
        ),
        ("nodes", SimpleNamespace(mode="local", report_path=None), schema("nodes")),
        ("other", SimpleNamespace(mode="remote", report_path=None), schema("other")),
        ("failed", SimpleNamespace(mode="local", report_path=None), None),
    ]
    schemas, stores = instance_inputs(extractions, tmp_path, data)
    assert schemas["genes"] is local, "The local schema is taken"
    assert set(schemas) == {"genes", "nodes", "other"}
    assert stores == {"genes": store, "nodes": data / "qlever_workdirs/nodes/scan-store"}


CLAIMS = f"""
<{GENE}7157> a <http://example.org/Gene> ;
    <http://www.w3.org/2002/07/owl#sameAs> <{ENSEMBL}ENSG00000141510> ,
        <https://identifiers.org/uniprot/P04637> , <{GENE}7158> .
<{ENSEMBL}ENSG00000012048> a <http://example.org/Transcript> .
"""


def test_identity_claims_checked_against_the_mapping_sets(corpus):
    registry, schemas, stores, downloads, out = corpus
    stores = {
        "genes": store_from_graph(
            Graph().parse(format="turtle", data=CLAIMS), out.parent / "claims" / "scan-store"
        ).path,
        "nodes": stores["nodes"],
    }
    schemas = {"genes": schema("genes"), "nodes": schemas["nodes"]}
    summary = derive_instance_mappings(registry, schemas, stores, out, downloads=downloads)
    assert summary["identity_claims"] == {"confirmed": 1, "unconfirmed": 1, "conflicting": 1}
    with (out / "mappings" / "identity-claims" / "genes.sssom.tsv").open() as handle:
        rows = csv.DictReader((line for line in handle if not line.startswith("#")), delimiter="\t")
        checks = {row["object_id"]: json.loads(row["other"])["check"] for row in rows}
    assert checks == {
        "ensembl:ENSG00000141510": "confirmed",
        "uniprot:P04637": "unconfirmed",
        "ncbigene:7158": "conflicting",
    }, "A set states 7157 exactMatch ENSG00000141510; no set relates the gene and the protein"
    void = Graph().parse(out / "mappings" / "identity-claims" / "identity-claims.ttl")
    assert len(list(void.subjects(RDF.type, URIRef(VOID + "Linkset")))) == 3 + 3, (
        "One linkset per claimed prefix and one subset per outcome"
    )
