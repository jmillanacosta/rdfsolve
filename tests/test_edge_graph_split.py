from rdfsolve.config import mint
from rdfsolve.mining.edge_graph_split import split_by_edge_graph
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

SUBSTANCE = "urn:graph:substance"
COMPOUND = "urn:graph:compound"


def _group() -> MinedSchema:
    return MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class="urn:Substance",
                property_uri="urn:cid",
                object_class="urn:Compound",
                count=5,
                graphs={SUBSTANCE: 5},
                distinct_subjects=5,
                distinct_objects=4,
            ),
            SchemaPattern(
                subject_class="urn:Compound",
                property_uri="urn:label",
                object_class="Literal",
                count=7,
                graphs={SUBSTANCE: 2, COMPOUND: 5},
                distinct_subjects=6,
            ),
            SchemaPattern(subject_class="urn:X", property_uri="urn:p", object_class="Resource"),
        ],
        about=AboutMetadata.build(
            dataset_name="pubchem.ftp",
            graph_uris=[SUBSTANCE, COMPOUND],
            started_at="2026-09-21T10:00:00+00:00",
            content_sha256="abc",
        ),
    )


def test_the_dataset_gets_its_own_identity_and_counts():
    part = split_by_edge_graph(
        _group(), COMPOUND, "pubchem.ftp.compound", declared_classes=frozenset({"urn:Compound"})
    )
    about = part.about
    assert about.dataset_name == "pubchem.ftp.compound"
    assert about.graph_uris == [COMPOUND]
    assert about.snapshot_id == mint(
        "snapshot", "pubchem.ftp.compound", "2026-09-21T10:00:00+00:00"
    )
    assert about.content_sha256 is None
    assert about.schema_uri == mint("schema", "pubchem.ftp.compound")
    assert (about.pattern_count, about.class_count, about.declared_class_count) == (1, 1, 1)
    assert part.navigation is None
