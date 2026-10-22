"""rdfsolve.mappings.sssom: mapping sets with rdfsolve's prefix, no assumed creator, and verified
links written as SSSOM with CURIEs, the sample and the rewrite."""

import json

import pytest
from sssom import Mapping
from sssom.context import ensure_converter
from sssom.parsers import parse_sssom_table
from sssom.writers import write_table

from rdfsolve.mappings.signatures import Link, LinkEvidence
from rdfsolve.mappings.sssom import create_sssom_mappings, links_to_sssom
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

G, P = "https://example.org/genes/", "http://purl.uniprot.org/core/"
MAPPING = Mapping(
    subject_id="rdfsolve:A",
    predicate_id="skos:relatedMatch",
    object_id="rdfsolve:B",
    mapping_justification="semapv:ManualMappingCuration",
)


@pytest.fixture
def no_defaults(monkeypatch):
    monkeypatch.setattr("sssom.context.get_converter", lambda: ensure_converter(use_defaults=False))


def test_rdfsolve_prefix_and_no_assumed_creator(no_defaults):
    result = create_sssom_mappings([MAPPING], "https://example.org/mappings")
    assert result.converter.expand("rdfsolve:A") == "https://w3id.org/rdfsolve/A"
    assert result.converter.compress("https://w3id.org/rdfsolve/B") == "rdfsolve:B"
    assert "creator_id" not in result.metadata, "The creator comes from the caller only"
    orcid = "https://orcid.org/0000-0000-0000-0000"
    assert create_sssom_mappings(
        [MAPPING], "https://example.org/mappings", creator_id=orcid
    ).metadata["creator_id"] == [orcid]


def test_links_become_a_mapping_set_of_curies(tmp_path):
    schemas = {
        "genes": MinedSchema(
            about=AboutMetadata.build(dataset_name="genes"),
            patterns=[
                SchemaPattern(
                    subject_class=G + "Gene", property_uri=G + "xref", object_class="Resource"
                )
            ],
            prefixes={"gene": G},
        ),
        "proteins": MinedSchema(
            about=AboutMetadata.build(dataset_name="proteins"),
            patterns=[
                SchemaPattern(
                    subject_class=P + "Protein", property_uri=P + "mnemonic", object_class="Literal"
                )
            ],
        ),
    }
    link = Link("join", "genes", G + "Gene", G + "xref", "uniprot", "proteins", P + "Protein")
    found = [
        LinkEvidence(link, 50, n, {"http://purl.uniprot.org/uniprot/{id}": n}, []) for n in (48, 5)
    ]
    mapping_set = links_to_sssom(found, schemas, "https://example.org/links", min_share=0.5)
    path = tmp_path / "links.sssom.tsv"
    with path.open("w") as handle:
        write_table(mapping_set, handle)
    (row,) = parse_sssom_table(path).df.to_dict("records")
    assert (row["subject_id"], row["predicate_id"]) == ("gene:Gene", "skos:relatedMatch")
    assert row["object_id"].endswith(":Protein") and "://" not in row["object_id"]
    assert row["mapping_justification"] == "semapv:InstanceBasedMatching"
    assert (row["similarity_score"], row["subject_match_field"]) == (0.96, "gene:xref")
    other = json.loads(row["other"])
    assert (other["sampled"], other["found"], other["identifier_type"]) == (50, 48, "uniprot")
    assert other["target_forms"] == {"http://purl.uniprot.org/uniprot/{id}": 48}
    assert other["identity_flags"] == {} and other["identity_statements_checked"] == 0
    assert "rdfsolve" in mapping_set.converter.prefix_map, "subject_source names the dataset"
