"""Verified links are written as an SSSOM mapping set with CURIEs, the sample and the rewrite."""

import json

from sssom.parsers import parse_sssom_table
from sssom.writers import write_table

from rdfsolve.mappings.signatures import Link, LinkEvidence
from rdfsolve.mappings.sssom import links_to_sssom
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

G, P = "https://example.org/genes/", "http://purl.uniprot.org/core/"


def schema(name, rows, prefixes):
    patterns = [SchemaPattern(subject_class=c, property_uri=p, object_class=o) for c, p, o in rows]
    return MinedSchema(about=AboutMetadata.build(dataset_name=name), patterns=patterns, prefixes=prefixes)


SCHEMAS = {
    "genes": schema("genes", [(G + "Gene", G + "xref", "Resource")], {"gene": G}),
    "proteins": schema("proteins", [(P + "Protein", P + "mnemonic", "Literal")], {}),
}


def evidence(found):
    link = Link("join", "genes", G + "Gene", G + "xref", "uniprot", "proteins", P + "Protein")
    return LinkEvidence(link, 50, found, {"http://purl.uniprot.org/uniprot/{id}": found}, [])


def test_links_become_a_mapping_set_of_curies(tmp_path):
    mapping_set = links_to_sssom(
        [evidence(48), evidence(5)], SCHEMAS, "https://example.org/links", min_share=0.5
    )
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
    assert "rdfsolve" in mapping_set.converter.prefix_map, "subject_source names the dataset"
