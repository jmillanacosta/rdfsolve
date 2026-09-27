"""Verified links between datasets become evidence edges of the connectivity graph."""

import pytest

from rdfsolve.analysis import build_connectivity
from rdfsolve.mappings.signatures import Link, LinkEvidence, read_links, write_links
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

UP = "http://purl.uniprot.org/uniprot/"


def schema(name, rows):
    patterns = [SchemaPattern(subject_class=c, property_uri=p, object_class=o) for c, p, o in rows]
    return MinedSchema(about=AboutMetadata.build(dataset_name=name), patterns=patterns)


SCHEMAS = {
    "genes": schema("genes", [("urn:Gene", "urn:xref", "Resource")]),
    "proteins": schema("proteins", [("urn:Protein", "urn:name", "Literal")]),
}
EVIDENCE = LinkEvidence(
    Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein"),
    sampled=50,
    found=48,
    target_forms={UP + "{id}": 48},
    examples=[("https://identifiers.org/uniprot:P04637", UP + "P04637")],
)


def test_verified_links_survive_a_file_and_become_evidence_edges(tmp_path):
    path = tmp_path / "links.tsv"
    write_links(path, [EVIDENCE])
    assert read_links(path) == [EVIDENCE], "The table keeps every field"
    graph = build_connectivity(SCHEMAS, links=read_links(path))
    (edge,) = [
        (a, b, d) for a, b, d in graph.edges(data=True) if d["kind"] == "verified_link"
    ]
    assert edge[:2] == (("genes", "urn:Gene"), ("proteins", "urn:Protein"))
    assert (edge[2]["predicate"], edge[2]["share"], edge[2]["sampled"]) == ("urn:xref", 0.96, 50)
    assert edge[2]["target_forms"] == {UP + "{id}": 48}, "The rewrite that applies the link"


def test_a_link_to_a_class_that_no_schema_has_is_refused():
    other = LinkEvidence(
        Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Missing"), 1, 1, {}, []
    )
    with pytest.raises(ValueError, match="absent"):
        build_connectivity(SCHEMAS, links=[other])
