"""The graph scope of a registry entry describes the endpoint of the source. A local index that
is built from files without graphs (N-Triples, Turtle, OWL) holds no named graph, so the scope
does not apply to it and the whole index is mined (rdfportal.medgen and chembl: the rebuilt
indexes were refused with "no triples in the selected graphs", 2026-09-30). A scope is kept
for files that can hold graphs (N-Quads, archives) and for files mapped to graphs, so that a
wrong scope is still reported by the miner."""

from scripts.pipeline_stages.local import local_graph_scope

SCOPE = ["http://rdfportal.org/dataset/medgen"]


def test_the_scope_of_the_endpoint_is_not_applied_to_files_without_graphs():
    assert local_graph_scope(SCOPE, {"download_nt": ["https://example.org/a.nt.gz"]}, {}) is None
    fields = {"download_ttl": ["https://example.org/a.ttl"], "download_owl": "https://example.org/o.owl"}
    assert local_graph_scope(SCOPE, fields, {}) is None


def test_the_scope_is_kept_where_the_files_can_hold_graphs():
    assert local_graph_scope(SCOPE, {"download_nq": ["https://example.org/a.nq.gz"]}, {}) == SCOPE
    assert local_graph_scope(SCOPE, {"download_tgz": "https://example.org/a.tgz"}, {}) == SCOPE
    mapped = {"urn:g": {"download_ttl": ["https://example.org/a.ttl"]}}
    assert local_graph_scope(["urn:g"], {}, mapped) == ["urn:g"]
    assert local_graph_scope(SCOPE, {}, {}) == SCOPE, "No files are known: the miner checks the scope"
    assert local_graph_scope(None, {"download_nt": ["https://example.org/a.nt"]}, {}) is None
