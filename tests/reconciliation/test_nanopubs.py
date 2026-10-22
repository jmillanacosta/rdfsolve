"""rdfsolve.reconciliation.nanopubs: an assertion graph is saved on disk as an unsigned
nanopublication whose trusty URI is computed from its content, read back with that URI
verified, refused when the file was changed, and grouped by a nanopublication index."""

import pytest
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.compare import isomorphic

from rdfsolve.reconciliation.nanopubs import index, load, nanopublication, save

NPX = Namespace("http://purl.org/nanopub/x/")
SH = Namespace("http://www.w3.org/ns/shacl#")
TOOL = "https://example.org/tool"


def rule(value="x"):
    graph = Graph()
    graph.add((URIRef("urn:rule"), URIRef("urn:p"), Literal(value)))
    return nanopublication(
        graph, kinds=[SH.SPARQLConstructExecutable], attributed_to=TOOL, created="2026-10-05"
    )


def test_an_assertion_is_saved_and_read_back_with_its_trusty_uri(tmp_path):
    saved = rule()
    assert saved.source_uri == rule().source_uri, "The URI follows from the content only"
    assert saved.source_uri != rule("y").source_uri
    path = save(saved, tmp_path)
    assert path.name == saved.source_uri.rsplit("/", 1)[1] + ".trig"
    back = load(path)
    assert back.source_uri == saved.source_uri and back.has_valid_trusty
    assert isomorphic(back.assertion, saved.assertion)
    assert (
        URIRef(back.source_uri),
        NPX.hasNanopubType,
        SH.SPARQLConstructExecutable,
    ) in back.pubinfo
    assert not list(back.rdf.triples((None, NPX.hasSignature, None))), "Not signed"
    assert (
        None,
        URIRef("http://www.w3.org/ns/prov#wasAttributedTo"),
        URIRef(TOOL),
    ) in back.provenance


def test_a_changed_file_is_refused(tmp_path):
    path = save(rule(), tmp_path)
    path.write_text(path.read_text().replace('"x"', '"changed"'))
    with pytest.raises(ValueError, match="trusty"):
        load(path)


def test_an_index_lists_its_nanopublications(tmp_path):
    members = [rule("a"), rule("b")]
    grouped = index(members, title="Rules", attributed_to=TOOL, created="2026-10-05")
    listed = set(grouped.assertion.objects(None, NPX.includesElement))
    assert listed == {URIRef(m.source_uri) for m in members}
    assert (URIRef(grouped.source_uri), NPX.hasNanopubType, NPX.NanopubIndex) in grouped.pubinfo
    assert load(save(grouped, tmp_path)).source_uri == grouped.source_uri
