from rdflib import Graph, URIRef

from rdfsolve.mining.typed_coverage import typed_match


def test_coverage_matches_edges_as_a_relation():
    graph = Graph().parse(
        data="""
        @prefix x: <urn:> .
        x:s a x:Record, x:Other;
            x:value "text", 7;
            x:link x:typed, x:untyped;
            x:blank [ x:label "node" ];
            x:missing "uncovered" .
        x:typed a x:Target, x:OtherTarget .
        x:untyped x:label "untyped" .
        x:outside x:value "uncovered" .
    """,
        format="turtle",
    )
    keys = [
        ("urn:Record", "urn:value", "Literal", "http://www.w3.org/2001/XMLSchema#string"),
        ("urn:Other", "urn:value", "Literal", "http://www.w3.org/2001/XMLSchema#string"),
        ("urn:Record", "urn:link", "urn:Target", None),
        ("urn:Record", "urn:link", "Resource", None),
        ("urn:Record", "urn:blank", "BlankNode", None),
    ]
    expected = {
        (s, p, o)
        for s, p, o in graph
        if s == URIRef("urn:s")
        and (
            p == URIRef("urn:link")
            or p == URIRef("urn:blank")
            or (p == URIRef("urn:value") and str(o) == "text")
        )
    }
    expression = typed_match(keys, None, None)
    joined = {
        tuple(row)
        for row in graph.query(
            f"SELECT ?s ?p ?o WHERE {{ ?s ?p ?o . BIND({expression} AS ?covered) FILTER(?covered) }}"
        )
    }
    correlated = {
        tuple(row)
        for row in graph.query(f"SELECT ?s ?p ?o WHERE {{ ?s ?p ?o . FILTER({expression}) }}")
    }
    assert joined == expected, "Coverage must classify the current edge"
    assert correlated == expected, "Type overlap must not change covered edge membership"
    batch = typed_match(keys, None, None, "urn:link", "FILTER(?o IN (<urn:typed>))")
    for test in (expression, batch):
        assert "VALUES" not in test, (
            "No VALUES list of profiles: QLever joins it with every type triple (Bgee: 455.7 GB) and"
            " evaluates EXISTS with a large VALUES wrongly; types are compared with IRIs"
        )
        assert "?_subjectType = <urn:Record>" in test and "?_objectType = <urn:Target>" in test
        assert "UNION" not in test, "RDFLib evaluates a UNION inside an EXISTS as false"
        assert "OPTIONAL" not in test, (
            "No OPTIONAL inside the test: Virtuoso counts 1 of 2 prov:used edges with it"
        )
    untyped = batch.split("!EXISTS {", 1)[1].split("?o a ?_anyObjectType")[0]
    assert "?s <urn:link> ?o ." in untyped and "FILTER(?o IN (<urn:typed>))" in untyped, (
        "The test of an untyped object repeats the edge and the batch: QLever evaluates the group"
        " on its own, and ?o a ?_anyObjectType alone reads every type triple (Bgee: 455.7 GB)"
    )


def test_the_coverage_test_does_not_rebind_outer_variables():
    """Virtuoso rejects VALUES that bind an outer variable inside EXISTS (SP031), and IF around
    EXISTS (SQ156)."""
    match = typed_match([("urn:A", "urn:p", "urn:B", None)], None, None)
    assert "VALUES" not in match and "?p = <urn:p>" in match, "The property is compared"
    assert "IF(EXISTS" not in match.replace(" ", "")
    body = match.split("EXISTS {", 1)[1]
    assert body.lstrip().startswith("?s ?p ?o ."), "Engines that join EXISTS need the pattern"


def test_the_coverage_test_of_one_property_reads_only_that_property():
    """QLever evaluates the group of EXISTS on its own: a constant property keeps it small
    (Bgee RO_0002162: 20 s, not 217 s, with the same counts)."""
    match = typed_match([("urn:A", "urn:p", "urn:B", None)], None, None, predicate="urn:p")
    body = match.split("EXISTS {", 1)[1]
    assert body.lstrip().startswith("?s <urn:p> ?o ."), "The group reads one property"
    assert "?p " not in body and "?p)" not in body, "No outer variable is bound again (SP031)"
