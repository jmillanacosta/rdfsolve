from rdflib import Graph, URIRef

from rdfsolve.mining.typed_coverage import typed_match


def test_coverage_matches_edges_as_a_relation():
    graph = Graph().parse(data="""
        @prefix x: <urn:> .
        x:s a x:Record, x:Other;
            x:value "text", 7;
            x:link x:typed, x:untyped;
            x:blank [ x:label "node" ];
            x:missing "uncovered" .
        x:typed a x:Target, x:OtherTarget .
        x:untyped x:label "untyped" .
        x:outside x:value "uncovered" .
    """, format="turtle")
    keys = [
        ("urn:Record", "urn:value", "Literal", "http://www.w3.org/2001/XMLSchema#string"),
        ("urn:Other", "urn:value", "Literal", "http://www.w3.org/2001/XMLSchema#string"),
        ("urn:Record", "urn:link", "urn:Target", None),
        ("urn:Record", "urn:link", "Resource", None),
        ("urn:Record", "urn:blank", "BlankNode", None),
    ]
    expected = {(s, p, o) for s, p, o in graph if s == URIRef("urn:s")
                and (p == URIRef("urn:link") or p == URIRef("urn:blank")
                     or (p == URIRef("urn:value") and str(o) == "text"))}
    expression = typed_match(keys, None, None)
    joined = {tuple(row) for row in graph.query(
        f"SELECT ?s ?p ?o WHERE {{ ?s ?p ?o . BIND({expression} AS ?covered) FILTER(?covered) }}")}
    correlated = {tuple(row) for row in graph.query(
        f"SELECT ?s ?p ?o WHERE {{ ?s ?p ?o . FILTER({expression}) }}")}
    assert joined == expected, "Coverage must classify the current edge"
    assert correlated == expected, "Type overlap must not change covered edge membership"
    for test in (expression, typed_match(keys, None, None, "urn:link", "VALUES ?o { <urn:typed> }")):
        inner = [group.split("}")[0] for group in test.split("!EXISTS")[1:]]
        assert inner and not any("?_covered" in group for group in inner), (
            "No inner EXISTS on a VALUES variable: Virtuoso evaluates it as false"
            " (AOP-Wiki: 1 of 23,729 owl:sameAs edges covered)"
        )
        assert "UNION" not in test, "RDFLib evaluates a UNION inside an EXISTS as false"
        assert "OPTIONAL" not in test, (
            "No OPTIONAL inside the test: Virtuoso counts 1 of 2 prov:used edges with it"
        )
