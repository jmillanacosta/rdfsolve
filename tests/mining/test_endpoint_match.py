"""rdfsolve.mining.endpoint_match: an endpoint is compared with the local data of its source."""

from rdflib import Graph

from rdfsolve import SchemaMiner
from rdfsolve.mining.endpoint_match import check_endpoint_matches
from rdfsolve.schema_models import AboutMetadata, MinedSchema

DATA = """@prefix e: <urn:ex:> .
e:a a e:A ; e:p e:b ; e:name "a" . e:b a e:B ; e:p e:a ."""
P, NAME, TYPE = "urn:ex:p", "urn:ex:name", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _local(partitions=True):
    counts = {P: 2, NAME: 1, TYPE: 2}
    about = AboutMetadata.build(
        dataset_name="fixture",
        property_partitions={p: {"triples": n} for p, n in counts.items()} if partitions else None,
    )
    return MinedSchema(about=about, patterns=[])


def _check(extra="", schema=None, **options):
    graph = Graph().parse(data=DATA + extra, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as endpoint:
        return check_endpoint_matches(schema or _local(), endpoint.helper, **options)


def test_the_same_data_is_equal():
    match = _check()
    assert match.state == "equal" and match.properties_checked == 3
    assert match.differing == {} and match.remote_only == {}


def test_a_different_count_is_a_difference():
    match = _check("<urn:ex:c> <urn:ex:p> <urn:ex:a> .")
    assert match.state == "differs"
    assert match.differing == {P: {"local": 2, "remote": 3}}


def test_a_property_of_the_endpoint_only_is_a_difference():
    match = _check('<urn:ex:a> <urn:ex:q> "x" .')
    assert match.state == "differs" and match.remote_only == {"urn:ex:q": 1}


def test_engine_data_is_listed_apart():
    virtrdf = "http://www.openlinksw.com/schemas/virtrdf#item"
    match = _check(f'<urn:ex:qm> <{virtrdf}> "x" .')
    assert match.state == "equal"
    assert match.engine_or_service == {virtrdf: 1} and match.remote_only == {}


def test_a_record_without_exact_counts_is_not_checked():
    match = _check(schema=_local(partitions=False))
    assert match.state == "not_checked" and match.reason


def test_a_refused_property_list_falls_back_to_counts_of_each_property(monkeypatch):
    from rdfsolve.mining import endpoint_match

    monkeypatch.setattr(endpoint_match, "_property_list_query", lambda dataset: "SELEC broken")
    match = _check()
    assert match.state == "partial" and match.remote_only is None
    assert match.differing == {} and match.properties_checked == 3
    assert "list of properties" in match.reason


def test_the_data_graph_of_the_endpoint_is_found(monkeypatch):
    """An endpoint that serves engine triples with general properties (rdf:type in the Virtuoso
    graph) differs without a scope; the check then finds the graph with the local triple count
    (AOP-Wiki: http://aopwiki.org/, 338,317 triples, equal in one more query)."""
    from rdflib import Dataset, URIRef

    data = Dataset(default_union=True)
    data.graph(URIRef("urn:graph:data")).parse(data=DATA, format="turtle")
    engine = data.graph(URIRef("http://www.openlinksw.com/schemas/virtrdf#"))
    engine.parse(data="<urn:qm> a <urn:ex:QuadMap> ; <urn:ex:p> <urn:x> .", format="turtle")
    with SchemaMiner.from_graph(data, delay=0) as endpoint:
        match = check_endpoint_matches(_local(), endpoint.helper)
    assert match.state == "equal"
    assert match.graph_uris == ["urn:graph:data"] and match.graphs_found_by_the_check
    assert match.differing == {} and match.remote_only == {}
