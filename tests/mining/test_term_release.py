"""rdfsolve.mining.term_release and rdfsolve.client.terms: the exact per-term layer as release
files, and its regrouping on the client without the data."""

from rdflib import Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF, RDFS

from rdfsolve.client.terms import (
    read_term_release,
    regroup,
    representatives,
    representatives_under,
    to_patterns,
)
from rdfsolve.mining.scan import store_from_graph
from rdfsolve.mining.scan_terms import group_terms
from rdfsolve.mining.term_release import write_term_release
from tests.mining.data import EX, FIXTURE, T

E = Namespace(EX)
N = Namespace(T)


def _release(tmp_path, graph, **options):
    store = store_from_graph(graph, tmp_path / "store")
    grouping = group_terms(store, minimal=True, **options)
    manifest = write_term_release(store, grouping, tmp_path / "out" / "fixture")
    return grouping, manifest, read_term_release(tmp_path / "out" / "fixture_terms.parquet")


def _counts(patterns):
    return {
        (p.subject_class, p.property_uri, p.object_class, p.datatype): (
            p.count,
            p.distinct_subjects,
        )
        for p in patterns
    }


def test_regrouping_at_the_default_budget_gives_the_default_grouping(tmp_path):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    grouping, manifest, release = _release(tmp_path, graph, budget=6)
    assert manifest["condition_holds"] is True
    assert manifest["types"] == "minimal"
    assert release.manifest["rows"] == release.terms.height == len(grouping.raw_patterns)
    assert representatives(release, 6) == grouping.representative == release.default
    table = regroup(release, release.default)
    assert table["triples_exact"].all()
    exact = _counts(grouping.patterns)
    for row in table.iter_rows(named=True):
        key = (row["subject_class"], row["property"], row["object_class"], row["datatype"])
        assert exact[key][0] == row["triples"], key
        if row["subjects_exact"]:
            assert exact[key][1] == row["distinct_subjects"], key
    ours = _counts(to_patterns(table))
    assert ours.keys() == exact.keys()


def test_grouping_before_mining_is_replayed_from_the_release(tmp_path):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    grouping, _, release = _release(tmp_path, graph, budget=6, group_before_mining=1)
    assert representatives(release, 6) == grouping.representative


def test_other_budgets_and_chosen_ancestors(tmp_path):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    _, _, release = _release(tmp_path, graph, budget=6)
    assert representatives(release, 1000) == {}, "Under the budget nothing is grouped"
    under, ambiguous = representatives_under(release, [T + "chemical"])
    assert under[T + "ethanol"] == T + "chemical" and not ambiguous
    rows = regroup(release, under)
    mass = rows.filter(
        (rows["subject_class"] == T + "chemical") & (rows["property"] == EX + "mass")
    )
    assert mass["triples"].sum() == 3


def test_two_grouped_types_on_one_record_are_marked(tmp_path):
    graph = Graph()
    for leaf in ("a", "b", "c", "d"):
        graph.add((N[leaf], RDFS.subClassOf, N.root))
        graph.add((N[leaf], RDF.type, OWL.Class))
    graph.add((E.x, RDF.type, N.a))
    graph.add((E.x, RDF.type, N.b))
    graph.add((E.x, E.name, Literal("x")))
    graph.add((E.y, RDF.type, N.c))
    graph.add((E.y, E.name, Literal("y")))
    graph.add((E.z, RDF.type, N.d))
    grouping, manifest, release = _release(tmp_path, graph, budget=2)
    assert manifest["records_with_several_groupable_types"] == 1
    assert manifest["condition_holds"] is False
    table = regroup(release, release.default)
    name = table.filter(
        (table["subject_class"] == str(N.root)) & (table["property"] == str(E.name))
    ).row(0, named=True)
    exact = next(
        p
        for p in grouping.patterns
        if p.subject_class == str(N.root) and p.property_uri == str(E.name)
    )
    assert (name["triples"], exact.count) == (3, 2), "The sum counts x twice"
    assert name["triples_exact"] is False and name["subjects_exact"] is False
    (pattern,) = [
        p
        for p in to_patterns(table)
        if p.subject_class == str(N.root) and p.property_uri == str(E.name)
    ]
    assert pattern.count_semantics == "upper_bound" and pattern.distinct_subjects is None
