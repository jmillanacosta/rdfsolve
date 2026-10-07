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


_LARGE_RELEASE = """
import json, resource, sys
from pathlib import Path

import polars as pl

from rdfsolve.mining.scan import RowStore
from rdfsolve.mining.scan_terms import TermGrouping
from rdfsolve.mining.term_release import write_term_release

path = Path(sys.argv[1])
records = 1_500_000
iri = pl.lit("http://example.org/a/rather/long/path/of/a/record/as/in/real/data/") + pl.int_range(
    records, eager=False
).cast(pl.String).str.zfill(60)
base = pl.select(s=pl.lit("<") + iri + pl.lit(">")).with_columns(sid=pl.col("s").hash())
leaves = [f"<{sys.argv[2]}{leaf}>" for leaf in ("a", "b")]
pl.concat([base.with_columns(c=pl.lit(c)) for c in leaves]).select("s", "c", "sid").write_parquet(
    path / "store" / "types.parquet"
)
del base
store = RowStore(path / "store")
grouping = TermGrouping(
    patterns=[], raw_patterns=[], representative={}, members={}, summary={},
    before_mining=None, types=store.graph_types(), base_types=store.graph_types(),
)
before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
manifest = write_term_release(store, grouping, path / "out" / "large")
after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(json.dumps({"grown_mb": (after - before) // 1024, "manifest": manifest}))
"""


def test_the_release_does_not_hold_the_type_table_by_text(tmp_path):
    """Two groupable types on each of 1.5 million records with long IRIs: the pairs of types
    are counted from integer codes, without the text of the records (allie job 115892 was
    killed at 61.5 GB in this step; agent-findings/term-release-memory.md)."""
    import json
    import os
    import subprocess
    import sys

    graph = Graph()
    for leaf in ("a", "b"):
        graph.add((N[leaf], RDFS.subClassOf, N.root))
    store_from_graph(graph, tmp_path / "store")
    script = tmp_path / "large.py"
    script.write_text(_LARGE_RELEASE)
    done = subprocess.run(
        [sys.executable, str(script), str(tmp_path), T],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "POLARS_MAX_THREADS": "4"},
    )
    result = json.loads(done.stdout.splitlines()[-1])
    manifest = result["manifest"]
    assert manifest["typed_records"] == 1_500_000
    assert manifest["records_with_several_groupable_types"] == 1_500_000
    assert manifest["groupable_pairs_sharing_records"] == 1
    # By text the table and the self-join of its records took 0.9 GB here (4 threads), by codes 0.3 GB.
    assert result["grown_mb"] < 600, result["grown_mb"]


def test_superclasses_reads_only_the_rows_of_the_terms_and_their_ancestors(tmp_path):
    """An index with many rdfs:subClassOf rows outside the hierarchy of the asked terms: they are
    not held in Python (bio2rdf.chembl, 1.4 million such rows, failed in this function)."""
    import tracemalloc

    from rdfsolve.mining.scan_terms import superclasses

    graph = Graph()
    graph.add((N.leaf, RDFS.subClassOf, N.middle))
    graph.add((N.middle, RDFS.subClassOf, N.root))
    graph.add((N.middle, RDFS.subClassOf, N.middle))
    for number in range(40_000):
        graph.add((N[f"other{number}"], RDFS.subClassOf, N[f"parent{number}"]))
    store = store_from_graph(graph, tmp_path / "store")
    tracemalloc.start()
    try:
        parents = superclasses(store, [str(N.leaf)])
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert parents == {
        str(N.leaf): {str(N.middle)},
        str(N.middle): {str(N.root)},
        str(N.root): set(),
    }
    assert peak < 2_000_000, peak
