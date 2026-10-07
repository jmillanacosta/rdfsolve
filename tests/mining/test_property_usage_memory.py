"""rdfsolve.mining.scan_structure.property_usage_evidence on large stores: records joined and
counted by id, value counts per subject without a set of values per subject (allie, job
115892, and ctd, job 115893, were killed in this step at 61.5 and 96 GB)."""

import json
import os
import subprocess
import sys

from rdflib import Dataset, Graph, Namespace, URIRef
from rdflib.namespace import RDF

from rdfsolve.mining.scan import store_from_graph
from rdfsolve.mining.scan_structure import property_usage_evidence

E = Namespace("urn:ex:")

_LARGE_EVIDENCE = """
import json, resource, sys
from pathlib import Path

import polars as pl

from rdfsolve.mining.scan import RowStore
from rdfsolve.mining.scan_structure import property_usage_evidence

path = Path(sys.argv[1])
records = 2_000_000
iri = (
    pl.lit("<http://example.org/a/rather/long/path/of/a/record/as/in/real/data/")
    + pl.int_range(records, eager=False).cast(pl.String).str.zfill(60)
    + pl.lit(">")
)
base = pl.select(s=iri).with_columns(sid=pl.col("s").hash())
base.with_columns(c=pl.lit("<urn:ex:A>")).select("s", "c", "sid").write_parquet(
    path / "store" / "types.parquet"
)
target = pl.lit("<http://example.org/target/") + pl.int_range(records).cast(pl.String).str.zfill(40)
rows = pl.concat([base.with_columns(o=target + pl.lit(f"/{k}>")) for k in range(2)])
manifest = json.loads((path / "store" / "store.json").read_text())
rows.with_columns(d=pl.lit(None, pl.String), oid=pl.col("o").hash()).select(
    "s", "o", "d", "sid", "oid"
).write_parquet(path / "store" / "rows" / manifest["predicates"]["urn:ex:p"])
del base, rows
store = RowStore(path / "store")
before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
evidence = property_usage_evidence(
    store, dataset_id="large", classes=["urn:ex:A"], class_entity_counts={"urn:ex:A": records},
    collect_histograms=True,
)
after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
(record,) = evidence.records
print(json.dumps({"grown_mb": (after - before) // 1024, "record": record.model_dump(mode="json")}))
"""


def test_property_usage_evidence_of_many_subjects_stays_small(tmp_path):
    """Two million subjects with two values each: 2.3 GB by text and a set per subject, 0.3 GB
    by id and row counts."""
    graph = Graph()
    graph.add((E.x, E.p, E.y))
    graph.add((E.x, RDF.type, E.A))
    store_from_graph(graph, tmp_path / "store")
    script = tmp_path / "large.py"
    script.write_text(_LARGE_EVIDENCE)
    done = subprocess.run(
        [sys.executable, str(script), str(tmp_path)],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "POLARS_MAX_THREADS": "4"},
    )
    result = json.loads(done.stdout.splitlines()[-1])
    record = result["record"]
    assert (record["triple_count"], record["subjects_with_property"]) == (4_000_000, 2_000_000)
    assert record["distinct_objects"] == 4_000_000
    assert record["value_count_histogram"] == {"2": 2_000_000, "0": 0}
    assert result["grown_mb"] < 1000, result["grown_mb"]


def test_a_triple_in_two_graphs_is_one_value_of_its_subject(tmp_path):
    """The values of a subject are counted as its rows, which hold each triple once."""
    data = Dataset()
    for name in ("urn:g:1", "urn:g:2"):
        graph = data.graph(URIRef(name))
        graph.add((E.x, RDF.type, E.A))
        graph.add((E.x, E.p, E.y))
    data.graph(URIRef("urn:g:1")).add((E.x, E.p, E.z))
    store = store_from_graph(data, tmp_path / "store")
    evidence = property_usage_evidence(
        store,
        dataset_id="graphs",
        classes=[str(E.A)],
        class_entity_counts={str(E.A): 1},
        graph_uris=["urn:g:1", "urn:g:2"],
        collect_histograms=True,
    )
    (record,) = [r for r in evidence.records if r.property_uri == str(E.p)]
    assert (record.triple_count, record.subjects_with_property) == (2, 1)
    assert record.value_count_histogram == {"2": 1, "0": 0}
