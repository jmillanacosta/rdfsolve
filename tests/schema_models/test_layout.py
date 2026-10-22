"""rdfsolve.schema_models.layout: a schema is written as a JSON summary and Parquet detail, read
back whole, and a schema of the earlier layout repaired into the same files."""

import json

import pyarrow.parquet as pq
import pytest

from rdfsolve.analysis.connectivity import network_paths, read_network_paths
from rdfsolve.client.terms import read_term_release
from rdfsolve.mining.term_release import _write, term_rows
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.core import iter_section_rows
from rdfsolve.schema_models.layout import _census_row, has_tested_paths, repair_schema, write_schema
from rdfsolve.schema_models import navigation
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary, read_tested_paths
from rdfsolve.schema_models.structural import StructuralPattern

LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


def _pattern(s, p, o, count=4, **fields):
    return SchemaPattern(subject_class=s, property_uri=p, object_class=o, count=count, **fields)


AB = _pattern("urn:A", "urn:ab", "urn:B", graphs={"urn:g1": 3, "urn:g2": 1})
BC = _pattern("urn:B", "urn:bc", "urn:C")
BA = _pattern("urn:B", "urn:ba", "urn:A")
AC = _pattern("urn:A", "urn:ac", "urn:C")
NAME = _pattern("urn:B", LABEL, "Literal", datatype="http://www.w3.org/2001/XMLSchema#string")


def _path(*steps, matched=2, at="2026-01-01T00:00:00+00:00"):
    return NavigationPath(
        steps=list(steps),
        evidence="instance_tested",
        instance_support="matched",
        matched_sources=matched,
        source_count=5,
        observed_at=at,
    )


def _census(selection, covered):
    return StructuralPattern(
        subject_properties=["urn:ab"],
        subject_kind="IRI",
        property_uri="urn:ab",
        object_kind="IRI",
        covered_types=covered,
        subject_selection=selection,
        count=2,
        distinct_subjects=2,
        distinct_objects=1,
        witness_query="q",
        recount_query="q",
    )


@pytest.fixture
def schema():
    return MinedSchema(
        about=AboutMetadata.build(dataset_name="x"),
        patterns=[AB, BC],
        raw_patterns=[AB, BC, BA, AC, NAME],
        term_patterns=[
            _pattern("urn:t1", "urn:ab", "urn:B", count=1, subject_binding="term"),
            _pattern("urn:A", "urn:ab", "urn:t2", count=1, object_binding="term",
                     graphs={"urn:g1": 1}, graph_distinct_subjects={"urn:g1": 1}),
        ],
        structural_patterns=[
            _census("untyped", [("urn:A", "urn:B", None)]),
            _census("uncovered", [("urn:A", "urn:B", None)]),
        ],
        navigation=NavigationSummary(
            max_hops=3,
            max_paths_per_length=0,
            edge_count=5,
            walk_counts={2: 4, 3: 1},
            strategy="tested",
            paths=[
                _path(AB, BC, matched=1),
                _path(AC, _pattern("urn:C", "urn:cb", "urn:B")),  # A to B as well, longer
                _path(AB, BA),  # a round trip
                _path(AB, NAME),  # through an annotation property
                _path(AC, _pattern("urn:C", "urn:cb", "urn:B"), BC),
                _path(AB, BC, matched=3, at="2026-01-02T00:00:00+00:00"),
            ],
        ),
    )


def _expected(schema):
    """The schema read back: untyped census rows keep no covered types."""
    census = [
        p.model_copy(update={"covered_types": []}) if p.subject_selection == "untyped" else p
        for p in schema.structural_patterns
    ]
    return schema.model_copy(update={"structural_patterns": census})


def _old_layout(schema, folder):
    """Write *schema* as the earlier layout did: all sections in an indented JSON, and the term
    release's terms file with its manifest."""
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "x_local_schema.json"
    path.write_text(json.dumps(schema.to_dict(), indent=2), encoding="utf-8")
    manifest = {"format": "rdfsolve-term-release", "version": 1, "classes_file": "x.parquet"}
    _write(term_rows(schema.raw_patterns), folder / "x_local_terms.parquet", manifest)
    return path


def test_a_written_schema_is_read_back_whole_and_its_json_is_the_summary(schema, tmp_path):
    path = tmp_path / "x_local_schema.json"
    summary = write_schema(schema, path)
    written = json.loads(path.read_text())["schema"]
    assert written["raw_patterns"] is None and written["term_patterns"] is None
    assert written["details"]["raw_patterns"] == {"file": "x_local_terms.parquet", "rows": 5}
    assert written["details"]["term_patterns"]["rows"] == 2
    assert written["details"]["navigation.paths"] == {"file": "x_local_paths.parquet", "rows": 6}
    assert written["structural_patterns"] is None
    assert written["details"]["structural_patterns"] == {"file": "x_local_census.parquet", "rows": 2}
    assert "\n  " not in path.read_text(), "The JSON is written without indentation"
    assert summary.navigation.paths_scope == "network"
    assert [p.signature() for p in summary.navigation.paths] == [
        p.signature() for p in network_paths(schema.navigation.paths)
    ], "The JSON keeps the network's paths"
    assert MinedSchema.from_json(path) == _expected(schema)
    light = MinedSchema.from_json(path, details=False)
    assert light.raw_patterns is None and len(light.navigation.paths) == 2


def test_the_paths_file_marks_detours_and_the_network(schema, tmp_path):
    path = tmp_path / "x_local_schema.json"
    write_schema(schema, path)
    table = pq.read_table(tmp_path / "x_local_paths.parquet").to_pydict()
    assert table["detour"] == [False, False, True, True, True, False]
    # A to B: the one path; A to C: of the two shortest, the one with the most matched.
    assert table["network"] == [False, True, False, False, False, True]


def test_a_repaired_schema_gives_the_files_of_a_written_one(schema, tmp_path):
    old = _old_layout(schema, tmp_path / "old")
    repaired = tmp_path / "repaired" / "x_local_schema.json"
    repair_schema(old, repaired)
    direct = tmp_path / "direct"
    _old_layout(schema, direct)  # the term release written before, as in a mining run
    write_schema(schema, direct / "x_local_schema.json")
    for name in ("x_local_schema.json",):
        assert (repaired.parent / name).read_bytes() == (direct / name).read_bytes()
    for name in ("x_local_terms.parquet", "x_local_paths.parquet", "x_local_census.parquet"):
        left, right = pq.read_table(repaired.parent / name), pq.read_table(direct / name)
        assert left.equals(right) and left.schema.metadata == right.schema.metadata, name
    assert MinedSchema.from_json(repaired) == _expected(schema)


def test_readers_give_the_same_rows_from_both_layouts(schema, tmp_path):
    old = _old_layout(schema, tmp_path / "old")
    new = tmp_path / "new" / "x_local_schema.json"
    repair_schema(old, new)
    names = ["raw_patterns", "term_patterns"]
    assert sorted(map(json.dumps, iter_section_rows(old, names))) == sorted(
        map(json.dumps, iter_section_rows(new, names))
    )
    census = [r for _, r in iter_section_rows(new, ["structural_patterns"]) if r is not None]
    assert census == [
        _census_row(r) for _, r in iter_section_rows(old, ["structural_patterns"]) if r is not None
    ], "The census rows are those of the old file, without unused covered types"
    assert read_tested_paths(old) == read_tested_paths(new)
    assert read_tested_paths(old, ["urn:A"], max_hops=2) == read_tested_paths(new, ["urn:A"], 2)
    assert navigation.tested_step_support(old) == navigation.tested_step_support(new)
    assert read_network_paths(old) == read_network_paths(new)
    release = read_term_release(
        new.parent / "x_local_terms.parquet", classes=tmp_path / "old" / "x_local_terms.parquet"
    )
    assert release.terms.equals(term_rows(schema.raw_patterns)), "Clients regroup the same rows"
    assert release.manifest["classes_file"] == "x.parquet", "The release manifest is kept"


def test_mined_census_rows_of_untyped_subjects_have_no_covered_types():
    row = _census("untyped", [("urn:A", "urn:B", None)]).model_dump(mode="json")
    assert _census_row(row)["covered_types"] == []
    row = _census("uncovered", [("urn:A", "urn:B", None)]).model_dump(mode="json")
    assert _census_row(row)["covered_types"] == [["urn:A", "urn:B", None]]


def test_a_schema_whose_tested_paths_are_all_detours_has_paths_shapes(schema, tmp_path):
    navigation = schema.navigation.model_copy(
        update={"paths": [_path(AB, BA), _path(AB, NAME)]}
    )
    summary = write_schema(
        schema.model_copy(update={"navigation": navigation}), tmp_path / "x_local_schema.json"
    )
    assert summary.navigation.paths == [] and has_tested_paths(summary)
    assert not has_tested_paths(summary.model_copy(update={"navigation": None}))


def test_a_written_schema_is_validated_row_by_row_without_building_its_paths(
    schema, tmp_path, monkeypatch
):
    from rdfsolve.release.validate import _validate_schema_streamed
    from rdfsolve.schema_models import layout

    path = tmp_path / "x_local_schema.json"
    write_schema(schema, path)

    def refuse(self, row):
        raise AssertionError("A path was built")

    monkeypatch.setattr(layout.PackedPaths, "path", refuse)
    _validate_schema_streamed(path)
    written = json.loads(path.read_text())
    written["schema"]["details"]["raw_patterns"]["rows"] = 9
    wrong = tmp_path / "y_local_schema.json"
    wrong.write_text(json.dumps(written))
    for name in ("terms", "paths", "census"):
        (tmp_path / f"y_local_{name}.parquet").write_bytes(
            (tmp_path / f"x_local_{name}.parquet").read_bytes()
        )
    for detail in written["schema"]["details"].values():
        detail["file"] = detail["file"].replace("x_local", "y_local")
    wrong.write_text(json.dumps(written))
    with pytest.raises(ValueError, match="raw_patterns: 5 rows"):
        _validate_schema_streamed(wrong)


def test_a_path_with_a_step_outside_the_edge_table_is_invalid(schema, tmp_path):
    import pyarrow as pa

    from rdfsolve.release.validate import _validate_schema_streamed

    path = tmp_path / "x_local_schema.json"
    write_schema(schema, path)
    paths = tmp_path / "x_local_paths.parquet"
    table = pq.read_table(paths)
    edges = table.column("edges").to_pylist()
    edges[0] = [0, 99]
    table = table.set_column(0, "edges", pa.array(edges, table.schema.field("edges").type))
    pq.write_table(table, paths)
    with pytest.raises(ValueError, match="is not a path"):
        _validate_schema_streamed(path)
