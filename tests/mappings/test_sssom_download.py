"""Mapping sets published as files: plain, gzipped, or in archives."""

import gzip
from contextlib import contextmanager
from pathlib import Path

import pytest

from rdfsolve.mappings import enrichment

TSV = (
    "#curie_map:\n#  A: http://example.org/a/\n#  B: http://example.org/b/\n"
    "subject_id\tpredicate_id\tobject_id\tmapping_justification\n"
    "A:1\tskos:exactMatch\tB:1\tsemapv:ManualMappingCuration\n"
)


@pytest.mark.parametrize("suffix", [".sssom.tsv", ".sssom.tsv.gz"])
def test_a_plain_or_gzipped_mapping_set_is_saved_as_tsv(tmp_path: Path, monkeypatch, suffix):
    body = gzip.compress(TSV.encode()) if suffix.endswith(".gz") else TSV.encode()

    class Answer:
        def raise_for_status(self) -> None:
            """The server answered."""

        def iter_bytes(self):
            yield body

    @contextmanager
    def stream(method, url, **kwargs):
        yield Answer()

    monkeypatch.setattr(enrichment.httpx, "stream", stream)
    files = enrichment.download_sssom_source(
        f"https://example.org/set{suffix}", tmp_path / "out", "set"
    )
    assert [f.name for f in files] == ["set.sssom.tsv"]
    assert files[0].read_text() == TSV


def test_only_rows_of_observed_classes_are_parsed_and_duplicate_prefixes_collapse(tmp_path: Path):
    """A set whose prefix map names one namespace twice parses; rows of other classes are left
    out; a prefix missing from the set's map is expanded with Bioregistry's."""
    oio = "http://www.geneontology.org/formats/oboInOwl#"
    tsv = tmp_path / "set.sssom.tsv"
    tsv.write_text(
        "#curie_map:\n"
        "#  OIO: " + oio + "\n"
        "#  oboInOwl: " + oio + "\n"
        "#  A: http://example.org/a/\n"
        "#mapping_set_id: https://example.org/set\n"
        "subject_id\tpredicate_id\tobject_id\tmapping_justification\n"
        "A:1\tskos:exactMatch\tMONDO:0005015\tsemapv:ManualMappingCuration\n"
        "A:2\toboInOwl:hasDbXref\tA:3\tsemapv:ManualMappingCuration\n"
        "A:4\tskos:exactMatch\tA:5\tsemapv:ManualMappingCuration\n"
    )
    observed = {
        "http://example.org/a/1": "one",
        "http://example.org/a/2": "one",
        "http://purl.obolibrary.org/obo/MONDO_0005015": "two",
    }
    msdf = enrichment.enrich_sssom_file(tsv, observed, {})
    assert msdf is not None
    assert sorted(msdf.df["subject_id"]) == ["A:1", "A:2"]
    assert "OIO:hasDbXref" in set(msdf.df["predicate_id"])
