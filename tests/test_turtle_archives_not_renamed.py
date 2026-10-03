"""A Turtle download with another extension (.owl) is named .ttl before indexing, but an archive
listed as Turtle keeps its name, so that it is extracted (WikiPathways lists zip archives of
Turtle files; renamed to .ttl, they were given to the index as Turtle, 2026-09-30)."""

from rdfsolve.qlever.utils import _rename_mislabelled_steps, analyse_source


def test_archives_listed_as_turtle_keep_their_names():
    entry = {
        "name": "fixture",
        "download_ttl": [
            "https://example.org/rdf/data-rdf-wp.zip",
            "https://example.org/rdf/more.tar.gz",
            "https://example.org/rdf/other.tgz",
            "https://example.org/rdf/model.owl",
            "https://example.org/rdf/void.ttl",
        ],
    }
    steps = " ".join(_rename_mislabelled_steps(analyse_source(entry)))
    assert '"model.owl" "model.ttl"' in steps
    assert "data-rdf-wp" not in steps and "more" not in steps and "other" not in steps
