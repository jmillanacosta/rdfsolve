"""A failed download stops the download step even when a later step renames files: the rename
step's ';' split the '&&' chain, so the step ended with 'true' and success (GlyCosmos: 62 of 107
files were not downloaded after one 404, and the index was built from the rest, 2026-09-30)."""

import os
import subprocess

from rdfsolve.qlever.utils import _build_get_data_steps, analyse_source


def test_a_failed_download_is_not_hidden_by_the_rename_step(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "wget").write_text(
        '#!/bin/sh\nfor a in "$@"; do case "$a" in *missing*) exit 8;; esac; done\n'
        'for a in "$@"; do case "$a" in http*) touch "$(basename "$a")";; esac; done\n'
    )
    (bin_dir / "wget").chmod(0o755)
    entry = {
        "name": "fixture",
        "download_ttl": [
            "https://example.org/a/model.owl",
            "https://example.org/b/missing.ttl",
            "https://example.org/c/data.ttl",
        ],
    }
    steps = _build_get_data_steps(analyse_source(entry), str(tmp_path / "rdf"))
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    done = subprocess.run(["bash"], input=" && ".join(steps), text=True, env=env, capture_output=True)
    assert done.returncode != 0, "The 404 of missing.ttl must fail the step"
    assert not (tmp_path / "rdf" / "data.ttl").exists()


def test_a_failed_download_is_not_hidden_by_the_decompression_step(tmp_path):
    """The decompression step ends with '|| true', which made the whole chain a success: ChEMBL
    was indexed from 1 of its 23 files after a download failed (job 114418, 2026-09-30)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "wget").write_text(
        '#!/bin/sh\nfor a in "$@"; do case "$a" in *missing*) exit 8;; esac; done\n'
        'for a in "$@"; do case "$a" in http*) touch "$(basename "$a")";; esac; done\n'
    )
    (bin_dir / "wget").chmod(0o755)
    urls = ["https://example.org/a/first.ttl.gz", "https://example.org/b/missing.ttl.gz"]
    steps = _build_get_data_steps(analyse_source({"name": "fixture", "download_ttl": urls}), str(tmp_path / "rdf"))
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    done = subprocess.run(["bash"], input=" && ".join(steps), text=True, env=env, capture_output=True)
    assert done.returncode != 0, "The failed download must fail the step"
    assert "download failed" in done.stderr
