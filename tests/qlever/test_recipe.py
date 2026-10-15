"""rdfsolve.qlever.recipe: the recipe of a local index is written portable, with the image that
built the index, without the values of the job that served it, and without data or index files."""

import json
from pathlib import Path

from rdfsolve.qlever.recipe import (
    EXPORT_MANIFEST,
    EXPORT_NOTES,
    RECIPE_FILE,
    engine_details,
    portable_qleverfile,
    write_index_recipe,
)

SHA = "f" * 64
ENGINE = {"image": "/somewhere/data/qlever.sif", "image_sha256": SHA, "index_build": "9ec88a"}
LABELS = {
    "org.label-schema.usage.singularity.deffile.from": "docker.io/adfreiburg/qlever:latest",
    "org.opencontainers.image.revision": "9ec88a09010c354c0ba4b0366b84718b0b5e109f",
}


def _workdir(home: Path, name: str = "demo") -> tuple[Path, Path]:
    """A work folder as the local stage leaves it, under a data directory in *home*."""
    data_dir = home / "rdfsolve" / "data"
    workdir = data_dir / "qlever_workdirs" / name
    (workdir / "rdf").mkdir(parents=True)
    (workdir / "scan-store").mkdir()
    (workdir / "rdf" / "demo.nt.gz").write_bytes(b"data")
    w = str(workdir)
    (workdir / "Qleverfile").write_text(
        f"""# Qleverfile for {name}
#
# Usage:
#  cd {w}
#  qlever index

[data]
NAME              = {name}
GET_DATA_CMD      = mkdir -p {w}/rdf && cd {w}/rdf && wget -c "https://example.org/x/demo.nt.gz" 2>/dev/null
FORMAT            = nt

[index]
MULTI_INPUT_JSON = [{{"cmd": "cat {w}/rdf/demo.nt", "format": "nt", "graph": "https://example.org/g"}}]
INPUT_FILES          = rdf/*.nt*
SETTINGS_JSON        = {{ "ascii-prefixes-only": false }}

[server]
PORT              = 36524
ACCESS_TOKEN      = {name}
MEMORY_FOR_QUERIES = 80G
TIMEOUT           = 600s

[runtime]
SYSTEM = singularity
IMAGE  = docker.io/adfreiburg/qlever:latest
"""
    )
    (workdir / "index-command.sh").write_text(
        f"#!/bin/bash\nset -euo pipefail\ncd {w}\n"
        f"qlever-index -i {name} -s {w}/{name}.settings.json -f .index-pipes/0.nt -F nt\n"
    )
    (workdir / "index-feed.sh").write_text("gzip -dc rdf/demo.nt.gz > .index-pipes/0.nt || true\n")
    (workdir / "index-feed.pipes").write_text(".index-pipes/0.nt\n")
    (workdir / f"{name}.settings.json").write_text('{ "ascii-prefixes-only": false }')
    (workdir / "literal-datatypes.json").write_text('{"properties": {}}')
    (workdir / "downloads.json").write_text('{"urls": ["https://example.org/x/demo.nt.gz"]}')
    (workdir / "inputs.json").write_text("{}")
    for index_file in (f"{name}.index.pso", f"{name}.meta-data.json", "server-1-x.log"):
        (workdir / index_file).write_text("index")
    return data_dir, workdir


def test_the_portable_qleverfile_has_no_home_path_a_pinned_image_and_no_job_values(tmp_path):
    data_dir, workdir = _workdir(tmp_path)
    engine = engine_details(ENGINE, LABELS)
    text, left = portable_qleverfile(
        (workdir / "Qleverfile").read_text(), workdir, engine, data_dir=data_dir
    )
    assert str(tmp_path) not in text and "/home/" not in text and not left
    assert "GET_DATA_CMD      = mkdir -p rdf && cd rdf && wget" in text
    assert '"cmd": "cat rdf/demo.nt"' in text
    assert "#  cd <the folder of this Qleverfile>" in text
    assert "adfreiburg/qlever:latest\n" not in text.replace("# That image was built from ", "")
    assert "IMAGE  = qlever.sif" in text and f"IMAGE_SHA256 = {SHA}" in text
    assert "INDEX_BUILD = 9ec88a" in text and "revision 9ec88a09010c" in text
    for key in ("PORT", "ACCESS_TOKEN", "MEMORY_FOR_QUERIES"):
        assert f"\n{key} " not in text, key
    assert "TIMEOUT           = 600s" in text, "Not a value of the job"
    assert "_local_inputs.json" in text, "Says where the checksums of the downloads are"


def test_the_recipe_holds_the_build_files_and_no_data(tmp_path):
    data_dir, workdir = _workdir(tmp_path)
    target = tmp_path / "run" / "demo" / "demo_local_index_recipe"
    recipe = write_index_recipe(
        workdir, target, name="demo", engine=engine_details(ENGINE, {}), data_dir=data_dir
    )
    assert sorted(p.name for p in target.iterdir()) == sorted(
        [
            "Qleverfile",
            "index-command.sh",
            "index-feed.sh",
            "index-feed.pipes",
            "demo.settings.json",
            "literal-datatypes.json",
            "downloads.json",
            RECIPE_FILE,
        ]
    ), "No RDF, index, server log, scan store, nor the inputs.json carried beside it"
    command = (target / "index-command.sh").read_text()
    assert 'cd "$(dirname "${BASH_SOURCE[0]}")"' in command
    assert "-s demo.settings.json" in command and str(tmp_path) not in command
    assert (target / "demo.settings.json").read_bytes() == (
        workdir / "demo.settings.json"
    ).read_bytes()
    assert recipe is not None and recipe["absolute_paths_left"] == {}
    assert json.loads((target / RECIPE_FILE).read_text())["engine"]["image_sha256"] == SHA
    assert "input-repairs.json" not in recipe["files"]


def test_repairs_and_the_export_manifest_are_in_the_recipe_when_there(tmp_path):
    data_dir, workdir = _workdir(tmp_path)
    export = data_dir / "exports" / "demo" / "20261006"
    export.mkdir(parents=True)
    (export / "manifest.json").write_text('{"source": "demo"}\n')
    (export / "manifest-notes.json").write_text('{"lenient_parses": []}\n')
    url = (export / "g0001.whole.ttl.gz").as_uri()
    (workdir / "downloads.json").write_text(json.dumps({"urls": [url]}))
    (workdir / "export_inputs.json").write_text(
        json.dumps({"endpoint_export": {"manifest": str(export / "manifest.json")}})
    )
    (workdir / "input-repairs.json").write_text('{"lines": []}')
    target = tmp_path / "run" / "demo" / "demo_local_index_recipe"
    recipe = write_index_recipe(
        workdir, target, name="demo", engine=engine_details(ENGINE, {}), data_dir=data_dir
    )
    assert recipe is not None
    assert (target / "input-repairs.json").read_text() == '{"lines": []}'
    assert (target / EXPORT_MANIFEST).read_bytes() == (export / "manifest.json").read_bytes()
    assert (target / EXPORT_NOTES).exists()
    record = (target / "downloads.json").read_text()
    assert "file://${RDFSOLVE_DATA_DIR}/exports/demo/20261006/g0001.whole.ttl.gz" in record
    assert str(tmp_path) not in record
    pins = (target / "export_inputs.json").read_text()
    assert "${RDFSOLVE_DATA_DIR}/exports/demo/20261006/manifest.json" in pins
    assert recipe["files"][EXPORT_MANIFEST]["from"] == (
        "${RDFSOLVE_DATA_DIR}/exports/demo/20261006/manifest.json"
    )


def test_a_folder_without_a_qleverfile_has_no_recipe(tmp_path):
    assert write_index_recipe(tmp_path, tmp_path / "out", name="x", engine={}) is None
    assert not (tmp_path / "out").exists()


def test_the_interpreter_of_a_converter_is_written_under_the_home_folder(tmp_path, monkeypatch):
    """A GET_DATA_CMD that converts RDF/XML names the interpreter that wrote it as the default
    of RDFSOLVE_PYTHON; a path under the home folder is written ${HOME}/..."""
    monkeypatch.setenv("HOME", str(tmp_path))
    data_dir, workdir = _workdir(tmp_path)
    python = tmp_path / "venv" / "bin" / "python"
    qleverfile = workdir / "Qleverfile"
    qleverfile.write_text(
        qleverfile.read_text().replace(
            "2>/dev/null", f'&& "${{RDFSOLVE_PYTHON:-{python}}}" -m rdfsolve.qlever.rdfxml a.rdf a.nt'
        )
    )
    text, left = portable_qleverfile(qleverfile.read_text(), workdir, {}, data_dir=data_dir)
    assert '"${RDFSOLVE_PYTHON:-${HOME}/venv/bin/python}" -m rdfsolve.qlever.rdfxml' in text
    assert str(tmp_path) not in text and not left
