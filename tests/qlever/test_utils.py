"""rdfsolve.qlever.utils: archives are extracted once and not renamed, downloads are retried, a failed
download or conversion stops the step."""

import configparser
import os
import subprocess
import zipfile

from rdfsolve.qlever import QleverConfig, build_qleverfile
from rdfsolve.qlever.utils import (
    _build_get_data_steps,
    _convert_obo_steps,
    _convert_rdfxml_steps,
    _extract_archives_steps,
    _rename_mislabelled_steps,
    _wget_cmd,
    analyse_source,
)


def test_qleverfile_preserves_parser_buffer(tmp_path):
    source = {"name": "fixture", "download_ttl": "https://example.org/data.ttl"}
    for settings, expected in [(None, "10M"), (QleverConfig(parser_buffer_size="20M"), "20M")]:
        output = build_qleverfile(source, tmp_path, 7000, "docker", cfg=settings)
        parsed = configparser.ConfigParser(interpolation=None)
        parsed.read_string(output)
        assert parsed["index"]["PARSER_BUFFER_SIZE"] == expected, (
            "Wrong parser buffer in Qleverfile"
        )

    import json
    import subprocess

    from rdflib import Graph, Namespace

    source = {
        "name": "fixture",
        "download_owl": ["https://example.org/a.owl", "https://example.org/b.owl"],
    }
    rdf = tmp_path / "rdf"
    rdf.mkdir()
    (rdf / "a.nq").write_text("_:same <urn:test:kind> <urn:test:Restriction> .\n")
    (rdf / "b.nq").write_text("_:same <urn:test:kind> <urn:test:Axiom> .\n")
    parsed.read_string(build_qleverfile(source, tmp_path, 7000, "docker"))
    index = parsed["index"]
    graph = Graph()
    if index.get("MULTI_INPUT_JSON"):
        assert not index["CAT_INPUT_FILES"], "Two input modes are configured"
        for spec in json.loads(index["MULTI_INPUT_JSON"]):
            for path in sorted(tmp_path.glob(spec["for-each"])):
                raw = subprocess.check_output(["bash", "-c", spec["cmd"].format(path)])
                graph.parse(data=raw.decode(), format="nt")
    else:
        raw = subprocess.check_output(
            ["bash", "-c", 'INPUT_FILES="rdf/*.nq"; ' + index["CAT_INPUT_FILES"]], cwd=tmp_path
        )
        graph.parse(data=raw.decode(), format="nt")
    ns = Namespace("urn:test:")
    restrictions = set(graph.subjects(ns.kind, ns.Restriction))
    axioms = set(graph.subjects(ns.kind, ns.Axiom))
    assert len(restrictions) == len(axioms) == 1, "A document lost its record"
    assert restrictions.isdisjoint(axioms), "Blank nodes from different documents were merged"


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


def test_the_second_pass_skips_extracted_archives(tmp_path):
    inner = tmp_path / "inner.zip"
    with zipfile.ZipFile(inner, "w") as z:
        z.writestr("deep/b.ttl", "<urn:b> <urn:p> <urn:o> .\n")
    with zipfile.ZipFile(tmp_path / "outer.zip", "w") as z:
        z.writestr("wp/a.ttl", "<urn:a> <urn:p> <urn:o> .\n")
        z.write(inner, "nested/inner.zip")
    inner.unlink()
    done = subprocess.run(
        ["bash"],
        input=" && ".join(_extract_archives_steps()),
        text=True,
        cwd=tmp_path,
        capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    turtle = sorted(p.name for p in tmp_path.glob("*.ttl"))
    assert turtle == ["a.ttl", "b.ttl"], "Each file once, the nested archive extracted too"


def test_an_empty_archive_member_is_left_out(tmp_path):
    with zipfile.ZipFile(tmp_path / "cordis.zip", "w") as z:
        z.writestr("extraction/Grant.nq", "<urn:a> <urn:p> <urn:o> <urn:g> .\n")
        z.writestr("extraction/Person.nq", "")
    done = subprocess.run(
        ["bash"],
        input=" && ".join(_extract_archives_steps()),
        text=True,
        cwd=tmp_path,
        capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    assert [p.name for p in tmp_path.rglob("*.nq")] == ["Grant.nq"]
    assert "empty archive member left out: ./extraction/Person.nq" in done.stdout


def test_each_download_command_retries_passing_faults():
    for url in ("https://example.org/a/data.ttl.gz", "https://example.org/download?id=7"):
        command = _wget_cmd(url)
        assert "--tries=5" in command and "--waitretry=20" in command
        assert "--retry-connrefused" in command
        assert "--retry-on-http-error=429,500,502,503,504" in command
        assert "404" not in command and command.startswith("wget -c -q ")


def test_a_failed_connection_is_tried_again_and_a_missing_file_is_not(tmp_path):
    """wget does not try again after "Unable to establish SSL connection" (exit 5). The download
    step tries such a file again; a missing file (exit 8) fails at once."""
    import os
    import subprocess

    from rdfsolve.qlever.utils import _build_get_data_steps, analyse_source

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "wget").write_text(
        "#!/bin/sh\n"
        f'echo x >> "{tmp_path}/calls"\n'
        'for a in "$@"; do case "$a" in *missing*) exit 8;; esac; done\n'
        f'[ "$(wc -l < "{tmp_path}/calls")" -lt 3 ] && exit 5\n'
        'for a in "$@"; do case "$a" in http*) touch "$(basename "$a")";; esac; done\n'
    )
    (bin_dir / "wget").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "RDFSOLVE_DOWNLOAD_WAIT": "0"}

    def run(url):
        steps = _build_get_data_steps(
            analyse_source({"name": "fixture", "download_ttl": [url]}), str(tmp_path / "rdf")
        )
        return subprocess.run(
            ["bash"], input=" && ".join(steps), text=True, env=env, capture_output=True
        )

    assert run("https://example.org/data.ttl").returncode == 0
    assert (tmp_path / "rdf" / "data.ttl").exists()
    assert len((tmp_path / "calls").read_text().splitlines()) == 3, (
        "Two failed connections, then the file"
    )
    (tmp_path / "calls").write_text("x\nx\nx\n")
    assert run("https://example.org/missing.ttl").returncode != 0
    assert len((tmp_path / "calls").read_text().splitlines()) == 4, (
        "A missing file is asked for once"
    )


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
    done = subprocess.run(
        ["bash"], input=" && ".join(steps), text=True, env=env, capture_output=True
    )
    assert done.returncode != 0, "The 404 of missing.ttl must fail the step"
    assert not (tmp_path / "rdf" / "data.ttl").exists()


def test_a_failed_download_is_not_hidden_by_the_decompression_step(tmp_path):
    """The decompression step ends with '|| true'; a failed download still fails the chain, so a
    source is not indexed from part of its files."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "wget").write_text(
        '#!/bin/sh\nfor a in "$@"; do case "$a" in *missing*) exit 8;; esac; done\n'
        'for a in "$@"; do case "$a" in http*) touch "$(basename "$a")";; esac; done\n'
    )
    (bin_dir / "wget").chmod(0o755)
    urls = ["https://example.org/a/first.ttl.gz", "https://example.org/b/missing.ttl.gz"]
    steps = _build_get_data_steps(
        analyse_source({"name": "fixture", "download_ttl": urls}), str(tmp_path / "rdf")
    )
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    done = subprocess.run(
        ["bash"], input=" && ".join(steps), text=True, env=env, capture_output=True
    )
    assert done.returncode != 0, "The failed download must fail the step"
    assert "download failed" in done.stderr


def _run(steps, tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("rapper", "java"):
        (bin_dir / tool).write_text("#!/bin/sh\necho broken >&2\nexit 1\n")
        (bin_dir / tool).chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    return subprocess.run(
        ["bash"], input=" && ".join(steps), text=True, cwd=tmp_path, env=env, capture_output=True
    )


def test_a_failed_rdfxml_conversion_stops_the_step(tmp_path):
    (tmp_path / "data.owl").write_text("<rdf/>")
    done = _run(_convert_rdfxml_steps(), tmp_path)
    assert done.returncode != 0 and "Conversion failed: data.owl" in done.stderr
    assert not (tmp_path / "data.nq").exists()


def test_a_failed_obo_conversion_stops_the_step(tmp_path):
    (tmp_path / "terms.obo").write_text("format-version: 1.2\n")
    (tmp_path / "robot.jar").write_text("")
    done = _run(_convert_obo_steps(), tmp_path)
    assert done.returncode != 0 and "Conversion failed: terms.obo" in done.stderr
    assert not (tmp_path / "terms.ttl").exists()


def test_turtle_in_an_owl_file_is_indexed_as_turtle(tmp_path):
    """A .owl file that holds Turtle is named .ttl, not given to the RDF/XML converter, which
    refuses it and would stop the build."""
    (tmp_path / "ontology.owl").write_text("@prefix : <urn:x#> .\n:a a :B .\n")
    (tmp_path / "model.owl").write_text('<?xml version="1.0"?>\n<rdf:RDF/>\n')
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "rapper").write_text('#!/bin/sh\necho "<urn:s> <urn:p> <urn:o> <urn:g> ."\n')
    (bin_dir / "rapper").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    done = subprocess.run(
        ["bash"],
        input=" && ".join(_convert_rdfxml_steps()),
        text=True,
        cwd=tmp_path,
        env=env,
        capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "ontology.ttl").read_text().startswith("@prefix")
    assert not (tmp_path / "ontology.nq").exists() and (tmp_path / "model.nq").exists()


def test_an_empty_file_is_skipped(tmp_path):
    """A published file can be empty: it has no statements, and the converter's "Premature end of
    file" would stop the build."""
    (tmp_path / "enzyme-hierarchy.rdf").write_text("")
    done = _run(_convert_rdfxml_steps(), tmp_path)
    assert done.returncode == 0, done.stderr
    assert not (tmp_path / "enzyme-hierarchy.nq").exists()


def test_a_compressed_file_behind_a_download_url_is_decompressed():
    """Zenodo serves a file at .../files/NAME.nq.gz/content: the file is saved as NAME.nq.gz,
    and its compression is read from that name, not from the end of the URL."""
    url = "https://zenodo.org/api/records/1/files/graph_v3.nq.gz/content"
    analysis = analyse_source({"name": "demo", "download_nq": url})
    assert analysis.needs_gz
    assert _wget_cmd(url).endswith(f'-O "graph_v3.nq.gz" "{url}"')
