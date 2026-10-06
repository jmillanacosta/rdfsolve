"""rdfsolve.qlever.utils: archives are extracted once and not renamed, downloads are retried, a failed
download or conversion stops the step."""

import configparser
import os
import subprocess
import zipfile

import pytest

from rdfsolve.qlever import QleverConfig, build_qleverfile
from rdfsolve.qlever.utils import (
    _BLAZEGRAPH_RUNNER,
    _build_get_data_steps,
    _convert_journal_steps,
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


def test_a_plain_tar_named_as_compressed_is_extracted(tmp_path):
    import tarfile

    member = tmp_path / "kg.ttl"
    member.write_text("<urn:a> <urn:p> <urn:o> .\n")
    with tarfile.open(tmp_path / "metrin-kg.tar.gz", "w") as archive:
        archive.add(member, "metrin-kg/KG/kg.ttl")
    member.unlink()
    done = subprocess.run(
        ["bash"],
        input=" && ".join(_extract_archives_steps()),
        text=True,
        cwd=tmp_path,
        capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "kg.ttl").is_file()


def test_a_published_folder_is_fetched_with_its_subfolders(tmp_path):
    import functools
    import http.server
    import threading

    from rdfsolve.qlever.utils import _build_get_data_steps, analyse_source

    site = tmp_path / "site" / "ntriples" / "pdb" / "latest" / "pdb"
    # RDF Portal's MassBank files are 6 folders down; wget -r stops at 5 by default.
    for folder, name in (("00", "100d"), ("01", "101d"), ("a/b/c/d/e/f", "deep")):
        (site / folder).mkdir(parents=True)
        (site / folder / f"{name}.nt").write_text(f"<urn:{name}> <urn:p> <urn:o> .\n")
    (site / "00" / "notes.txt").write_text("not RDF")
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(tmp_path / "site")
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/ntriples/pdb/latest/pdb/"
        steps = _build_get_data_steps(analyse_source({"download_nt": [url]}), str(tmp_path / "rdf"))
        env = {**os.environ, "no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"}
        done = subprocess.run(
            ["bash"], input=" && ".join(steps), text=True, capture_output=True, env=env
        )
    finally:
        server.shutdown()
    assert done.returncode == 0, done.stderr
    assert sorted(p.name for p in (tmp_path / "rdf").glob("*.nt")) == ["100d.nt", "101d.nt", "deep.nt"]
    assert not list((tmp_path / "rdf").rglob("*.txt")) and not list(
        (tmp_path / "rdf").rglob("index.html*")
    )


def test_downloads_with_one_file_name_are_saved_apart():
    from rdfsolve.qlever.utils import _build_get_data_steps, analyse_source

    urls = [f"https://zenodo.org/records/{r}/files/dataset.nq" for r in (1, 2)]
    entry = {"download_nq": [*urls, "https://zenodo.org/records/3/files/other.nq"]}
    script = " ".join(_build_get_data_steps(analyse_source(entry), "rdf"))
    assert f'-O "1__dataset.nq" "{urls[0]}"' in script
    assert f'-O "2__dataset.nq" "{urls[1]}"' in script
    assert '"https://zenodo.org/records/3/files/other.nq"' in script and "__other" not in script


def test_a_download_without_an_rdf_name_is_named_by_its_field():
    from rdfsolve.qlever.utils import _build_get_data_steps, analyse_source

    url = "https://ftp.dbcls.jp/allie/allie_rdf/pubmed_rdf_nt_latest.gz"
    script = " ".join(_build_get_data_steps(analyse_source({"download_nt": [url]}), "rdf"))
    assert f'-O "pubmed_rdf_nt_latest.nt.gz" "{url}"' in script
    assert "*.nt.gz" in script, "Decompressed as N-Triples"


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


# One statement in RDF/XML: <urn:s> <urn:p> <urn:o> .
RDFXML = (
    '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:u="urn:">'
    '<rdf:Description rdf:about="urn:s"><u:p rdf:resource="urn:o"/></rdf:Description></rdf:RDF>'
)


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
    (tmp_path / "model.owl").write_text(f'<?xml version="1.0"?>\n{RDFXML}\n')
    done = subprocess.run(
        ["bash"],
        input=" && ".join(_convert_rdfxml_steps()),
        text=True,
        cwd=tmp_path,
        capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "ontology.ttl").read_text().startswith("@prefix")
    assert not (tmp_path / "ontology.nq").exists()
    assert (tmp_path / "model.nq").read_text() == "<urn:s> <urn:p> <urn:o> .\n"


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


def test_rdfxml_mapped_to_a_graph_is_converted_and_an_empty_file_is_accounted_for(tmp_path):
    """RDF/XML inputs of a graph mapping become N-Triples with the graph of the mapping. A file
    published empty (UniProt's enzyme-hierarchy.rdf.xz) is marked, not converted: the converter
    would stop the build at it, and the index would lack an input that the entry lists."""
    import functools
    import http.server
    import json
    import lzma
    import threading

    from rdfsolve.qlever.inputs import empty_inputs, graph_input_directory, mapped_input_files

    site = tmp_path / "site"
    site.mkdir()
    (site / "enzyme.rdf.xz").write_bytes(lzma.compress(RDFXML.encode()))
    (site / "enzyme-hierarchy.rdf.xz").write_bytes(lzma.compress(b""))
    (site / "core.owl").write_text(RDFXML)
    (site / "go.owl.xz").write_bytes(lzma.compress(b""))
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(site))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    entry = {
        "name": "fixture",
        "graph_sources": {
            "urn:enzymes": {
                "download_rdf": [f"{base}/enzyme.rdf.xz", f"{base}/enzyme-hierarchy.rdf.xz"]
            },
            "urn:core": {"download_owl": [f"{base}/core.owl"]},
            # A graph whose only input is empty holds nothing; it is not an error.
            "urn:go": {"download_owl": [f"{base}/go.owl.xz"]},
        },
    }
    workdir = tmp_path / "work"
    parsed = configparser.ConfigParser(interpolation=None)
    parsed.read_string(build_qleverfile(entry, tmp_path, 7000, "docker", workdir=workdir))
    env = {
        **os.environ,
        "no_proxy": "127.0.0.1",
        "NO_PROXY": "127.0.0.1",
    }
    try:
        done = subprocess.run(
            ["bash"], input=parsed["data"]["GET_DATA_CMD"], text=True, capture_output=True, env=env
        )
    finally:
        server.shutdown()
    assert done.returncode == 0, done.stderr

    mapped = mapped_input_files(workdir, list(entry["graph_sources"]))
    assert sorted(graph for _, graph in mapped) == ["urn:core", "urn:enzymes"]
    assert all(path.suffix == ".nt" for path, _ in mapped)
    assert len(empty_inputs(graph_input_directory(workdir, "urn:enzymes"))) == 1
    assert len(empty_inputs(graph_input_directory(workdir, "urn:go"))) == 1
    streams = json.loads(parsed["index"]["MULTI_INPUT_JSON"])
    outputs = [
        subprocess.run(["bash"], input=s["cmd"], text=True, capture_output=True, check=True).stdout
        for s in streams
    ]
    assert sorted(o for o in outputs if o) == ["<urn:s> <urn:p> <urn:o> .\n"] * 2
    assert {s["format"] for s in streams} == {"nt"}


def test_archive_members_that_are_not_data_are_left_out_of_the_index(tmp_path):
    """Cellosaurus's RDF archive holds its query examples and a VoID description beside the
    data: the entry names them, and they are kept in left_out/, where the index does not look."""
    import functools
    import http.server
    import tarfile
    import threading

    from rdfsolve.qlever.inputs import index_inputs
    from rdfsolve.qlever.utils import _build_get_data_steps, analyse_source

    members = tmp_path / "members" / "rdf_data"
    members.mkdir(parents=True)
    for name in ("data_cell_lines.ttl", "ontology.ttl", "queries.ttl", "void-cello.ttl"):
        (members / name).write_text(f"<urn:{name}> <urn:p> <urn:o> .\n")
    site = tmp_path / "site"
    site.mkdir()
    with tarfile.open(site / "dump_ttl.tar.gz", "w:gz") as archive:
        archive.add(members, arcname="rdf_data")
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(site))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    entry = {
        "download_tgz": f"http://127.0.0.1:{server.server_port}/dump_ttl.tar.gz",
        "archive_members_left_out": ["queries.ttl", "void-*.ttl"],
    }
    try:
        steps = _build_get_data_steps(analyse_source(entry), str(tmp_path / "work" / "rdf"))
        env = {**os.environ, "no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"}
        done = subprocess.run(
            ["bash"], input=" && ".join(steps), text=True, capture_output=True, env=env
        )
    finally:
        server.shutdown()
    assert done.returncode == 0, done.stderr
    assert "left out of the index: queries.ttl" in done.stdout
    assert [p.name for p in index_inputs(tmp_path / "work")] == [
        "data_cell_lines.ttl",
        "ontology.ttl",
    ]
    left_out = tmp_path / "work" / "rdf" / "left_out"
    assert sorted(p.name for p in left_out.iterdir()) == ["queries.ttl", "void-cello.ttl"]


@pytest.mark.parametrize("pattern", ["../queries.ttl", "rdf/queries.ttl", "*", "a b.ttl", "$(x)"])
def test_left_out_patterns_are_file_names(pattern):
    from rdfsolve.models.source_model import SourceModel

    with pytest.raises(ValueError):
        SourceModel.model_validate({"name": "x", "archive_members_left_out": [pattern]})


# A Blazegraph journal (GO-CAM) is exported to N-Quads with blazegraph-runner before indexing.

JOURNAL_URL = "https://example.org/store/blazegraph-production.jnl.gz"


def _run_export(tmp_path, java="exit 0"):
    """Run the export steps with a java stand-in (the runner stand-in does not call it)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "java").write_text(f"#!/bin/sh\n{java}\n")
    (bin_dir / "java").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    return subprocess.run(
        ["bash"],
        input=" && ".join(_convert_journal_steps()),
        text=True,
        cwd=tmp_path,
        env=env,
        capture_output=True,
    )


def _fake_runner(tmp_path, body):
    """Put a blazegraph-runner stand-in where the step looks for it (no download)."""
    runner = tmp_path / _BLAZEGRAPH_RUNNER
    runner.parent.mkdir(parents=True)
    runner.write_text("#!/bin/sh\n" + body)
    runner.chmod(0o755)
    return runner


def test_a_journal_is_exported_to_compressed_nquads_without_the_runner_log(tmp_path):
    import gzip

    (tmp_path / "store.jnl").write_bytes(b"journal")
    # The runner logs to its standard output; the data go to the file it is given (last argument).
    _fake_runner(
        tmp_path,
        'echo "INFO opening journal"\n'
        "for a; do out=$a; done\n"
        'printf "<urn:s> <urn:p> <urn:o> <urn:g> .\\n" > "$out"\n',
    )
    done = _run_export(tmp_path)
    assert done.returncode == 0, done.stderr
    assert gzip.decompress((tmp_path / "store.nq.gz").read_bytes()) == (
        b"<urn:s> <urn:p> <urn:o> <urn:g> .\n"
    )
    assert not (tmp_path / "store.nq.gz.part").exists()
    assert not (tmp_path / ".jnl-export.fifo").exists()


def test_a_failed_journal_export_stops_the_step_and_leaves_no_output(tmp_path):
    (tmp_path / "store.jnl").write_bytes(b"journal")
    _fake_runner(tmp_path, 'for a; do out=$a; done\necho partial > "$out"\nexit 1\n')
    done = _run_export(tmp_path)
    assert done.returncode != 0 and "Journal export failed: store.jnl" in done.stderr
    assert not (tmp_path / "store.nq.gz").exists()
    assert not (tmp_path / "store.nq.gz.part").exists()


def test_an_exported_journal_is_not_exported_again(tmp_path):
    (tmp_path / "store.jnl").write_bytes(b"journal")
    (tmp_path / "store.nq.gz").write_bytes(b"kept")
    _fake_runner(tmp_path, "exit 1\n")
    done = _run_export(tmp_path)
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "store.nq.gz").read_bytes() == b"kept"


def test_a_journal_download_is_indexed_as_nquads_from_its_export(tmp_path):
    from pathlib import Path

    from rdfsolve.qlever.downloads import download_names, unlisted_inputs
    from rdfsolve.sources import SourceModel, classify_source_mode

    entry = {"name": "store", "endpoint": "https://example.org/sparql", "download_jnl": JOURNAL_URL}
    analysis = analyse_source(entry)
    assert analysis.needs_journal_conversion and analysis.needs_gz
    steps = " && ".join(_build_get_data_steps(analysis, str(tmp_path)))
    assert "blazegraph-runner" in steps and "dump" in steps
    qleverfile = build_qleverfile(entry, tmp_path, 7001, "native")
    assert "FORMAT            = nq" in qleverfile
    # The export is the download converted: an index input the registry entry accounts for.
    assert download_names(entry) == {JOURNAL_URL: "blazegraph-production.jnl.gz"}
    assert unlisted_inputs(download_names(entry), [Path("blazegraph-production.nq.gz")]) == []
    assert classify_source_mode(SourceModel.model_validate(entry)) == "both"


def test_a_journal_export_without_java_stops_before_the_runner(tmp_path):
    (tmp_path / "store.jnl").write_bytes(b"journal")
    _fake_runner(tmp_path, "touch ran\n")
    done = _run_export(tmp_path, java="exit 127")
    assert done.returncode != 0 and "needs java on the PATH" in done.stderr
    assert not (tmp_path / "ran").exists()
