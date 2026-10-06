"""Find prepared RDF inputs without silently dropping a directory layout."""

import gzip
import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import IO, cast

RDF_SUFFIXES = ("ttl", "nt", "nq", "trig", "n3")

# An .n3 download is read as Turtle: published .n3 dumps are Turtle or N-Triples (GtoPdb).
_QLEVER_FORMATS = {"ttl": "ttl", "nt": "nt", "nq": "nq", "trig": "ttl", "n3": "ttl"}

_GZIP_MAGIC = b"\x1f\x8b"

# The named pipes through which compressed inputs reach qlever-index, in the work folder.
PIPES = ".index-pipes"
# The script that fills them, beside the index command.
FEED = "index-feed.sh"
FEED_PIPES = "index-feed.pipes"
# The compressed inputs that the feed could not decompress to the end (one per line).
FEED_FAILED = ".index-feed.failed"
# The log of qlever-index, the name that the qlever command line gives it.
INDEX_LOG = "{name}.index-log.txt"
# Beside a converted input that was published empty: the download is accounted for and holds no
# statement (UniProt publishes enzyme-hierarchy.rdf.xz empty), so there is no file to index.
EMPTY = ".empty"


def qlever_format(path: Path) -> str:
    """Map a prepared input to a QLever input format (a .gz suffix is skipped).

    An N-Triples file whose statements name a graph is read as N-Quads: RDF Portal publishes
    the Human Protein Atlas as quads in .nt.gz files, which QLever refuses as N-Triples.
    """
    suffix = (path.with_suffix("") if path.suffix == ".gz" else path).suffix.lstrip(".").lower()
    try:
        found = _QLEVER_FORMATS[suffix]
    except KeyError:
        raise ValueError(f"Unsupported RDF input format: {path}") from None
    return "nq" if found == "nt" and path.exists() and _holds_quads(path) else found


def _holds_quads(path: Path, lines: int = 100) -> bool:
    """Whether the first statements of a line-based file name a graph."""
    from itertools import islice

    from pyoxigraph import DefaultGraph, RdfFormat, parse

    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as stream:
        head = b"".join(islice(stream, lines))
    try:
        return any(
            not isinstance(quad.graph_name, DefaultGraph)
            for quad in parse(head, RdfFormat.N_QUADS, lenient=True)
        )
    except SyntaxError:
        return False


def _is_gzip(path: Path) -> bool:
    with path.open("rb") as stream:
        return stream.read(2) == _GZIP_MAGIC


def cached_archives(workdir: Path) -> list[Path]:
    """List cached compressed inputs in the root or rdf/ layout."""
    found: dict[str, Path] = {}
    for directory in (workdir, workdir / "rdf"):
        for suffix in RDF_SUFFIXES:
            for path in sorted(directory.glob(f"*.{suffix}.gz")):
                if path.is_file():
                    found.setdefault(path.name, path)
    return sorted(found.values())


def unusable_inputs(workdir: Path) -> list[tuple[Path, str]]:
    """Report cached inputs that cannot be parsed, without decompressing them."""
    problems: list[tuple[Path, str]] = []
    for directory in (workdir, workdir / "rdf"):
        for suffix in RDF_SUFFIXES:
            for path in sorted([*directory.glob(f"*.{suffix}"), *directory.glob(f"*.{suffix}.gz")]):
                if not path.is_file():
                    problems.append((path, "missing target"))
                elif not path.stat().st_size:
                    problems.append((path, "empty file"))
                elif path.suffix == ".gz" and not _is_gzip(path):
                    problems.append((path, "not gzip data"))
    return problems


def _converted(trig: Path) -> Path:
    """Return the N-Quads file that a TriG input (plain or compressed) is converted to."""
    return trig.with_name(f"{trig.name.removesuffix('.gz')}.nq")


def convert_trig(workdir: Path) -> list[Path]:
    """Write each TriG input as N-Quads beside it. Return the files created.

    QLever reads Turtle and N-Quads, not TriG: a TriG file read as Turtle fails at its first
    graph block (HPA's nanopublications, 2.3 GB).
    """
    from pyoxigraph import RdfFormat, parse, serialize

    created: list[Path] = []
    for directory in (workdir, workdir / "rdf"):
        for trig in sorted([*directory.glob("*.trig"), *directory.glob("*.trig.gz")]):
            target = _converted(trig)
            if not trig.is_file() or target.exists():
                continue
            partial = target.with_name(f"{target.name}.part")
            try:
                with (
                    gzip.open(trig, "rb") if trig.suffix == ".gz" else trig.open("rb") as stream,
                    partial.open("wb") as output,
                ):
                    quads = parse(cast("IO[bytes]", stream), RdfFormat.TRIG, lenient=True)
                    serialize(quads, output, RdfFormat.N_QUADS)
                partial.replace(target)
            except BaseException:
                partial.unlink(missing_ok=True)
                raise
            created.append(target)
    return created


def expand_inputs(workdir: Path) -> list[Path]:
    """Decompress cached inputs beside the archive and convert TriG. Return the files created."""
    created: list[Path] = []
    for archive in cached_archives(workdir):
        target = archive.with_suffix("")
        if target.exists():
            continue
        if not _is_gzip(archive):
            raise ValueError(f"Cached input is not gzip data: {archive}")
        partial = target.with_name(f"{target.name}.part")
        try:
            with gzip.open(archive, "rb") as stream, partial.open("wb") as output:
                shutil.copyfileobj(stream, output, 8 * 1024 * 1024)
            partial.replace(target)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        created.append(target)
    return created + convert_trig(workdir)


def rdf_input_files(workdir: Path) -> list[Path]:
    """Read root or rdf/ inputs. Reject ambiguous copies and empty files."""
    found: dict[str, Path] = {}
    for directory in (workdir, workdir / "rdf"):
        for suffix in RDF_SUFFIXES:
            for path in sorted(directory.glob(f"*.{suffix}")):
                if suffix == "trig" and _converted(path).is_file():
                    continue
                if not path.is_file() or path.stat().st_size == 0:
                    raise ValueError(f"Missing or empty RDF input: {path}")
                previous = found.get(path.name)
                if previous is not None and not previous.samefile(path):
                    raise ValueError(f"Ambiguous RDF inputs: {previous} and {path}")
                found[path.name] = path
    return sorted(found.values())


def index_inputs(workdir: Path) -> list[Path]:
    """Return the inputs to index: the plain files, and the compressed ones without a plain copy.

    A compressed input is streamed to the index (index_command), not decompressed beside it:
    RDF Portal's DDBJ is 508 GB compressed, several TB plain.
    """
    plain = rdf_input_files(workdir)
    names = {path.name for path in plain}
    compressed = []
    for path in cached_archives(workdir):
        if path.name.removesuffix(".gz") in names:
            continue
        if path.name.endswith(".trig.gz") and _converted(path).is_file():
            continue
        if not path.stat().st_size or not _is_gzip(path):
            raise ValueError(f"Missing, empty or not gzip RDF input: {path}")
        compressed.append(path)
    return sorted([*plain, *compressed])


def graph_input_directory(workdir: Path, graph: str) -> Path:
    """Return the input directory for a named graph."""
    return workdir / "graphs" / hashlib.sha256(graph.encode()).hexdigest()


def empty_inputs(directory: Path) -> list[Path]:
    """List the markers of inputs that were published empty, in the root or rdf/ layout."""
    return sorted(
        path for folder in (directory, directory / "rdf") for path in folder.glob(f"*{EMPTY}")
    )


def mapped_input_files(workdir: Path, graphs: list[str]) -> list[tuple[Path, str]]:
    """Require prepared triple files for every mapped graph.

    A graph whose inputs were all published empty has no file to index and is not an error.
    """
    inputs: list[tuple[Path, str]] = []
    for graph in graphs:
        directory = graph_input_directory(workdir, graph)
        files = index_inputs(directory)
        if not files and not empty_inputs(directory):
            raise ValueError(f"No prepared inputs for graph {graph}")
        if any(qlever_format(path) not in {"ttl", "nt"} for path in files):
            raise ValueError(f"Mapped graph {graph} requires triple inputs")
        inputs.extend((path, graph) for path in files)
    return inputs


def index_command(
    image: Path,
    data_dir: Path,
    workdir: Path,
    name: str,
    settings_path: Path,
    mapped: list[tuple[Path, str]],
    *,
    parallel: str,
    buffer: str,
    memory: str,
) -> list[str]:
    """Write the qlever-index command to WORKDIR/index-command.sh and return the command that runs it.

    The inputs are given relative to the work folder, and the container runs the script, so the
    file list does not pass through the command line of Singularity, which refuses a long one
    (WikiPathways: 12,543 files). Each input stays its own file, so that blank nodes of different
    documents stay apart.

    qlever-index does not read gzip: given a .gz file it builds an empty index and reports
    success. A compressed input is given as a named pipe that one feeder fills with the
    decompressed file, in the order of the inputs, which is the order qlever-index reads them;
    no plain copy is written. When qlever-index ends, the feeder is stopped. A file is fed
    decompressed when its suffix is .gz or its first bytes are gzip's (a .nt file that holds
    gzip data); a file that gzip cannot decompress to the end is named in FEED_FAILED and the
    command fails after qlever-index, which reads a truncated stream without an error.

    The output of qlever-index is written to INDEX_LOG as well: qlever-index skips the rest of
    an input after a statement it cannot read, logs it and succeeds
    (rdfsolve.qlever.index_check.unparsed_input reads the log).
    """
    import os
    import shlex

    args = ["qlever-index", "-i", name, "-s", str(settings_path)]
    feeds: list[tuple[str, str]] = []
    for path, graph in mapped:
        relative = os.path.relpath(path, workdir)
        if path.suffix == ".gz" or (path.is_file() and _is_gzip(path)):
            pipe = f"{PIPES}/{len(feeds)}.{qlever_format(path)}"
            feeds.append((relative, pipe))
            relative = pipe
        args += ["-f", relative, "-F", qlever_format(path)]
        if graph:
            args += ["-g", graph]
    args += ["-p", parallel, "-b", buffer, "-m", memory]
    # set -o pipefail keeps the exit status of qlever-index.
    run = f"{shlex.join(args)} 2>&1 | tee {shlex.quote(INDEX_LOG.format(name=name))}"
    lines = [
        "#!/bin/bash",
        "# Written by rdfsolve: the index command, run inside the container.",
        "set -euo pipefail",
        f"cd {shlex.quote(str(workdir))}",
    ]
    if feeds:
        # The feed is a script of its own: one argument cannot hold it (PDB: 220,000 files).
        (workdir / FEED_PIPES).write_text("".join(f"{pipe}\n" for _, pipe in feeds))
        (workdir / FEED).write_text(
            # A write that fails (qlever-index stopped reading a pipe) goes on to the next pipe:
            # every pipe that qlever-index opens gets a writer, so that it ends, with its error.
            "".join(
                f"gzip -dc {shlex.quote(src)} > {shlex.quote(pipe)} || "
                f"echo {shlex.quote(src)} >> {FEED_FAILED}\n"
                for src, pipe in feeds
            )
        )
        lines += [
            f"rm -rf {PIPES} {FEED_FAILED} && mkdir {PIPES}",
            f"xargs -d '\\n' -a {FEED_PIPES} mkfifo --",
            # The feeder is a process group of its own: a gzip waiting for its pipe is stopped
            # with it when qlever-index ends early.
            f"setsid bash {FEED} &",
            "feeder=$!",
            f"trap 'kill -- -$feeder 2>/dev/null || true; rm -rf {PIPES}' EXIT",
            run,
            "wait $feeder",
            (
                f"if [ -s {FEED_FAILED} ]; then echo 'Inputs not decompressed to the end:' >&2; "
                f"cat {FEED_FAILED} >&2; exit 1; fi"
            ),
        ]
    else:
        lines.append(run)
    script = workdir / "index-command.sh"
    script.write_text("\n".join(lines) + "\n")
    return [
        "singularity",
        "exec",
        "--bind",
        f"{data_dir}:{data_dir}",
        str(image),
        "bash",
        str(script),
    ]
