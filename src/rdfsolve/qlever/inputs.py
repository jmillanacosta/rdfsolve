"""Find prepared RDF inputs without silently dropping a directory layout."""

import gzip
import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import IO, cast

RDF_SUFFIXES = ("ttl", "nt", "nq", "trig", "n3")

# An .n3 download is read as Turtle: published .n3 dumps are Turtle or N-Triples.
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
# statement, so there is no file to index.
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


def _holds_quads(path: Path, size: int = 8 << 20) -> bool:
    """Whether a statement in the first SIZE bytes of a line-based file names a graph.

    A file can open with thousands of triples and only then hold quads, so a window of a few
    lines would say N-Triples and qlever-index would refuse the file. A head that
    the parser refuses is read line by line, so that one bad line does not hide the quads.
    """
    from pyoxigraph import DefaultGraph, RdfFormat, parse

    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as stream:
        head = stream.read(size)
    if len(head) == size:
        head = head[: head.rfind(b"\n") + 1]

    def named(data: bytes) -> bool:
        """Whether a statement of DATA names a graph."""
        return any(
            not isinstance(quad.graph_name, DefaultGraph)
            for quad in parse(data, RdfFormat.N_QUADS, lenient=True)
        )

    try:
        return named(head)
    except SyntaxError:
        for line in head.splitlines(keepends=True):
            try:
                if named(line):
                    return True
            except SyntaxError:
                continue
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
    graph block.
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
    a plain copy of a large source can take terabytes.
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


# The most bytes the qlever-index arguments may take: above it, line-based inputs share pipes.
# (The kernel refuses a command line over its argument limit, a few MB at most.)
ARG_BUDGET = 1 << 20
# The most pipes the inputs that share pipes are spread over.
SHARED_PIPES = 1000
# Blank nodes of a file that shares a pipe are renamed with this prefix and the file's number.
_BLANK_PREFIX = "f"
# Renames the blank nodes of N-Triples or N-Quads read from stdin, in the subject, object and
# graph positions (not in literals), by putting $1 before each label.
_RELABEL = r"""set -o pipefail
relabel() {
  LC_ALL=C sed -E "/_:/{s/^_:/_:$1/;s/^((<[^>]*>|_:[^ \t]+)[ \t]+<[^>]*>[ \t]+)_:/\1_:$1/;s/^((<[^>]*>|_:[^ \t]+)[ \t]+<[^>]*>[ \t]+(<[^>]*>|_:[^ \t]+|\"([^\"\\\\]|\\\\.)*\"(@[A-Za-z0-9-]+|\^\^<[^>]*>)?)[ \t]+)_:/\1_:$1/}"
}
"""


def _input_groups(inputs: list[tuple[str, str, str, Path]]) -> list[list[int]]:
    """Group the inputs so that the qlever-index arguments stay under ARG_BUDGET.

    Consecutive N-Triples or N-Quads inputs of one graph share a pipe, in order, spread over at
    most SHARED_PIPES pipes; other inputs keep their own. Turtle cannot be joined (each file
    has its own prefixes), so too many Turtle inputs raise a ValueError.
    """
    line_based = [i for i, (_, fmt, _, _) in enumerate(inputs) if fmt in {"nt", "nq"}]
    rest = sum(len(r) + len(g) + 16 for r, fmt, g, _ in inputs if fmt not in {"nt", "nq"})
    if rest > ARG_BUDGET // 2:
        raise ValueError(
            f"{len(inputs) - len(line_based)} Turtle inputs are too many to index in one "
            "qlever-index call; convert them to N-Triples, which can share pipes"
        )
    per_pipe = max(1, -(-len(line_based) // SHARED_PIPES))
    groups: list[list[int]] = []
    for i, (_, fmt, graph, _) in enumerate(inputs):
        last = groups[-1] if groups else None
        if (
            fmt in {"nt", "nq"}
            and last
            and len(last) < per_pipe
            and inputs[last[0]][1:3] == (fmt, graph)
            and last[-1] == i - 1
        ):
            last.append(i)
        else:
            groups.append([i])
    return groups


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
    file list does not pass through the command line of Singularity, which refuses a long one.
    Each input stays its own file, so that blank nodes of different
    documents stay apart. When the arguments would pass ARG_BUDGET (the kernel refuses a
    command line over its argument limit), consecutive N-Triples or N-Quads inputs of one
    graph share a pipe instead, each file's blank nodes renamed apart (_input_groups).

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

    inputs = [
        (os.path.relpath(path, workdir), qlever_format(path), graph, path) for path, graph in mapped
    ]
    size = sum(len(r) + len(g) + 16 for r, _, g, _ in inputs)
    groups = _input_groups(inputs) if size > ARG_BUDGET else [[i] for i in range(len(inputs))]
    args = ["qlever-index", "-i", name, "-s", str(settings_path)]
    feeds: list[tuple[list[str], str]] = []
    for group in groups:
        relative, fmt, graph, path = inputs[group[0]]
        if len(group) > 1:
            pipe = f"{PIPES}/{len(feeds)}.{fmt}"
            feeds.append(([inputs[i][0] for i in group], pipe))
            relative = pipe
        elif path.suffix == ".gz" or (path.is_file() and _is_gzip(path)):
            pipe = f"{PIPES}/{len(feeds)}.{fmt}"
            feeds.append(([relative], pipe))
            relative = pipe
        args += ["-f", relative, "-F", fmt]
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
        # The feed is a script of its own: one argument cannot hold it for many files.
        (workdir / FEED_PIPES).write_text("".join(f"{pipe}\n" for _, pipe in feeds))
        (workdir / FEED).write_text(
            # A write that fails (qlever-index stopped reading a pipe) goes on to the next pipe:
            # every pipe that qlever-index opens gets a writer, so that it ends, with its error.
            # Files that share a pipe are written one after the other, each with its blank
            # nodes renamed apart (gzip -dcf passes a plain file through).
            (_RELABEL if any(len(srcs) > 1 for srcs, _ in feeds) else "")
            + "".join(
                (
                    f"gzip -dc {shlex.quote(srcs[0])} > {shlex.quote(pipe)} || "
                    f"echo {shlex.quote(srcs[0])} >> {FEED_FAILED}\n"
                )
                if len(srcs) == 1
                else "{\n"
                + "".join(
                    f"gzip -dcf {shlex.quote(src)} | relabel {_BLANK_PREFIX}{n}_ || "
                    f"echo {shlex.quote(src)} >> {FEED_FAILED}\n"
                    for n, src in enumerate(srcs)
                )
                + f"}} > {shlex.quote(pipe)}\n"
                for srcs, pipe in feeds
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
