"""Literal datatypes that a QLever index does not keep, counted from the source files.

QLever stores numbers as values: it returns every integer type as xsd:int, and DATATYPE() gives
xsd:double for a decimal (a documented deviation from SPARQL 1.1). AOP-Wiki void:triples is
xsd:integer and Disease Ontology owl:qualifiedCardinality is xsd:nonNegativeInteger in the source,
xsd:int in the index. The source files are read once when the index is built; the numeric
literals of each property are counted by datatype, and mining restores the datatype of the
source when the property has one datatype of the group that QLever reports.
"""

from __future__ import annotations

import gzip
import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any, cast

XSD = "http://www.w3.org/2001/XMLSchema#"
INTEGERS = frozenset(
    XSD + name
    for name in (
        "integer",
        "nonNegativeInteger",
        "positiveInteger",
        "nonPositiveInteger",
        "negativeInteger",
        "long",
        "int",
        "short",
        "byte",
        "unsignedLong",
        "unsignedInt",
        "unsignedShort",
        "unsignedByte",
    )
)
DECIMALS = frozenset(XSD + name for name in ("decimal", "double", "float"))
# The census of an index, in its work directory.
CENSUS_FILE = "literal-datatypes.json"
# The datatype that QLever reports, and the source datatypes that it stands for.
REPORTED = {XSD + "int": INTEGERS, XSD + "double": DECIMALS}
NUMERIC = INTEGERS | DECIMALS
# The suffixes of RDF files, also with .gz.
RDF_SUFFIXES = (".ttl", ".nt", ".nq", ".trig", ".n3", ".owl", ".rdf")
# A member of a zip archive is written ARCHIVE.zip!MEMBER.
MEMBER = ".zip!"
# Line-based formats: a line that the census parser refuses is left out and recorded.
LINE_FORMATS = frozenset({"nt", "nq"})
# Lines parsed at once; a block that the parser refuses is read line by line.
BLOCK_LINES = 100_000
# The refused lines kept as a sample in the census, and the characters kept of each.
SAMPLE_LINES = 20
SAMPLE_CHARS = 500


def _format(name: str) -> Any:
    """Return the pyoxigraph format of a QLever input format (ttl, nt, nq)."""
    from pyoxigraph import RdfFormat

    formats = {
        "ttl": RdfFormat.TURTLE,
        "n3": RdfFormat.TURTLE,
        "nt": RdfFormat.N_TRIPLES,
        "nq": RdfFormat.N_QUADS,
    }
    xml = {"owl": RdfFormat.RDF_XML, "rdf": RdfFormat.RDF_XML, "xml": RdfFormat.RDF_XML}
    return {**formats, **xml, "trig": RdfFormat.TRIG}[name]


def input_format(path: Path) -> str:
    """Return the format name of an input file from its suffix (a .gz suffix is skipped)."""
    suffixes = [s.lstrip(".").lower() for s in Path(path).suffixes if s != ".gz"]
    return suffixes[-1] if suffixes else ""


def zip_members(archive: Path) -> list[Path]:
    """Return the RDF members of a zip archive, written ARCHIVE.zip!MEMBER, in name order."""
    import zipfile

    with zipfile.ZipFile(archive) as opened:
        names = sorted(
            info.filename
            for info in opened.infolist()
            if not info.is_dir()
            and any(
                info.filename.endswith(s) or info.filename.endswith(s + ".gz") for s in RDF_SUFFIXES
            )
        )
    return [Path(f"{archive}!{name}") for name in names]


def _open(path: Path, stack: Any) -> IO[bytes]:
    """Open a file or a member of a zip archive, and decompress it when its name ends in .gz."""
    import zipfile

    text = str(path)
    # The ExitStack of the caller closes every opened file.
    if MEMBER in text:
        archive, member = text.split(MEMBER, 1)
        opened = stack.enter_context(zipfile.ZipFile(archive + ".zip"))
        raw = stack.enter_context(opened.open(member))
    else:
        raw = stack.enter_context(open(path, "rb"))  # noqa: SIM115
    if text.endswith(".gz"):
        raw = stack.enter_context(gzip.open(raw, "rb"))  # noqa: SIM115
    return cast(IO[bytes], raw)


def _name(path: Path) -> str:
    """Return the file name, or ARCHIVE.zip!MEMBER for a member of a zip archive."""
    text = str(path)
    if MEMBER not in text:
        return Path(path).name
    archive, member = text.split(MEMBER, 1)
    return Path(archive).name + MEMBER + member


def _size(path: Path) -> int:
    """Return the size of a file, or the uncompressed size of a member of a zip archive."""
    import zipfile

    text = str(path)
    if MEMBER not in text:
        return Path(path).stat().st_size
    archive, member = text.split(MEMBER, 1)
    with zipfile.ZipFile(archive + ".zip") as opened:
        return opened.getinfo(member).file_size


def merge_counts(parts: Iterable[dict[str, dict[str, int]]]) -> dict[str, dict[str, int]]:
    """Add the counts of several files."""
    total: dict[str, Counter[str]] = defaultdict(Counter)
    for part in parts:
        for prop, found in part.items():
            total[prop].update(found)
    return {prop: dict(found) for prop, found in sorted(total.items())}


def _count(statements: Iterable[Any], counts: dict[str, Counter[str]]) -> None:
    """Add the numeric literals of STATEMENTS to COUNTS, by property and datatype."""
    from pyoxigraph import Literal

    for statement in statements:
        value = statement.object
        if isinstance(value, Literal) and value.datatype.value in NUMERIC:
            counts[statement.predicate.value][value.datatype.value] += 1


def _count_lines(
    stream: IO[bytes], name: str, file: str, counts: dict[str, Counter[str]]
) -> list[dict[str, Any]]:
    """Count a line-based file block by block; return the lines that the parser refuses.

    A block that the parser refuses is read line by line, and each refused line is left out of
    the counts and returned with its number, the parser's message and its text.
    """
    from pyoxigraph import parse

    unread: list[dict[str, Any]] = []
    block: list[bytes] = []
    first = 1

    def read(lines: list[bytes], start: int) -> None:
        """Count LINES (from line START); split a block the parser refuses down to its lines."""
        try:
            statements = list(parse(b"".join(lines), _format(name), lenient=True))
        except SyntaxError as error:
            if len(lines) > 1:
                for offset, line in enumerate(lines):
                    read([line], start + offset)
                return
            text = lines[0].decode("utf-8", "replace").rstrip("\r\n")[:SAMPLE_CHARS]
            unread.append({"file": file, "line": start, "error": str(error), "text": text})
            return
        _count(statements, counts)

    for number, line in enumerate(stream, 1):
        block.append(line)
        if len(block) == BLOCK_LINES:
            read(block, first)
            block, first = [], number + 1
    if block:
        read(block, first)
    return unread


def census_of_files(files: Iterable[tuple[Path, str]]) -> dict[str, Any]:
    """Count the numeric literals of each property by datatype; record what was not read.

    *files* are (path, QLever input format) pairs; a path may be gzip-compressed or a member of a
    zip archive (zip_members). The parser is lenient, as QLever is (AOP-Wiki has IRIs without a
    scheme). The census is a statistic beside an index that is already built, so a statement
    that its parser refuses never fails the build: an N-Triples or N-Quads file is read again
    line by line and each refused line is left out and recorded (rdfportal.clinvar has an IRI
    holding >); another format is counted up to the error, and the file is recorded.
    Returns {"properties": counts, "unread": record} (unread_record).
    """
    from contextlib import ExitStack

    from pyoxigraph import parse

    counts: dict[str, Counter[str]] = defaultdict(Counter)
    lines: list[dict[str, Any]] = []
    stopped: list[dict[str, Any]] = []
    for path, name in files:
        file = _name(path)
        read: dict[str, Counter[str]] = defaultdict(Counter)
        try:
            with ExitStack() as stack:
                _count(parse(_open(path, stack), _format(name), lenient=True), read)
        except SyntaxError as error:
            if name in LINE_FORMATS:
                read = defaultdict(Counter)
                with ExitStack() as stack:
                    lines += _count_lines(_open(path, stack), name, file, read)
            else:
                stopped.append({"file": file, "error": str(error)})
        for prop, found in read.items():
            counts[prop].update(found)
    return {
        "properties": {prop: dict(found) for prop, found in sorted(counts.items())},
        "unread": unread_record(lines, stopped),
    }


def unread_record(
    lines: list[dict[str, Any]], stopped: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """Count the refused lines by file, with a sample, and the files counted up to an error."""
    by_file = Counter(line["file"] for line in lines)
    return {
        "lines": len(lines),
        "lines_by_file": dict(sorted(by_file.items())),
        "sample": lines[:SAMPLE_LINES],
        "files_counted_up_to_an_error": stopped or [],
    }


def merge_census(parts: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Add the censuses of several files (census_of_files)."""
    parts = list(parts)
    lines = [line for part in parts for line in part["unread"]["sample"]]
    by_file: Counter[str] = Counter()
    for part in parts:
        by_file.update(part["unread"]["lines_by_file"])
    unread = unread_record(
        lines, [f for p in parts for f in p["unread"]["files_counted_up_to_an_error"]]
    )
    unread.update(lines=sum(by_file.values()), lines_by_file=dict(sorted(by_file.items())))
    return {"properties": merge_counts(p["properties"] for p in parts), "unread": unread}


def count_literal_datatypes(files: Iterable[tuple[Path, str]]) -> dict[str, dict[str, int]]:
    """Count the numeric literals of each property by datatype (census_of_files, counts only)."""
    properties: dict[str, dict[str, int]] = census_of_files(files)["properties"]
    return properties


def write_census(
    path: Path,
    counts: dict[str, dict[str, int]],
    sources: Iterable[Path],
    *,
    fetched: str | None = None,
    unread: dict[str, Any] | None = None,
) -> None:
    """Write the counts with the names and sizes of the source files and when they were fetched.

    *unread* records the statements that the census could not read (census_of_files).
    """
    files = [{"file": _name(Path(s)), "bytes": _size(Path(s))} for s in sources]
    record: dict[str, Any] = {"sources": files, "fetched": fetched, "properties": counts}
    if unread is not None:
        record["unread"] = unread
    path.write_text(json.dumps(record, indent=1, sort_keys=True))


def read_unread(path: Path) -> dict[str, Any] | None:
    """Read the record of the statements that a census could not read (None: not recorded)."""
    unread: dict[str, Any] | None = json.loads(Path(path).read_text()).get("unread")
    return unread


def read_census(path: Path) -> dict[str, dict[str, int]]:
    """Read the counts of a census file."""
    properties: dict[str, dict[str, int]] = json.loads(Path(path).read_text())["properties"]
    return properties


def restore_datatypes(patterns: Iterable[Any], census: dict[str, dict[str, int]]) -> dict[str, Any]:
    """Replace a datatype that QLever reports by the one source datatype of the property.

    A pattern keeps the reported datatype when the property has several datatypes of the group
    in the source (they are recorded) or none. Return the numbers for the mining report.
    """
    restored, ambiguous = 0, {}
    for pattern in patterns:
        group = REPORTED.get(getattr(pattern, "datatype", None) or "")
        if not group:
            continue
        source = {dt: n for dt, n in census.get(pattern.property_uri, {}).items() if dt in group}
        if len(source) == 1:
            (datatype,) = source
            if datatype != pattern.datatype:
                pattern.datatype = datatype
                restored += 1
        elif source:
            ambiguous[pattern.property_uri] = source
    return {"restored": restored, "ambiguous": ambiguous}
