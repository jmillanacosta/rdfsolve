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


def _format(name: str) -> Any:
    """Return the pyoxigraph format of a QLever input format (ttl, nt, nq)."""
    from pyoxigraph import RdfFormat

    formats = {"ttl": RdfFormat.TURTLE, "nt": RdfFormat.N_TRIPLES, "nq": RdfFormat.N_QUADS}
    xml = {"owl": RdfFormat.RDF_XML, "rdf": RdfFormat.RDF_XML, "xml": RdfFormat.RDF_XML}
    return {**formats, **xml, "trig": RdfFormat.TRIG}[name]


def input_format(path: Path) -> str:
    """Return the format name of an input file from its suffix (a .gz suffix is skipped)."""
    suffixes = [s.lstrip(".").lower() for s in Path(path).suffixes if s != ".gz"]
    return suffixes[-1] if suffixes else ""


def merge_counts(parts: Iterable[dict[str, dict[str, int]]]) -> dict[str, dict[str, int]]:
    """Add the counts of several files."""
    total: dict[str, Counter[str]] = defaultdict(Counter)
    for part in parts:
        for prop, found in part.items():
            total[prop].update(found)
    return {prop: dict(found) for prop, found in sorted(total.items())}


def count_literal_datatypes(files: Iterable[tuple[Path, str]]) -> dict[str, dict[str, int]]:
    """Count the numeric literals of each property by datatype in the source files.

    *files* are (path, QLever input format) pairs; a path may be gzip-compressed. The parser is
    lenient, as QLever is (AOP-Wiki has IRIs without a scheme).
    """
    from pyoxigraph import Literal, parse

    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for path, name in files:
        with gzip.open(path, "rb") if str(path).endswith(".gz") else open(path, "rb") as raw:
            stream = cast(IO[bytes], raw)
            for statement in parse(stream, _format(name), lenient=True):
                value = statement.object
                if isinstance(value, Literal) and value.datatype.value in NUMERIC:
                    counts[statement.predicate.value][value.datatype.value] += 1
    return {prop: dict(found) for prop, found in sorted(counts.items())}


def write_census(
    path: Path,
    counts: dict[str, dict[str, int]],
    sources: Iterable[Path],
    *,
    fetched: str | None = None,
) -> None:
    """Write the counts with the names and sizes of the source files and when they were fetched."""
    files = [{"file": Path(s).name, "bytes": Path(s).stat().st_size} for s in sources]
    record = {"sources": files, "fetched": fetched, "properties": counts}
    path.write_text(json.dumps(record, indent=1, sort_keys=True))


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
