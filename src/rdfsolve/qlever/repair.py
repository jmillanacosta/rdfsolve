"""Terms that QLever cannot read in an N-Triples or N-Quads input, written so that it can.

QLever refuses a whole file for one term it cannot read. Two kinds are written anew, and each
changed line is recorded with its original text as a data-quality finding beside the index
(REPAIRS_FILE); no triple is left out.

- An IRI holding <, > or ": QLever ends an IRI at the first of them. ONCO cites Wiley DOIs with
  the angle brackets of their SICI form
  (<https://doi.org/10.1002/1097-0142(197601)37:1<141::AID-CNCR2820370121>3.0.CO;2-Y>). In a
  line-based format a term still ends unambiguously: an IRI ends at the > that whitespace (or
  the closing dot) follows. The characters are written %3C, %3E and %22, the IRI's valid form
  (doi.org resolves it to the same DOI).
- An ill-typed literal of a datatype that QLever reads as a value: xsd:integer, xsd:decimal,
  xsd:float, xsd:double and xsd:boolean (NanoSolveIT has 12,050 ""^^xsd:float). It is still RDF,
  but QLever refuses it; it is written as a plain string with the same lexical form. QLever reads
  other ill-typed literals (""^^xsd:int, "x"^^xsd:dateTime) and they are kept.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from pathlib import Path
from typing import Any

# The record of an index's repaired lines, in its work directory.
REPAIRS_FILE = "input-repairs.json"
LINE_FORMATS = (".nt", ".nq")
_ENCODED = {"<": "%3C", ">": "%3E", '"': "%22"}
_XSD = "http://www.w3.org/2001/XMLSchema#"
# The lexical forms that QLever reads, by datatype (whitespace is not collapsed).
_LEXICAL = {
    _XSD + "integer": re.compile(r"[+-]?[0-9]+"),
    _XSD + "decimal": re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)"),
    _XSD + "boolean": re.compile(r"true|false|1|0"),
}
_FLOATING = {_XSD + "float", _XSD + "double"}
_FLOAT = re.compile(r"(?:[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|[+-]?INF|NaN)")
# A line that may hold an IRI with one of these characters, or a literal of a datatype that
# QLever reads as a value.
_SUSPECT = re.compile(
    r'<[^>\s]*[<"]|"\^\^<' + re.escape(_XSD) + r"(?:integer|decimal|float|double|boolean)>"
)
_IRI_END = re.compile(r">(?=[ \t]|\.[ \t]*\r?\n?$)")
_LITERAL = re.compile(r'"(?:[^"\\]|\\.)*"')
_LANGUAGE = re.compile(r"@[A-Za-z]+(?:-[A-Za-z0-9]+)*")
_BLANK = re.compile(r"_:[^\s.]+(?:\.[^\s.]+)*")
_SPACE = re.compile(r"[ \t]*")


def readable_value(lexical: str, datatype: str) -> bool:
    """Whether QLever reads LEXICAL as a value of DATATYPE (True for a datatype it keeps as text)."""
    if datatype in _FLOATING:
        return bool(_FLOAT.fullmatch(lexical)) and (
            lexical.strip("+-") in ("INF", "NaN") or math.isfinite(float(lexical))
        )
    pattern = _LEXICAL.get(datatype)
    return pattern is None or bool(pattern.fullmatch(lexical))


def _iri(line: str, start: int) -> tuple[str, int] | None:
    """Read the IRI at START, encoded; return it and the position after it."""
    end = _IRI_END.search(line, start + 1)
    if end is None:
        return None
    body = line[start + 1 : end.start()]
    return "<" + "".join(_ENCODED.get(c, c) for c in body) + ">", end.end()


def _term(line: str, start: int) -> tuple[str, int, str] | None:
    """Read the term at START: an IRI, a blank node or a literal; return it, its end, the change."""
    if line.startswith("<", start):
        read = _iri(line, start)
        return None if read is None else (*read, "iri")
    match = _BLANK.match(line, start) or _LITERAL.match(line, start)
    if match is None:
        return None
    text, position = match.group(), match.end()
    if text.startswith('"'):
        language = _LANGUAGE.match(line, position)
        if language:
            return text + language.group(), language.end(), ""
        if line.startswith("^^<", position):
            datatype = _iri(line, position + 2)
            if datatype is None:
                return None
            if not readable_value(text[1:-1], datatype[0][1:-1]):
                return text, datatype[1], "literal"
            return text + "^^" + datatype[0], datatype[1], "iri"
    return text, position, ""


def repair_line(line: str) -> tuple[str, list[str]]:
    """Return LINE written so that QLever reads it, and the kinds of change (none: unchanged)."""
    if not _SUSPECT.search(line):
        return line, []
    terms: list[str] = []
    kinds: list[str] = []
    position = _SPACE.match(line).end()  # type: ignore[union-attr]
    while position < len(line) and line[position] != ".":
        read = _term(line, position)
        if read is None:
            return line, []
        term, end, kind = read
        terms.append(term)
        if term != line[position:end]:
            kinds.append(kind)
        position = _SPACE.match(line, end).end()  # type: ignore[union-attr]
    if not kinds or len(terms) not in (3, 4) or line[position:].strip() != ".":
        return line, []
    return " ".join(terms) + " .\n", kinds


def repair_file(path: Path) -> list[dict[str, Any]]:
    """Write the unreadable terms of one N-Triples or N-Quads file anew; return the changes."""
    changes: list[dict[str, Any]] = []
    partial = path.with_name(f"{path.name}.part")
    with (
        path.open(encoding="utf-8", newline="") as stream,
        partial.open("w", encoding="utf-8", newline="") as out,
    ):
        for number, line in enumerate(stream, 1):
            repaired, kinds = repair_line(line)
            if kinds:
                changes.append(
                    {
                        "file": path.name,
                        "line": number,
                        "changes": sorted(set(kinds)),
                        "original": line.rstrip("\r\n"),
                        "written": repaired.rstrip("\n"),
                    }
                )
            out.write(repaired)
    if changes:
        partial.replace(path)
    else:
        partial.unlink()
    return changes


def repair_inputs(workdir: Path, paths: list[Path]) -> list[dict[str, Any]]:
    """Repair the line-based inputs and record the changes in WORKDIR/REPAIRS_FILE."""
    changes = [
        change for path in paths if path.suffix in LINE_FORMATS for change in repair_file(path)
    ]
    if changes:
        record = {
            "findings": {
                "iri": "IRIs with characters that an IRI cannot hold, written percent-encoded",
                "literal": "ill-typed literals of a datatype QLever reads as a value, written as "
                "plain strings",
            },
            "encoded": _ENCODED,
            "lines_by_change": dict(Counter(kind for c in changes for kind in c["changes"])),
            "lines": changes,
        }
        (workdir / REPAIRS_FILE).write_text(
            json.dumps(record, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    return changes
