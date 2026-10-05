"""IRIs that QLever cannot read in an N-Triples or N-Quads input, written percent-encoded.

QLever ends an IRI at the first <, " or > and then refuses the whole file. ONCO cites Wiley
DOIs with the angle brackets of their SICI form
(<https://doi.org/10.1002/1097-0142(197601)37:1<141::AID-CNCR2820370121>3.0.CO;2-Y>). In a
line-based format, a term still ends unambiguously: an IRI ends at the > that whitespace follows.
These characters are written as %3C, %3E and %22 in such an IRI, which is the IRI's valid form
(doi.org resolves it to the same DOI). No triple is left out. Each changed line is recorded with
its original text as a data-quality finding beside the index (REPAIRS_FILE).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

# The record of an index's repaired lines, in its work directory.
REPAIRS_FILE = "input-repairs.json"
LINE_FORMATS = (".nt", ".nq")
_ENCODED = {"<": "%3C", ">": "%3E", '"': "%22"}
# A line that may hold an IRI with one of these characters (a literal can match too).
_SUSPECT = re.compile(r'<[^>\s]*[<"]')
_IRI_END = re.compile(r">(?=[ \t])")
_LITERAL = re.compile(r'"(?:[^"\\]|\\.)*"')
_LANGUAGE = re.compile(r"@[A-Za-z]+(?:-[A-Za-z0-9]+)*")
_BLANK = re.compile(r"_:\S+")
_SPACE = re.compile(r"[ \t]*")


def _iri(line: str, start: int) -> tuple[str, int] | None:
    """Read the IRI at START, encoded; return it and the position after it."""
    end = _IRI_END.search(line, start + 1)
    if end is None:
        return None
    body = line[start + 1 : end.start()]
    return "<" + "".join(_ENCODED.get(c, c) for c in body) + ">", end.end()


def _term(line: str, start: int) -> tuple[str, int] | None:
    """Read the term at START: an IRI, a blank node or a literal."""
    if line.startswith("<", start):
        return _iri(line, start)
    match = _BLANK.match(line, start) or _LITERAL.match(line, start)
    if match is None:
        return None
    text, position = match.group(), match.end()
    if text.startswith('"'):
        language = _LANGUAGE.match(line, position)
        if language:
            return text + language.group(), language.end()
        if line.startswith("^^<", position):
            datatype = _iri(line, position + 2)
            if datatype is None:
                return None
            return text + "^^" + datatype[0], datatype[1]
    return text, position


def repair_line(line: str) -> str:
    """Return LINE with < > " encoded inside its IRIs; unchanged when it cannot be read."""
    if not _SUSPECT.search(line):
        return line
    terms: list[str] = []
    changed = False
    position = _SPACE.match(line).end()  # type: ignore[union-attr]
    while position < len(line) and line[position] != ".":
        read = _term(line, position)
        if read is None:
            return line
        terms.append(read[0])
        changed = changed or read[0] != line[position : read[1]]
        position = _SPACE.match(line, read[1]).end()  # type: ignore[union-attr]
    if not changed or len(terms) not in (3, 4) or line[position:].strip() != ".":
        return line
    return " ".join(terms) + " .\n"


def repair_file(path: Path) -> list[dict[str, Any]]:
    """Encode the unreadable IRIs of one N-Triples or N-Quads file in place; return the changes."""
    changes: list[dict[str, Any]] = []
    partial = path.with_name(f"{path.name}.part")
    with (
        path.open(encoding="utf-8", newline="") as stream,
        partial.open("w", encoding="utf-8", newline="") as out,
    ):
        for number, line in enumerate(stream, 1):
            repaired = repair_line(line)
            if repaired != line:
                changes.append(
                    {
                        "file": path.name,
                        "line": number,
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
            "finding": "IRIs with characters that an IRI cannot hold, written percent-encoded",
            "encoded": _ENCODED,
            "lines": changes,
        }
        (workdir / REPAIRS_FILE).write_text(
            json.dumps(record, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    return changes
