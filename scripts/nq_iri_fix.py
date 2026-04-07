#!/usr/bin/env python3
"""
nq_iri_fix.py - stdin → stdout filter for N-Quads streams.

Does two things:
  1. Joins continuation lines: some bio2rdf NQ files contain IRIs wrapped
     across two lines (IRI opens with '<' on one line, '>' appears at the
     start of the next).  QLever requires every quad on a single line.
  2. Percent-encodes spaces and double-quotes inside angle-bracket IRIs
     (e.g. '<http://example.org/foo bar>' → '<http://example.org/foo%20bar>'),
     but only inside IRI tokens — quoted string literals are never touched.
"""
import sys
import re


def enc(ln: str) -> str:
    def fix(m):
        s = m.group(1).replace(' ', '%20').replace('"', '%22')
        return '<' + s + '>'
    return re.sub(r'<([^<>\n]*)>', fix, ln)


def has_open_iri(ln: str) -> bool:
    """True when a line ends mid-IRI (unmatched '<' before end of line)."""
    stripped = re.sub(r'<[^<>\n]*>', '', ln)
    return '<' in stripped


inp = sys.stdin.buffer
out = sys.stdout
pending = ''
for raw in inp:
    line = raw.decode('utf-8', 'replace')
    if pending:
        line = pending.rstrip('\n') + line.lstrip()
        pending = ''
    if has_open_iri(line.rstrip('\n')):
        pending = line
        continue
    out.write(enc(line))
if pending:
    out.write(enc(pending))
out.flush()
