"""Convert RDF/XML to N-Triples or N-Quads with pyoxigraph, before indexing.

QLever does not read RDF/XML. The converter is part of rdfsolve (pyoxigraph is a dependency), so
a job needs no external tool on its PATH. pyoxigraph's RDF/XML parser streams: memory stays flat
for files of any size.

Usage in a Qleverfile's GET_DATA_CMD (rdfsolve.qlever.converters.converter_command)::

    python -m rdfsolve.qlever.rdfxml INPUT.rdf OUTPUT.nt

The output is written to OUTPUT.part and renamed when complete, so an interrupted conversion
leaves no output that looks complete. Relative IRIs resolve against the input's file: URI, as
Raptor's rapper and Jena's riot resolve them.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from pathlib import Path

__all__ = ["convert_rdfxml", "main"]


def convert_rdfxml(source: str | Path, target: str | Path, *, lenient: bool = True) -> int:
    """Write the statements of the RDF/XML file *source* to *target*; return how many.

    The statements are in the default graph, so the lines are N-Triples, read as such whether
    *target* is named .nt or .nq. *lenient* (the default, as Raptor's rapper) keeps an IRI
    that RDF excludes, such as one with a space, as published: the pipeline mines such terms and
    reports them (rdfsolve.schema_models.iri_quality) instead of losing the whole file to one term.
    ``lenient=False`` refuses the file at the first such IRI.
    """
    import pyoxigraph

    source, target = Path(source), Path(target)
    part = target.with_name(target.name + ".part")
    count = 0

    def statements() -> Iterator[pyoxigraph.Quad]:
        """Yield the statements of the input file, one at a time."""
        nonlocal count
        with source.open("rb") as stream:
            for quad in pyoxigraph.parse(
                stream,
                pyoxigraph.RdfFormat.RDF_XML,
                base_iri=source.resolve().as_uri(),
                lenient=lenient,
            ):
                count += 1
                yield quad

    try:
        # N-Quads of the default graph are N-Triples lines, escaped as canonical N-Triples.
        pyoxigraph.serialize(statements(), str(part), pyoxigraph.RdfFormat.N_QUADS)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    part.replace(target)
    return count


def main(argv: list[str] | None = None) -> int:
    """Command line: convert one RDF/XML file; exit 1 with the parser's message on failure."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("source")
    parser.add_argument("target")
    parser.add_argument(
        "--strict", action="store_true", help="refuse the file at the first IRI that RDF excludes"
    )
    args = parser.parse_args(argv)
    try:
        count = convert_rdfxml(args.source, args.target, lenient=not args.strict)
    except (SyntaxError, OSError, ValueError) as error:
        sys.stderr.write(f"RDF/XML conversion failed: {args.source}: {error}\n")
        return 1
    sys.stderr.write(f"{args.source}: {count} statements -> {args.target}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
