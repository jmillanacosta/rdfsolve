"""The converters that a Qleverfile's GET_DATA_CMD runs before indexing, named for the run.

Kept apart from rdfsolve.qlever.rdfxml, which the command runs as ``python -m``: the package
imports this module, not the one it executes.
"""

from __future__ import annotations

import sys

__all__ = ["CONVERTER", "PYTHON_VARIABLE", "converter_command", "preflight"]

#: The RDF/XML converter, named in logs and in the preflight check.
CONVERTER = "pyoxigraph (python -m rdfsolve.qlever.rdfxml)"
#: The variable that names the interpreter of the converter in a Qleverfile's GET_DATA_CMD.
PYTHON_VARIABLE = "RDFSOLVE_PYTHON"


def converter_command(source: str, target: str) -> str:
    """Return the shell command that converts *source* to *target* (both already shell-quoted).

    The command runs this module with the interpreter named by ``$RDFSOLVE_PYTHON`` (the pipeline
    sets it to its own when it runs a Qleverfile's GET_DATA_CMD), else with the interpreter that
    wrote the Qleverfile: it does not depend on what ``python3`` a job finds on its PATH.
    """
    import shlex

    python = f'"${{{PYTHON_VARIABLE}:-{shlex.quote(sys.executable)}}}"'
    return f"{python} -m rdfsolve.qlever.rdfxml {source} {target}"


def preflight() -> str:
    """Check that the RDF/XML converter works; return its name and version for the log.

    Raises RuntimeError with what to do when pyoxigraph is missing or cannot read RDF/XML.
    """
    try:
        import pyoxigraph

        document = (
            b'<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            b'<rdf:Description rdf:about="urn:s"><rdf:value>1</rdf:value></rdf:Description>'
            b"</rdf:RDF>"
        )
        if len(list(pyoxigraph.parse(document, pyoxigraph.RdfFormat.RDF_XML))) != 1:
            raise ValueError("a one-statement document gave another number of statements")
    except Exception as error:
        raise RuntimeError(
            f"The RDF/XML converter ({CONVERTER}) does not work in {sys.executable}: {error}. "
            "Install rdfsolve with its dependencies (pyoxigraph) in this environment."
        ) from error
    return f"{CONVERTER}, pyoxigraph {pyoxigraph.__version__}"
