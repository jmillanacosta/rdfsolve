"""Read HDT downloads: the shell steps that write each HDT file as gzip N-Triples before indexing.

QLever reads N-Triples, N-Quads and Turtle, not HDT (Header Dictionary Triples). Some publishers
release only HDT.

The converter is hdt-java's ``hdt2rdf`` (rdfhdt/hdt-java, LGPL), fetched as a pinned release and
checked against its SHA-256 before use, as the OBO steps fetch ROBOT: it needs only Java
(``JAVA_HOME`` or ``java`` on the PATH; the steps stop first when there is none). There is no Python reader with Linux wheels (rdflib-hdt 3.3 publishes a macOS
wheel only; hdt 2.3 builds hdt-cpp from source), and hdt-cpp publishes no binaries.

``hdt2rdf`` writes the triples to a pipe that gzip compresses: the HDT stays the pinned
download, and the index input is ``NAME.hdt.nt.gz`` beside it, which the index feed streams to
qlever-index (rdfsolve.qlever.inputs.index_command). No plain N-Triples copy is written. The
output is written to ``.part`` and renamed when complete; a conversion that fails removes it.
"""

from __future__ import annotations

__all__ = [
    "HDT_JAVA_SHA256",
    "HDT_JAVA_URL",
    "HDT_JAVA_VERSION",
    "HDT_OUTPUT_SUFFIX",
    "convert_hdt_steps",
]

HDT_JAVA_VERSION = "3.0.10"
#: The hdt-java command-line package of the release (hdt2rdf.sh and its jars).
HDT_JAVA_URL = (
    f"https://github.com/rdfhdt/hdt-java/releases/download/v{HDT_JAVA_VERSION}/rdfhdt.tar.gz"
)
#: SHA-256 of rdfhdt.tar.gz v3.0.10 (19,705,133 bytes), checked before the tools are unpacked.
HDT_JAVA_SHA256 = "9b97f28ebe91fbd66492db36d36531af40536a56bd786c8bb98cf70ae7a978c5"
#: The suffix of the N-Triples written for NAME.hdt: NAME.hdt.nt.gz.
HDT_OUTPUT_SUFFIX = ".nt.gz"
#: The folder the package unpacks to, removed after the conversions.
_TOOL_DIR = f"hdt-java-package-{HDT_JAVA_VERSION}"


def convert_hdt_steps() -> list[str]:
    """Return the GET_DATA_CMD steps that write each *.hdt in the folder as NAME.hdt.nt.gz.

    The heap of the converter is ``$RDFSOLVE_HDT_HEAP`` (default 16g): hdt2rdf loads the HDT's
    dictionary and triples into memory, about the size of the file.
    """
    archive = "rdfhdt.tar.gz"
    return [
        f"echo 'Converting HDT -> N-Triples (hdt-java {HDT_JAVA_VERSION} hdt2rdf) ...'",
        # hdt2rdf.sh runs $JAVA_HOME/bin/java, else java on the PATH: a job without Java stops
        # here, before the download is converted (compute nodes may need a Java module).
        (
            '{ [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ] || command -v java >/dev/null; } '
            "|| { echo 'hdt2rdf needs Java: set JAVA_HOME or put java on the PATH' >&2; exit 1; }"
        ),
        (
            # The tools are fetched only when an HDT file still needs converting. The step is
            # one group: the steps are joined with &&.
            '{ need=0; for f in *.hdt; do [ -f "$f" ] && [ ! -f "$f.nt.gz" ] && need=1; done; '
            f"if [ $need = 1 ] && [ ! -d {_TOOL_DIR} ]; then "
            f'wget -q -O {archive} "{HDT_JAVA_URL}" && '
            f'echo "{HDT_JAVA_SHA256}  {archive}" | sha256sum -c --quiet - && '
            f"tar xzf {archive} && rm -f {archive} || "
            f"{{ rm -rf {archive} {_TOOL_DIR}; echo 'hdt-java could not be fetched' >&2; exit 1; }}; fi; }}"
        ),
        (
            'for f in *.hdt; do [ -f "$f" ] || continue; out="$f.nt.gz"; '
            '[ -f "$out" ] && continue; '
            # pipefail: a failed hdt2rdf is not hidden by the gzip after it.
            '( set -o pipefail; JAVA_OPTIONS="-Xmx${RDFSOLVE_HDT_HEAP:-16g}" '
            f'bash {_TOOL_DIR}/bin/hdt2rdf.sh "$f" /dev/stdout 2>> hdt2rdf.log '
            '| gzip -1 > "$out.part" ) && mv "$out.part" "$out" || '
            '{ rm -f "$out.part"; echo "HDT conversion failed: $f (see hdt2rdf.log)" >&2; exit 1; }; '
            "done"
        ),
        f"rm -rf {_TOOL_DIR}",
    ]
