"""QLever Qleverfile generation from sources.yaml entries."""

from __future__ import annotations

import hashlib
import json
import re
import shlex
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rdfsolve.qlever.converters import CONVERTER, converter_command

__all__ = [
    "FORMAT_REGISTRY",
    "QLEVERFILE_TEMPLATE",
    "FormatSpec",
    "QleverConfig",
    "SourceAnalysis",
    "analyse_source",
    "build_provider_qleverfile",
    "build_qleverfile",
    "detect_data_format",
    "graph_uri_to_tar_folder",
    "tar_source_qleverfile_parts",
    "urls_from_field",
]


# Configuration


@dataclass
class QleverConfig:
    """Tunable parameters written into every generated Qleverfile.

    All fields have defaults.
    """

    memory_for_queries: str = "500G"
    timeout: str = "9999999999s"
    parser_buffer_size: str = "10M"
    stxxl_memory: str = "16GB"
    parallel_parsing: bool = False
    num_triples_per_batch: int = 1_000_000
    access_token: str | None = None
    image: str = "docker.io/adfreiburg/qlever:latest"

    @property
    def settings_json(self) -> str:
        """Return SETTINGS_JSON value for Qleverfile."""
        return (
            '{ "ascii-prefixes-only": false, '
            f'"num-triples-per-batch": {self.num_triples_per_batch}, '
            '"parser-integer-overflow-behavior": '
            '"overflowing-integers-become-doubles" }'
        )


# Qleverfile template

QLEVERFILE_TEMPLATE = """\
# Qleverfile for {name}
# Auto-generated with rdfsolve
#
# Usage:
#  cd {workdir}
#  qlever index
#  qlever start
#  qlever stop


[data]
NAME              = {name}
GET_DATA_CMD      = {get_data_cmd}
FORMAT            = {rdf_format}
DESCRIPTION       = {name} - rdfsolve-generated Qleverfile

[index]
INPUT_FILES          = {input_files}
CAT_INPUT_FILES      = {cat_input_files}
SETTINGS_JSON        = {settings_json}
PARALLEL_PARSING     = {parallel_parsing}
PARSER_BUFFER_SIZE   = {parser_buffer_size}
STXXL_MEMORY         = {stxxl_memory}

[server]
PORT              = {port}
ACCESS_TOKEN      = {access_token}
MEMORY_FOR_QUERIES = {memory_for_queries}
TIMEOUT           = {timeout}

[runtime]
SYSTEM = {runtime}
IMAGE  = {image}

[ui]
UI_CONFIG = default
"""


# Format registry -- the single source of truth


@dataclass(frozen=True)
class FormatSpec:
    """How QLever should ingest a given RDF serialization.

    Attributes
    ----------
    qlever_format : str
        Value for the FORMAT key (ttl, nq, nt).
    glob : str
        Shell glob for INPUT_FILES (relative to the rdf subdir).
    cat : str
        Shell expression for CAT_INPUT_FILES.
    needs_conversion : bool
        Whether the raw download needs a conversion step before QLever
        can read it (e.g. RDF/XML -> NQ, OBO -> TTL, JSON-LD -> TTL).
    """

    qlever_format: str
    glob: str
    cat: str
    needs_conversion: bool = False


# Each input decompressed when it holds gzip data and read as it is otherwise (gzip -f). The
# former "( zcat FILES || cat FILES )" wrote every file raw, gzip data too, after the part that
# zcat had decompressed when one file was not gzip, and an index stopped at the raw bytes.
DECOMPRESS_EACH = "gzip -dcf ${INPUT_FILES} | grep -v '^$'"

# Order matters: first match wins when multiple download_* keys exist.
FORMAT_REGISTRY: dict[str, FormatSpec] = {
    # Quad formats
    "nq": FormatSpec(
        qlever_format="nq",
        glob="*.nq",
        cat="cat ${INPUT_FILES}",
    ),
    "nquads": FormatSpec(  # alias
        qlever_format="nq",
        glob="*.nq",
        cat="cat ${INPUT_FILES}",
    ),
    "trig": FormatSpec(
        qlever_format="nq",
        glob="*.trig* *.nq*",
        cat=DECOMPRESS_EACH,
    ),
    # Triple formats
    "nt": FormatSpec(
        qlever_format="nt",
        glob="*.nt*",
        cat=DECOMPRESS_EACH,
    ),
    "ttl": FormatSpec(
        qlever_format="ttl",
        glob="*.ttl",
        cat="cat ${INPUT_FILES}",
    ),
    "n3": FormatSpec(
        qlever_format="ttl",
        glob="*.n3",
        cat="cat ${INPUT_FILES}",
    ),
    # Formats requiring conversion
    "rdf": FormatSpec(
        qlever_format="nq",
        glob="*.nq",
        cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "rdfxml": FormatSpec(
        qlever_format="nq",
        glob="*.nq",
        cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "owl": FormatSpec(
        qlever_format="nq",
        glob="*.nq",
        cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "obo": FormatSpec(
        qlever_format="ttl",
        glob="*.ttl",
        cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "jsonld": FormatSpec(
        qlever_format="ttl",
        glob="*.ttl",
        cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    # A Blazegraph journal (a store published only as a journal), exported to
    # compressed N-Quads with the graph of each statement.
    "jnl": FormatSpec(
        qlever_format="nq",
        glob="*.nq*",
        cat=DECOMPRESS_EACH,
        needs_conversion=True,
    ),
    # HDT is written as gzip N-Triples beside it (rdfsolve.qlever.hdt).
    "hdt": FormatSpec(
        qlever_format="nt",
        glob="*.hdt.nt.gz",
        cat=DECOMPRESS_EACH,
        needs_conversion=True,
    ),
    # Archive-only keys (format decided by archive contents)
    "tar_gz": FormatSpec(
        qlever_format="ttl",
        glob="*.ttl",
        cat="cat ${INPUT_FILES}",
    ),
    "tgz": FormatSpec(
        qlever_format="ttl",
        glob="*.ttl",
        cat="cat ${INPUT_FILES}",
    ),
    "zip": FormatSpec(
        qlever_format="ttl",
        glob="*.ttl",
        cat="cat ${INPUT_FILES}",
    ),
}

# The formats a download field can name for a file whose name has no RDF extension.
_DATA_FORMATS = ("nt", "nq", "ttl", "trig", "n3")

# Extensions we recognise in a URL for smart wget naming.
_RDF_EXTS = (
    ".ttl",
    ".ttl.gz",
    ".nt",
    ".nt.gz",
    ".nq",
    ".nq.gz",
    ".trig",
    ".trig.gz",
    ".n3",
    ".owl",
    ".rdf",
    ".rdf.gz",
    ".rdf.xz",
    ".owl.xz",
    ".xml.gz",
    ".jsonld",
    ".obo",
    ".jnl.gz",
    ".jnl",
    ".hdt",
    ".tar.gz",
    ".tgz",
    ".zip",
)


# Source analysis


@dataclass
class SourceAnalysis:
    """Result of scanning a source entry's download_* fields.

    Collected in a single pass by analyse_source().
    """

    urls: list[str]
    """Every download URL, in order."""

    suffixes: set[str]
    """The set of download_* suffixes present (e.g. {"ttl", "rdf"})."""

    needs_gz: bool = False
    """At least one URL ends in .gz (but not .tar.gz)."""

    needs_xz: bool = False
    """At least one URL ends in .xz."""

    needs_archive: bool = False
    """At least one archive (.zip / .tar.gz / .tgz) is present."""

    urls_by_suffix: dict[str, list[str]] = field(default_factory=dict)
    """Download URLs grouped by their ``download_*`` suffix."""

    left_out: list[str] = field(default_factory=list)
    """Name patterns of downloaded or extracted files that are not indexed
    (``archive_members_left_out``)."""

    @property
    def needs_rdfxml_conversion(self) -> bool:
        """Check if source requires RDF/XML to NQuads conversion."""
        return bool(self.suffixes & {"rdf", "rdfxml", "owl"})

    @property
    def needs_obo_conversion(self) -> bool:
        """Check if source requires OBO to Turtle conversion."""
        return "obo" in self.suffixes

    @property
    def needs_journal_conversion(self) -> bool:
        """Check if source requires a Blazegraph journal to N-Quads export."""
        return "jnl" in self.suffixes

    @property
    def needs_jsonld_conversion(self) -> bool:
        """Check if source requires JSON-LD to Turtle conversion."""
        return "jsonld" in self.suffixes

    @property
    def needs_hdt_conversion(self) -> bool:
        """Check if source requires HDT to N-Triples conversion."""
        return "hdt" in self.suffixes

    @property
    def needs_decompression(self) -> bool:
        """Check if source requires decompression step."""
        return self.needs_gz or self.needs_archive

    def pick_format_spec(self) -> FormatSpec:
        """Choose the best FormatSpec by registry priority.

        Special case: n3 + ttl together -> merged glob.
        """
        if "n3" in self.suffixes and "ttl" in self.suffixes:
            return FormatSpec(
                qlever_format="ttl",
                glob="*.ttl *.n3",
                cat="cat ${INPUT_FILES}",
            )
        for suffix, spec in FORMAT_REGISTRY.items():
            if suffix in self.suffixes:
                return spec
        # Fallback
        return FormatSpec(qlever_format="ttl", glob="*.ttl", cat="cat ${INPUT_FILES}")


def analyse_source(entry: dict[str, Any]) -> SourceAnalysis:
    """Scan all download_* fields on entry in a single pass."""
    urls: list[str] = []
    urls_by_suffix: dict[str, list[str]] = {}
    suffixes: set[str] = set()
    needs_gz = False
    needs_xz = False
    needs_archive = False

    _ARCHIVE_EXTS = {".zip", ".tar.gz", ".tgz"}
    _ARCHIVE_SUFFIXES = {"tar_gz", "tgz", "zip"}

    for key in sorted(entry):
        if not key.startswith("download_") or not entry.get(key):
            continue
        suffix = key.removeprefix("download_")
        suffixes.add(suffix)
        for u in urls_from_field(entry, key):
            urls.append(u)
            urls_by_suffix.setdefault(suffix, []).append(u)
            low = (_file_name(u) or u).lower()
            if low.endswith(".gz") and not low.endswith(".tar.gz"):
                needs_gz = True
            if low.endswith(".xz"):
                needs_xz = True
            if any(low.endswith(ext) for ext in _ARCHIVE_EXTS):
                needs_archive = True
        if suffix in _ARCHIVE_SUFFIXES:
            needs_archive = True

    return SourceAnalysis(
        urls=urls,
        urls_by_suffix=urls_by_suffix,
        suffixes=suffixes,
        needs_gz=needs_gz,
        needs_xz=needs_xz,
        needs_archive=needs_archive,
        left_out=left_out_patterns(entry),
    )


# A pattern names files in the download folder, never a path: letters, digits, '.', '_', '-'
# and the glob characters '*' and '?'.
_LEFT_OUT_PATTERN = re.compile(r"[A-Za-z0-9._*?-]+")
LEFT_OUT_DIR = "left_out"


def left_out_patterns(entry: dict[str, Any]) -> list[str]:
    """Return the entry's ``archive_members_left_out`` patterns, refusing one that is a path."""
    value = entry.get("archive_members_left_out") or []
    patterns = [value] if isinstance(value, str) else list(value)
    for pattern in patterns:
        if not _LEFT_OUT_PATTERN.fullmatch(str(pattern)) or set(str(pattern)) <= {"*", "?", "."}:
            raise ValueError(f"archive_members_left_out takes file name patterns: {pattern!r}")
    return [str(p) for p in patterns]


# Small helpers


def detect_data_format(entry: Any) -> str | None:
    """Return a short format label, or None if no download is available."""
    if entry.get("local_tar_url"):
        return "trig"
    dl_keys: list[str] = [k for k in entry if k.startswith("download_") and entry.get(k)]
    if not dl_keys:
        return None
    for suffix in FORMAT_REGISTRY:
        if f"download_{suffix}" in dl_keys:
            return suffix
    first_key: str = dl_keys[0]
    return first_key.removeprefix("download_")


def urls_from_field(entry: dict[str, Any], field_name: str) -> list[str]:
    """Extract a flat URL list from a YAML field (string or list)."""
    raw = entry.get(field_name, "")
    if not raw:
        return []
    items = raw if isinstance(raw, list) else [raw]
    return [u for u in items if u]


def graph_uri_to_tar_folder(uri: str) -> str:
    """Convert a named-graph URI to its folder name in a tar of one folder per graph."""
    no_scheme = re.sub(r"^https?://", "", uri)
    return "http_" + no_scheme.replace("/", "_")


# Shell-step builders returning list[str] of shell fragments


# A download is tried again after a fault that passes; a missing file (404) is not.
_RETRY = "--tries=5 --waitretry=20 --retry-connrefused --retry-on-http-error=429,500,502,503,504"


# wget does not try again after a failed connection (exit 4: network; exit 5: SSL). This shell
# function tries such a file up to 5 times; another failure, as a missing file, ends at once.
_WGET_AGAIN = (
    'wget() { local c i; for i in 1 2 3 4 5; do command wget "$@"; c=$?; '
    'case $c in 0) return 0;; 4|5) sleep "${RDFSOLVE_DOWNLOAD_WAIT:-20}";; *) return $c;; esac; '
    "done; return $c; }"
)


def _file_name(url: str) -> str | None:
    """Return the name a download is saved under: the last part of its URL path that names an
    RDF file (a service may serve NAME.nq.gz at .../NAME.nq.gz/content), or None.
    """
    parts = url.rstrip("/").split("/")
    return next(
        (p for p in reversed(parts) if any(p.lower().endswith(e) for e in _RDF_EXTS)),
        None,
    )


def _folder_cmd(url: str, suffix: str) -> str:
    """Return a wget command that fetches the SUFFIX files of a published folder (a URL ending
    in /) and of its subfolders, kept in a folder named after it.

    A provider can publish one dataset as hundreds of thousands of files in nested folders. The
    listing pages are read and not kept; a fetch that stops resumes its files. The folders are
    followed to any depth (-l inf): wget -r stops at 5 levels by default, so deeper files would
    leave only empty folders.
    """
    from urllib.parse import urlparse

    depth = len([part for part in urlparse(url).path.split("/") if part]) - 1
    accept = f"*.{suffix},*.{suffix}.gz"
    return (
        f'wget -r -l inf -np -nH --cut-dirs={depth} -c -q {_RETRY} -A "{accept}" -R "index.html*" '
        f'"{url}"'
    )


def _saved_names(urls_by_suffix: dict[str, list[str]]) -> dict[str, str]:
    """Return the name a download is saved under when its own name does not serve.

    - A file whose name has no RDF extension is named by the format of its download field
      (NAME.gz from download_nt is saved as NAME.nt.gz), so that it is decompressed and indexed.
    - Several downloads can share a file name (records of one repository whose files have the
      same name); under one name, wget -c would take the second as the first, complete, and not
      fetch it. Each such download is saved as N__NAME.
    """
    names: list[tuple[str, str | None]] = []
    for suffix, urls in urls_by_suffix.items():
        for url in urls:
            if url.endswith("/"):
                continue
            name = _file_name(url)
            base = url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
            # A DVC remote serves each file under its hash, without an extension (BioBricks).
            if name is None and suffix in (*_DATA_FORMATS, "hdt") and base:
                stem, gz = (base[:-3], ".gz") if base.endswith(".gz") else (base, "")
                name = f"{stem}.{suffix}{gz}"
            names.append((url, name))
    counts = Counter(name for _, name in names if name)
    seen: Counter[str] = Counter()
    saved = {}
    for url, name in names:
        if name and counts[name] > 1:
            seen[name] += 1
            saved[url] = f"{seen[name]}__{name}"
        elif name and name != _file_name(url):
            saved[url] = name
    return saved


def graph_download_name(url: str, field_name: str) -> str:
    """Return the name a download mapped to a named graph is saved under: the SHA-256 of its
    URL, the format of its download field, and its compression.
    """
    suffix = field_name.removeprefix("download_")
    suffix = "rdf" if suffix == "rdfxml" else suffix
    compression = ".gz" if url.endswith(".gz") else ".xz" if url.endswith(".xz") else ""
    return hashlib.sha256(url.encode()).hexdigest() + "." + suffix + compression


def download_file_names(entry: dict[str, Any]) -> dict[str, str | None]:
    """Return the name each download of a registry entry is saved under in its rdf/ folder,
    as the download command names it (_wget_cmd); None for a published folder and for a file
    named by the server (Content-Disposition).
    """
    analysis = analyse_source(entry)
    saved = _saved_names(analysis.urls_by_suffix)
    names: dict[str, str | None] = {}
    for url in analysis.urls:
        fname = url.rsplit("/", 1)[-1]
        if url.endswith("/"):
            names[url] = None
        elif url in saved:
            names[url] = saved[url]
        elif any(fname.lower().endswith(ext) for ext in _RDF_EXTS):
            names[url] = fname
        else:
            names[url] = _file_name(url)
    return names


def _wget_cmd(url: str, name: str | None = None) -> str:
    """Return a single wget command string for url, saved as NAME when given."""
    if name:
        return f'wget -c -q {_RETRY} -O "{name}" "{url}"'
    fname = url.rsplit("/", 1)[-1]
    if any(fname.lower().endswith(ext) for ext in _RDF_EXTS):
        return f'wget -c -q {_RETRY} "{url}"'
    derived = _file_name(url)
    if derived:
        return f'wget -c -q {_RETRY} -O "{derived}" "{url}"'
    return f'wget -c -q {_RETRY} --content-disposition "{url}"'


def _collect_from_subdirs_step(*, include_archives: bool = False) -> str:
    """Shell fragment: move RDF files from subdirs to the working dir.

    De-duplicates by prefixing with the parent dirname on collision.
    """
    exts = (
        '-name "*.ttl" -o -name "*.ttl.gz" -o -name "*.nt" -o '
        '-name "*.nt.gz" -o -name "*.nq" -o -name "*.nq.gz" -o '
        '-name "*.trig" -o -name "*.trig.gz" -o '
        '-name "*.n3" -o -name "*.owl" -o -name "*.rdf" -o '
        '-name "*.rdf.gz" -o -name "*.jsonld"'
    )
    if include_archives:
        exts += ' -o -name "*.tar.gz" -o -name "*.tgz" -o -name "*.zip"'
    return (
        f"find . -mindepth 2 \\( {exts} \\) -print0 | "
        "while IFS= read -r -d '' fp; do "
        'bn=$(basename "$fp"); dest=$bn; '
        'if [ -e "./$dest" ]; then '
        'dn=$(dirname "$fp" | tr "/" "_" | sed "s/^\\._//"); '
        'dest=$dn"__"$bn; fi; '
        'n=1; while [ -e "./$dest" ]; do dest=$n"__"$bn; n=$((n+1)); done; '
        'mv "$fp" "./$dest"; '
        "done 2>/dev/null || true"
    )


def _drop_empty_members_step() -> str:
    """Shell fragment: leave out the empty files that archives hold, and name each.

    An archive can hold an empty member; it has no statements, and the index refuses an empty
    input. A download that is empty is not affected.
    """
    return (
        "find . -mindepth 2 -type f -empty \\( -name '*.ttl' -o -name '*.nt' -o -name '*.nq' "
        "-o -name '*.trig' -o -name '*.n3' -o -name '*.owl' -o -name '*.rdf' -o -name '*.jsonld' "
        '\\) -print0 | while IFS= read -r -d "" fp; do '
        'echo "  empty archive member left out: $fp"; rm -f "$fp"; done'
    )


def _rename_mislabelled_steps(analysis: SourceAnalysis) -> list[str]:
    """Give a Turtle download a .ttl name when its URL says otherwise.

    Some sources serve Turtle from a .owl or .rdf URL. The index globs by
    extension, so the file has to carry the extension of what is inside it.
    """
    renames = []
    for url in analysis.urls_by_suffix.get("ttl", []):
        name = url.rstrip("/").rsplit("/", 1)[-1]
        # An archive keeps its name: it is extracted, and its members carry their own names.
        if name.endswith((".zip", ".tar.gz", ".tgz", ".gz", ".bz2", ".xz")):
            continue
        if name and not name.endswith((".ttl", ".ttl.gz", ".n3")):
            stem = name.rsplit(".", 1)[0] if "." in name else name
            renames.append((name, f"{stem}.ttl"))
    if not renames:
        return []
    moves = " ".join(f'[ -f "{src}" ] && mv -f "{src}" "{dst}";' for src, dst in renames)
    # Grouped, so that its ';' does not split the '&&' chain of the download step.
    return ["echo 'Naming Turtle downloads by content ...'", f"{{ {moves} true; }}"]


def _extract_archives_steps() -> list[str]:
    """Shell steps: extract archives, collect, repeat for nested archives.

    tar reads the compression from the content: MetRIn-KG publishes a plain tar as .tar.gz.
    """
    _tar = (
        'for f in *.tar.gz *.tgz; do [ -f "$f" ] || continue; '
        'echo "  extracting $f"; tar xf "$f"; echo "$f" >> .extracted-archives; done'
    )
    _zip = (
        'for f in *.zip; do [ -f "$f" ] || continue; '
        'echo "  extracting $f"; '
        "python3 -c \"import zipfile; z=zipfile.ZipFile('$f'); z.extractall('.'); "
        "print(f'Extracted {len(z.namelist())} files'); z.close()\"; "
        'echo "$f" >> .extracted-archives; done'
    )
    _nested_tar = (
        'for f in *.tar.gz *.tgz; do [ -f "$f" ] || continue; '
        'grep -qxF -- "$f" .extracted-archives 2>/dev/null && continue; '
        'echo "  extracting nested $f"; tar xf "$f" 2>/dev/null || true; done'
    )
    _nested_zip = (
        'for f in *.zip; do [ -f "$f" ] || continue; '
        'grep -qxF -- "$f" .extracted-archives 2>/dev/null && continue; '
        'echo "  extracting nested $f"; '
        "python3 -c \"import zipfile; z=zipfile.ZipFile('$f'); z.extractall('.'); "
        "print(f'Extracted {len(z.namelist())} files'); z.close()\" 2>/dev/null || true; done"
    )
    return [
        "echo 'Extracting archives ...'",
        _tar,
        _zip,
        "echo 'Collecting files from subdirectories ...'",
        _drop_empty_members_step(),
        _collect_from_subdirs_step(include_archives=True),
        # Pass 2 -- nested archives that were moved up.
        "echo 'Extracting nested archives (pass 2) ...'",
        _nested_tar,
        _nested_zip,
        "echo 'Collecting files from nested extraction ...'",
        _drop_empty_members_step(),
        _collect_from_subdirs_step(include_archives=False),
        "rm -f .extracted-archives",
    ]


def _leave_out_steps(patterns: list[str]) -> list[str]:
    """Shell steps: move the files that the entry leaves out of the index to left_out/.

    A provider can publish files that are not its data beside it (SPARQL query examples, a
    VoID description of its endpoint). They are
    kept, named in the log, and not indexed: the index reads the download folder only, not its
    subfolders.
    """
    loop = " ".join(
        f'for f in {pattern}; do [ -f "$f" ] || continue; '
        f'echo "  left out of the index: $f"; mv -f -- "$f" {LEFT_OUT_DIR}/; done;'
        for pattern in patterns
    )
    return [
        "echo 'Leaving out files that are not data ...'",
        f"{{ mkdir -p {LEFT_OUT_DIR} && {loop} true; }}",
    ]


def _decompress_xz_steps() -> list[str]:
    return [
        "echo 'Decompressing .xz files ...'",
        'for f in *.xz; do [ -f "$f" ] || continue; xz -dk "$f" 2>/dev/null || true; done',
    ]


def _decompress_gz_steps(*, include_data_formats: bool = False) -> list[str]:
    """Shell steps to gunzip the .gz files that are converted before indexing (RDF/XML, OWL,
    JSON-LD, ...).

    Turtle, N-Triples, N-Quads, TriG and N3 stay compressed: the index reads them streamed
    (rdfsolve.qlever.inputs.index_command), so no plain copy fills the disk. *include_data_formats*
    is kept for callers and has no effect.
    """
    del include_data_formats
    loop = (
        'for f in *.gz; do [ -f "$f" ] || continue; '
        'case "$f" in *.tar.gz|*.ttl.gz|*.nt.gz|*.nq.gz|*.trig.gz|*.n3.gz) continue;; esac; '
        # A journal already exported is not decompressed again.
        'case "$f" in *.jnl.gz) [ -s "${f%.jnl.gz}.nq.gz" ] && continue;; esac; '
        'gunzip -fk "$f" 2>/dev/null || true; done'
    )
    return ["echo 'Decompressing .gz files to be converted ...'", loop]


def _convert_rdfxml_steps() -> list[str]:
    convert = converter_command('"$f"', '"$nq"')
    return [
        f"echo 'Converting RDF/XML -> N-Quads ({CONVERTER}) ...'",
        (
            # An empty file has no statements.
            'for f in *.rdf *.owl *.xml; do [ -s "$f" ] || continue; '
            'nq=$(echo "$f" | sed "s/\\.[^.]*$/.nq/"); '
            '[ -f "$nq" ] && continue; '
            # A file that starts as Turtle is Turtle under an RDF/XML name: it is named .ttl.
            'if head -c 4096 "$f" | grep -q -E "^[[:space:]]*(@prefix|@base|PREFIX|BASE)[[:space:]]"; '
            'then mv "$f" "$(echo "$f" | sed "s/\\.[^.]*$/.ttl/")"; continue; fi; '
            f"{convert} || "
            '{ rm -f "$nq"; echo "Conversion failed: $f" >&2; exit 1; }; done'
        ),
    ]


def _convert_obo_steps() -> list[str]:
    return [
        "echo 'Converting OBO -> Turtle via ROBOT ...'",
        (
            "[ -f robot.jar ] || wget -q -O robot.jar "
            '"https://github.com/ontodev/robot/releases/download/v1.9.10/robot.jar"'
        ),
        (
            'for f in *.obo; do [ -f "$f" ] || continue; '
            'ttl=$(echo "$f" | sed "s/\\.[^.]*$/.ttl/"); '
            '[ -f "$ttl" ] && continue; '
            'java -jar robot.jar convert --input "$f" --output "$ttl" '
            '--format ttl || { rm -f "$ttl"; echo "Conversion failed: $f" >&2; exit 1; }; done'
        ),
        "rm -f robot.jar robot.log",
    ]


def _convert_jsonld_steps() -> list[str]:
    return [
        "echo 'Converting JSON-LD -> Turtle ...'",
        (
            'python3 -c "'
            "import glob, os; "
            "from rdflib import Graph; "
            "[Graph().parse(f,format='json-ld')"
            ".serialize(os.path.splitext(f)[0]+'.ttl',format='turtle') "
            "for f in glob.glob('*.jsonld') "
            "if not os.path.exists(os.path.splitext(f)[0]+'.ttl')]"
            '"'
        ),
    ]


# The exporter of Blazegraph journals: blazegraph-runner, the tool the Gene Ontology pipeline
# builds its journals with. Its release is pinned by URL and SHA-256.
BLAZEGRAPH_RUNNER_URL = (
    "https://github.com/balhoff/blazegraph-runner/releases/download/v1.7/blazegraph-runner-1.7.tgz"
)
BLAZEGRAPH_RUNNER_SHA256 = "d96aab4abad0d473c130207070820e8a48e5572bd5a57fed68262f3c5e8d1cb0"
_BLAZEGRAPH_RUNNER = ".blazegraph-runner/blazegraph-runner-1.7/bin/blazegraph-runner"


def _convert_journal_steps() -> list[str]:
    """Shell steps: export each Blazegraph journal (X.jnl) to X.nq.gz, every named graph kept.

    The export streams through a named pipe to gzip, so no plain copy fills the disk; the
    runner's log goes to its standard output, not into the data. The Java heap is
    ``$RDFSOLVE_BLAZEGRAPH_JAVA_OPTS`` (default -Xmx16G). A failed export leaves no output.
    """
    export = (
        'for f in *.jnl; do [ -f "$f" ] || continue; '
        'nq="${f%.jnl}.nq.gz"; [ -s "$nq" ] && continue; '
        'rm -f "$nq.part" .jnl-export.fifo; mkfifo .jnl-export.fifo; '
        '"$(command -v pigz || echo gzip)" -c < .jnl-export.fifo > "$nq.part" & z=$!; '
        'JAVA_OPTS="${RDFSOLVE_BLAZEGRAPH_JAVA_OPTS:--Xmx16G}" '
        f'{_BLAZEGRAPH_RUNNER} dump --journal="$PWD/$f" --outformat=nquads .jnl-export.fifo; r=$?; '
        "wait $z; g=$?; rm -f .jnl-export.fifo; "
        'if [ $r -ne 0 ] || [ $g -ne 0 ]; then rm -f "$nq.part"; '
        'echo "Journal export failed: $f" >&2; exit 1; fi; mv -f "$nq.part" "$nq"; done'
    )
    return [
        "echo 'Exporting Blazegraph journals -> N-Quads (blazegraph-runner 1.7) ...'",
        # The runner calls java; named here, the job's tool check refuses a job without it.
        (
            "{ java -version >/dev/null 2>&1 || "
            "{ echo 'blazegraph-runner needs java on the PATH' >&2; exit 1; }; }"
        ),
        (
            "{ [ -x " + _BLAZEGRAPH_RUNNER + " ] || { "
            f'wget -q -O .blazegraph-runner.tgz "{BLAZEGRAPH_RUNNER_URL}" && '
            f'echo "{BLAZEGRAPH_RUNNER_SHA256}  .blazegraph-runner.tgz" | sha256sum -c --quiet && '
            "mkdir -p .blazegraph-runner && tar xzf .blazegraph-runner.tgz -C .blazegraph-runner && "
            "rm -f .blazegraph-runner.tgz; }; } || "
            "{ echo 'blazegraph-runner could not be fetched or did not match its SHA-256' >&2; "
            "exit 1; }"
        ),
        export,
    ]


# subdir tar helpers


def tar_source_qleverfile_parts(
    tar_url: str,
    tar_subdirs: list[str],
    src_data_dir: str,
    rdf_subdir: str,
) -> tuple[str, str, str, str]:
    """Return (get_data_cmd, rdf_format, input_files, cat_input_files)
    for a source whose data lives inside a remote tar of one folder per graph.
    """
    steps: list[str] = [
        f"mkdir -p {src_data_dir}",
        f"cd {src_data_dir}",
        _WGET_AGAIN,
        # Discover tar root prefix from the first header block.
        (
            f'TAR_ROOT=$(curl -s --range 0-511 "{tar_url}" | '
            'python3 -c "'
            "import sys; b=sys.stdin.buffer.read(512); "
            "print(b[:100].rstrip(b'\\x00').decode('utf-8','replace').split('/')[0]) "
            "if len(b)==512 else print('')"
            '")'
        ),
    ]
    for subdir in tar_subdirs:
        steps.append(
            f'echo "Streaming {subdir} ..." && '
            f'curl -s "{tar_url}" | '
            f'tar -xzf - --wildcards "${{TAR_ROOT}}/{subdir}/*.trig.gz" '
            f"--strip-components=2 --no-anchored 2>/dev/null || true"
        )

    return (
        " && ".join(steps),
        "ttl",  # QLever reads TriG as Turtle superset
        f"{rdf_subdir}/*.trig.gz",
        "zcat ${INPUT_FILES} 2>/dev/null | grep -v '^$'",
    )


# GET_DATA_CMD assembly


def _build_get_data_steps(
    analysis: SourceAnalysis,
    src_data_dir: str,
) -> list[str]:
    """Assemble the full GET_DATA_CMD shell steps from an analysis."""
    saved = _saved_names(analysis.urls_by_suffix)
    steps: list[str] = [
        f"mkdir -p {src_data_dir}",
        f"cd {src_data_dir}",
        _WGET_AGAIN,
        # A later step can end with '|| true'; the downloads end the script when one fails.
        "{ "
        + " && ".join(
            _folder_cmd(u, suffix) if u.endswith("/") else _wget_cmd(u, saved.get(u))
            for suffix, urls in analysis.urls_by_suffix.items()
            for u in urls
        )
        + "; } || { echo 'A download failed' >&2; exit 1; }",
    ]

    steps.extend(_rename_mislabelled_steps(analysis))

    if any(u.endswith("/") for u in analysis.urls):
        steps.extend(
            ["echo 'Collecting files from fetched folders ...'", _collect_from_subdirs_step()]
        )

    if analysis.needs_archive:
        steps.extend(_extract_archives_steps())

    if analysis.left_out:
        steps.extend(_leave_out_steps(analysis.left_out))

    if analysis.needs_xz:
        steps.extend(_decompress_xz_steps())

    if analysis.needs_decompression:
        include_data = bool(analysis.suffixes & {"nq", "nquads", "nt"})
        steps.extend(_decompress_gz_steps(include_data_formats=include_data))

    if analysis.needs_rdfxml_conversion:
        steps.extend(_convert_rdfxml_steps())

    if analysis.needs_obo_conversion:
        steps.extend(_convert_obo_steps())

    if analysis.needs_jsonld_conversion:
        steps.extend(_convert_jsonld_steps())

    if analysis.needs_journal_conversion:
        steps.extend(_convert_journal_steps())

    if analysis.needs_hdt_conversion:
        from rdfsolve.qlever.hdt import convert_hdt_steps

        steps.extend(convert_hdt_steps())

    return steps


# Qleverfile rendering


def _render_qleverfile(
    *,
    name: str,
    workdir: Path,
    port: int,
    runtime: str,
    rdf_format: str,
    input_files: str,
    cat_input_files: str,
    get_data_cmd: str,
    cfg: QleverConfig,
) -> str:
    """Fill in the Qleverfile template."""
    try:
        cat_input_files = cat_input_files.format(workdir=workdir)
    except Exception:
        pass

    streams = []
    if cat_input_files == "cat ${INPUT_FILES}":
        streams = [
            {
                "cmd": 'cat "{}"',
                "format": rdf_format,
                "for-each": pattern,
                "parallel": "true" if cfg.parallel_parsing else "false",
            }
            for pattern in shlex.split(input_files)
        ]
        cat_input_files = ""

    content = QLEVERFILE_TEMPLATE.format(
        name=name,
        workdir=workdir,
        port=port,
        rdf_format=rdf_format,
        input_files=input_files,
        cat_input_files=cat_input_files,
        get_data_cmd=get_data_cmd,
        settings_json=cfg.settings_json,
        access_token=cfg.access_token or name,
        runtime=runtime,
        parallel_parsing="true" if cfg.parallel_parsing else "false",
        parser_buffer_size=cfg.parser_buffer_size,
        stxxl_memory=cfg.stxxl_memory,
        memory_for_queries=cfg.memory_for_queries,
        timeout=cfg.timeout,
        image=cfg.image,
    )

    if streams:
        return content.replace(
            "[index]\n", "[index]\nMULTI_INPUT_JSON = " + json.dumps(streams) + "\n"
        )
    return content


# Public builders


def build_qleverfile(
    entry: Any,
    data_dir: Path,
    port: int,
    runtime: str,
    cfg: QleverConfig | None = None,
    *,
    workdir: Path | None = None,
) -> str:
    """Build a Qleverfile for a single sources.yaml entry.

    Handles every download_* flavour, bulk tars of one folder per graph
    (local_tar_url), archives, and format conversions.
    """
    cfg = cfg or QleverConfig()
    name = entry.get("name", "unknown")
    workdir = (workdir or data_dir / "qlever_workdirs" / name).resolve()
    rdf_subdir = "rdf"
    src_data_dir = f"{workdir}/{rdf_subdir}"

    graph_sources = entry.get("graph_sources")
    if graph_sources:
        from rdfsolve.qlever.inputs import EMPTY, graph_input_directory

        commands: list[str] = []
        streams: list[dict[str, str]] = []
        files: list[str] = []
        for graph, fields in graph_sources.items():
            directory = graph_input_directory(workdir, graph) / "rdf"
            commands.extend(
                [f"mkdir -p {shlex.quote(str(directory))}", f"cd {shlex.quote(str(directory))}"]
            )
            for key, urls in fields.items():
                suffix = key.removeprefix("download_")
                suffix = "rdf" if suffix == "rdfxml" else suffix
                for url in urls:
                    compression = (
                        ".gz" if url.endswith(".gz") else ".xz" if url.endswith(".xz") else ""
                    )
                    filename = graph_download_name(url, key)
                    commands.append(f"wget -c -q -O {filename} {shlex.quote(url)}")
                    if compression:
                        executable = "gunzip" if compression == ".gz" else "xz -d"
                        commands.append(f"{executable} -fk {filename}")
                        filename = filename.removesuffix(compression)
                    cat = ""
                    if suffix in {"rdf", "owl"}:
                        # RDF/XML is converted to N-Triples, which take the graph of the mapping.
                        # A file published empty has no statements and would stop the
                        # converter: it is marked, not converted.
                        target = filename.rsplit(".", 1)[0] + ".nt"
                        marker = f"{target}{EMPTY}"
                        commands.append(
                            f"if [ -s {filename} ]; then rm -f {marker} && "
                            f"{converter_command(filename, target)}; "
                            f"else rm -f {target} && touch {marker}; fi"
                        )
                        filename = target
                        cat = f"[ -e {shlex.quote(str(directory / marker))} ] || "
                    path = directory / filename
                    files.append(shlex.quote(str(path)))
                    streams.append(
                        {
                            "cmd": f"{cat}cat {shlex.quote(str(path))}",
                            "format": path.suffix[1:],
                            "graph": graph,
                        }
                    )
        content = _render_qleverfile(
            name=name,
            workdir=workdir,
            port=port,
            runtime=runtime,
            rdf_format="ttl",
            input_files=" ".join(files),
            cat_input_files="",
            get_data_cmd=" && ".join(commands),
            cfg=cfg,
        )
        return content.replace(
            "[index]\n", "[index]\nMULTI_INPUT_JSON = " + json.dumps(streams) + "\n"
        )

    # Bulk-tar path
    local_tar_url = entry.get("local_tar_url", "")
    if local_tar_url:
        graph_uris: list[str] = entry.get("graph_uris") or []
        if not graph_uris:
            raise ValueError(f"Source '{name}' has local_tar_url but no graph_uris")
        tar_subdirs = [graph_uri_to_tar_folder(g) for g in graph_uris]
        get_data_cmd, rdf_format, input_files, cat_input_files = tar_source_qleverfile_parts(
            local_tar_url, tar_subdirs, src_data_dir, rdf_subdir
        )
        return _render_qleverfile(
            name=name,
            workdir=workdir,
            port=port,
            runtime=runtime,
            rdf_format=rdf_format,
            input_files=input_files,
            cat_input_files=cat_input_files,
            get_data_cmd=get_data_cmd,
            cfg=cfg,
        )

    # -- Generic download path -----------------------------------------
    analysis = analyse_source(entry)
    if not analysis.urls:
        raise ValueError(f"Source '{name}' has no download_* fields")

    spec = analysis.pick_format_spec()
    steps = _build_get_data_steps(analysis, src_data_dir)

    return _render_qleverfile(
        name=name,
        workdir=workdir,
        port=port,
        runtime=runtime,
        rdf_format=spec.qlever_format,
        input_files=f"{rdf_subdir}/{spec.glob}",
        cat_input_files=spec.cat,
        get_data_cmd=" && ".join(steps),
        cfg=cfg,
    )


def build_provider_qleverfile(
    provider: str,
    members: list[Any],
    data_dir: Path,
    port: int,
    runtime: str,
    cfg: QleverConfig | None = None,
    *,
    workdir: Path | None = None,
) -> str:
    """Build a combined Qleverfile for all members of a provider group."""
    cfg = cfg or QleverConfig()
    workdir = (workdir or data_dir / "qlever_workdirs" / provider).resolve()
    rdf_subdir = "rdf"
    src_data_dir = f"{workdir}/{rdf_subdir}"

    tar_members = [m for m in members if m.get("local_tar_url")]
    dl_members = [
        m
        for m in members
        if not m.get("local_tar_url") and any(k.startswith("download_") for k in m)
    ]

    tar_url = tar_members[0].get("local_tar_url", "") if tar_members else ""

    # -- Pure download-based provider (no tar) -------------------------
    if not tar_url:
        merged: dict[str, Any] = {"name": provider}
        for m in dl_members:
            for key in m:
                if not key.startswith("download_"):
                    continue
                new_urls = urls_from_field(m, key)
                existing = merged.get(key)
                if existing is None:
                    merged[key] = (
                        new_urls if len(new_urls) > 1 else (new_urls[0] if new_urls else "")
                    )
                else:
                    merged[key] = (
                        existing if isinstance(existing, list) else [existing]
                    ) + new_urls
        return build_qleverfile(merged, data_dir, port, runtime, cfg=cfg, workdir=workdir)

    # Tar-based provider
    all_subdirs: list[str] = []
    for m in tar_members:
        for g in m.get("graph_uris") or []:
            folder = graph_uri_to_tar_folder(g)
            if folder not in all_subdirs:
                all_subdirs.append(folder)

    get_data_cmd, rdf_format, input_files, cat_input_files = tar_source_qleverfile_parts(
        tar_url, all_subdirs, src_data_dir, rdf_subdir
    )

    # Append download steps for non-tar members.
    if dl_members:
        extra: list[str] = []
        for m in dl_members:
            mname = m.get("name", "?")
            for key in sorted(k for k in m if k.startswith("download_")):
                for url in urls_from_field(m, key):
                    extra.append(
                        f'echo "Downloading {mname}: {url}" && '
                        f'wget -c -q --content-disposition "{url}" 2>/dev/null || '
                        f'wget -c -q -O "$(basename {url})" "{url}"'
                    )
        if extra:
            get_data_cmd += " && " + " && ".join(extra)

    return _render_qleverfile(
        name=provider,
        workdir=workdir,
        port=port,
        runtime=runtime,
        rdf_format=rdf_format,
        input_files=input_files,
        cat_input_files=cat_input_files,
        get_data_cmd=get_data_cmd,
        cfg=cfg,
    )
