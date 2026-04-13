"""QLever utilities -- Qleverfile generation from sources.yaml entries.

Public API (re-exported from ``rdfsolve.qlever.__init__``):
    QleverConfig, build_qleverfile, build_provider_qleverfile,
    detect_data_format, urls_from_field, graph_uri_to_tar_folder,
    tar_source_qleverfile_parts, QLEVERFILE_TEMPLATE, FORMAT_REGISTRY
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "QleverConfig",
    "QLEVERFILE_TEMPLATE",
    "FORMAT_REGISTRY",
    "FormatSpec",
    "SourceAnalysis",
    "detect_data_format",
    "analyse_source",
    "urls_from_field",
    "graph_uri_to_tar_folder",
    "tar_source_qleverfile_parts",
    "build_qleverfile",
    "build_provider_qleverfile",
]


# ===================================================================
# Configuration
# ===================================================================

@dataclass
class QleverConfig:
    """Tunable parameters written into every generated Qleverfile.

    All fields have defaults.
    """

    memory_for_queries: str = "500G"
    timeout: str = "9999999999s"
    parser_buffer_size: str = "8GB"
    parallel_parsing: bool = False
    num_triples_per_batch: int = 1_000_000
    access_token: str | None = None
    image: str = "docker.io/adfreiburg/qlever:latest"

    @property
    def settings_json(self) -> str:
        return (
            '{ "ascii-prefixes-only": false, '
            f'"num-triples-per-batch": {self.num_triples_per_batch}, '
            '"parser-integer-overflow-behavior": '
            '"overflowing-integers-become-doubles" }'
        )


# ===================================================================
# Qleverfile template
# ===================================================================

QLEVERFILE_TEMPLATE = """\
# Qleverfile for {name}
# Auto-generated with rdfsolve
#
# Usage:
#   cd {workdir}
#   qlever index
#   qlever start
#   qlever stop


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


# ===================================================================
# Format registry -- the single source of truth
# ===================================================================

@dataclass(frozen=True)
class FormatSpec:
    """How QLever should ingest a given RDF serialisation.

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


# Order matters: first match wins when multiple download_* keys exist.
FORMAT_REGISTRY: dict[str, FormatSpec] = {
    # -- Quad formats ---------------------------------------------------
    "nq": FormatSpec(
        qlever_format="nq",
        glob="*.nq",
        cat=(
            "cat ${INPUT_FILES} | "
            r"perl -pe 's{<([^<>]*)>}{my $i=$1; $i=~s/\x22/%22/g; qq{<$i>}}ge' | "
            "grep -v '^$'"
        ),
    ),
    "nquads": FormatSpec(  # alias
        qlever_format="nq",
        glob="*.nq",
        cat=(
            "cat ${INPUT_FILES} | "
            r"perl -pe 's{<([^<>]*)>}{my $i=$1; $i=~s/\x22/%22/g; qq{<$i>}}ge' | "
            "grep -v '^$'"
        ),
    ),
    "trig": FormatSpec(
        qlever_format="nq",
        glob="*.trig* *.nq*",
        cat="( zcat ${INPUT_FILES} 2>/dev/null || cat ${INPUT_FILES} 2>/dev/null ) | grep -v '^$'",
    ),
    # -- Triple formats -------------------------------------------------
    "nt": FormatSpec(
        qlever_format="nt",
        glob="*.nt*",
        cat="( zcat ${INPUT_FILES} 2>/dev/null || cat ${INPUT_FILES} 2>/dev/null ) | grep -v '^$'",
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
    # -- Formats requiring conversion -----------------------------------
    "rdf": FormatSpec(
        qlever_format="nq", glob="*.nq", cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "rdfxml": FormatSpec(
        qlever_format="nq", glob="*.nq", cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "owl": FormatSpec(
        qlever_format="nq", glob="*.nq", cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "obo": FormatSpec(
        qlever_format="ttl", glob="*.ttl", cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    "jsonld": FormatSpec(
        qlever_format="ttl", glob="*.ttl", cat="cat ${INPUT_FILES}",
        needs_conversion=True,
    ),
    # -- Archive-only keys (format decided by archive contents) ---------
    "tar_gz": FormatSpec(
        qlever_format="ttl", glob="*.ttl", cat="cat ${INPUT_FILES}",
    ),
    "tgz": FormatSpec(
        qlever_format="ttl", glob="*.ttl", cat="cat ${INPUT_FILES}",
    ),
    "zip": FormatSpec(
        qlever_format="ttl", glob="*.ttl", cat="cat ${INPUT_FILES}",
    ),
}

# Extensions we recognise in a URL for smart wget naming.
_RDF_EXTS = (
    ".ttl", ".ttl.gz", ".nt", ".nt.gz", ".nq", ".nq.gz",
    ".trig", ".trig.gz", ".n3", ".owl", ".rdf", ".rdf.gz",
    ".rdf.xz", ".owl.xz", ".xml.gz", ".jsonld", ".obo",
    ".tar.gz", ".tgz", ".zip",
)


# ===================================================================
# Source analysis -- one pass, no booleans
# ===================================================================

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

    @property
    def needs_rdfxml_conversion(self) -> bool:
        return bool(self.suffixes & {"rdf", "rdfxml", "owl"})

    @property
    def needs_obo_conversion(self) -> bool:
        return "obo" in self.suffixes

    @property
    def needs_jsonld_conversion(self) -> bool:
        return "jsonld" in self.suffixes

    @property
    def needs_decompression(self) -> bool:
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
        # Fallback -- unreachable if caller checked detect_data_format first.
        return FormatSpec(qlever_format="ttl", glob="*.ttl", cat="cat ${INPUT_FILES}")


def analyse_source(entry: dict) -> SourceAnalysis:
    """Scan all download_* fields on entry in a single pass."""
    urls: list[str] = []
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
            low = u.lower()
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
        suffixes=suffixes,
        needs_gz=needs_gz,
        needs_xz=needs_xz,
        needs_archive=needs_archive,
    )


# ===================================================================
# Small helpers
# ===================================================================

def detect_data_format(entry: Any) -> str | None:
    """Return a short format label, or None if no download is available."""
    if entry.get("local_tar_url"):
        return "trig"
    dl_keys = [k for k in entry if k.startswith("download_") and entry.get(k)]
    if not dl_keys:
        return None
    for suffix in FORMAT_REGISTRY:
        if f"download_{suffix}" in dl_keys:
            return suffix
    return dl_keys[0].removeprefix("download_")


def urls_from_field(entry: dict, field_name: str) -> list[str]:
    """Extract a flat URL list from a YAML field (string or list)."""
    raw = entry.get(field_name, "")
    if not raw:
        return []
    items = raw if isinstance(raw, list) else [raw]
    return [u for u in items if u]


def graph_uri_to_tar_folder(uri: str) -> str:
    """Convert a named-graph URI to the IDSM-style tar folder name."""
    no_scheme = re.sub(r"^https?://", "", uri)
    return "http_" + no_scheme.replace("/", "_")


# ===================================================================
# Shell-step builders -- each returns list[str] of shell fragments
# ===================================================================

def _wget_cmd(url: str) -> str:
    """Return a single wget command string for url."""
    fname = url.rsplit("/", 1)[-1]
    if any(fname.lower().endswith(ext) for ext in _RDF_EXTS):
        return f'wget -c -q "{url}"'
    # Try to derive a recognisable filename from the URL path.
    parts = url.rstrip("/").split("/")
    derived = next(
        (p for p in reversed(parts)
         if any(p.lower().endswith(e) for e in _RDF_EXTS)),
        None,
    )
    if derived:
        return f'wget -c -q -O "{derived}" "{url}"'
    return f'wget -c -q --content-disposition "{url}"'


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


def _extract_archives_steps() -> list[str]:
    """Shell steps: extract archives, collect, repeat for nested archives."""
    _tar = (
        'for f in *.tar.gz *.tgz; do [ -f "$f" ] || continue; '
        'echo "  extracting $f"; tar xzf "$f"; done'
    )
    _zip = (
        'for f in *.zip; do [ -f "$f" ] || continue; '
        'echo "  extracting $f"; '
        "python3 -c \"import zipfile; z=zipfile.ZipFile('$f'); z.extractall('.'); "
        "print(f'Extracted {len(z.namelist())} files'); z.close()\"; done"
    )
    _nested_tar = (
        'for f in *.tar.gz *.tgz; do [ -f "$f" ] || continue; '
        'echo "  extracting nested $f"; tar xzf "$f" 2>/dev/null || true; done'
    )
    _nested_zip = (
        'for f in *.zip; do [ -f "$f" ] || continue; '
        'echo "  extracting nested $f"; '
        "python3 -c \"import zipfile; z=zipfile.ZipFile('$f'); z.extractall('.'); "
        "print(f'Extracted {len(z.namelist())} files'); z.close()\" 2>/dev/null || true; done"
    )
    return [
        "echo 'Extracting archives ...'",
        _tar,
        _zip,
        "echo 'Collecting files from subdirectories ...'",
        _collect_from_subdirs_step(include_archives=True),
        # Pass 2 -- nested archives that were moved up.
        "echo 'Extracting nested archives (pass 2) ...'",
        _nested_tar,
        _nested_zip,
        "echo 'Collecting files from nested extraction ...'",
        _collect_from_subdirs_step(include_archives=False),
    ]


def _decompress_xz_steps() -> list[str]:
    return [
        "echo 'Decompressing .xz files ...'",
        'for f in *.xz; do [ -f "$f" ] || continue; xz -dk "$f" 2>/dev/null || true; done',
    ]


def _decompress_gz_steps(*, include_data_formats: bool = False) -> list[str]:
    """Shell steps to gunzip .gz files.

    Parameters
    ----------
    include_data_formats:
        When True, also decompress .nq.gz / .nt.gz (needed when
        the primary format is NQ/NT and we want uniform plain-text files).
    """
    if include_data_formats:
        globs = "*.ttl.gz *.owl.gz *.rdf.gz *.n3.gz *.jsonld.gz *.nq.gz *.nt.gz"
        loop = (
            f'for f in {globs}; do [ -f "$f" ] || continue; '
            'gunzip -fk "$f" 2>/dev/null || true; done'
        )
    else:
        loop = (
            'for f in *.gz; do [ -f "$f" ] || continue; '
            'case "$f" in *.tar.gz) continue;; esac; '
            'gunzip -fk "$f" 2>/dev/null || true; done'
        )
    return ["echo 'Decompressing .gz files ...'", loop]


def _convert_rdfxml_steps() -> list[str]:
    return [
        "echo 'Converting RDF/XML -> N-Quads ...'",
        (
            'for f in *.rdf *.owl *.xml; do [ -f "$f" ] || continue; '
            'nq=$(echo "$f" | sed "s/\\.[^.]*$/.nq/"); '
            '[ -f "$nq" ] && continue; '
            'rapper -q -i rdfxml -o nquads "$f" > "$nq" 2>/dev/null || rm -f "$nq"; done'
        ),
    ]


def _convert_obo_steps() -> list[str]:
    return [
        "echo 'Converting OBO -> Turtle via ROBOT ...'",
        (
            '[ -f robot.jar ] || wget -q -O robot.jar '
            '"https://github.com/ontodev/robot/releases/download/v1.9.10/robot.jar"'
        ),
        (
            'for f in *.obo; do [ -f "$f" ] || continue; '
            'ttl=$(echo "$f" | sed "s/\\.[^.]*$/.ttl/"); '
            '[ -f "$ttl" ] && continue; '
            'java -jar robot.jar convert --input "$f" --output "$ttl" '
            '--format ttl 2>/dev/null || rm -f "$ttl"; done'
        ),
        "rm -f robot.jar robot.log",
    ]


def _convert_jsonld_steps() -> list[str]:
    return [
        "echo 'Converting JSON-LD -> Turtle ...'",
        (
            "python3 -c \""
            "import glob, os; "
            "from rdflib import Graph; "
            "[Graph().parse(f,format='json-ld')"
            ".serialize(os.path.splitext(f)[0]+'.ttl',format='turtle') "
            "for f in glob.glob('*.jsonld') "
            "if not os.path.exists(os.path.splitext(f)[0]+'.ttl')]"
            '"'
        ),
    ]


# ===================================================================
# subdir tar helpers
# ===================================================================

def tar_source_qleverfile_parts(
    tar_url: str,
    tar_subdirs: list[str],
    src_data_dir: str,
    rdf_subdir: str,
) -> tuple[str, str, str, str]:
    """Return (get_data_cmd, rdf_format, input_files, cat_input_files)
    for a source whose data lives inside an IDSM-style remote tar.
    """
    steps: list[str] = [
        f"mkdir -p {src_data_dir}",
        f"cd {src_data_dir}",
        # Discover tar root prefix from the first header block.
        f'TAR_ROOT=$(curl -s --range 0-511 "{tar_url}" | '
        "python3 -c \""
        "import sys; b=sys.stdin.buffer.read(512); "
        "print(b[:100].rstrip(b'\\x00').decode('utf-8','replace').split('/')[0]) "
        "if len(b)==512 else print('')"
        "\")",
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


# ===================================================================
# GET_DATA_CMD assembly
# ===================================================================

def _build_get_data_steps(
    analysis: SourceAnalysis,
    src_data_dir: str,
) -> list[str]:
    """Assemble the full GET_DATA_CMD shell steps from an analysis."""
    steps: list[str] = [
        f"mkdir -p {src_data_dir}",
        f"cd {src_data_dir}",
        " && ".join(_wget_cmd(u) for u in analysis.urls),
    ]

    if analysis.needs_archive:
        steps.extend(_extract_archives_steps())

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

    return steps


# ===================================================================
# Qleverfile rendering
# ===================================================================

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

    return QLEVERFILE_TEMPLATE.format(
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
        memory_for_queries=cfg.memory_for_queries,
        timeout=cfg.timeout,
        image=cfg.image,
    )


# ===================================================================
# Public builders
# ===================================================================

def build_qleverfile(
    entry: Any,
    data_dir: Path,
    port: int,
    runtime: str,
    cfg: QleverConfig | None = None,
) -> str:
    """Build a Qleverfile for a single sources.yaml entry.

    Handles every download_* flavour, IDSM-style bulk tars
    (local_tar_url), archives, and format conversions.
    """
    cfg = cfg or QleverConfig()
    name = entry.get("name", "unknown")
    workdir = (data_dir / "qlever_workdirs" / name).resolve()
    rdf_subdir = "rdf"
    src_data_dir = f"{workdir}/{rdf_subdir}"

    # -- Bulk-tar path --------------------------------------
    local_tar_url = entry.get("local_tar_url", "")
    if local_tar_url:
        graph_uris: list[str] = entry.get("graph_uris") or []
        if not graph_uris:
            raise ValueError(f"Source '{name}' has local_tar_url but no graph_uris")
        tar_subdirs = [graph_uri_to_tar_folder(g) for g in graph_uris]
        get_data_cmd, rdf_format, input_files, cat_input_files = (
            tar_source_qleverfile_parts(local_tar_url, tar_subdirs, src_data_dir, rdf_subdir)
        )
        return _render_qleverfile(
            name=name, workdir=workdir, port=port, runtime=runtime,
            rdf_format=rdf_format, input_files=input_files,
            cat_input_files=cat_input_files, get_data_cmd=get_data_cmd,
            cfg=cfg,
        )

    # -- Generic download path -----------------------------------------
    analysis = analyse_source(entry)
    if not analysis.urls:
        raise ValueError(f"Source '{name}' has no download_* fields")

    spec = analysis.pick_format_spec()
    steps = _build_get_data_steps(analysis, src_data_dir)

    return _render_qleverfile(
        name=name, workdir=workdir, port=port, runtime=runtime,
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
) -> str:
    """Build a combined Qleverfile for all members of a provider group."""
    cfg = cfg or QleverConfig()
    workdir = (data_dir / "qlever_workdirs" / provider).resolve()
    rdf_subdir = "rdf"
    src_data_dir = f"{workdir}/{rdf_subdir}"

    tar_members = [m for m in members if m.get("local_tar_url")]
    dl_members = [
        m for m in members
        if not m.get("local_tar_url")
        and any(k.startswith("download_") for k in m)
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
                    merged[key] = new_urls if len(new_urls) > 1 else (new_urls[0] if new_urls else "")
                else:
                    merged[key] = (
                        (existing if isinstance(existing, list) else [existing])
                        + new_urls
                    )
        return build_qleverfile(merged, data_dir, port, runtime, cfg=cfg)

    # -- Tar-based provider
    all_subdirs: list[str] = []
    for m in tar_members:
        for g in m.get("graph_uris") or []:
            folder = graph_uri_to_tar_folder(g)
            if folder not in all_subdirs:
                all_subdirs.append(folder)

    get_data_cmd, rdf_format, input_files, cat_input_files = (
        tar_source_qleverfile_parts(tar_url, all_subdirs, src_data_dir, rdf_subdir)
    )

    # Append download steps for non-tar members (e.g. chebi inside IDSM).
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
        name=provider, workdir=workdir, port=port, runtime=runtime,
        rdf_format=rdf_format, input_files=input_files,
        cat_input_files=cat_input_files, get_data_cmd=get_data_cmd,
        cfg=cfg,
    )
