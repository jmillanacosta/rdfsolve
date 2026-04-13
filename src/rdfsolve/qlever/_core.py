"""Core QLever utilities — Qleverfile generation and source helpers.

Internal implementation module.  Public API is re-exported from
:mod:`rdfsolve.qlever`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = [
    "QleverConfig",
    "QLEVERFILE_TEMPLATE",
    "DIRECT_FORMATS",
    "detect_data_format",
    "urls_from_field",
    "graph_uri_to_tar_folder",
    "tar_source_qleverfile_parts",
    "build_qleverfile",
    "build_provider_qleverfile",
]


# ═══════════════════════════════════════════════════════════════════
# Configuration dataclass
# ═══════════════════════════════════════════════════════════════════


@dataclass
class QleverConfig:
    """Tunable parameters for Qleverfile generation.

    Every field has a sensible default that matches the original
    hard-coded values so existing callers don't need to change.
    """

    memory_for_queries: str = "500G"
    """Memory budget for query processing (QLever ``-m`` flag)."""

    timeout: str = "9999999999s"
    """Server-side query timeout."""

    parser_buffer_size: str = "8GB"
    """Parser buffer size for indexing."""

    parallel_parsing: bool = False
    """Whether to enable QLever parallel parsing."""

    num_triples_per_batch: int = 1_000_000
    """Triples per batch in the index settings JSON."""

    access_token: str | None = None
    """Server access token.  Defaults to the dataset name when ``None``."""

    image: str = "docker.io/adfreiburg/qlever:latest"
    """Docker / Singularity image reference."""

    @property
    def settings_json(self) -> str:
        """Return the ``SETTINGS_JSON`` value for the Qleverfile."""
        return (
            '{ "ascii-prefixes-only": false, '
            f'"num-triples-per-batch": {self.num_triples_per_batch}, '
            '"parser-integer-overflow-behavior": '
            '"overflowing-integers-become-doubles" }'
        )


# ═══════════════════════════════════════════════════════════════════
# Qleverfile template
# ═══════════════════════════════════════════════════════════════════

QLEVERFILE_TEMPLATE = """\
# Qleverfile for {name}
# Auto-generated with rdfsolve
#
# Usage:
#   cd {workdir}
#   qlever index          # build index from the RDF files
#   qlever start          # start SPARQL endpoint on port {port}
#   qlever stop           # stop the endpoint
#   # then:


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


# ═══════════════════════════════════════════════════════════════════
# Format detection
# ═══════════════════════════════════════════════════════════════════


def detect_data_format(entry: Any) -> str | None:
    """Return format string if the entry has any download field or local_tar_url.

    Returns ``None`` when the entry has neither ``download_*`` fields
    nor a ``local_tar_url``.
    """
    if entry.get("local_tar_url"):
        return "trig"
    dl_keys = [k for k in entry if k.startswith("download_") and entry.get(k)]
    if not dl_keys:
        return None
    priority = [
        "download_nq", "download_nquads", "download_trig", "download_nt",
        "download_n3", "download_ttl", "download_rdf", "download_rdfxml",
        "download_owl", "download_obo", "download_jsonld", "download_zip",
        "download_tar_gz", "download_tgz", "download_ftp",
    ]
    for k in priority:
        if k in dl_keys:
            return k.removeprefix("download_")
    return dl_keys[0].removeprefix("download_")


# ═══════════════════════════════════════════════════════════════════
# Download / tar helpers
# ═══════════════════════════════════════════════════════════════════


def urls_from_field(entry: dict, field_name: str) -> list[str]:
    """Extract a flat URL list from a YAML field (string or list)."""
    raw = entry.get(field_name, "")
    if not raw:
        return []
    urls = raw if isinstance(raw, list) else [raw]
    return [u for u in urls if u]


def graph_uri_to_tar_folder(uri: str) -> str:
    """Convert a named-graph URI to the folder name used inside IDSM-style tars."""
    no_scheme = re.sub(r'^https?://', '', uri)
    return 'http_' + no_scheme.replace('/', '_')


def tar_source_qleverfile_parts(
    tar_url: str,
    tar_subdirs: list[str],
    src_data_dir: str,
    rdf_subdir: str,
) -> tuple[str, str, str, str]:
    """Return (get_data_cmd, rdf_format, input_files, cat_input_files)
    for a source whose data lives inside an IDSM-style remote tar.
    """
    steps_tar: list[str] = [
        f"mkdir -p {src_data_dir}",
        f"cd {src_data_dir}",
        f'TAR_ROOT=$(curl -s --range 0-511 "{tar_url}" | '
        "python3 -c \""
        "import sys; b=sys.stdin.buffer.read(512); "
        "print(b[:100].rstrip(b'\\x00').decode('utf-8','replace').split('/')[0]) "
        "if len(b)==512 else print('')"
        "\")",
    ]
    for subdir in tar_subdirs:
        steps_tar.append(
            f'echo "Streaming {subdir} …" && '
            f'curl -s "{tar_url}" | '
            f'tar -xzf - --wildcards "${{TAR_ROOT}}/{subdir}/*.trig.gz" '
            f'--strip-components=2 '
            f'--no-anchored 2>/dev/null || true'
        )

    get_data_cmd = " && ".join(steps_tar)
    rdf_format = "ttl"
    input_files = f"{rdf_subdir}/*.trig.gz"
    cat_input_files = "zcat ${INPUT_FILES} 2>/dev/null | grep -v '^$'"

    return get_data_cmd, rdf_format, input_files, cat_input_files


# Map download_* suffix -> (QLever FORMAT, input glob, cat command)
DIRECT_FORMATS: dict[str, tuple[str, str, str]] = {
    "nq":      ("nq",  "*.nq.gz *.nq",    "zcat *.nq.gz 2>/dev/null; cat *.nq 2>/dev/null"),
    "nquads":  ("nq",  "*.nq.gz *.nq",    "zcat *.nq.gz 2>/dev/null; cat *.nq 2>/dev/null"),
    "ttl":     ("ttl", "*.ttl",            "cat *.ttl"),
    "nt":      ("nt",  "*.nt.gz *.nt",     "zcat *.nt.gz 2>/dev/null; cat *.nt 2>/dev/null"),
    "n3":      ("ttl", "*.n3",             "cat *.n3"),
    "owl":     ("ttl", "*.ttl",            "cat *.ttl"),
    "rdf":     ("ttl", "*.ttl",            "cat *.ttl"),
    "rdfxml":  ("ttl", "*.ttl",            "cat *.ttl"),
    "jsonld":  ("ttl", "*.ttl",            "cat *.ttl"),
}


# ═══════════════════════════════════════════════════════════════════
# Qleverfile builders
# ═══════════════════════════════════════════════════════════════════

_RDF_EXTS = (
    ".ttl", ".ttl.gz", ".nt", ".nt.gz", ".nq", ".nq.gz",
    ".trig", ".trig.gz", ".n3", ".owl", ".rdf", ".rdf.gz",
    ".rdf.xz", ".owl.xz", ".xml.gz", ".jsonld", ".obo",
    ".tar.gz", ".tgz", ".zip",
)


def _format_qleverfile(
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
    """Apply the template with all parameters."""
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


def build_qleverfile(
    entry: Any,
    data_dir: Path,
    port: int,
    runtime: str,
    cfg: QleverConfig | None = None,
) -> str:
    """Build Qleverfile content string for one source entry.

    Parameters
    ----------
    entry:
        Source dict from ``sources.yaml``.
    data_dir:
        Root data directory (Qleverfile workdirs are created below this).
    port:
        SPARQL server port.
    runtime:
        QLever runtime: ``"docker"`` or ``"native"``.
    cfg:
        Optional tuning parameters.  Uses defaults when ``None``.
    """
    if cfg is None:
        cfg = QleverConfig()

    name = entry.get("name", "unknown")
    local_tar_url = entry.get("local_tar_url", "")

    workdir = (data_dir / "qlever_workdirs" / name).resolve()
    rdf_subdir = "rdf"
    src_data_dir = f"{workdir}/{rdf_subdir}"

    # ── Provider bulk-tar path ────────────────────────────────────
    if local_tar_url:
        graph_uris: list[str] = entry.get("graph_uris") or []
        if not graph_uris:
            raise ValueError(
                f"Source '{name}' has local_tar_url but no graph_uris"
            )
        tar_subdirs = [graph_uri_to_tar_folder(g) for g in graph_uris]
        get_data_cmd, rdf_format, input_files, cat_input_files = \
            tar_source_qleverfile_parts(
                local_tar_url, tar_subdirs, src_data_dir, rdf_subdir
            )
        return _format_qleverfile(
            name=name, workdir=workdir, port=port, runtime=runtime,
            rdf_format=rdf_format, input_files=input_files,
            cat_input_files=cat_input_files, get_data_cmd=get_data_cmd,
            cfg=cfg,
        )

    # ── Collect ALL download URLs, grouped by type ────────────────
    dl_keys = sorted(k for k in entry if k.startswith("download_") and entry.get(k))
    if not dl_keys:
        raise ValueError(f"Source '{name}' has no download_* fields")

    all_urls: list[str] = []
    has_gz = False
    has_xz = False
    has_archive = False
    has_rdfxml = False
    has_obo = False
    has_jsonld = False
    has_nq = False
    has_trig = False
    has_nt = False
    has_n3 = False
    has_ttl = False

    for dk in dl_keys:
        suffix = dk.removeprefix("download_")
        urls = urls_from_field(entry, dk)
        all_urls.extend(urls)

        for u in urls:
            low = u.lower()
            if low.endswith(".gz") and not low.endswith(".tar.gz"):
                has_gz = True
            if low.endswith(".xz"):
                has_xz = True
            if low.endswith(".zip") or low.endswith(".tar.gz") or low.endswith(".tgz"):
                has_archive = True

        if suffix in ("tar_gz", "tgz", "zip"):
            has_archive = True
        if suffix in ("rdf", "rdfxml", "owl"):
            has_rdfxml = True
        if suffix == "obo":
            has_obo = True
        if suffix == "jsonld":
            has_jsonld = True
        if suffix in ("nq", "nquads"):
            has_nq = True
        if suffix == "trig":
            has_trig = True
        if suffix == "nt":
            has_nt = True
        if suffix == "n3":
            has_n3 = True
        if suffix == "ttl":
            has_ttl = True

    # ── Decide QLever FORMAT and INPUT_FILES / CAT_INPUT_FILES ────
    if has_nq:
        rdf_format = "nq"
        input_files = f"{rdf_subdir}/*.nq"
        cat_input_files = (
            "cat ${INPUT_FILES} | "
            r"perl -pe 's{<([^<>]*)>}{my $i=$1; $i=~s/\x22/%22/g; qq{<$i>}}ge' | "
            "grep -v '^$'"
        )
    elif has_trig:
        rdf_format = "nq"
        input_files = f"{rdf_subdir}/*.trig* {rdf_subdir}/*.nq*"
        cat_input_files = (
            "( zcat ${INPUT_FILES} 2>/dev/null || "
            "cat ${INPUT_FILES} 2>/dev/null ) | "
            "grep -v '^$'"
        )
    elif has_nt:
        rdf_format = "nt"
        input_files = f"{rdf_subdir}/*.nt*"
        cat_input_files = (
            "( zcat ${INPUT_FILES} 2>/dev/null || "
            "cat ${INPUT_FILES} 2>/dev/null ) | "
            "grep -v '^$'"
        )
    elif has_n3 and has_ttl:
        rdf_format = "ttl"
        input_files = f"{rdf_subdir}/*.ttl {rdf_subdir}/*.n3"
        cat_input_files = "cat ${INPUT_FILES}"
    elif has_n3:
        rdf_format = "ttl"
        input_files = f"{rdf_subdir}/*.n3"
        cat_input_files = "cat ${INPUT_FILES}"
    elif has_rdfxml:
        rdf_format = "nq"
        input_files = f"{rdf_subdir}/*.nq"
        cat_input_files = "cat ${INPUT_FILES}"
    elif has_obo:
        rdf_format = "ttl"
        input_files = f"{rdf_subdir}/*.ttl"
        cat_input_files = "cat ${INPUT_FILES}"
    else:
        rdf_format = "ttl"
        input_files = f"{rdf_subdir}/*.ttl"
        cat_input_files = "cat ${INPUT_FILES}"

    # ── Build GET_DATA_CMD ────────────────────────────────────────
    wget_parts: list[str] = []
    for u in all_urls:
        fname = u.rsplit("/", 1)[-1]
        if any(fname.lower().endswith(ext) for ext in _RDF_EXTS):
            wget_parts.append(f'wget -c -q "{u}"')
        else:
            parts = u.rstrip("/").split("/")
            derived = next(
                (p for p in reversed(parts)
                 if any(p.lower().endswith(e) for e in _RDF_EXTS)),
                None,
            )
            if derived:
                wget_parts.append(f'wget -c -q -O "{derived}" "{u}"')
            else:
                wget_parts.append(f'wget -c -q --content-disposition "{u}"')

    wget_lines = " && ".join(wget_parts)
    steps: list[str] = [
        f"mkdir -p {src_data_dir}",
        f"cd {src_data_dir}",
        wget_lines,
    ]

    # Extract archives
    if has_archive:
        steps.append("echo 'Extracting archives …'")
        steps.append(
            'for f in *.tar.gz *.tgz; do '
            '[ -f "$f" ] || continue; '
            'echo "  extracting $f"; '
            'tar xzf "$f"; '
            'done'
        )
        steps.append(
            'for f in *.zip; do '
            '[ -f "$f" ] || continue; '
            'echo "  extracting $f"; '
            'python3 -c "import zipfile; z=zipfile.ZipFile(\'$f\'); z.extractall(\'.\'); '
            "print(f'Extracted {len(z.namelist())} files'); z.close()\"; "
            'done'
        )
        # Collect RDF files + nested archives from subdirectories
        steps.append(
            "echo 'Collecting RDF files from subdirectories …' "
            "&& find . -mindepth 2 \\( "
            '-name "*.ttl" -o -name "*.ttl.gz" -o -name "*.nt" -o '
            '-name "*.nt.gz" -o -name "*.nq" -o -name "*.nq.gz" -o '
            '-name "*.trig" -o -name "*.trig.gz" -o '
            '-name "*.n3" -o -name "*.owl" -o -name "*.rdf" -o '
            '-name "*.rdf.gz" -o -name "*.jsonld" -o '
            '-name "*.tar.gz" -o -name "*.tgz" -o -name "*.zip" '
            "\\) -print0 | while IFS= read -r -d '' fp; do "
            'bn=$(basename "$fp"); '
            'dest=$bn; '
            'if [ -e "./$dest" ]; then '
            'dn=$(dirname "$fp" | tr "/" "_" | sed "s/^\\._//"); '
            'dest=$dn"__"$bn; '
            'fi; '
            'n=1; '
            'while [ -e "./$dest" ]; do '
            'dest=$n"__"$bn; '
            'n=$((n+1)); '
            'done; '
            'mv "$fp" "./$dest"; '
            "done 2>/dev/null || true"
        )
        # Second pass for nested archives
        steps.append("echo 'Extracting nested archives (pass 2) …'")
        steps.append(
            'for f in *.tar.gz *.tgz; do '
            '[ -f "$f" ] || continue; '
            'echo "  extracting nested $f"; '
            'tar xzf "$f" 2>/dev/null || true; '
            'done'
        )
        steps.append(
            'for f in *.zip; do '
            '[ -f "$f" ] || continue; '
            'echo "  extracting nested $f"; '
            'python3 -c "import zipfile; z=zipfile.ZipFile(\'$f\'); z.extractall(\'.\'); '
            "print(f'Extracted {len(z.namelist())} files'); z.close()\" 2>/dev/null || true; "
            'done'
        )
        steps.append(
            "echo 'Collecting RDF files from nested extraction …' "
            "&& find . -mindepth 2 \\( "
            '-name "*.ttl" -o -name "*.ttl.gz" -o -name "*.nt" -o '
            '-name "*.nt.gz" -o -name "*.nq" -o -name "*.nq.gz" -o '
            '-name "*.trig" -o -name "*.trig.gz" -o '
            '-name "*.n3" -o -name "*.owl" -o -name "*.rdf" -o '
            '-name "*.rdf.gz" -o -name "*.jsonld" '
            "\\) -print0 | while IFS= read -r -d '' fp; do "
            'bn=$(basename "$fp"); '
            'dest=$bn; '
            'if [ -e "./$dest" ]; then '
            'dn=$(dirname "$fp" | tr "/" "_" | sed "s/^\\._//"); '
            'dest=$dn"__"$bn; '
            'fi; '
            'n=1; '
            'while [ -e "./$dest" ]; do '
            'dest=$n"__"$bn; '
            'n=$((n+1)); '
            'done; '
            'mv "$fp" "./$dest"; '
            "done 2>/dev/null || true"
        )

    # Decompress .xz
    if has_xz:
        steps.append("echo 'Decompressing .xz files …'")
        steps.append(
            'for f in *.xz; do '
            '[ -f "$f" ] || continue; '
            'xz -dk "$f" 2>/dev/null || true; '
            'done'
        )

    # Decompress .gz (also after archive extraction)
    if has_gz or has_archive:
        steps.append("echo 'Decompressing .gz files …'")
        if has_nq or has_nt:
            steps.append(
                'for f in *.ttl.gz *.owl.gz *.rdf.gz *.n3.gz *.jsonld.gz *.nq.gz *.nt.gz; do '
                '[ -f "$f" ] || continue; '
                'gunzip -fk "$f" 2>/dev/null || true; '
                'done'
            )
        else:
            steps.append(
                'for f in *.gz; do '
                '[ -f "$f" ] || continue; '
                'case "$f" in *.tar.gz) continue;; esac; '
                'gunzip -fk "$f" 2>/dev/null || true; '
                'done'
            )

    # Convert RDF/XML -> N-Quads
    if has_rdfxml:
        steps.append("echo 'Converting RDF/XML -> N-Quads …'")
        steps.append(
            'for f in *.rdf *.owl *.xml; do '
            '[ -f "$f" ] || continue; '
            'nq=$(echo "$f" | sed "s/\\.[^.]*$/.nq/"); '
            '[ -f "$nq" ] && continue; '
            'rapper -q -i rdfxml -o nquads "$f" > "$nq" 2>/dev/null || rm -f "$nq"; '
            'done'
        )

    # Convert OBO -> Turtle
    if has_obo:
        steps.append("echo 'Converting OBO -> Turtle via ROBOT …'")
        steps.append(
            '[ -f robot.jar ] || wget -q -O robot.jar '
            '"https://github.com/ontodev/robot/releases/download/v1.9.10/robot.jar"'
        )
        steps.append(
            'for f in *.obo; do '
            '[ -f "$f" ] || continue; '
            'ttl=$(echo "$f" | sed "s/\\.[^.]*$/.ttl/"); '
            '[ -f "$ttl" ] && continue; '
            'java -jar robot.jar convert --input "$f" --output "$ttl" --format ttl 2>/dev/null || rm -f "$ttl"; '
            'done'
        )
        steps.append('rm -f robot.jar robot.log')

    # Convert JSON-LD -> Turtle
    if has_jsonld:
        steps.append("echo 'Converting JSON-LD -> Turtle …'")
        steps.append(
            "python3 -c \""
            "import glob, os; "
            "from rdflib import Graph; "
            "[Graph().parse(f,format='json-ld')"
            ".serialize(os.path.splitext(f)[0]+'.ttl',format='turtle') "
            "for f in glob.glob('*.jsonld') "
            "if not os.path.exists(os.path.splitext(f)[0]+'.ttl')]"
            '"'
        )

    get_data_cmd = " && ".join(steps)

    return _format_qleverfile(
        name=name, workdir=workdir, port=port, runtime=runtime,
        rdf_format=rdf_format, input_files=input_files,
        cat_input_files=cat_input_files, get_data_cmd=get_data_cmd,
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
    """Build a combined Qleverfile that indexes ALL members of a provider."""
    if cfg is None:
        cfg = QleverConfig()

    workdir = (data_dir / "qlever_workdirs" / provider).resolve()
    rdf_subdir = "rdf"
    src_data_dir = f"{workdir}/{rdf_subdir}"

    tar_members = [m for m in members if m.get("local_tar_url")]
    dl_members = [m for m in members if not m.get("local_tar_url")
                  and any(k.startswith("download_") for k in m)]

    tar_url = tar_members[0].get("local_tar_url", "") if tar_members else ""

    # Pure download-based provider
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
                    merged[key] = (existing if isinstance(existing, list) else [existing]) + new_urls
        return build_qleverfile(merged, data_dir, port, runtime, cfg=cfg)

    # Tar-based provider
    all_subdirs: list[str] = []
    for m in tar_members:
        for g in (m.get("graph_uris") or []):
            folder = graph_uri_to_tar_folder(g)
            if folder not in all_subdirs:
                all_subdirs.append(folder)

    get_data_cmd, rdf_format, input_files, cat_input_files = \
        tar_source_qleverfile_parts(
            tar_url, all_subdirs, src_data_dir, rdf_subdir
        )

    if dl_members:
        extra_steps: list[str] = []
        for m in dl_members:
            mname = m.get("name", "?")
            for key in sorted(k for k in m if k.startswith("download_")):
                for url in urls_from_field(m, key):
                    extra_steps.append(
                        f'echo "Downloading {mname}: {url}" && '
                        f'wget -c -q --content-disposition "{url}" 2>/dev/null || '
                        f'wget -c -q -O "$(basename {url})" "{url}"'
                    )
        if extra_steps:
            get_data_cmd = get_data_cmd + " && " + " && ".join(extra_steps)

    return _format_qleverfile(
        name=provider, workdir=workdir, port=port, runtime=runtime,
        rdf_format=rdf_format, input_files=input_files,
        cat_input_files=cat_input_files, get_data_cmd=get_data_cmd,
        cfg=cfg,
    )
