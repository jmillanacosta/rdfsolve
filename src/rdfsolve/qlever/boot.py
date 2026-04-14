"""QLever Singularity endpoint booter.

Generates a Qleverfile for one or more sources from ``sources.yaml``,
downloads the data, builds the QLever index, and starts the SPARQL
server — all via Singularity.

Public API
----------
- :func:`boot_source` — run the full pipeline for a single source
- :func:`list_downloadable_sources` — list sources that can be booted
- Step functions: :func:`step_setup`, :func:`step_get_data`,
  :func:`step_index`, :func:`step_start`, :func:`step_stop`
"""

from __future__ import annotations

import configparser
import logging
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

from rdfsolve.qlever import (
    QleverConfig,
    build_qleverfile,
    detect_data_format,
)

log = logging.getLogger(__name__)

# ─── Defaults ─────────────────────────────────────────────────────
DEFAULT_DATA_DIR = Path("./data")
DEFAULT_PORT = 7019
DEFAULT_RUNTIME = "native"
DEFAULT_SINGULARITY_IMAGE = "./data/qlever.sif"
DEFAULT_QLEVER_DOCKER_IMAGE = "docker://adfreiburg/qlever:latest"
DEFAULT_NUM_THREADS = 8
DEFAULT_CACHE_SIZE = "8G"
DEFAULT_MEMORY_FOR_QUERIES_SERVER = "40G"
DEFAULT_WAIT_TIMEOUT = 120  # seconds


# ═══════════════════════════════════════════════════════════════════
# Source helpers
# ═══════════════════════════════════════════════════════════════════


def _load_sources(path: str | Path) -> list[dict[str, Any]]:
    """Load entries from ``sources.yaml``."""
    import yaml

    with open(path) as f:
        data = yaml.safe_load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "sources" in data:
        return data["sources"]
    raise ValueError(f"Unrecognised sources.yaml structure in {path}")


def select_entries(
    entries: list[dict[str, Any]],
    names: list[str] | None = None,
    name_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Filter entries by explicit name list or regex filter."""
    downloadable = [e for e in entries if detect_data_format(e) is not None]

    if names:
        name_set = set(names)
        selected = [e for e in downloadable if e.get("name") in name_set]
        missing = name_set - {e.get("name") for e in selected}
        if missing:
            log.warning(
                "Sources not found (or not downloadable): %s",
                ", ".join(sorted(missing)),
            )
        return selected

    if name_filter:
        pat = re.compile(name_filter)
        return [e for e in downloadable if pat.search(e.get("name", ""))]

    return downloadable


def list_downloadable_sources(
    sources_yaml: str | Path = "data/sources.yaml",
) -> list[dict[str, str]]:
    """Return a list of ``{name, format, provider}`` dicts for downloadable sources."""
    entries = _load_sources(sources_yaml)
    downloadable = [e for e in entries if detect_data_format(e) is not None]
    return [
        {
            "name": e.get("name", "?"),
            "format": detect_data_format(e) or "?",
            "provider": e.get("local_provider", ""),
        }
        for e in sorted(downloadable, key=lambda x: x.get("name", ""))
    ]


# ═══════════════════════════════════════════════════════════════════
# Singularity helpers
# ═══════════════════════════════════════════════════════════════════


def ensure_singularity_image(image_path: str, docker_ref: str) -> None:
    """Pull the Singularity image if not already present."""
    if os.path.isfile(image_path):
        log.info("Singularity image already present: %s", image_path)
        return
    log.info("Pulling QLever Singularity image from %s …", docker_ref)
    os.makedirs(os.path.dirname(image_path) or ".", exist_ok=True)
    subprocess.check_call(
        ["singularity", "pull", "--disable-cache", image_path, docker_ref],
    )
    log.info("Image saved: %s", image_path)


def _singularity_exec(
    image: str,
    workdir: str,
    data_dir: str,
    cmd: list[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    """Run a command inside the QLever Singularity container."""
    full_cmd = [
        "singularity",
        "exec",
        "--bind",
        f"{workdir}:{workdir}",
        "--bind",
        f"{data_dir}:{data_dir}",
        image,
        *cmd,
    ]
    log.debug("$ %s", " ".join(full_cmd))
    return subprocess.run(full_cmd, cwd=workdir, **kwargs)


# ═══════════════════════════════════════════════════════════════════
# Qleverfile parsing helpers
# ═══════════════════════════════════════════════════════════════════


def _parse_qleverfile(path: Path) -> dict[str, dict[str, str]]:
    cfg = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation()
    )
    cfg.read(str(path))
    return {s: dict(cfg.items(s)) for s in cfg.sections()}


def _parse_qleverfile_raw(path: Path) -> dict[str, str]:
    """Parse a Qleverfile returning flat key-value pairs."""
    result: dict[str, str] = {}
    with open(path) as f:
        content = f.read()
    content = re.sub(r"\n([ \t]+)", r" ", content)
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("["):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            result[key.strip()] = val.strip()
    return result


# ═══════════════════════════════════════════════════════════════════
# Pipeline steps
# ═══════════════════════════════════════════════════════════════════


def step_setup(
    entry: dict[str, Any],
    *,
    data_dir: Path,
    port: int,
    runtime: str,
    cfg: QleverConfig,
) -> Path:
    """Generate the Qleverfile for *entry* and return the workdir path."""
    name = entry.get("name", "unknown")
    workdir = data_dir / "qlever_workdirs" / name

    content = build_qleverfile(entry, data_dir, port, runtime, cfg=cfg)
    workdir.mkdir(parents=True, exist_ok=True)
    qleverfile = workdir / "Qleverfile"
    qleverfile.write_text(content)
    log.info("[%s] Qleverfile written → %s", name, qleverfile)
    return workdir


def step_get_data(workdir: Path) -> None:
    """Run the GET_DATA_CMD from the Qleverfile."""
    kv = _parse_qleverfile_raw(workdir / "Qleverfile")
    cmd = kv.get("GET_DATA_CMD", "")
    if not cmd:
        raise RuntimeError(f"No GET_DATA_CMD in {workdir / 'Qleverfile'}")
    log.info("[%s] Running GET_DATA_CMD …", workdir.name)
    subprocess.check_call(["bash", "-c", cmd], cwd=str(workdir))
    log.info("[%s] Data download complete.", workdir.name)


def step_index(
    workdir: Path,
    *,
    name: str,
    image: str,
    data_dir: str,
    num_threads: int = DEFAULT_NUM_THREADS,
) -> None:
    """Build the QLever index via Singularity."""
    kv = _parse_qleverfile_raw(workdir / "Qleverfile")

    rdf_format = kv.get("FORMAT", "ttl").strip()
    mem_for_queries = kv.get("MEMORY_FOR_QUERIES", "300G").strip()
    settings_raw = kv.get("SETTINGS_JSON", "{}").strip()
    input_files_raw = kv.get("INPUT_FILES", "").strip()
    cat_cmd = kv.get("CAT_INPUT_FILES", "").strip()

    settings_path = workdir / f"{name}.settings.json"
    settings_path.write_text(settings_raw)

    os.environ["INPUT_FILES"] = input_files_raw

    use_direct = cat_cmd == "cat ${INPUT_FILES}"

    # ── Overflow integer workaround ───────────────────────────────
    log.info("[%s] Checking for int64-overflowing integers …", name)
    patched = 0
    for glob_pat in input_files_raw.split():
        for fpath in workdir.glob(glob_pat):
            if not fpath.is_file():
                continue
            with open(fpath, "rb") as fh:
                sample = fh.read(10_000_000)
            if re.search(rb"\t\d{20,}\s", sample):
                log.info("  fixing overflows in %s", fpath.name)
                subprocess.check_call(
                    [
                        "sed",
                        "-i",
                        "-E",
                        r's/\t([0-9]{20,})(\s)/\t"\1"^^<http:\/\/www.w3.org\/2001\/XMLSchema#double>\2/g',
                        str(fpath),
                    ],
                )
                patched += 1
    if patched:
        log.info("[%s] Patched overflowing integers in %d file(s)", name, patched)

    # ── Strip illegal control chars ─────────────────────────────────
    # Characters like \x01 and \x7F in bio2rdf data break QLever's
    # IRI / N-Quads parser.  Strip everything except \t \n \r.
    log.info("[%s] Checking for illegal control characters …", name)
    sanitised = 0
    for glob_pat in input_files_raw.split():
        for fpath in workdir.glob(glob_pat):
            if not fpath.is_file():
                continue
            with open(fpath, "rb") as fh:
                sample = fh.read(10_000_000)
            if re.search(rb"[\x00-\x08\x0e-\x1f\x7f]", sample):
                log.info("  stripping control chars from %s", fpath.name)
                subprocess.check_call(
                    [
                        "perl", "-pi", "-e",
                        r"s/[\x00-\x08\x0e-\x1f\x7f]//g;",
                        str(fpath),
                    ],
                )
                sanitised += 1
    if sanitised:
        log.info("[%s] Sanitised %d file(s)", name, sanitised)

    if use_direct:
        file_list: list[str] = []
        for glob_pat in input_files_raw.split():
            file_list.extend(
                str(p) for p in sorted(workdir.glob(glob_pat)) if p.is_file()
            )
        if not file_list:
            raise RuntimeError(
                f"No input files matched INPUT_FILES='{input_files_raw}' in {workdir}"
            )
        log.info("[%s] Direct file input: %d files", name, len(file_list))
        file_flags: list[str] = []
        for fp in file_list:
            file_flags.extend(["-f", fp])

        index_cmd = [
            "qlever-index",
            "-i",
            name,
            "-s",
            str(settings_path),
            "--vocabulary-type",
            "on-disk-compressed",
            "-m",
            mem_for_queries,
            "-F",
            rdf_format,
            *file_flags,
            "-p",
            "false",
        ]
        log_path = workdir / f"{name}.index-log.txt"
        log.info("[%s] Indexing (direct) …", name)
        with open(log_path, "w") as lf:
            proc = _singularity_exec(
                image,
                str(workdir),
                data_dir,
                index_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if proc.stdout:
                output = proc.stdout.decode("utf-8", errors="replace")
                lf.write(output)
                for line in output.splitlines()[-5:]:
                    log.info("  %s", line)
        if proc.returncode:
            raise RuntimeError(
                f"qlever-index failed (rc={proc.returncode}); see {log_path}"
            )
    else:
        index_cmd_str = (
            f"cd '{workdir}' && {cat_cmd} | "
            f"singularity exec "
            f"--bind '{workdir}:{workdir}' "
            f"--bind '{data_dir}:{data_dir}' "
            f"'{image}' "
            f"qlever-index -i '{name}' "
            f"-s '{settings_path}' "
            f"--vocabulary-type on-disk-compressed "
            f"-m '{mem_for_queries}' "
            f"-F '{rdf_format}' -f - -p false"
        )
        log_path = workdir / f"{name}.index-log.txt"
        log.info("[%s] Indexing (pipe) …", name)
        with open(log_path, "w") as lf:
            proc = subprocess.run(
                ["bash", "-c", index_cmd_str],
                cwd=str(workdir),
                env={**os.environ, "INPUT_FILES": input_files_raw},
                stdout=lf,
                stderr=subprocess.STDOUT,
            )
        if proc.returncode:
            raise RuntimeError(
                f"qlever-index failed (rc={proc.returncode}); see {log_path}"
            )

    log.info("[%s] Index built successfully.", name)


def step_start(
    workdir: Path,
    *,
    name: str,
    port: int,
    image: str,
    data_dir: str,
    num_threads: int = DEFAULT_NUM_THREADS,
    cache_size: str = DEFAULT_CACHE_SIZE,
    memory_for_queries_server: str = DEFAULT_MEMORY_FOR_QUERIES_SERVER,
    wait_timeout: int = DEFAULT_WAIT_TIMEOUT,
) -> str:
    """Start the QLever server in a Singularity instance.

    Returns the Singularity instance name.
    """
    kv = _parse_qleverfile_raw(workdir / "Qleverfile")
    access_token = kv.get("ACCESS_TOKEN", name)
    timeout_val = kv.get("TIMEOUT", "1000s")

    instance_name = f"qlever_{name}"

    # Stop any stale instance
    subprocess.run(
        ["singularity", "instance", "stop", instance_name],
        capture_output=True,
    )

    log.info("[%s] Starting Singularity instance '%s' …", name, instance_name)
    start_log = workdir / "start.log"
    with open(start_log, "w") as lf:
        subprocess.check_call(
            [
                "singularity",
                "instance",
                "start",
                "--bind",
                f"{workdir}:{workdir}",
                "--bind",
                f"{data_dir}:{data_dir}",
                "-W",
                str(workdir),
                image,
                instance_name,
            ],
            stdout=lf,
            stderr=lf,
        )

    server_cmd = (
        f"cd '{workdir}' && exec qlever-server "
        f"-i '{name}' "
        f"-j {num_threads} "
        f"-p {port} "
        f"-m '{memory_for_queries_server}' "
        f"-c '{cache_size}' "
        f"-e '4G' "
        f"-k 200 "
        f"-s '{timeout_val}' "
        f"-a '{access_token}'"
    )
    server_log = workdir / "server.log"
    with open(server_log, "w") as lf:
        subprocess.Popen(
            [
                "singularity",
                "exec",
                f"instance://{instance_name}",
                "bash",
                "-c",
                server_cmd,
            ],
            stdout=lf,
            stderr=lf,
        )

    log.info("[%s] Waiting for SPARQL endpoint on port %d …", name, port)
    elapsed = 0
    while elapsed < wait_timeout:
        try:
            r = subprocess.run(
                [
                    "curl",
                    "--noproxy",
                    "*",
                    "-sf",
                    f"http://localhost:{port}/?query=ASK%7B%7D",
                ],
                capture_output=True,
                timeout=5,
            )
            if r.returncode == 0:
                log.info("[%s] QLever server ready on port %d.", name, port)
                return instance_name
        except subprocess.TimeoutExpired:
            pass
        time.sleep(2)
        elapsed += 2

    raise RuntimeError(
        f"[{name}] QLever did not start within {wait_timeout}s; check {server_log}"
    )


def step_stop(name: str, workdir: Path) -> None:
    """Stop a running QLever Singularity instance."""
    instance_name = f"qlever_{name}"
    stop_log = workdir / "stop.log"
    log.info("[%s] Stopping instance '%s' …", name, instance_name)
    with open(stop_log, "w") as lf:
        subprocess.run(
            ["singularity", "instance", "stop", instance_name],
            stdout=lf,
            stderr=lf,
        )


# ═══════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════

STEP_SETS: dict[str, tuple[str, ...]] = {
    "all": ("setup", "get-data", "index", "start"),
    "setup": ("setup",),
    "get-data": ("get-data",),
    "index": ("index",),
    "start": ("start",),
    "stop": ("stop",),
    "index-start": ("index", "start"),
    "setup-data": ("setup", "get-data"),
}


def boot_source(
    entry: dict[str, Any],
    *,
    data_dir: Path,
    port: int,
    runtime: str,
    cfg: QleverConfig,
    image: str,
    steps: tuple[str, ...],
    num_threads: int = DEFAULT_NUM_THREADS,
    cache_size: str = DEFAULT_CACHE_SIZE,
    memory_for_queries_server: str = DEFAULT_MEMORY_FOR_QUERIES_SERVER,
    wait_timeout: int = DEFAULT_WAIT_TIMEOUT,
) -> dict[str, Any]:
    """Run the requested pipeline steps for a single source."""
    name = entry.get("name", "unknown")
    workdir = data_dir / "qlever_workdirs" / name
    result: dict[str, Any] = {
        "name": name,
        "port": port,
        "workdir": str(workdir),
    }

    try:
        if "setup" in steps:
            workdir = step_setup(
                entry,
                data_dir=data_dir,
                port=port,
                runtime=runtime,
                cfg=cfg,
            )

        if "get-data" in steps:
            step_get_data(workdir)

        if "index" in steps:
            step_index(
                workdir,
                name=name,
                image=image,
                data_dir=str(data_dir),
                num_threads=num_threads,
            )

        if "start" in steps:
            instance = step_start(
                workdir,
                name=name,
                port=port,
                image=image,
                data_dir=str(data_dir),
                num_threads=num_threads,
                cache_size=cache_size,
                memory_for_queries_server=memory_for_queries_server,
                wait_timeout=wait_timeout,
            )
            result["instance"] = instance
            result["endpoint"] = f"http://localhost:{port}"

        if "stop" in steps:
            step_stop(name, workdir)

        result["status"] = "ok"

    except Exception as exc:
        log.error("[%s] FAILED: %s", name, exc)
        result["status"] = "failed"
        result["error"] = str(exc)

    return result


def boot_sources(
    sources_yaml: str | Path = "data/sources.yaml",
    *,
    source_names: list[str] | None = None,
    name_filter: str | None = None,
    step: str = "all",
    data_dir: str | Path = DEFAULT_DATA_DIR,
    base_port: int = DEFAULT_PORT,
    runtime: str = DEFAULT_RUNTIME,
    singularity_image: str = DEFAULT_SINGULARITY_IMAGE,
    docker_ref: str = DEFAULT_QLEVER_DOCKER_IMAGE,
    memory_for_queries: str = "500G",
    timeout: str = "9999999999s",
    parser_buffer_size: str = "8GB",
    parallel_parsing: bool = False,
    num_triples_per_batch: int = 1_000_000,
    qlever_image: str = "docker.io/adfreiburg/qlever:latest",
    num_threads: int = DEFAULT_NUM_THREADS,
    cache_size: str = DEFAULT_CACHE_SIZE,
    server_memory: str = DEFAULT_MEMORY_FOR_QUERIES_SERVER,
    wait_timeout: int = DEFAULT_WAIT_TIMEOUT,
) -> list[dict[str, Any]]:
    """Boot one or more QLever endpoints.

    High-level entry point that wraps :func:`boot_source` for multiple
    sources and handles Singularity image pulling.

    Returns a list of result dicts (one per source).
    """
    import json as _json

    sources_path = Path(sources_yaml)
    if not sources_path.exists():
        raise FileNotFoundError(f"sources.yaml not found: {sources_path}")

    entries = _load_sources(sources_path)
    selected = select_entries(entries, source_names, name_filter)
    if not selected:
        raise ValueError("No matching downloadable sources found.")

    cfg = QleverConfig(
        memory_for_queries=memory_for_queries,
        timeout=timeout,
        parser_buffer_size=parser_buffer_size,
        parallel_parsing=parallel_parsing,
        num_triples_per_batch=num_triples_per_batch,
        image=qlever_image,
    )

    data_dir_p = Path(data_dir).resolve()
    steps = STEP_SETS[step]
    image = str(Path(singularity_image).resolve())

    if step != "stop":
        ensure_singularity_image(image, docker_ref)

    results: list[dict[str, Any]] = []
    for idx, entry in enumerate(selected):
        port = base_port + idx
        log.info(
            "═══ [%d/%d] %s (port %d) ═══",
            idx + 1,
            len(selected),
            entry.get("name", "?"),
            port,
        )
        r = boot_source(
            entry,
            data_dir=data_dir_p,
            port=port,
            runtime=runtime,
            cfg=cfg,
            image=image,
            steps=steps,
            num_threads=num_threads,
            cache_size=cache_size,
            memory_for_queries_server=server_memory,
            wait_timeout=wait_timeout,
        )
        results.append(r)

    # Write results manifest
    manifest = data_dir_p / "qlever_workdirs" / "boot_results.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest, "w") as f:
        _json.dump(results, f, indent=2)
    log.info("Results written to %s", manifest)

    return results
