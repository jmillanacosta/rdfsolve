"""Start and stop only QLever processes owned by this run."""

from __future__ import annotations

import configparser
import os
import re
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def read_qleverfile(workdir: Path) -> configparser.ConfigParser:
    """Read settings without expanding shell variables or percent escapes."""
    config = configparser.ConfigParser(interpolation=None)
    path = workdir / "Qleverfile"
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            config.read_file(stream)
    return config


def index_name(workdir: Path, fallback: str) -> str:
    """Use the cached index name from the existing Qleverfile."""
    name = read_qleverfile(workdir).get("data", "NAME", fallback=fallback)
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name) or name in {".", ".."}:
        raise ValueError(f"Invalid index name in {workdir / 'Qleverfile'}")
    return name


def index_build(workdir: Path, name: str) -> str | None:
    """Return the QLever build (git hash) that made the index, from its metadata file."""
    import json

    path = workdir / f"{index_name(workdir, name)}.meta-data.json"
    if not path.is_file():
        return None
    build = json.loads(path.read_text(encoding="utf-8")).get("git-hash")
    return str(build) if build else None


def image_for_index(data_dir: Path, workdir: Path, name: str) -> Path:
    """Return the QLever image that serves an index: the image of the build that made it.

    The image catalogue qlever_images.yaml in the data directory lists images and their builds
    (- image: qlever.sif / git_hash: 9ec88a0). Without a catalogue, and for an index that is
    not yet built, the data directory's qlever.sif is used. With a catalogue, an index whose build no image has is refused: a server of
    another build cannot read its index format. Build hashes are compared by prefix, since an
    index records a shorter hash than the server prints.
    """
    import yaml

    catalogue = data_dir / "qlever_images.yaml"
    if not catalogue.is_file():
        return data_dir / "qlever.sif"
    build = index_build(workdir, name)
    if build is None:
        return data_dir / "qlever.sif"
    for entry in yaml.safe_load(catalogue.read_text(encoding="utf-8")) or []:
        known = str(entry.get("git_hash", ""))
        if known and (known.startswith(build) or build.startswith(known)):
            image = Path(entry["image"])
            return image if image.is_absolute() else data_dir / image
    raise ValueError(f"No image in {catalogue} for the build {build} of the index {name}")


def _query_memory(value: str) -> str:
    """Return the query memory of the Qleverfile, at most a share of the SLURM allocation.

    The share is 0.6, or RDFSOLVE_QLEVER_MEMORY_SHARE (above 0, below 1) for a job whose
    queries need more (Bgee: one census query needs more than 455 GB of a 740 GB job).
    """
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KMGT])B?", value.upper())
    if not match:
        raise ValueError(f"Invalid QLever query memory limit: {value!r}")
    megabytes = float(match[1]) * {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024**2}[match[2]]
    allocation = os.environ.get("SLURM_MEM_PER_NODE")
    if allocation:
        text = os.environ.get("RDFSOLVE_QLEVER_MEMORY_SHARE", "0.6")
        try:
            share = float(text)
        except ValueError:
            share = 0.0
        if not 0 < share < 1:
            raise ValueError(f"RDFSOLVE_QLEVER_MEMORY_SHARE must be above 0 and below 1: {text!r}")
        megabytes = min(megabytes, float(allocation) * share)
    return f"{max(1, int(megabytes))}MB"


def stop_server(process: subprocess.Popen[bytes]) -> None:
    """Stop an owned process and reap it before returning."""
    if process.poll() is not None:
        process.wait()
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def start_server(
    image: Path,
    workdir: Path,
    name: str,
    port: int,
    *,
    startup_timeout: float = 600,
) -> subprocess.Popen[bytes]:
    """Start a cached index. Refuse occupied ports; never kill another server."""
    if not image.is_file():
        raise FileNotFoundError(f"QLever image not found: {image}")
    with socket.socket() as check:
        try:
            check.bind(("0.0.0.0", port))  # noqa: S104 - Check only; do not listen.
        except OSError as error:
            raise RuntimeError(f"Port {port} is in use; choose another --base-port") from error
    config = read_qleverfile(workdir)
    name = index_name(workdir, name)
    memory = _query_memory(config.get("server", "MEMORY_FOR_QUERIES", fallback="30G"))
    timeout = config.get("server", "TIMEOUT", fallback="600s")
    token = config.get("server", "ACCESS_TOKEN", fallback=name)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    log_path = workdir / f"server-{port}-{stamp}.log"
    command = [
        "singularity",
        "exec",
        "--bind",
        f"{workdir}:{workdir}",
        str(image),
        "qlever-server",
        "-i",
        name,
        "-p",
        str(port),
        "-j",
        "1",
        "-m",
        memory,
        "-c",
        "1GB",
        "-e",
        "256MB",
        "-s",
        timeout,
        "-a",
        token,
    ]
    with log_path.open("xb") as output:
        process = subprocess.Popen(command, cwd=workdir, stdout=output, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + startup_timeout
        tail = ""
        with log_path.open(encoding="utf-8", errors="replace") as output:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(
                        f"QLever exited with status {process.returncode}; see {log_path}"
                    )
                tail = (tail + output.read())[-4096:]
                if "The server is ready, listening for requests" in tail:
                    return process
                time.sleep(1)
        raise TimeoutError(f"QLever did not start within {startup_timeout}s; see {log_path}")
    except BaseException:
        stop_server(process)
        raise
