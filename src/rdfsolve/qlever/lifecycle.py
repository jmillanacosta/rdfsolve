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


def _query_memory(value: str) -> str:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KMGT])B?", value.upper())
    if not match:
        raise ValueError(f"Invalid QLever query memory limit: {value!r}")
    megabytes = float(match[1]) * {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024**2}[match[2]]
    allocation = os.environ.get("SLURM_MEM_PER_NODE")
    if allocation:
        megabytes = min(megabytes, float(allocation) * 0.6)
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
