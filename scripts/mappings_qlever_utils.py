#!/usr/bin/env python3
"""Shared utilities for managing local QLever servers."""
import logging
import os
import subprocess
import time
from pathlib import Path

log = logging.getLogger(__name__)


def get_qlever_workdirs(data_dir: Path):
    """Get list of source names with QLever indices."""
    workdir = data_dir / "qlever_workdirs"
    if not workdir.exists():
        return []

    sources = []
    for source_dir in workdir.iterdir():
        if not source_dir.is_dir():
            continue
        index_file = source_dir / f"{source_dir.name}.index.spo"
        if index_file.exists():
            sources.append(source_dir.name)

    return sources


def start_qlever_server(workdir: Path, source_name: str, port: int, data_dir: Path, timeout: int = 120):
    """Start QLever server, return PID or None."""
    # Kill any existing server on this port
    kill_qlever_on_port(port)

    image_path = data_dir / "qlever.sif"
    if not image_path.exists():
        log.error(f"QLever image not found: {image_path}")
        return None

    cmd = [
        "singularity",
        "exec",
        "--bind",
        f"{workdir}:{workdir}",
        "-W",
        str(workdir),
        str(image_path),
        "bash",
        "-c",
        f"cd '{workdir}' && exec qlever-server -i '{source_name}' -j 8 -p '{port}' -m 40G -c 8G -e 4G -k 200 -s 1000s -a '{source_name}'",
    ]

    log_path = workdir / "server.log"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

    # Wait for server to start
    iterations = timeout // 2

    for i in range(iterations):
        time.sleep(2)

        if proc.poll() is not None:
            log.error(f"QLever server for {source_name} died during startup")
            if log_path.exists():
                log.error(f"  Last log:\n{log_path.read_text()[-500:]}")
            return None

        if log_path.exists():
            log_content = log_path.read_text()
            if "The server is ready, listening for requests" in log_content:
                return proc.pid

    log.error(f"QLever server for {source_name} did not start in {timeout}s")
    proc.terminate()
    return None


def stop_qlever_server(pid: int):
    """Stop QLever server."""
    import signal

    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    except ProcessLookupError:
        pass


def kill_qlever_on_port(port: int):
    """Kill any process using the given port."""
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True,
            text=True,
        )
        if result.stdout.strip():
            pids = result.stdout.strip().split()
            for pid in pids:
                try:
                    os.kill(int(pid), 9)
                except ProcessLookupError:
                    pass
    except Exception:
        pass
