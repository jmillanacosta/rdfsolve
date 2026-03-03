"""Benchmarking and metrics collection for RDFSolve mining runs.

Captures system-level performance data suitable for computer-science
papers and reproducibility analysis:

- Machine specifications (CPU, RAM, OS, kernel, hostname)
- Per-run resource usage (wall time, peak RSS, CPU time, disk I/O)
- QLever index/server metrics (index size, indexing time)
- Environment info (Python version, rdfsolve version, QLever version)
- Per-dataset statistics (triple count, file sizes, errors)

All data is written to ``benchmarks.jsonl`` (one JSON object per line)
for easy aggregation with pandas / polars.

Usage from ``mine_local.py``::

    from rdfsolve.tools.benchmark import BenchmarkCollector

    bench = BenchmarkCollector(output_dir=Path("mined_schemas"))
    with bench.track("affymetrix", method="local-mine") as run:
        _mine_single_local(endpoint, name, out, args)
        run.add_extra("triples_indexed", 123456)
    # run data auto-flushed to benchmarks.jsonl
"""

from __future__ import annotations

import json
import logging
import os
import platform
import resource
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# Machine info — gathered once per session
# ═══════════════════════════════════════════════════════════════════


@dataclass
class MachineInfo:
    """Static information about the host machine."""

    hostname: str = ""
    os_name: str = ""       # e.g. "Linux"
    os_release: str = ""    # kernel version
    os_version: str = ""    # e.g. "#1 SMP ..."
    architecture: str = ""  # e.g. "x86_64"
    cpu_model: str = ""
    cpu_count_logical: int = 0
    cpu_count_physical: int = 0
    ram_total_gb: float = 0.0
    python_version: str = ""
    rdfsolve_version: str = ""
    qlever_version: str = ""


def collect_machine_info() -> MachineInfo:
    """Gather static machine specifications."""
    info = MachineInfo()
    info.hostname = platform.node()
    info.os_name = platform.system()
    info.os_release = platform.release()
    info.os_version = platform.version()
    info.architecture = platform.machine()
    info.python_version = platform.python_version()

    # CPU model — Linux only (/proc/cpuinfo)
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("model name"):
                    info.cpu_model = line.split(":", 1)[1].strip()
                    break
    except OSError:
        info.cpu_model = platform.processor() or "unknown"

    # CPU count
    info.cpu_count_logical = os.cpu_count() or 0
    try:
        info.cpu_count_physical = len(
            os.sched_getaffinity(0)
        )
    except AttributeError:
        info.cpu_count_physical = info.cpu_count_logical

    # RAM total
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    kb = int(line.split()[1])
                    info.ram_total_gb = round(kb / 1048576, 2)
                    break
    except OSError:
        pass

    # rdfsolve version
    try:
        from importlib.metadata import version
        info.rdfsolve_version = version("rdfsolve")
    except Exception:
        info.rdfsolve_version = "unknown"

    # qlever version
    try:
        import subprocess
        proc = subprocess.run(
            ["qlever", "--version"],
            capture_output=True, text=True, timeout=5,
        )
        info.qlever_version = proc.stdout.strip() or "unknown"
    except Exception:
        info.qlever_version = "not installed"

    return info


# ═══════════════════════════════════════════════════════════════════
# Per-run metrics
# ═══════════════════════════════════════════════════════════════════


@dataclass
class RunMetrics:
    """Resource usage captured for a single mining run."""

    # Identity
    dataset: str = ""
    method: str = ""          # discover / mine / local-mine
    endpoint: str = ""

    # Timing
    started_at: str = ""      # ISO-8601
    finished_at: str = ""
    wall_time_s: float = 0.0

    # CPU
    cpu_user_s: float = 0.0   # user-mode CPU seconds
    cpu_system_s: float = 0.0  # kernel CPU seconds

    # Memory
    peak_rss_mb: float = 0.0  # peak resident set size (MB)

    # Disk I/O (Linux only, from /proc/self/io)
    read_bytes: int = 0
    write_bytes: int = 0

    # Outcome
    success: bool = False
    error: str = ""

    # Schema summary (filled by caller)
    classes_found: int = 0
    properties_found: int = 0
    triples_count: int = 0

    # Output files
    output_files: dict[str, str] = field(default_factory=dict)
    output_sizes_mb: dict[str, float] = field(default_factory=dict)

    # Extra — arbitrary key-value pairs for ad-hoc metrics
    extra: dict[str, Any] = field(default_factory=dict)

    def add_extra(self, key: str, value: Any) -> None:
        """Store an additional metric."""
        self.extra[key] = value


def _read_proc_io() -> dict[str, int]:
    """Read /proc/self/io for disk I/O counters."""
    result: dict[str, int] = {}
    try:
        with open("/proc/self/io", encoding="utf-8") as f:
            for line in f:
                key, _, val = line.partition(":")
                result[key.strip()] = int(val.strip())
    except OSError:
        pass
    return result


def _get_rusage() -> tuple[float, float, float]:
    """Return (user_time_s, system_time_s, max_rss_mb)."""
    r = resource.getrusage(resource.RUSAGE_SELF)
    # ru_maxrss is in KB on Linux, bytes on macOS
    divisor = 1024 if platform.system() == "Linux" else 1048576
    return r.ru_utime, r.ru_stime, r.ru_maxrss / divisor


# ═══════════════════════════════════════════════════════════════════
# Collector — context-manager API
# ═══════════════════════════════════════════════════════════════════


class BenchmarkCollector:
    """Collects and persists benchmark data for mining runs.

    Usage::

        collector = BenchmarkCollector(output_dir)
        with collector.track("drugbank", method="local-mine") as run:
            # … do the actual mining …
            run.add_extra("index_size_mb", 420)
        # run metrics auto-saved to benchmarks.jsonl
    """

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.output_dir / "benchmarks.jsonl"
        self._machine = collect_machine_info()

    @property
    def machine_info(self) -> MachineInfo:
        """Return the static machine info."""
        return self._machine

    @contextmanager
    def track(
        self,
        dataset: str,
        method: str = "unknown",
        endpoint: str = "",
    ) -> Generator[RunMetrics, None, None]:
        """Context manager that captures resource usage."""
        run = RunMetrics(
            dataset=dataset,
            method=method,
            endpoint=endpoint,
        )

        # Snapshot before
        run.started_at = datetime.now(timezone.utc).isoformat()
        t0 = time.monotonic()
        cpu0_user, cpu0_sys, _ = _get_rusage()
        io0 = _read_proc_io()

        try:
            yield run
            run.success = True
        except Exception as exc:
            run.success = False
            run.error = str(exc)
            raise
        finally:
            # Snapshot after
            t1 = time.monotonic()
            cpu1_user, cpu1_sys, peak_rss = _get_rusage()
            io1 = _read_proc_io()

            run.finished_at = (
                datetime.now(timezone.utc).isoformat()
            )
            run.wall_time_s = round(t1 - t0, 3)
            run.cpu_user_s = round(cpu1_user - cpu0_user, 3)
            run.cpu_system_s = round(cpu1_sys - cpu0_sys, 3)
            run.peak_rss_mb = round(peak_rss, 2)
            run.read_bytes = (
                io1.get("read_bytes", 0) - io0.get("read_bytes", 0)
            )
            run.write_bytes = (
                io1.get("write_bytes", 0)
                - io0.get("write_bytes", 0)
            )

            # Measure output file sizes
            for label, path_str in run.output_files.items():
                p = Path(path_str)
                if p.exists():
                    size_mb = p.stat().st_size / 1048576
                    run.output_sizes_mb[label] = round(size_mb, 3)

            # Persist
            self._flush(run)

    def _flush(self, run: RunMetrics) -> None:
        """Append one run record to the JSONL file."""
        record = {
            "machine": asdict(self._machine),
            "run": asdict(run),
        }
        try:
            with open(
                self.jsonl_path, "a", encoding="utf-8",
            ) as f:
                f.write(json.dumps(record, default=str))
                f.write("\n")
            logger.info(
                "Benchmark appended → %s (%s, %.1fs)",
                self.jsonl_path, run.dataset, run.wall_time_s,
            )
        except OSError as exc:
            logger.warning(
                "Failed to write benchmark: %s", exc,
            )

    def write_summary_csv(self) -> Path:
        """Read benchmarks.jsonl and produce a summary CSV.

        Returns the path to the generated CSV file.
        """
        csv_path = self.output_dir / "benchmarks_summary.csv"
        records: list[dict[str, Any]] = []

        try:
            with open(
                self.jsonl_path, encoding="utf-8",
            ) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    flat: dict[str, Any] = {}
                    # Flatten machine info (prefix with m_)
                    for k, v in obj.get("machine", {}).items():
                        flat[f"m_{k}"] = v
                    # Flatten run metrics (prefix with r_)
                    for k, v in obj.get("run", {}).items():
                        if isinstance(v, dict):
                            for k2, v2 in v.items():
                                flat[f"r_{k}_{k2}"] = v2
                        else:
                            flat[f"r_{k}"] = v
                    records.append(flat)
        except FileNotFoundError:
            logger.warning("No benchmarks.jsonl found")
            return csv_path

        if not records:
            return csv_path

        # Write CSV
        import csv
        all_keys = list(dict.fromkeys(
            k for r in records for k in r
        ))
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(records)

        logger.info(
            "Benchmark summary → %s (%d records)",
            csv_path, len(records),
        )
        return csv_path
