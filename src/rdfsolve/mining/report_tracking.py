"""Report tracking and analytics for schema mining."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rdfsolve.models import MiningReport, PhaseReport, QueryStats

logger = logging.getLogger(__name__)

__all__ = ["ReportCollector"]


class ReportCollector:
    """Accumulates analytics during a mining run.

    Writes the JSON report to *report_path* after each phase so
    partial data is persisted even if the process crashes.

    Also captures resource-usage snapshots (CPU, memory, disk I/O)
    at init and finalise time so that every report is self-contained.
    """

    def __init__(
        self,
        report: MiningReport,
        report_path: Path | None = None,
    ) -> None:
        """Set up the collector with *report* and optional *report_path*."""
        self._report = report
        self._path = report_path

        # Resource-usage snapshots (populated in _snapshot_start)
        self._t0: float = 0.0
        self._cpu0_user: float = 0.0
        self._cpu0_sys: float = 0.0
        self._io0: dict[str, int] = {}
        self._snapshot_start()

    # Resource snapshots

    @staticmethod
    def _read_proc_io() -> dict[str, int]:
        result: dict[str, int] = {}
        try:
            with open("/proc/self/io", encoding="utf-8") as f:
                for line in f:
                    key, _, val = line.partition(":")
                    result[key.strip()] = int(val.strip())
        except OSError:
            pass
        return result

    @staticmethod
    def _get_rusage() -> tuple[float, float, float]:
        import platform as _platform
        import resource as _resource

        r = _resource.getrusage(_resource.RUSAGE_SELF)
        div = 1024 if _platform.system() == "Linux" else 1048576
        return r.ru_utime, r.ru_stime, r.ru_maxrss / div

    def _snapshot_start(self) -> None:
        import time as _time

        self._t0 = _time.monotonic()
        self._cpu0_user, self._cpu0_sys, _ = self._get_rusage()
        self._io0 = self._read_proc_io()

    def _collect_machine_info(self) -> dict[str, Any]:
        """Gather static machine info (lightweight)."""
        import platform as _platform

        info: dict[str, Any] = {
            "hostname": _platform.node(),
            "os_name": _platform.system(),
            "os_release": _platform.release(),
            "architecture": _platform.machine(),
            "cpu_model": _platform.processor() or "",
            "cpu_count_logical": os.cpu_count() or 0,
            "python_version": _platform.python_version(),
        }
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("model name"):
                        info["cpu_model"] = line.split(":", 1)[1].strip()
                        break
        except OSError:
            pass
        try:
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        kb = int(line.split()[1])
                        info["ram_total_gb"] = round(
                            kb / 1048576,
                            2,
                        )
                        break
        except OSError:
            pass
        return info

    def _collect_resource_usage(self) -> dict[str, Any]:
        """Capture delta resource usage since init."""
        import time as _time

        t1 = _time.monotonic()
        cpu1_user, cpu1_sys, peak_rss = self._get_rusage()
        io1 = self._read_proc_io()
        return {
            "wall_time_s": round(t1 - self._t0, 3),
            "cpu_user_s": round(cpu1_user - self._cpu0_user, 3),
            "cpu_system_s": round(cpu1_sys - self._cpu0_sys, 3),
            "peak_rss_mb": round(peak_rss, 2),
            "read_bytes": (io1.get("read_bytes", 0) - self._io0.get("read_bytes", 0)),
            "write_bytes": (io1.get("write_bytes", 0) - self._io0.get("write_bytes", 0)),
        }

    # Query tracking

    def record_query(
        self,
        purpose: str,
        duration_s: float,
        success: bool = True,
    ) -> None:
        """Record one query execution."""
        stats = self._report.query_stats.setdefault(
            purpose,
            QueryStats(),
        )
        stats.sent += 1
        stats.total_time_s += duration_s
        self._report.total_queries_sent += 1
        if not success:
            stats.failed += 1
            self._report.total_queries_failed += 1

    _MAX_DROPPED_SAMPLES: int = 20

    def record_dropped_uri(self, sample: str) -> None:
        """Record a pattern dropped due to an invalid URI value.

        Increments the counter and keeps the first few examples so
        the report gives actionable debugging info.
        """
        self._report.dropped_invalid_uris += 1
        if len(self._report.dropped_invalid_uri_samples) < self._MAX_DROPPED_SAMPLES:
            self._report.dropped_invalid_uri_samples.append(sample)

    # Phase tracking

    def start_phase(self, name: str) -> PhaseReport:
        """Start a new phase and return its report object."""
        phase = PhaseReport(
            name=name,
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        self._report.phases.append(phase)
        return phase

    def finish_phase(
        self,
        phase: PhaseReport,
        items: int = 0,
        error: str | None = None,
    ) -> None:
        """Mark a phase as finished and flush the report."""
        now = datetime.now(timezone.utc)
        phase.finished_at = now.isoformat()
        if phase.started_at:
            started = datetime.fromisoformat(phase.started_at)
            phase.duration_s = round(
                (now - started).total_seconds(),
                3,
            )
        phase.items_discovered = items
        phase.error = error
        self.flush()

    def set_abort_reason(self, reason: str) -> None:
        """Record why mining was cut short and flush."""
        self._report.abort_reason = reason
        self.flush()

    def finalise(
        self,
        pattern_count: int,
        class_count: int,
        property_count: int,
        uris_labelled: int,
    ) -> MiningReport:
        """Set final summary fields and flush."""
        r = self._report
        r.finished_at = datetime.now(
            timezone.utc,
        ).isoformat()
        if r.started_at:
            started = datetime.fromisoformat(r.started_at)
            finished = datetime.fromisoformat(r.finished_at)
            r.total_duration_s = round(
                (finished - started).total_seconds(),
                3,
            )
        r.pattern_count = pattern_count
        r.class_count = class_count
        r.property_count = property_count
        r.unique_uris_labelled = uris_labelled

        # Embed machine info and resource usage
        r.machine = self._collect_machine_info()
        r.benchmark = self._collect_resource_usage()

        self.flush()
        return r

    # I/O

    def flush(self) -> None:
        """Write current state to disk (if a path was given)."""
        if self._path is None:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(
                    self._report.model_dump(),
                    indent=2,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("Could not write report: %s", exc)

    @property
    def report(self) -> MiningReport:
        """Return the accumulated :class:`MiningReport`."""
        return self._report
