"""Join the chunked runs of one frozen corpus into one run directory, for one release.

Local sources run in chunks that can be resumed; each chunk is its own run directory. A release
is built from one run directory. The runs must share the code commit, the registry, the identity
overrides, the SSSOM sources and the run options (their output directory, source selection, port and resume
directory differ). Each dataset must come from one run. The dataset folders are copied, the per-stage
results are merged, and assembly.json records the run of each dataset.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import yaml

__all__ = ["assemble_runs"]

SHARED = ("code_commit.txt", "sources.yaml", "identity_overrides.yaml", "sssom_sources.yaml")
PER_RUN_OPTIONS = ("output_dir", "selected_sources", "base_port", "resume_from")  # set per job
STATE_ORDER = ("complete", "partial", "failed")


def _digest(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def _options(run: Path) -> dict[str, Any]:
    path = run / "pipeline_config.yaml"
    config = yaml.safe_load(path.read_text()) if path.exists() else {}
    return {k: v for k, v in (config or {}).items() if k not in PER_RUN_OPTIONS}


def _datasets(run: Path) -> list[str]:
    return sorted(p.name for p in run.iterdir() if p.is_dir() and not p.name.startswith("."))


def _merge_results(parts: list[dict[str, Any]]) -> dict[str, Any]:
    """Join the result records of one pipeline stage: lists are joined, states take the worst."""
    merged: dict[str, Any] = {}
    for part in parts:
        for key, value in part.items():
            if isinstance(value, list):
                merged.setdefault(key, [])
                merged[key] += [v for v in value if v not in merged[key]]
            elif key == "state":
                states = [merged.get("state", "complete"), value]
                merged["state"] = max(
                    states, key=lambda s: STATE_ORDER.index(s) if s in STATE_ORDER else 0
                )
            elif key == "success":
                merged["success"] = merged.get("success", True) and bool(value)
            elif isinstance(value, (int, float)) and key.endswith("seconds"):
                merged[key] = merged.get(key, 0) + value
            else:
                merged.setdefault(key, value)
    return merged


def assemble_runs(run_dirs: list[str | Path], output_dir: str | Path) -> Path:
    """Join *run_dirs* into the new directory *output_dir* and return it."""
    runs = [Path(r) for r in run_dirs]
    output = Path(output_dir)
    first = runs[0]
    for name in SHARED:
        digests = {run.name: _digest(run / name) for run in runs}
        if len(set(digests.values())) > 1:
            label = "code commit" if name == "code_commit.txt" else name
            raise ValueError(f"The runs differ in their {label}: {digests}")
    options = {run.name: _options(run) for run in runs}
    if any(o != options[first.name] for o in options.values()):
        raise ValueError(f"The runs differ in their run options: {sorted(options)}")
    dataset_runs: dict[str, str] = {}
    for run in runs:
        for dataset in _datasets(run):
            if dataset in dataset_runs:
                raise ValueError(
                    f"{dataset} is in more than one run: {dataset_runs[dataset]}, {run.name}"
                )
            dataset_runs[dataset] = run.name
    output.mkdir(parents=True, exist_ok=False)
    for name in (*SHARED, "environment.txt"):
        if (first / name).exists():
            shutil.copy2(first / name, output / name)
    by_name = {run.name: run for run in runs}
    for dataset, run_name in sorted(dataset_runs.items()):
        shutil.copytree(by_name[run_name] / dataset, output / dataset, symlinks=False)
    for results in sorted({p.name for run in runs for p in run.glob("pipeline_results_*.json")}):
        stages: dict[str, list[dict[str, Any]]] = {}
        totals = 0.0
        for run in runs:
            path = run / results
            if not path.exists():
                continue
            data = json.loads(path.read_text())
            for stage, record in data.items():
                if isinstance(record, dict):
                    stages.setdefault(stage, []).append(record)
                elif isinstance(record, (int, float)):
                    totals += record
        merged: dict[str, Any] = {stage: _merge_results(parts) for stage, parts in stages.items()}
        merged["total_elapsed_seconds"] = totals
        (output / results).write_text(json.dumps(merged, indent=2))
    config = (
        yaml.safe_load((first / "pipeline_config.yaml").read_text())
        if (first / "pipeline_config.yaml").exists()
        else {}
    )
    config = dict(config or {})
    config["output_dir"] = str(output)
    config["selected_sources"] = sorted(
        {
            s
            for run in runs
            for s in (yaml.safe_load((run / "pipeline_config.yaml").read_text()) or {}).get(
                "selected_sources", []
            )
        }
    )
    (output / "pipeline_config.yaml").write_text(yaml.safe_dump(config, sort_keys=True))
    record = {
        "runs": [
            {"name": run.name, "path": str(run.resolve()), "datasets": _datasets(run)}
            for run in runs
        ],
        "code_commit": (first / "code_commit.txt").read_text().strip()
        if (first / "code_commit.txt").exists()
        else None,
        "dataset_runs": dataset_runs,
    }
    (output / "assembly.json").write_text(json.dumps(record, indent=2))
    return output
