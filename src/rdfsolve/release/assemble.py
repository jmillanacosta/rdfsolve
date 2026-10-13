"""Join the runs of one corpus into one run directory, for one release.

assemble_runs joins the chunked runs of one frozen corpus, which share the code commit and the
run options. assemble_selected joins dataset folders that runs of different code mined, each
dataset from its own run, with one registry for the release and each dataset's commit recorded.

A release is built from one run directory. For assemble_runs, the runs must share the code
commit, the registry, the identity overrides, the SSSOM sources and the run options (their
output directory, source selection, port and resume directory differ). In both, each dataset
comes from one run, the dataset folders are copied and assembly.json records the run of each
dataset.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

__all__ = ["assemble_runs", "assemble_selected"]

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


def assemble_selected(
    selection: Mapping[str, str | Path],
    output_dir: str | Path,
    registry: Mapping[str, str | Path],
    *,
    rewritten: Mapping[str, str | Path] | None = None,
    rewritten_by_commit: str | None = None,
    code_commit: str | None = None,
    layers: Sequence[tuple[Mapping[str, str | Path], str | None]] = (),
) -> Path:
    """Join dataset folders mined by different runs, with different code, into one release.

    *selection* maps each dataset to its folder in a run (the run is the folder's parent).
    *registry* gives the files the release shares (sources.yaml, identity_overrides.yaml,
    sssom_sources.yaml): one registry for the whole release, whatever each run used.
    *rewritten* maps a dataset to a folder of files that a later step wrote again (by
    *rewritten_by_commit*): they replace the files of the same name. *code_commit* is the
    commit of the code that builds the release. *layers* are further such steps, applied in
    order after *rewritten*, each a mapping of datasets to folders and the commit of its code;
    a layer's files replace those at the same path in the dataset folder, in subfolders as
    well. The folders are copied; the runs are not changed. assembly.json records, per dataset, the run, the commit of the code that mined it
    (the run's code_commit.txt), the digests of the run's registry files and the files written
    again.
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    for name, path in registry.items():
        if name not in SHARED or name == "code_commit.txt":
            raise ValueError(f"Not a shared registry file: {name}")
        shutil.copy2(Path(path), output / name)
    if code_commit:
        (output / "code_commit.txt").write_text(code_commit + "\n")
    rewritten = rewritten or {}
    records: dict[str, dict[str, Any]] = {}
    for dataset, folder in sorted(selection.items()):
        source = Path(folder)
        run = source.parent
        if not source.is_dir():
            raise ValueError(f"No dataset folder for {dataset}: {source}")
        shutil.copytree(source, output / dataset, symlinks=False)
        rewrites: list[dict[str, Any]] = []
        for folders, layer_commit in [(rewritten, rewritten_by_commit), *layers]:
            if dataset not in folders:
                continue
            layer = Path(folders[dataset])
            files = []
            for path in sorted(layer.rglob("*")):
                if path.is_file():
                    relative = path.relative_to(layer)
                    (output / dataset / relative).parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, output / dataset / relative)
                    files.append(str(relative))
            rewrites.append(
                {"commit": layer_commit, "folder": str(layer.resolve()), "files": files}
            )
        replaced = rewrites[0]["files"] if rewrites and dataset in rewritten else []
        commit = run / "code_commit.txt"
        records[dataset] = {
            "run": str(run.resolve()),
            "code_commit": commit.read_text().strip() if commit.exists() else None,
            "registry": {name: _digest(run / name) for name in SHARED if name != "code_commit.txt"},
            "rewritten_files": replaced,
            "rewritten_by_commit": rewritten_by_commit if replaced else None,
            "rewrites": rewrites,
        }
    record = {
        "code_commit": code_commit,
        "registry": {name: _digest(output / name) for name in registry},
        "datasets": records,
    }
    (output / "assembly.json").write_text(json.dumps(record, indent=2))
    return output
