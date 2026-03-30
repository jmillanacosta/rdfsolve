#!/bin/bash
# =============================================================================
# slurm_full_pipeline_hpc.sh - SLURM job for the full rdfsolve pipeline
# =============================================================================
#
# Submit with:
#   sbatch scripts/slurm_full_pipeline_hpc.sh
#
# Monitor with:
#   squeue -u $USER
#   tail -f /trinity/home/p70085013/rdfsolve/logs/full-pipeline-<JOBID>.out
#
# =============================================================================

#SBATCH --job-name=rdfsolve-full-pipeline
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --output=/trinity/home/p70085013/rdfsolve/logs/full-pipeline-%j.out
#SBATCH --error=/trinity/home/p70085013/rdfsolve/logs/full-pipeline-%j.err

set -euo pipefail

# ── Notifications via ntfy.sh ─────────────────────────────────────────────
# ntfy.sh is a free push notification service — no account needed.
# Subscribe on iPhone: install "ntfy" from the App Store, add topic below.
export NTFY_TOPIC="rdfsolve-alerts"

_notify() {
    local title="$1" msg="$2" priority="${3:-default}"
    curl -s \
        -H "Title: ${title}" \
        -H "Priority: ${priority}" \
        -H "Tags: slurm" \
        -d "${msg}" \
        "https://ntfy.sh/${NTFY_TOPIC}" >/dev/null || true
}
export -f _notify

# Parse a report JSON and send a formatted ntfy notification.
# Usage: _notify_report <title> <json_file> [extra_msg]
_notify_report() {
    local title="$1" json="$2" extra="${3:-}"
    if [[ ! -f "${json}" ]]; then
        if command -v _notify >/dev/null 2>&1; then
            _notify "${title}" "${extra}(no report file found at ${json})" "default"
        fi
        return
    fi
    local msg
    msg=$(python3 - <<PYEOF
import json, sys
with open("${json}") as f:
    r = json.load(f)
lines = []
ds   = r.get("dataset") or r.get("dataset_name", "?")
src  = r.get("source", "")
ep   = r.get("endpoint") or r.get("endpoint_url", "")
lines.append(f"Dataset: {ds}" + (f" ({src})" if src else ""))
if ep:
    lines.append(f"Endpoint: {ep}")
if "graphs_found" in r:
    lines.append(f"Graphs: {r['graphs_found']}  |  Partitions: {r['partitions_found']}")
if "total_duration_s" in r:
    lines.append(f"Duration: {r['total_duration_s']:.1f}s")
if "total_queries_sent" in r:
    failed = r.get("total_queries_failed", 0)
    lines.append(f"Queries: {r['total_queries_sent']} sent, {failed} failed")
if "phases" in r:
    for ph in r["phases"]:
        items = ph.get("items_discovered", "?")
        err   = f" WARN {ph['error']}" if ph.get("error") else ""
        lines.append(f"  {ph['name']}: {items} items ({ph['duration_s']:.2f}s){err}")
bm = r.get("benchmark", {})
if bm:
    lines.append(f"Wall time: {bm.get('wall_time_s','?')}s  |  Peak RSS: {bm.get('peak_rss_mb','?')} MB")
print("\n".join(lines))
PYEOF
    )
    local file_count
    file_count=$(find "$(dirname "${json}")" -type f 2>/dev/null | wc -l)
    local full_msg="${msg}
Files in dir: ${file_count}"
    [[ -n "${extra}" ]] && full_msg="${extra}
${full_msg}"
    _notify "${title}" "${full_msg}" "default"
    echo -e "\033[0;32m▸ ntfy: ${title}\033[0m"
}
export -f _notify_report

# Fires on any non-zero exit: explicit errors, set -e failures, OOM, timeout
_on_exit() {
    local code=$?
    
    local logfile="/trinity/home/p70085013/rdfsolve/logs/full-pipeline-${SLURM_JOB_ID}.out"
    local tail_msg=""
    if [[ -f "${logfile}" ]]; then
        tail_msg="$(tail -n 200 "${logfile}")"
    else
        tail_msg="(Log file not found at ${logfile})"
    fi

    if [[ ${code} -ne 0 ]]; then
        _notify "FAIL: Pipeline FAILED" \
            "Job ${SLURM_JOB_ID} (full-pipeline) failed with exit ${code} on $(hostname) at $(date)
            
Last lines of log:
${tail_msg}" \
            high
    else
        _notify "OK: Pipeline done" \
            "Job ${SLURM_JOB_ID} (full-pipeline) finished successfully on $(hostname) at $(date)
            
Last lines of log:
${tail_msg}"
    fi
}
trap '_on_exit' EXIT

# ── Storage layout ─────────────────────────────────────────────────────────
# /trinity/home/p70085013/ — home dir on TGX-HPC (scripts, venv, logs)
# /trinity/storage/        — shared storage
# For now DATA_DIR is in home
#
#   BASE/rdfsolve-2/   ← git repo (code, scripts, sources.yaml)
#   BASE/data/         ← RDF downloads + QLever index files
#   BASE/output/       ← mined schemas, VoID reports, benchmarks
#   BASE/results/      ← final collected results
#   BASE/logs/         ← SLURM stdout/stderr logs

BASE=/trinity/home/p70085013/rdfsolve
REPO="${BASE}/rdfsolve-2"
DATA_DIR="${BASE}/data"
OUTPUT_DIR="${BASE}/output"
RESULTS_DIR="${BASE}/results"
LOG_DIR="${BASE}/logs"

mkdir -p "${DATA_DIR}" "${OUTPUT_DIR}" "${RESULTS_DIR}" "${LOG_DIR}"

echo "================================================================"
echo "  Job:       ${SLURM_JOB_ID}"
echo "  Node:      $(hostname)"
echo "  Dataset:   ALL sources"
echo "  Start:     $(date)"
echo "  Data dir:  ${DATA_DIR}"
echo "  Output:    ${OUTPUT_DIR}"
echo "================================================================"
echo ""

_notify "Pipeline started" \
    "Job ${SLURM_JOB_ID} (full-pipeline) running on $(hostname)"

# ── Repo: clone or update ─────────────────────────────────────────────────
# The repo must be on /trinity/storage (not home) so all scripts,
# sources.yaml, and data/ subdirs are reachable during the job.
# We pull --ff-only so we never clobber local edits accidentally.
if [[ ! -d "${REPO}/.git" ]]; then
    echo "Cloning repository to ${REPO} …"
    git clone https://github.com/jmillanacosta/rdfsolve.git "${REPO}"
else
    echo "Repo present — pulling latest commits (fast-forward only) …"
    git -C "${REPO}" pull --ff-only \
        || echo "  (pull skipped — local uncommitted changes detected)"
fi
echo "  Repo at: $(git -C "${REPO}" log --oneline -1)"
echo ""

# ── Python + uv ──────────────────────────────────────────────────────────
# We load the HPC Python module first so uv has a base interpreter, then
# install uv itself into ~/.local/bin (user space, no root needed).
module load Python/3.12.3-GCCcore-13.3.0

UV="${HOME}/.local/bin/uv"
if [[ ! -x "${UV}" ]]; then
    echo "Installing uv (fast Python package manager) …"
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="${HOME}/.local/bin:${PATH}"
echo "uv: $(uv --version)"

# Create the virtual environment once; skip if it already exists.
# uv venv is equivalent to python -m venv but faster and uv-aware.
VENV="${BASE}/venv"
if [[ ! -d "${VENV}" ]]; then
    echo "Creating venv at ${VENV} …"
    uv venv "${VENV}" --python python3
fi
source "${VENV}/bin/activate"

# Install rdfsolve (editable) + qlever CLI
if ! python -c "import rdfsolve" 2>/dev/null; then
    echo "Installing rdfsolve …"
    uv pip install --no-cache -e "${REPO}"
fi
if ! python -c "import qlever" 2>/dev/null; then
    echo "Installing qlever CLI …"
    uv pip install --no-cache qlever
fi

echo "Python:   $(python --version)"
echo "rdfsolve: $(python -c 'import rdfsolve; print(rdfsolve.__version__)' 2>/dev/null || echo ok)"
echo "qlever:   $(qlever --version 2>/dev/null || echo ok)"
echo ""

# ── Run pipeline ──────────────────────────────────────────────────────────
# run_pipeline_hpc.sh
# Key flags:
#   --data-dir            → where QLever workdirs + index files are written
#   --output-dir          → where mined schemas/JSON-LD outputs go
#   --base-port 7019      → QLever server port base offset
#   --chunk-size 50000    → SPARQL result page size for schema mining queries
# Extra flags (e.g. --skip-remote) can be passed as sbatch trailing args.

export DATA_DIR OUTPUT_DIR RESULTS_DIR
export SINGULARITY_IMAGE="${DATA_DIR}/qlever.sif"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --data-dir   "${DATA_DIR}" \
    --output-dir "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port  7019 \
    --chunk-size 50000 \
    "$@"

# ── Validate output ───────────────────────────────────────────────────────
FILE_COUNT=$(find "${RESULTS_DIR}" -type f | wc -l)
if [[ "${FILE_COUNT}" -eq 0 ]]; then
    echo "ERROR: Pipeline finished but produced 0 files in results directory."
    exit 1
fi

echo ""
echo "================================================================"
echo "  Finished: $(date)"
echo "  Results:  ${RESULTS_DIR}"
echo "  Output files:"
find "${RESULTS_DIR}" -type f | head -20
echo "================================================================"
