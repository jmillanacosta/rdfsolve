#!/bin/bash
# =============================================================================
# slurm_mine_only_hpc.sh - SLURM job: mining step onwards (no download/index)
# =============================================================================
#
# Use this when RDF downloads and QLever indexes already exist on the HPC
# (i.e. the full pipeline was run before and .index.done sentinels are present).
#
# This job:
#   - Skips Steps 1-2 (remote VoID discovery + remote mining)
#   - Skips the download + index phases of Steps 3-4 (cached via .index.done)
#   - Runs the mine phase for each locally-indexed dataset (Step 4: mine)
#   - Runs Steps 4b-10 (schema selection, mappings, LSLOD, class derivation)
#
# Submit with:
#   sbatch scripts/slurm_mine_only_hpc.sh
#
# Monitor with:
#   squeue -u $USER
#   tail -f /trinity/home/p70085013/rdfsolve/logs/mine-only-<JOBID>.out
#
# =============================================================================

#SBATCH --job-name=rdfsolve-mine-only
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=400G
#SBATCH --output=/trinity/home/p70085013/rdfsolve/logs/mine-only-%j.out
#SBATCH --error=/trinity/home/p70085013/rdfsolve/logs/mine-only-%j.err

set -euo pipefail

# ── Notifications via ntfy.sh ─────────────────────────────────────────────
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

_on_exit() {
    local code=$?
    local logfile="/trinity/home/p70085013/rdfsolve/logs/mine-only-${SLURM_JOB_ID}.out"
    local tail_msg=""
    if [[ -f "${logfile}" ]]; then
        tail_msg="$(tail -n 200 "${logfile}")"
    else
        tail_msg="(Log file not found at ${logfile})"
    fi

    if [[ ${code} -ne 0 ]]; then
        _notify "FAIL: Mine-only FAILED" \
            "Job ${SLURM_JOB_ID} failed with exit ${code} on $(hostname) at $(date)

Last lines of log:
${tail_msg}" \
            high
    else
        _notify "OK: Mine-only done" \
            "Job ${SLURM_JOB_ID} finished successfully on $(hostname) at $(date)

Last lines of log:
${tail_msg}"
    fi
}
trap '_on_exit' EXIT

# ── Storage layout ────────────────────────────────────────────────────────
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
echo "  Mode:      mine-only (skip remote + skip download/index)"
echo "  Start:     $(date)"
echo "  Data dir:  ${DATA_DIR}"
echo "  Output:    ${OUTPUT_DIR}"
echo "================================================================"
echo ""

_notify "Mine-only job started" \
    "Job ${SLURM_JOB_ID} running on $(hostname) — skipping remote discovery and download/index"

# ── Repo: update ──────────────────────────────────────────────────────────
if [[ ! -d "${REPO}/.git" ]]; then
    echo "Cloning repository to ${REPO} …"
    git clone https://github.com/jmillanacosta/rdfsolve.git "${REPO}"
    git -C "${REPO}" checkout mcp
else
    echo "Repo present — pulling latest commits …"
    git -C "${REPO}" pull --ff-only \
        || echo "  (pull skipped — local uncommitted changes detected)"
fi
echo "  Repo at: $(git -C "${REPO}" log --oneline -1)"
echo ""

# ── Python + uv ───────────────────────────────────────────────────────────
module load Python/3.12.3-GCCcore-13.3.0

UV="${HOME}/.local/bin/uv"
if [[ ! -x "${UV}" ]]; then
    echo "Installing uv …"
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="${HOME}/.local/bin:${HOME}/local/bin:${PATH}"
echo "uv: $(uv --version)"

VENV="${BASE}/venv"
if [[ ! -d "${VENV}" ]]; then
    echo "Creating venv at ${VENV} …"
    uv venv "${VENV}" --python python3
fi
source "${VENV}/bin/activate"

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
echo ""

# ── Run pipeline: skip remote steps 1-2, keep local steps 3-10 ───────────
# --skip-remote  → skip Steps 1 (remote VoID discovery) and 2 (remote mining)
# No --skip-local → local block runs, but .index.done sentinels mean
#                   download + index are already cached and skipped automatically.

export DATA_DIR OUTPUT_DIR RESULTS_DIR
export SINGULARITY_IMAGE="${DATA_DIR}/qlever.sif"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --skip-remote \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --chunk-size  10000

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
