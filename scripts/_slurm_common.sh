#!/usr/bin/env bash
# _slurm_common.sh — shared setup for all SLURM wrappers
# Source this file, don't run it directly.
#
# Provides: _notify, _on_exit trap, repo clone/pull, Python/uv/venv setup
# Requires: BASE (set by caller before sourcing), SLURM_JOB_ID, SLURM_JOB_NAME
# Sets:     REPO, DATA_DIR, OUTPUT_DIR, RESULTS_DIR, SINGULARITY_IMAGE
set -euo pipefail

if [[ -z "${BASE:-}" ]]; then
    echo "ERROR: BASE must be set before sourcing _slurm_common.sh" >&2
    exit 1
fi
REPO="${BASE}/rdfsolve-2"
DATA_DIR="${BASE}/data"
OUTPUT_DIR="${BASE}/output"
RESULTS_DIR="${BASE}/results"
LOG_DIR="${BASE}/logs"

mkdir -p "${DATA_DIR}" "${OUTPUT_DIR}" "${RESULTS_DIR}" "${LOG_DIR}"

# ── ntfy.sh notifications ────────────────────────────────────────
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

# ── Exit trap (fires on any non-zero exit) ───────────────────────
_on_exit() {
    local code=$?
    local logfile="${LOG_DIR}/${SLURM_JOB_NAME:-rdfsolve}-${SLURM_JOB_ID:-0}.out"
    local tail_msg="(no log)"
    [[ -f "${logfile}" ]] && tail_msg="$(tail -n 50 "${logfile}")"

    if [[ ${code} -ne 0 ]]; then
        _notify "FAIL: ${SLURM_JOB_NAME}" \
            "Job ${SLURM_JOB_ID} failed (exit ${code}) on $(hostname) at $(date)
${tail_msg}" high
    else
        _notify "OK: ${SLURM_JOB_NAME}" \
            "Job ${SLURM_JOB_ID} finished on $(hostname) at $(date)
${tail_msg}"
    fi
}
trap '_on_exit' EXIT

# ── Job info ──────────────────────────────────────────────────────
echo "Job:  ${SLURM_JOB_ID} (${SLURM_JOB_NAME}) on $(hostname)"
echo "Date: $(date)"
echo "Data: ${DATA_DIR}"
echo ""

# ── Repo: clone or pull ──────────────────────────────────────────
if [[ ! -d "${REPO}/.git" ]]; then
    git clone https://github.com/jmillanacosta/rdfsolve.git "${REPO}"
else
    git -C "${REPO}" pull --ff-only \
        || echo "(pull skipped — local changes)"
fi
echo "Repo: $(git -C "${REPO}" log --oneline -1)"

# ── Python + uv + venv ───────────────────────────────────────────
module load Python/3.12.3-GCCcore-13.3.0

UV="${HOME}/.local/bin/uv"
if [[ ! -x "${UV}" ]]; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="${HOME}/.local/bin:${PATH}"

VENV="${BASE}/venv"
if [[ ! -d "${VENV}" ]]; then
    uv venv "${VENV}" --python python3
fi
source "${VENV}/bin/activate"

if ! python -c "import rdfsolve" 2>/dev/null; then
    uv pip install --no-cache -e "${REPO}"
fi
if ! python -c "import qlever" 2>/dev/null; then
    uv pip install --no-cache qlever
fi

echo "Python: $(python --version)"
echo ""

# ── Exports for the pipeline script ──────────────────────────────
export DATA_DIR OUTPUT_DIR RESULTS_DIR
export SINGULARITY_IMAGE="${DATA_DIR}/qlever.sif"
