#!/bin/bash
# =============================================================================
# slurm_aopwikirdf_test.sh - SLURM test job for aopwikirdf
# =============================================================================
#
# Submit with:
#   sbatch scripts/slurm_aopwikirdf_test.sh
#
# Monitor with:
#   squeue -u $USER
#   tail -f /trinity/home/p70085013/rdfsolve/logs/aopwikirdf-test-<JOBID>.out
#
# =============================================================================

#SBATCH --job-name=rdfsolve-aopwikirdf-test
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --output=/trinity/home/p70085013/rdfsolve/logs/aopwikirdf-test-%j.out
#SBATCH --error=/trinity/home/p70085013/rdfsolve/logs/aopwikirdf-test-%j.err

set -euo pipefail

# ── Notifications via ntfy.sh ─────────────────────────────────────────────
# SLURM --mail-type is not functional on this cluster (MailDomain = null).
# ntfy.sh is a free push notification service — no account needed.
# Subscribe on iPhone: install "ntfy" from the App Store, add topic below.
export NTFY_TOPIC="rdfsolve-p70085013-alerts"

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

# Fires on any non-zero exit: explicit errors, set -e failures, OOM, timeout
_on_exit() {
    local code=$?
    
    local logfile="/trinity/home/p70085013/rdfsolve/logs/aopwikirdf-test-${SLURM_JOB_ID}.out"
    local tail_msg=""
    if [[ -f "${logfile}" ]]; then
        tail_msg="$(tail -n 200 "${logfile}")"
    else
        tail_msg="(Log file not found at ${logfile})"
    fi

    if [[ ${code} -ne 0 ]]; then
        _notify "FAIL: Pipeline FAILED" \
            "Job ${SLURM_JOB_ID} (aopwikirdf) failed with exit ${code} on $(hostname) at $(date)
    
Last lines of log:
${tail_msg}" \
            high
    else
        _notify "OK: Pipeline done" \
            "Job ${SLURM_JOB_ID} (aopwikirdf) finished successfully on $(hostname) at $(date)
            
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
echo "  Dataset:   aopwikirdf (test)"
echo "  Start:     $(date)"
echo "  Data dir:  ${DATA_DIR}"
echo "  Output:    ${OUTPUT_DIR}"
echo "================================================================"
echo ""

_notify "Pipeline started" \
    "Job ${SLURM_JOB_ID} (aopwikirdf) running on $(hostname)"

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
# uv is the same package manager used in the Docker image (see
# docker/Dockerfile.pipeline). It is dramatically faster than pip for
# installing rdfsolve's dependency tree, and it creates an isolated venv
# without touching the system Python.
#
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

# Install rdfsolve (editable) + qlever CLI — mirrors exactly what
# docker/Dockerfile.pipeline does with `uv pip install --system`.
# Editable (-e) means code changes in ${REPO}/src/ take effect immediately
# without reinstalling.
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
# run_pipeline_hpc.sh is the Singularity-native version of run_pipeline.sh.
# Key flags used here:
#   --dataset aopwikirdf  → process only this one source (test scope)
#   --skip-remote         → skip SPARQL endpoint mining; test local indexing only
#   --data-dir            → where QLever workdirs + index files are written
#   --output-dir          → where mined schemas/JSON-LD outputs go
#   --base-port 7019      → QLever server port (one dataset = one port needed)
#   --chunk-size 10000    → SPARQL result page size for schema mining queries
#
# The script will:
#   1. Pull qlever.sif from Docker Hub (once, ~1 GB, saved to DATA_DIR)
#   2. Generate a Qleverfile for aopwikirdf
#   3. Download the 3 TTL files (~few MB from GitHub)
#   4. Index them with QLever inside Singularity
#   5. Start QLever server, run schema mining, stop server
#   6. Delete the raw TTL files (index stays for the final LSOLD step)
#   7. Copy results to RESULTS_DIR

export DATA_DIR OUTPUT_DIR RESULTS_DIR
export SINGULARITY_IMAGE="${DATA_DIR}/qlever.sif"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --dataset    aopwikirdf \
    --skip-remote \
    --data-dir   "${DATA_DIR}" \
    --output-dir "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port  7019 \
    --chunk-size 10000

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
