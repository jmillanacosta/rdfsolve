#!/bin/bash
# slurm_local_mapping.sh — Instance matching + class derivation for local datasets
#
# Starts each indexed local QLever dataset one at a time, runs
# instance-match seed and class derivation, then stops it.
# Skips download, indexing, and schema mining (--skip-mine).
#
# Run after slurm_local_mining.sh (indexes must exist).
#
# Usage:
#   export BASE=/trinity/home/$USER/rdfsolve
#   sbatch scripts/slurm_local_mapping.sh

#SBATCH --job-name=rdfsolve-local-map
#SBATCH --time=1-00:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
#SBATCH --signal=USR1@120

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

_notify "Local mapping started" "Job ${SLURM_JOB_ID} on $(hostname) — instance match + class derivation (local)"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --skip-remote \
    --skip-seeding \
    --skip-mine \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --timeout     1000 \
    --chunk-size  50000 \
    || { _notify "Local mapping FAILED" "Job ${SLURM_JOB_ID} failed" high; exit 1; }

_notify "Local mapping done" "Pipeline completed (job ${SLURM_JOB_ID})"
