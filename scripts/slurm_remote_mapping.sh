#!/bin/bash
# slurm_remote_mapping.sh — Instance matching + class derivation against remote endpoints
#
# Queries remote SPARQL endpoints directly (no local QLever).
# Throttled with inter-request delay to avoid overwhelming public endpoints.
#
# Run after slurm_remote_mining.sh and after sssom/semra seeding.
#
# Usage:
#   export BASE=/trinity/home/$USER/rdfsolve
#   sbatch scripts/slurm_remote_mapping.sh

#SBATCH --job-name=rdfsolve-remote-map
#SBATCH --time=1-00:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
#SBATCH --signal=USR1@120

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

_notify "Remote mapping started" "Job ${SLURM_JOB_ID} on $(hostname) — instance match + class derivation (remote)"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --skip-local \
    --skip-discovery \
    --skip-seeding \
    --skip-mining \
    --remote-mappings \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --timeout     300 \
    || { _notify "Remote mapping FAILED" "Job ${SLURM_JOB_ID} failed" high; exit 1; }

_notify "Remote mapping done" "Pipeline completed (job ${SLURM_JOB_ID})"
