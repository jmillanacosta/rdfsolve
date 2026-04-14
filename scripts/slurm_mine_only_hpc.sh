#!/bin/bash
# slurm_mine_only_hpc.sh — Skip remote steps, re-mine locally indexed data
#
# Usage: sbatch scripts/slurm_mine_only_hpc.sh
#
# Set BASE to your project root before submitting, e.g.:
#   export BASE=/trinity/home/$USER/rdfsolve; sbatch scripts/slurm_mine_only_hpc.sh

#SBATCH --job-name=rdfsolve-mine-only
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=400G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

_notify "Mine-only started" "Job ${SLURM_JOB_ID} on $(hostname) — skip remote"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --skip-remote \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --chunk-size  10000
