#!/bin/bash
# slurm_local_mining.sh — Download + index + mine local sources (ttl, nq, etc.)
#
# Usage: sbatch scripts/slurm_local_mining.sh
#   export BASE=/trinity/home/$USER/rdfsolve; sbatch scripts/slurm_local_mining.sh

#SBATCH --job-name=rdfsolve-local
#SBATCH --time=2-00:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=400G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
#SBATCH --signal=USR1@120

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

_notify "Local mining started" "Job ${SLURM_JOB_ID} on $(hostname) — local download+index+mine"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --skip-remote \
    --skip-mappings \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --chunk-size  10000
