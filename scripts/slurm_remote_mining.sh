#!/bin/bash
# slurm_remote_mining.sh — Mine all UP remote SPARQL endpoints
#
# Usage: sbatch scripts/slurm_remote_mining.sh
#   export BASE=/trinity/home/$USER/rdfsolve; sbatch scripts/slurm_remote_mining.sh

#SBATCH --job-name=rdfsolve-remote
#SBATCH --time=2-00:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

_notify "Remote mining started" "Job ${SLURM_JOB_ID} on $(hostname) — remote endpoints only"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --skip-local \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --chunk-size  10000
