#!/bin/bash
# slurm_full_pipeline_hpc.sh — Full pipeline (all sources, all steps)
#
# Usage:
#   export BASE=/trinity/home/$USER/rdfsolve
#   sbatch scripts/slurm_full_pipeline_hpc.sh [extra flags for pipeline]

#SBATCH --job-name=rdfsolve
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

_notify "Pipeline started" "Job ${SLURM_JOB_ID} on $(hostname) — full pipeline"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --chunk-size  50000 \
    "$@"
