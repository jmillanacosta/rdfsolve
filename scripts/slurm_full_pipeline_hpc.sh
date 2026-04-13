#!/bin/bash
# slurm_full_pipeline_hpc.sh — Full pipeline (all sources, all steps)
#
# Usage: sbatch scripts/slurm_full_pipeline_hpc.sh [extra flags for pipeline]

#SBATCH --job-name=rdfsolve
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --output=/trinity/home/p70085013/rdfsolve/logs/%x-%j.out
#SBATCH --error=/trinity/home/p70085013/rdfsolve/logs/%x-%j.err

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_slurm_common.sh"

_notify "Pipeline started" "Job ${SLURM_JOB_ID} on $(hostname) — full pipeline"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --chunk-size  50000 \
    "$@"
