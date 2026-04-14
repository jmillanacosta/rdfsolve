#!/bin/bash
# slurm_single_source.sh — Run pipeline for one or more named datasets
#
# Usage:
#   export BASE=/trinity/home/$USER/rdfsolve
#   sbatch scripts/slurm_single_source.sh cellosaurus
#   sbatch scripts/slurm_single_source.sh chebi rdfportal.chebi

#SBATCH --job-name=rdfsolve
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=400G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

if [[ $# -eq 0 ]]; then
    echo "Usage: sbatch slurm_single_source.sh <name1> [name2 ...]"
    exit 1
fi

IFS='|' FILTER="^($(echo "$*" | tr ' ' '|'))$"
LABEL="$(echo "$*" | tr ' ' '_')"

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

_notify "${LABEL} started" "Job ${SLURM_JOB_ID} on $(hostname) — ${LABEL}"

bash "${REPO}/scripts/run_pipeline_hpc.sh" \
    --skip-remote \
    --filter "${FILTER}" \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --chunk-size  10000
