#!/bin/bash
# slurm_mappings_only.sh — Run the mappings pipeline (Steps 5–10)
#
# Skips remote discovery, local mining, AND seeding (all already done).
# Instance matching and class derivation run per-dataset during Step 4.
#   Step 5–7: SKIPPED (seeding already done)
#   Step 8:  Inference expansion
#   Step 9:  Build graphs > Parquet
#   Step 10: Collect results
#
# Usage:
#   export BASE=/trinity/home/$USER/rdfsolve
#   sbatch scripts/slurm_mappings_only.sh

#SBATCH --job-name=rdfsolve
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

PIPELINE="${REPO}/scripts/run_pipeline_hpc.sh"

_notify "Mappings pipeline" "Starting mappings-only run (job ${SLURM_JOB_ID})"

bash "${PIPELINE}" \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --timeout     1000 \
    --chunk-size  50000 \
    --skip-remote \
    --skip-mining \
    --skip-seeding \
    || { _notify "Mappings FAILED" "Job ${SLURM_JOB_ID} failed" high; exit 1; }

_notify "Mappings done" "Pipeline completed (job ${SLURM_JOB_ID})"
