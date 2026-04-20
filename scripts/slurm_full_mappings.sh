#!/bin/bash
# slurm_full_mappings.sh — Complete mappings workflow from scratch
#
# Runs the FULL mappings pipeline including re-seeding:
#   Step 0:    Ensure QLever Singularity image
#   Step 5:    Schema selection
#   Step 6:    Seed SSSOM mappings (re-download + parse with curie_map)
#   Step 7:    Seed SeMRA mappings
#   Step 8:    Start QLever + instance matching
#   Step 9:    Class derivation from semra/ + sssom/ entity-level files
#   Step 10:   Stop QLever
#   Step 11:   Inference expansion
#   Step 12:   Build graphs → Parquet
#   Step 13:   Collect results
#
# Skips: remote discovery (step 1), remote mining (step 2),
#        local download/index/mine (steps 3-4).
#
# Usage:
#   export BASE=/trinity/home/$USER/rdfsolve
#   sbatch scripts/slurm_full_mappings.sh

#SBATCH --job-name=rdfsolve
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=500G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

PIPELINE="${REPO}/scripts/run_pipeline_hpc.sh"

_notify "Full mappings" "Starting full mappings workflow (job ${SLURM_JOB_ID})"

bash "${PIPELINE}" \
    --data-dir    "${DATA_DIR}" \
    --output-dir  "${OUTPUT_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --base-port   7019 \
    --timeout     1000 \
    --chunk-size  50000 \
    --skip-remote \
    --skip-mining \
    || { _notify "Full mappings FAILED" "Job ${SLURM_JOB_ID} failed" high; exit 1; }

_notify "Full mappings done" "Pipeline completed (job ${SLURM_JOB_ID})"
