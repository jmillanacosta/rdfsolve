#!/bin/bash
# slurm_mine_and_map.sh — Mine missing local sources, then mappings for all
#
# Phase 1: Local-mine the 7 indexed sources that have no local schema
# Phase 2: Run mappings + inference + build graph for ALL schemas
#
# Usage:
#   export BASE=/trinity/home/$USER/rdfsolve
#   sbatch scripts/slurm_mine_and_map.sh

#SBATCH --job-name=rdfsolve
#SBATCH --time=0
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

BASE="${BASE:?Set BASE to your project root, e.g. export BASE=/trinity/home/\$USER/rdfsolve}"
source "${BASE}/rdfsolve-2/scripts/_slurm_common.sh"

PIPELINE="${REPO}/scripts/run_pipeline_hpc.sh"
COMMON="--data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} --results-dir ${RESULTS_DIR} --base-port 7019 --chunk-size 50000"

# ── Phase 1: Mine the 7 missing local sources ────────────────────
_notify "Phase 1: mining" "Mining 7 missing local sources"

MISSING=(
    diseaseontology
    globi
    hra-kg
    pathophenodb
    reactome
    tera
    ubergraph
)

DS_FLAGS=""
for ds in "${MISSING[@]}"; do DS_FLAGS+=" --dataset ${ds}"; done

bash ${PIPELINE} ${COMMON} \
    --skip-remote \
    --skip-mappings \
    ${DS_FLAGS} \
    || { _notify "Phase 1 FAILED" "Mining failed — aborting" high; exit 1; }

_notify "Phase 1 done" "Mining complete, starting mappings"

# ── Phase 2: Mappings + inference + graph for ALL schemas ─────────
_notify "Phase 2: mappings" "Running mappings + inference for all schemas"

bash ${PIPELINE} ${COMMON} \
    --skip-remote \
    --skip-mining \
    || { _notify "Phase 2 FAILED" "Mappings failed" high; exit 1; }

_notify "All done" "Mine + map pipeline completed"
