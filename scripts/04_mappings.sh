#!/bin/bash
#SBATCH --job-name=04-mappings
#SBATCH --partition=defq
#SBATCH --time=24:00:00
#SBATCH --mem=128G
#SBATCH --cpus-per-task=16
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

# =============================================================================
# STEP 04: LSLOD Mapping Pipeline
# =============================================================================
# Generate cross-dataset mappings from mined schemas
# Stages:
#   1. Load external mappings
#   2. Extract cross-references with classes
#   3. Infer class mappings
#   4. Consolidate and run semra
# =============================================================================

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-$(pwd)/..}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output_$(date +%Y-%m-%d)}"

export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"

mkdir -p "$RDFSOLVE_BASE/logs" "$OUTPUT_DIR"

echo "=========================================="
echo "RDFSolve Step 04: Mapping Pipeline"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "CPUs: ${SLURM_CPUS_PER_TASK:-$(nproc)}"
echo "Memory: ${SLURM_MEM_PER_NODE:-unknown}"
echo "Output: $OUTPUT_DIR"
echo "=========================================="

source "$VENV_PATH/bin/activate"

cd "$RDFSOLVE_REPO"

# Stage 1: Load external mappings
echo ""
echo "=== Stage 1: External Mappings ==="
python scripts/mappings_01_external.py

# Stage 2: Extract cross-references WITH classes
echo ""
echo "=== Stage 2: Cross-References + Classes ==="
python scripts/mappings_02_crossrefs.py

# Stage 3: Infer class mappings
echo ""
echo "=== Stage 3: Class Mappings ==="
python scripts/mappings_03_class_mappings.py

# Stage 4: Consolidate and run semra
echo ""
echo "=== Stage 4: Consolidation + Semra ==="
python scripts/mappings_04_consolidate.py

echo ""
echo "=========================================="
echo "Step 04 complete: $(date)"
echo "=========================================="
