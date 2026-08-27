#!/bin/bash
#SBATCH --job-name=05-analysis
#SBATCH --partition=defq
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

# =============================================================================
# STEP 05: Analysis + Visualization
# =============================================================================
# Generate cross-dataset analysis, graphs, and reports
# =============================================================================

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-$(pwd)/..}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output_$(date +%Y-%m-%d)}"

mkdir -p "$RDFSOLVE_BASE/logs" "$OUTPUT_DIR"

echo "=========================================="
echo "RDFSolve Step 05: Analysis"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "Output: $OUTPUT_DIR"
echo "=========================================="

source "$VENV_PATH/bin/activate"

# Run analysis pipeline
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --analysis-only \
    --output-dir "$OUTPUT_DIR"

echo ""
echo "=========================================="
echo "Step 05 complete: $(date)"
echo "=========================================="
echo ""
echo "All pipeline steps complete!"
echo "Final output: $OUTPUT_DIR"
echo "=========================================="
