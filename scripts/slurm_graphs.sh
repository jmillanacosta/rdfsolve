#!/bin/bash
#SBATCH --job-name=rdfsolve-graphs
#SBATCH --partition=defq
#SBATCH --time=4:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --output=logs/graphs_%j.out
#SBATCH --error=logs/graphs_%j.err

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
SCHEMAS_DIR="${SCHEMAS_DIR:-$RDFSOLVE_BASE/output}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/results/graphs}"

mkdir -p "$RDFSOLVE_BASE/logs"

echo "=========================================="
echo "RDFSolve Graph Construction"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Schemas: $SCHEMAS_DIR"
echo "Output: $OUTPUT_DIR"
echo "=========================================="

source "$VENV_PATH/bin/activate"

python "$RDFSOLVE_REPO/scripts/build_graphs.py" \
    "$SCHEMAS_DIR" \
    --output "$OUTPUT_DIR"

echo "=========================================="
echo "Graph construction complete: $(date)"
echo "=========================================="
