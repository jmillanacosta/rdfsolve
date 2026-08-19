#!/bin/bash
#SBATCH --job-name=rdfsolve-inference
#SBATCH --partition=defq
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --output=logs/inference_%j.out
#SBATCH --error=logs/inference_%j.err

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
MAPPINGS_DIR="${MAPPINGS_DIR:-$RDFSOLVE_BASE/output/mappings}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output/mappings/inferenced}"

mkdir -p "$RDFSOLVE_BASE/logs"

echo "=========================================="
echo "RDFSolve Mapping Inference"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Mappings: $MAPPINGS_DIR"
echo "Output: $OUTPUT_DIR"
echo "=========================================="

source "$VENV_PATH/bin/activate"

python "$RDFSOLVE_REPO/scripts/infer_mappings.py" \
    "$MAPPINGS_DIR"/*.jsonld \
    -o "$OUTPUT_DIR/inferenced_mappings.jsonld" \
    --inversion \
    --transitivity \
    --chain-cutoff 3

echo "=========================================="
echo "Inference complete: $(date)"
echo "=========================================="
