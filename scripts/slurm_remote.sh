#!/bin/bash
#SBATCH --job-name=rdfsolve-remote
#SBATCH --partition=defq
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --output=logs/remote_%j.out
#SBATCH --error=logs/remote_%j.err

# Remote mining: query SPARQL endpoints directly
# This job mines schemas from all sources that have endpoints
# Can run concurrently with slurm_local.sh

set -euo pipefail

# Configuration - override via environment or modify here
RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
TIMEOUT="${TIMEOUT:-300}"

# Skip providers with known slow/unreliable connections
SKIP_PROVIDERS="${SKIP_PROVIDERS:-idsm}"

# Create log directory
mkdir -p "$RDFSOLVE_BASE/logs"

echo "=========================================="
echo "RDFSolve Remote Mining"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "Output: $OUTPUT_DIR"
echo "Skip providers: $SKIP_PROVIDERS"
echo "=========================================="

# Activate environment
source "$VENV_PATH/bin/activate"

# Run pipeline - remote only, skip IDSM
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --remote-only \
    --skip-providers $SKIP_PROVIDERS \
    --output-dir "$OUTPUT_DIR" \
    --timeout "$TIMEOUT" \
    --skip-mappings \
    --skip-inference

echo "=========================================="
echo "Remote mining complete: $(date)"
echo "=========================================="
