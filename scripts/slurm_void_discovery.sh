#!/bin/bash
#SBATCH --job-name=rdfsolve-void
#SBATCH --partition=defq
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/void_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/void_%j.err

# =============================================================================
# VoID Discovery (Optional/Standalone)
# =============================================================================
# Query endpoints for VoID metadata (class partitions, predicates)
# Discovers what endpoints publish about themselves
#
# Dependencies: None
# =============================================================================

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
VOID_DIR="$OUTPUT_DIR/void"

mkdir -p "$RDFSOLVE_BASE/logs" "$VOID_DIR"

echo "=========================================="
echo "RDFSolve VoID Discovery"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "Output: $VOID_DIR"
echo "=========================================="

cd "$RDFSOLVE_REPO"
source "$VENV_PATH/bin/activate"

python scripts/discover_void_partitions.py \
    --output-dir "$VOID_DIR" \
    --sources data/sources.yaml \
    --verbose

echo "=========================================="
echo "VoID discovery complete: $(date)"
echo "Results saved to: $VOID_DIR"
echo "=========================================="
