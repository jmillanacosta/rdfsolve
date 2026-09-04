#!/bin/bash
#SBATCH --job-name=remote-mining
#SBATCH --partition=defq
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/remote_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/remote_%j.err

# =============================================================================
# PHASE 1a: Remote Mining
# =============================================================================
# Query SPARQL endpoints directly to mine schemas
# Can run CONCURRENTLY with slurm_local.sh
#
# Dependencies: None
# Next step: slurm_lslod_cloud.sh (after both remote and local complete)
# =============================================================================

set -euo pipefail

# Compute nodes cannot resolve proxy hostname - use IP address
export http_proxy=http://137.120.13.46:3128
export https_proxy=http://137.120.13.46:3128

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
TODAY=$(date +%Y-%m-%d)
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output_$TODAY}"
TIMEOUT="${TIMEOUT:-300}"
SKIP_PROVIDERS="${SKIP_PROVIDERS:-idsm}"

mkdir -p "$RDFSOLVE_BASE/logs"

echo "=========================================="
echo "RDFSolve Remote Mining (Phase 1a)"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "Output: $OUTPUT_DIR"
echo "Skip providers: $SKIP_PROVIDERS"
echo "=========================================="

source "$VENV_PATH/bin/activate"

# Skip health check for now - mining will detect down endpoints
echo "Skipping pre-flight health check (mining will detect failures)"

# Run pipeline - remote only
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --remote-only \
    --skip-providers $SKIP_PROVIDERS \
    --skip-completed \
    --output-dir "$OUTPUT_DIR" \
    --output-suffix _remote \
    --timeout "$TIMEOUT" \
    --extract-ontology \
    --extract-metadata \
    --skip-mappings \
    --skip-inference \
    --skip-analysis

echo "=========================================="
echo "Remote mining complete: $(date)"
echo "Next: Run slurm_lslod_cloud.sh after local mining completes"
echo "=========================================="
