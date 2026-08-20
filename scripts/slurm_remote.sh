#!/bin/bash
#SBATCH --job-name=rdfsolve-remote
#SBATCH --partition=defq
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
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

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
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

# Health check: test endpoints before mining
echo "Running endpoint health check..."
python "$RDFSOLVE_REPO/scripts/check_endpoints.py" --output "$OUTPUT_DIR/endpoint_status.json"
ENDPOINT_STATUS="$OUTPUT_DIR/endpoint_status.json"

# Run pipeline - remote only
if [ -f "$ENDPOINT_STATUS" ]; then
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --remote-only \
        --skip-providers $SKIP_PROVIDERS \
        --output-dir "$OUTPUT_DIR" \
        --output-suffix _remote \
        --timeout "$TIMEOUT" \
        --endpoint-status-file "$ENDPOINT_STATUS" \
        --skip-mappings \
        --skip-inference \
        --skip-analysis
else
    echo "WARNING: Endpoint status file not found, proceeding anyway"
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --remote-only \
        --skip-providers $SKIP_PROVIDERS \
        --output-dir "$OUTPUT_DIR" \
        --output-suffix _remote \
        --timeout "$TIMEOUT" \
        --skip-mappings \
        --skip-inference \
        --skip-analysis
fi

echo "=========================================="
echo "Remote mining complete: $(date)"
echo "Next: Run slurm_lslod_cloud.sh after local mining completes"
echo "=========================================="
