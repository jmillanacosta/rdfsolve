#!/bin/bash
#SBATCH --job-name=01-remote
#SBATCH --partition=defq
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

# =============================================================================
# STEP 01: Remote Mining
# =============================================================================
# Query remote SPARQL endpoints to mine schemas
# Generates: JSON-LD, VoID, JSON Schema, Pydantic, SHACL
# =============================================================================

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-$(pwd)/..}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output_$(date +%Y-%m-%d)}"
TIMEOUT="${TIMEOUT:-300}"
SKIP_PROVIDERS="${SKIP_PROVIDERS:-idsm}"
SKIP_COMPLETED="${SKIP_COMPLETED:-false}"

mkdir -p "$RDFSOLVE_BASE/logs" "$OUTPUT_DIR"

echo "=========================================="
echo "RDFSolve Step 01: Remote Mining"
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

# Run pipeline - remote only
PIPELINE_ARGS="--remote-only --skip-providers $SKIP_PROVIDERS --output-dir $OUTPUT_DIR --timeout $TIMEOUT --endpoint-status-file $OUTPUT_DIR/endpoint_status.json --skip-mappings --skip-inference --skip-analysis"
if [ "$SKIP_COMPLETED" = "true" ]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --skip-completed"
fi

python "$RDFSOLVE_REPO/scripts/pipeline.py" $PIPELINE_ARGS

echo "=========================================="
echo "Step 01 complete: $(date)"
echo "Output: $OUTPUT_DIR"
echo ""
echo "Generated per source:"
echo "  - {source}_schema.jsonld  (with descriptions/cardinality/examples)"
echo "  - {source}_void.ttl       (VoID metadata)"
echo "  - {source}_schema.json    (JSON Schema with \$ref)"
echo "  - {source}_models.py      (Pydantic models, domain classes only)"
echo "  - {source}_shapes.ttl     (SHACL shapes with constraints)"
echo "=========================================="
