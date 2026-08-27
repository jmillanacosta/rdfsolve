#!/bin/bash
#SBATCH --job-name=02-local
#SBATCH --partition=defq
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

# =============================================================================
# STEP 02: Local Mining
# =============================================================================
# Download RDF dumps, index with QLever, mine locally
# Generates: JSON-LD, VoID, JSON Schema, Pydantic, SHACL per dataset
# =============================================================================

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-$(pwd)/..}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output_$(date +%Y-%m-%d)}"
DATA_DIR="${DATA_DIR:-$RDFSOLVE_BASE/data}"
TIMEOUT="${TIMEOUT:-600}"
SKIP_PROVIDERS="${SKIP_PROVIDERS:-}"

export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"
mkdir -p "$RDFSOLVE_BASE/logs" "$OUTPUT_DIR" "$DATA_DIR"

echo "=========================================="
echo "RDFSolve Step 02: Local Mining"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "CPUs: ${SLURM_CPUS_PER_TASK:-$(nproc)}"
echo "Memory: ${SLURM_MEM_PER_NODE:-unknown}"
echo "Output: $OUTPUT_DIR"
echo "Data: $DATA_DIR"
echo "Skip providers: $SKIP_PROVIDERS"
echo "=========================================="

source "$VENV_PATH/bin/activate"

# Pull QLever image if not present
QLEVER_IMAGE="$DATA_DIR/qlever.sif"
if [ ! -f "$QLEVER_IMAGE" ]; then
    echo "Pulling QLever Singularity image..."
    singularity pull --disable-cache "$QLEVER_IMAGE" docker://docker.io/adfreiburg/qlever:latest
fi

# Health check: test downloads before mining
echo "Running download health check..."
python "$RDFSOLVE_REPO/scripts/check_downloads.py" --output "$OUTPUT_DIR/download_status.json"

# Run pipeline - local only
if [ -n "$SKIP_PROVIDERS" ]; then
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --local-only \
        --skip-providers $SKIP_PROVIDERS \
        --skip-completed \
        --output-dir "$OUTPUT_DIR" \
        --data-dir "$DATA_DIR" \
        --timeout "$TIMEOUT" \
        --download-status-file "$OUTPUT_DIR/download_status.json" \
        --skip-mappings \
        --skip-inference \
        --skip-analysis
else
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --local-only \
        --skip-completed \
        --output-dir "$OUTPUT_DIR" \
        --data-dir "$DATA_DIR" \
        --timeout "$TIMEOUT" \
        --download-status-file "$OUTPUT_DIR/download_status.json" \
        --skip-mappings \
        --skip-inference \
        --skip-analysis
fi

echo "=========================================="
echo "Step 02 complete: $(date)"
echo "Output: $OUTPUT_DIR"
echo "=========================================="
