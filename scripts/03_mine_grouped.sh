#!/bin/bash
#SBATCH --job-name=03-grouped
#SBATCH --partition=defq
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=256G
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

# =============================================================================
# STEP 03: Grouped Mining + LSLOD Cloud
# =============================================================================
# Mine multi-file provider groups (Bio2RDF, PubChem, etc.)
# Combine all sources in mega-QLever for LSLOD cloud analysis
# Generates cross-dataset SSSOM mappings
# =============================================================================

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-$(pwd)/..}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output_$(date +%Y-%m-%d)}"
DATA_DIR="${DATA_DIR:-$RDFSOLVE_BASE/data}"
SKIP_COMPLETED="${SKIP_COMPLETED:-false}"

export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"
mkdir -p "$RDFSOLVE_BASE/logs" "$OUTPUT_DIR" "$DATA_DIR"

echo "=========================================="
echo "RDFSolve Step 03: Grouped + LSLOD Cloud"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "CPUs: ${SLURM_CPUS_PER_TASK:-$(nproc)}"
echo "Memory: ${SLURM_MEM_PER_NODE:-256G}"
echo "Output: $OUTPUT_DIR"
echo "Data: $DATA_DIR"
echo "=========================================="

source "$VENV_PATH/bin/activate"

# Pull QLever image if not present
QLEVER_IMAGE="$DATA_DIR/qlever.sif"
if [ ! -f "$QLEVER_IMAGE" ]; then
    echo "Pulling QLever Singularity image..."
    singularity pull --disable-cache "$QLEVER_IMAGE" docker://docker.io/adfreiburg/qlever:latest
fi

echo "Running download health check..."
python "$RDFSOLVE_REPO/scripts/check_downloads.py" --output "$OUTPUT_DIR/download_status.json"

# Step 3a: Grouped mining
echo ""
echo "=========================================="
echo "Step 3a: Grouped Mining (Provider-Level)"
echo "=========================================="
PIPELINE_ARGS="--grouped-only --output-dir $OUTPUT_DIR --data-dir $DATA_DIR --download-status-file $OUTPUT_DIR/download_status.json --skip-mappings --skip-inference --skip-analysis"
if [ "$SKIP_COMPLETED" = "true" ]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --skip-completed"
fi

python "$RDFSOLVE_REPO/scripts/pipeline.py" $PIPELINE_ARGS

echo ""
echo "Step 3a complete: $(date)"

# Step 3b: LSLOD cloud (all sources combined)
echo ""
echo "=========================================="
echo "Step 3b: LSLOD Cloud (All Sources)"
echo "=========================================="
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --lslod-cloud-only \
    --output-dir "$OUTPUT_DIR" \
    --data-dir "$DATA_DIR" \
    --skip-inference

echo ""
echo "=========================================="
echo "Step 03 complete: $(date)"
echo "=========================================="
echo ""
echo "Outputs:"
echo "  - Grouped schemas per provider"
echo "  - LSLOD cloud schema: $OUTPUT_DIR/lslod_cloud/"
echo "  - SSSOM mappings: $OUTPUT_DIR/mappings/*.sssom.tsv"
echo "=========================================="
