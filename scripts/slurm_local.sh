#!/bin/bash
#SBATCH --job-name=local-mining
#SBATCH --partition=defq
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/local_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/local_%j.err

# =============================================================================
# PHASE 1b: Local Mining
# =============================================================================
# Download RDF dumps, index with QLever, mine locally
# Can run CONCURRENTLY with slurm_remote.sh
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
DATA_DIR="${DATA_DIR:-$RDFSOLVE_BASE/data}"
TIMEOUT="${TIMEOUT:-600}"
SKIP_PROVIDERS="${SKIP_PROVIDERS:-}"

export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"

mkdir -p "$RDFSOLVE_BASE/logs" "$DATA_DIR"

echo "=========================================="
echo "RDFSolve Local Mining (Phase 1b)"
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
DOWNLOAD_STATUS="$OUTPUT_DIR/download_status.json"

# Run pipeline - local only
if [ -n "$SKIP_PROVIDERS" ]; then
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --local-only \
        --skip-providers $SKIP_PROVIDERS \
        --skip-completed \
        --output-dir "$OUTPUT_DIR" \
        --output-suffix _local \
        --data-dir "$DATA_DIR" \
        --timeout "$TIMEOUT" \
        --download-status-file "$DOWNLOAD_STATUS" \
        --extract-ontology \
        --extract-metadata \
        --skip-mappings \
        --skip-inference \
        --skip-analysis
else
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --local-only \
        --skip-completed \
        --output-dir "$OUTPUT_DIR" \
        --output-suffix _local \
        --data-dir "$DATA_DIR" \
        --timeout "$TIMEOUT" \
        --download-status-file "$DOWNLOAD_STATUS" \
        --extract-ontology \
        --extract-metadata \
        --skip-mappings \
        --skip-inference \
        --skip-analysis
fi

echo "=========================================="
echo "Local mining complete: $(date)"
echo "Next: Run slurm_lslod_cloud.sh"
echo "=========================================="
