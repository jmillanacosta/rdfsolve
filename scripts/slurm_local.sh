#!/bin/bash
#SBATCH --job-name=rdfsolve-local
#SBATCH --partition=defq
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --output=logs/local_%j.out
#SBATCH --error=logs/local_%j.err

# Local mining: download RDF dumps, index with QLever, mine locally
# This job handles sources that need local processing
# Can run concurrently with slurm_remote.sh

set -euo pipefail

# Configuration - override via environment or modify here
RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
DATA_DIR="${DATA_DIR:-$RDFSOLVE_BASE/data}"
TIMEOUT="${TIMEOUT:-600}"

SKIP_PROVIDERS="${SKIP_PROVIDERS:-}"

# Singularity settings
export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"

# Create directories
mkdir -p "$RDFSOLVE_BASE/logs" "$DATA_DIR"

echo "=========================================="
echo "RDFSolve Local Mining"
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

# Activate environment
source "$VENV_PATH/bin/activate"

# Pull QLever image if not present
QLEVER_IMAGE="$DATA_DIR/qlever.sif"
if [ ! -f "$QLEVER_IMAGE" ]; then
    echo "Pulling QLever Singularity image..."
    singularity pull --disable-cache "$QLEVER_IMAGE" docker://docker.io/adfreiburg/qlever:latest
fi

# Run pipeline - local only
if [ -n "$SKIP_PROVIDERS" ]; then
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --local-only \
        --skip-providers $SKIP_PROVIDERS \
        --output-dir "$OUTPUT_DIR" \
        --data-dir "$DATA_DIR" \
        --timeout "$TIMEOUT" \
        --skip-mappings \
        --skip-inference
else
    python "$RDFSOLVE_REPO/scripts/pipeline.py" \
        --local-only \
        --output-dir "$OUTPUT_DIR" \
        --data-dir "$DATA_DIR" \
        --timeout "$TIMEOUT" \
        --skip-mappings \
        --skip-inference
fi

echo "=========================================="
echo "Local mining complete: $(date)"
echo "=========================================="
