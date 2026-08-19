#!/bin/bash
#SBATCH --job-name=rdfsolve-full
#SBATCH --partition=defq
#SBATCH --time=96:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --output=logs/full_%j.out
#SBATCH --error=logs/full_%j.err

# Full pipeline: mining (remote + local) + mappings + inference + analysis
# This is the complete LOD cloud analysis pipeline
# NOTE: For faster execution, submit slurm_remote.sh and slurm_local.sh concurrently instead

set -euo pipefail

# Configuration - override via environment or modify here
RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
DATA_DIR="${DATA_DIR:-$RDFSOLVE_BASE/data}"
TIMEOUT="${TIMEOUT:-300}"

# Skip providers with known slow/unreliable connections
SKIP_PROVIDERS="${SKIP_PROVIDERS:-idsm}"

# Singularity settings
export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"

# Create directories
mkdir -p "$RDFSOLVE_BASE/logs" "$DATA_DIR"

echo "=========================================="
echo "RDFSolve Full Pipeline"
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

# Pull QLever image if not present (needed for local mining)
QLEVER_IMAGE="$DATA_DIR/qlever.sif"
if [ ! -f "$QLEVER_IMAGE" ]; then
    echo "Pulling QLever Singularity image..."
    singularity pull --disable-cache "$QLEVER_IMAGE" docker://docker.io/adfreiburg/qlever:latest
fi

# Run full pipeline, skip IDSM
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --skip-providers $SKIP_PROVIDERS \
    --output-dir "$OUTPUT_DIR" \
    --timeout "$TIMEOUT"

echo "=========================================="
echo "Full pipeline complete: $(date)"
echo "Results: $OUTPUT_DIR"
echo "=========================================="
