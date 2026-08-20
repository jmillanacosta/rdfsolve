#!/bin/bash
#SBATCH --job-name=rdfsolve-grouped
#SBATCH --partition=defq
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=80G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/grouped_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/grouped_%j.err

# =============================================================================
# Grouped Mining: Provider-Level QLever Instances
# =============================================================================
# Handles multi-file sources (PubChem, Bio2RDF, RDFPortal, DBCLS)
# Groups by hostname, one QLever per provider
#
# Dependencies: Downloads available
# =============================================================================

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
DATA_DIR="${DATA_DIR:-$RDFSOLVE_BASE/data}"

export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"

mkdir -p "$RDFSOLVE_BASE/logs" "$DATA_DIR"

echo "=========================================="
echo "RDFSolve Grouped Mining (Provider-Level)"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "CPUs: ${SLURM_CPUS_PER_TASK:-$(nproc)}"
echo "Memory: ${SLURM_MEM_PER_NODE:-80G}"
echo "Output: $OUTPUT_DIR"
echo "Data: $DATA_DIR"
echo "=========================================="

source "$VENV_PATH/bin/activate"

QLEVER_IMAGE="$DATA_DIR/qlever.sif"
if [ ! -f "$QLEVER_IMAGE" ]; then
    echo "Pulling QLever Singularity image..."
    singularity pull --disable-cache "$QLEVER_IMAGE" docker://docker.io/adfreiburg/qlever:latest
fi

echo "Running download health check..."
python "$RDFSOLVE_REPO/scripts/check_downloads.py" --output "$OUTPUT_DIR/download_status.json"

python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --grouped-only \
    --output-dir "$OUTPUT_DIR" \
    --data-dir "$DATA_DIR" \
    --download-status-file "$OUTPUT_DIR/download_status.json" \
    --output-suffix _grouped \
    --skip-mappings \
    --skip-inference \
    --skip-analysis

echo "=========================================="
echo "Grouped mining complete: $(date)"
echo "=========================================="
