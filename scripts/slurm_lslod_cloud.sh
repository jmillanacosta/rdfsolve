#!/bin/bash
#SBATCH --job-name=rdfsolve-lslod
#SBATCH --partition=defq
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=256G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/lslod_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/lslod_%j.err

# =============================================================================
# PHASE 2: LSLOD Cloud Analysis + SSSOM Mapping Discovery
# =============================================================================
# Combine ALL local sources in mega-QLever instance
# Generates cross-dataset SSSOM mappings based on schema patterns
# High memory required: 256GB
#
# Dependencies: slurm_local.sh (needs downloaded RDF dumps)
# Next step: Analysis notebooks or slurm_analysis.sh
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
echo "RDFSolve LSLOD Cloud Mining (Phase 2)"
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

# Run LSLOD cloud mining (generates SSSOM mappings)
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --lslod-cloud-only \
    --output-dir "$OUTPUT_DIR" \
    --data-dir "$DATA_DIR" \
    --skip-inference

echo ""
echo "=========================================="
echo "LSLOD Cloud mining complete: $(date)"
echo "=========================================="
echo ""
echo "Outputs:"
echo "  - LSLOD schema: $OUTPUT_DIR/lslod_cloud/lslod_cloud_schema.jsonld"
echo "  - SSSOM mappings: $OUTPUT_DIR/mappings/*.sssom.tsv"
echo ""
echo "Next: Run analysis notebooks"
echo "=========================================="
