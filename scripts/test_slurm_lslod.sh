#!/bin/bash
#SBATCH --job-name=test-lslod
#SBATCH --partition=defq
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/test_lslod_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/test_lslod_%j.err

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

echo "TEST: LS-LOD Cloud (aopwikirdf, wikipathways)"
echo "Job ID: ${SLURM_JOB_ID:-local}"

source "$VENV_PATH/bin/activate"

QLEVER_IMAGE="$DATA_DIR/qlever.sif"
if [ ! -f "$QLEVER_IMAGE" ]; then
    singularity pull --disable-cache "$QLEVER_IMAGE" docker://docker.io/adfreiburg/qlever:latest
fi

python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --lslod-cloud-only \
    --sources aopwikirdf wikipathways \
    --output-dir "$OUTPUT_DIR" \
    --data-dir "$DATA_DIR" \
    --skip-mappings \
    --skip-inference

echo "TEST complete"
