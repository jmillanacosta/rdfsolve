#!/bin/bash
#SBATCH --job-name=test-remote
#SBATCH --partition=defq
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/test_remote_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/test_remote_%j.err

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
DATA_DIR="${DATA_DIR:-$RDFSOLVE_BASE/data}"

mkdir -p "$RDFSOLVE_BASE/logs"

echo "TEST: Remote mining (aopwikirdf, wikipathways)"
echo "Job ID: ${SLURM_JOB_ID:-local}"

source "$VENV_PATH/bin/activate"

python "$RDFSOLVE_REPO/scripts/check_endpoints.py"

python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --remote-only \
    --sources aopwikirdf wikipathways \
    --output-dir "$OUTPUT_DIR" \
    --data-dir "$DATA_DIR" \
    --endpoint-status-file "$OUTPUT_DIR/endpoint_status.json" \
    --output-suffix _remote \
    --skip-mappings \
    --skip-inference

echo "TEST complete"
