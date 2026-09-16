#!/bin/bash
#SBATCH --job-name=lslod-map-34
#SBATCH --partition=defq
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/mappings_stage34_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/mappings_stage34_%j.err
#SBATCH --time=2:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=4

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"

echo "=========================================="
echo "LSLOD Mapping Pipeline - Stages 3-4"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "=========================================="

# Activate environment
export PATH="$VENV_PATH/bin:$PATH"
export VIRTUAL_ENV="$VENV_PATH"
export PYTHONPATH="$RDFSOLVE_REPO/src${PYTHONPATH:+:$PYTHONPATH}"

cd "$RDFSOLVE_REPO"

python scripts/analyze_mappings.py "${RDFSOLVE_SCHEMAS:?Set the canonical schema directory}" \
  --instances "${RDFSOLVE_INSTANCES:-$RDFSOLVE_REPO/output/mappings/instances}" \
  --output "${RDFSOLVE_ANALYSIS_OUTPUT:-$RDFSOLVE_BASE/results/mapping-analysis-${SLURM_JOB_ID:-local}}" "$@"
