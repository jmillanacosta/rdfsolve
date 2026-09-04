#!/bin/bash
#SBATCH --job-name=lslod-mappings
#SBATCH --partition=defq
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/mappings_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/mappings_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=128G
#SBATCH --cpus-per-task=16

set -euo pipefail

RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"

export SINGULARITY_CACHEDIR="${SINGULARITY_CACHEDIR:-$HOME/.singularity/cache}"
export SINGULARITY_TMPDIR="${SINGULARITY_TMPDIR:-$HOME/.singularity/tmp}"
mkdir -p "$SINGULARITY_CACHEDIR" "$SINGULARITY_TMPDIR"

echo "=========================================="
echo "LSLOD Mapping Pipeline"
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

# Stage 1: Load external mappings
echo ""
echo "=== Stage 1: External Mappings ==="
python scripts/mappings_01_external.py

# Stage 2: Extract cross-references WITH classes
echo ""
echo "=== Stage 2: Cross-References + Classes ==="
python scripts/mappings_02_crossrefs.py

# Stage 3: Infer class mappings
echo ""
echo "=== Stage 3: Class Mappings ==="
python scripts/mappings_03_class_mappings.py

# Stage 4: Consolidate and run semra
echo ""
echo "=== Stage 4: Consolidation + Semra ==="
python scripts/mappings_04_consolidate.py

echo ""
echo "=========================================="
echo "Pipeline complete"
echo "Date: $(date)"
echo "=========================================="
