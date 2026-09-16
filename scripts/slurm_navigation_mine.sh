#!/bin/bash
#SBATCH --job-name=rdfsolve-navigation
#SBATCH --partition=defq
#SBATCH --array=0-1
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --output=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/navigation-%A_%a.out
#SBATCH --error=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/navigation-%A_%a.err
set -euo pipefail
export RDFSOLVE_ROOT="${RDFSOLVE_ROOT:-$SLURM_SUBMIT_DIR}"
export PYTHONPATH="$RDFSOLVE_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
export http_proxy="${http_proxy:-http://proxy.unimaas.nl:3128}"
export https_proxy="$http_proxy"
export no_proxy="localhost,127.0.0.1,${no_proxy:-}"
source "$RDFSOLVE_ROOT/.venv/bin/activate"
datasets=(aopwikirdf wikipathways)
export RDFSOLVE_DATASETS="${datasets[$SLURM_ARRAY_TASK_ID]}"
export RDFSOLVE_OUTPUT="$RDFSOLVE_ROOT/../logs/mcp-test/navigation-${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$RDFSOLVE_OUTPUT"
cd "$RDFSOLVE_ROOT"
python -m nbconvert --to notebook --execute notebooks/miner/navigation.ipynb \
  --output-dir "$RDFSOLVE_OUTPUT" --output executed.ipynb \
  --ExecutePreprocessor.kernel_name=rdfsolve --ExecutePreprocessor.timeout=42000
