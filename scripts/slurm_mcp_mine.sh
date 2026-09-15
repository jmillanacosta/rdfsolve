#!/bin/bash
#SBATCH --job-name=rdfsolve-mcp-mine
#SBATCH --partition=defq
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --output=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/slurm-%x-%j.out
#SBATCH --error=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/slurm-%x-%j.err
set -euo pipefail
export RDFSOLVE_ROOT="${RDFSOLVE_ROOT:-$SLURM_SUBMIT_DIR}"
export http_proxy="${http_proxy:-http://proxy.unimaas.nl:3128}"
export https_proxy="$http_proxy"
export no_proxy="localhost,127.0.0.1,${no_proxy:-}"
source "$RDFSOLVE_ROOT/.venv/bin/activate"
OUTPUT="$RDFSOLVE_ROOT/../logs/mcp-test/mine-$SLURM_JOB_ID"
mkdir -p "$OUTPUT"
cd "$RDFSOLVE_ROOT/notebooks/mcp"
python -m nbconvert --to notebook --execute 00_mine.ipynb --output-dir "$OUTPUT" --output executed.ipynb --ExecutePreprocessor.kernel_name=rdfsolve --ExecutePreprocessor.timeout=1200
