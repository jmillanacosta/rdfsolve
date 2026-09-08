#!/bin/bash
#SBATCH --job-name=grouped-mining
#SBATCH --partition=defq
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=80G
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail
# SLURM runs a spool copy. Resolve the repo from the submit directory.
export RDFSOLVE_REPO="${RDFSOLVE_REPO:-${SLURM_SUBMIT_DIR:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}}"
exec bash "$RDFSOLVE_REPO/scripts/run_mining.sh" grouped "$@"
