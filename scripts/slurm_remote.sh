#!/bin/bash
#SBATCH --job-name=remote-mining
#SBATCH --partition=defq
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail
# SLURM runs a spool copy. Resolve the repo from the submit directory.
export RDFSOLVE_REPO="${RDFSOLVE_REPO:-${SLURM_SUBMIT_DIR:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}}"
exec bash "$RDFSOLVE_REPO/scripts/run_mining.sh" remote "$@"
