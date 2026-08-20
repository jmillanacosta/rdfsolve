#!/bin/bash
#SBATCH --job-name=rdfsolve-sssom-test
#SBATCH --partition=defq
#SBATCH --time=4:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --output=/home/javier.millanacosta/rdfsolve/logs/sssom_test_%j.out
#SBATCH --error=/home/javier.millanacosta/rdfsolve/logs/sssom_test_%j.err

# Test SSSOM mapping discovery with aopwikirdf and wikipathways
# This will mine schemas for both sources and then discover cross-dataset mappings

set -euo pipefail

# Configuration
RDFSOLVE_BASE="${RDFSOLVE_BASE:-/home/javier.millanacosta/rdfsolve}"
RDFSOLVE_REPO="${RDFSOLVE_REPO:-$RDFSOLVE_BASE/rdfsolve-2}"
VENV_PATH="${VENV_PATH:-$RDFSOLVE_REPO/.venv}"
OUTPUT_DIR="${OUTPUT_DIR:-$RDFSOLVE_BASE/output}"
TIMEOUT="${TIMEOUT:-300}"

# Create log directory
mkdir -p "$RDFSOLVE_BASE/logs"

echo "=========================================="
echo "RDFSolve SSSOM Mapping Discovery Test"
echo "=========================================="
echo "Date: $(date)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Node: ${SLURM_NODELIST:-$(hostname)}"
echo "Output: $OUTPUT_DIR"
echo "Sources: aopwikirdf wikipathways"
echo "=========================================="

# Activate environment
source "$VENV_PATH/bin/activate"

# Step 1: Mine individual schemas for aopwikirdf and wikipathways
echo ""
echo "Step 1: Mining individual schemas..."
echo "=========================================="
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --sources aopwikirdf wikipathways \
    --output-dir "$OUTPUT_DIR" \
    --timeout "$TIMEOUT" \
    --skip-mappings \
    --skip-inference \
    --skip-analysis

# Step 2: Mine LSLOD cloud to discover SSSOM mappings
echo ""
echo "Step 2: Discovering SSSOM mappings in LSLOD cloud..."
echo "=========================================="
python "$RDFSOLVE_REPO/scripts/pipeline.py" \
    --lslod-cloud-only \
    --sources aopwikirdf wikipathways \
    --output-dir "$OUTPUT_DIR" \
    --timeout "$TIMEOUT" \
    --skip-mappings \
    --skip-inference \
    --skip-analysis

echo ""
echo "=========================================="
echo "SSSOM mapping discovery test complete: $(date)"
echo "=========================================="
echo ""
echo "Check outputs:"
echo "  - Individual schemas: $OUTPUT_DIR/aopwikirdf/ and $OUTPUT_DIR/wikipathways/"
echo "  - SSSOM mappings: $OUTPUT_DIR/mappings/"
echo ""

# Step 3: List discovered mappings
echo ""
echo "Step 3: Listing discovered SSSOM mappings..."
echo "=========================================="
if [ -d "$OUTPUT_DIR/mappings" ]; then
    echo "SSSOM mapping files:"
    find "$OUTPUT_DIR/mappings" -name "*.sssom.tsv" -exec echo "  - {}" \;
    echo ""
    echo "Mapping counts:"
    for f in "$OUTPUT_DIR/mappings"/*.sssom.tsv; do
        if [ -f "$f" ]; then
            # Count non-comment, non-header lines
            count=$(grep -v "^#" "$f" | tail -n +2 | wc -l)
            echo "  $(basename "$f"): $count mappings"
        fi
    done
else
    echo "WARNING: Mappings directory not found at $OUTPUT_DIR/mappings"
fi

echo ""
echo "=========================================="
echo "Analysis complete: $(date)"
echo "=========================================="
