#!/bin/bash
# Submit from the repository root: sbatch scripts/slurm_qwen_mcp.sh
#SBATCH --job-name=rdfsolve-qwen-mcp
#SBATCH --partition=defq
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --gres=gpu:1
#SBATCH --qos=gpu
#SBATCH --export=ALL
#SBATCH --output=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/slurm-%x-%j.out
#SBATCH --error=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/slurm-%x-%j.err

set -euo pipefail
REPO="${RDFSOLVE_REPO:-${SLURM_SUBMIT_DIR:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}}"
cd -- "$REPO"
REPO="$PWD"

MODEL_PATH="${MODEL_PATH:-/trinity/shared/llm-models/qwen36-35b-a3b/Qwen3.6-35B-A3B-Q8_0.gguf}"
IMAGE="${LLAMA_SERVER_IMAGE:-/trinity/shared/containers/llama-server-cuda.sif}"
PORT="${QWEN_PORT:-8080}"
OUTPUT_DIR="${QWEN_OUTPUT_DIR:-$REPO/../logs/mcp-test/qwen-${SLURM_JOB_ID:-manual}}"
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd -- "$OUTPUT_DIR" && pwd)"

export QWEN_MODEL="${RDFSOLVE_MODEL:-${QWEN_MODEL:-qwen36-35b-a3b}}"
export QWEN_BASE_URL="http://127.0.0.1:${PORT}/v1"
export RDFSOLVE_MODEL="$QWEN_MODEL" RDFSOLVE_MODEL_BASE_URL="$QWEN_BASE_URL"
export RDFSOLVE_ROOT="$REPO"
export RDFSOLVE_SCHEMA="${RDFSOLVE_SCHEMA:-$REPO/notebooks/mcp/schemas/aopwikirdf.schema.json}"
export RDFSOLVE_OUTPUT="$OUTPUT_DIR"
export RDFSOLVE_MAX_TOKENS="${RDFSOLVE_MAX_TOKENS:-65536}"
export RDFSOLVE_MODEL_TIMEOUT="${RDFSOLVE_MODEL_TIMEOUT:-7200}"

# Route external endpoint requests through the cluster proxy.
export http_proxy="${http_proxy:-${HTTP_PROXY:-http://proxy.unimaas.nl:3128}}"
export https_proxy="${https_proxy:-${HTTPS_PROXY:-$http_proxy}}"
export HTTP_PROXY="$http_proxy" HTTPS_PROXY="$https_proxy"
export no_proxy="127.0.0.1,localhost,::1,${no_proxy:-},${NO_PROXY:-}"
export NO_PROXY="$no_proxy"
unset all_proxy ALL_PROXY

source "$REPO/.venv/bin/activate"
uv pip install --python "$REPO/.venv/bin/python" -e .
python -m ipykernel install --sys-prefix --name rdfsolve

[[ -r "$MODEL_PATH" ]] || { echo "Model not readable: $MODEL_PATH" >&2; exit 2; }
[[ -r "$IMAGE" ]] || { echo "Container not readable: $IMAGE" >&2; exit 2; }
[[ -r "$RDFSOLVE_SCHEMA" ]] || { echo "Schema not readable: $RDFSOLVE_SCHEMA" >&2; exit 2; }

cleanup() { kill "${SERVER_PID:-}" 2>/dev/null || true; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

singularity exec --nv --bind /trinity/shared:/trinity/shared \
  --env LD_LIBRARY_PATH=/app "$IMAGE" /app/llama-server \
  --model "$MODEL_PATH" --alias "$QWEN_MODEL" --host 127.0.0.1 --port "$PORT" \
  --n-gpu-layers 999 --ctx-size "${QWEN_CTX_SIZE:-131072}" --parallel 1 --jinja \
  --n-predict "$RDFSOLVE_MAX_TOKENS" --timeout 7500 \
  >"$OUTPUT_DIR/llama-server.out" 2>"$OUTPUT_DIR/llama-server.err" &
SERVER_PID=$!

# Wait for model loading, stopping if the server exits or startup takes too long.
deadline=$((SECONDS + 1800))
until curl --noproxy '*' --max-time 5 -fsS \
    "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; do
  if ! kill -0 "$SERVER_PID" 2>/dev/null || (( SECONDS >= deadline )); then
    tail -n 50 "$OUTPUT_DIR/llama-server.err" >&2
    echo "llama-server failed to start or timed out" >&2
    exit 1
  fi
  sleep 5
done
kill -0 "$SERVER_PID"

cd "$REPO/notebooks/mcp"
python -m nbconvert --to notebook --execute "${RDFSOLVE_NOTEBOOK:-test-mcp.ipynb}" \
  --output-dir "$OUTPUT_DIR" --output executed.ipynb \
  --ExecutePreprocessor.kernel_name=rdfsolve \
  --ExecutePreprocessor.timeout=-1
