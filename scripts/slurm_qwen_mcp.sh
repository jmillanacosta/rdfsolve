#!/bin/bash
# Serve the staged Qwen GGUF and execute the RDFSolve MCP notebook on one H100.
# Submit from the repository root: sbatch scripts/slurm_qwen_mcp.sh
# Override QWEN_MODEL, MODEL_PATH, and QWEN_BASE_URL when comparing another install.
#SBATCH --job-name=rdfsolve-qwen-mcp
#SBATCH --partition=defq
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --gres=gpu:1
#SBATCH --qos=gpu
#SBATCH --export=ALL
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail
REPO="${RDFSOLVE_REPO:-${SLURM_SUBMIT_DIR:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}}"
cd -- "$REPO"
MODEL_PATH="${MODEL_PATH:-/trinity/shared/llm-models/qwen36-35b-a3b/Qwen3.6-35B-A3B-Q8_0.gguf}"
MODEL_NAME="${QWEN_MODEL:-qwen36-35b-a3b}"
PORT="${QWEN_PORT:-8080}"
IMAGE="${LLAMA_SERVER_IMAGE:-/trinity/shared/containers/llama-server-cuda.sif}"
OUTPUT_DIR="${QWEN_OUTPUT_DIR:-$REPO/notebooks/mcp/output/qwen-${SLURM_JOB_ID:-manual}}"
mkdir -p "$OUTPUT_DIR"
export QWEN_BASE_URL="${QWEN_BASE_URL:-http://127.0.0.1:${PORT}/v1}"
export QWEN_MODEL="$MODEL_NAME"
# Slurm jobs may not inherit the login shell's proxy variables.  Keep the
# cluster proxy for remote SPARQL, while the local Qwen endpoint bypasses it.
export http_proxy="${http_proxy:-http://proxy.unimaas.nl:3128}"
export https_proxy="${https_proxy:-http://proxy.unimaas.nl:3128}"
export HTTP_PROXY="$http_proxy"
export HTTPS_PROXY="$https_proxy"
export no_proxy="127.0.0.1,localhost${no_proxy:+,$no_proxy}"
export NO_PROXY="$no_proxy"
unset all_proxy ALL_PROXY
echo "Proxy environment: http_proxy=$http_proxy https_proxy=$https_proxy no_proxy=$no_proxy" >&2

test -r "$MODEL_PATH" || { echo "Qwen model not readable: $MODEL_PATH" >&2; exit 2; }
test -r "$IMAGE" || { echo "llama.cpp image not readable: $IMAGE" >&2; exit 2; }
cleanup() { [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

singularity exec --nv --bind /trinity/shared:/trinity/shared \
  --env LD_LIBRARY_PATH=/app "$IMAGE" /app/llama-server \
  --model "$MODEL_PATH" --alias "$MODEL_NAME" --host 127.0.0.1 --port "$PORT" \
  --n-gpu-layers 999 --ctx-size 32768 --parallel 1 --jinja \
  >"$OUTPUT_DIR/llama-server.out" 2>"$OUTPUT_DIR/llama-server.err" &
SERVER_PID=$!
for attempt in $(seq 1 180); do
  if curl --noproxy '*' -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then break; fi
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$OUTPUT_DIR/llama-server.err" >&2; exit 1; }
  sleep 5
  [[ "$attempt" == 180 ]] && { echo 'Timed out waiting for llama-server' >&2; exit 1; }
done
curl --noproxy '*' -fsS "http://127.0.0.1:${PORT}/v1/models" | tee "$OUTPUT_DIR/models.json"

export RDFSOLVE_QWEN_OUTPUT="$OUTPUT_DIR"
source "$REPO/.venv/bin/activate"
uv pip install -e .
python -c 'import mcp, pydantic_ai, rdfsolve; print("Environment ready:", pydantic_ai.__version__, rdfsolve.__file__)'
python - <<'PY'
import os
import requests

endpoint = os.getenv("RDFSOLVE_ENDPOINT", "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql")
response = requests.get(endpoint, timeout=30)
response.raise_for_status()
print(f"SPARQL endpoint reachable through proxy: {endpoint} ({response.status_code})")
PY
cd "$REPO/notebooks/mcp"
jupyter nbconvert --to notebook --execute 01_ask_aopwiki_qwen.ipynb \
  --output-dir "$OUTPUT_DIR" --output executed.ipynb --ExecutePreprocessor.timeout=3600
