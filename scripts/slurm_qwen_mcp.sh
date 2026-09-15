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
#SBATCH --signal=B:USR1@120
#SBATCH --output=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/slurm-%x-%j.out
#SBATCH --error=/trinity/home/javier.millanacosta/rdfsolve/logs/mcp-test/slurm-%x-%j.err

set -euo pipefail
: "${SLURM_JOB_ID:?Submit this script through SLURM}"
REPO="${RDFSOLVE_REPO:-${SLURM_SUBMIT_DIR:?Missing repository directory}}"
cd -- "$REPO"
REPO="$PWD"
PYTHON="$REPO/.venv/bin/python"
[[ -x "$PYTHON" ]] || { echo "Missing Python environment: $PYTHON" >&2; exit 2; }
export PATH="$REPO/.venv/bin:$PATH"
export RDFSOLVE_NOTEBOOK="${RDFSOLVE_NOTEBOOK:-test-mcp.ipynb}"

MODEL_PATH="${MODEL_PATH:-/trinity/shared/llm-models/qwen36-35b-a3b/Qwen3.6-35B-A3B-Q8_0.gguf}"
IMAGE="${LLAMA_SERVER_IMAGE:-/trinity/shared/containers/llama-server-cuda.sif}"
PORT="${QWEN_PORT:-$((20000 + ${SLURM_JOB_ID:-0} % 30000))}"
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

export http_proxy="${http_proxy:-${HTTP_PROXY:-http://proxy.unimaas.nl:3128}}"
export https_proxy="${https_proxy:-${HTTPS_PROXY:-$http_proxy}}"
export HTTP_PROXY="$http_proxy" HTTPS_PROXY="$https_proxy"
export no_proxy="127.0.0.1,localhost,::1,${no_proxy:-},${NO_PROXY:-}"
export NO_PROXY="$no_proxy"
unset all_proxy ALL_PROXY

cleanup() {
  status=$?
  trap - EXIT INT TERM USR1
  set +e
  for pid in "${NOTEBOOK_PID:-}" "${SERVER_PID:-}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
    fi
  done
  "$PYTHON" - "$OUTPUT_DIR" "$status" "$stage" <<'PYCODE'
import json, sys
from pathlib import Path
output, status, stage = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
(output / 'job-exit.json').write_text(json.dumps({'exit_code': status, 'stage': stage}))
path = output / 'metrics.json'
if status and path.exists():
    records = json.loads(path.read_text())
    for row in records:
        if row['state'] == 'running':
            row.update(state='interrupted', f1=0.0, exact=False,
                       error={'code': 'scheduler_exit', 'message': f'Job exited with code {status}'})
    path.write_text(json.dumps(records, indent=2))
PYCODE
  exit "$status"
}
stage=preflight
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 124' USR1

for file in "$MODEL_PATH" "$IMAGE" "$RDFSOLVE_SCHEMA" "$REPO/notebooks/mcp/$RDFSOLVE_NOTEBOOK"; do
  [[ -r "$file" ]] || { echo "Required file is not readable: $file" >&2; exit 2; }
done
for command in singularity curl nvidia-smi; do
  command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 2; }
done
"$PYTHON" -c 'import ipykernel, nbconvert, pydantic_ai, mcp'
"$PYTHON" -m ipykernel install --prefix "$OUTPUT_DIR/jupyter" --name rdfsolve
export JUPYTER_PATH="$OUTPUT_DIR/jupyter/share/jupyter${JUPYTER_PATH:+:$JUPYTER_PATH}"
export PYTHONPATH="$REPO/src:$REPO/scripts${PYTHONPATH:+:$PYTHONPATH}"

stage=model_start
printf 'Job %s: loading %s; output %s\n' "$SLURM_JOB_ID" "$QWEN_MODEL" "$OUTPUT_DIR"
singularity exec --nv --bind /trinity/shared:/trinity/shared \
  --env LD_LIBRARY_PATH=/app "$IMAGE" /app/llama-server \
  --model "$MODEL_PATH" --alias "$QWEN_MODEL" --host 127.0.0.1 --port "$PORT" \
  --n-gpu-layers 999 --ctx-size "${QWEN_CTX_SIZE:-131072}" --parallel 1 --jinja \
  --top-k 0 --top-p 1 --min-p 0 --repeat-penalty 1 \
  --n-predict "$RDFSOLVE_MAX_TOKENS" --timeout 7500 \
  >"$OUTPUT_DIR/llama-server.out" 2>"$OUTPUT_DIR/llama-server.err" &
SERVER_PID=$!

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
stage=notebook
"$PYTHON" -m nbconvert --to notebook --execute "$REPO/notebooks/mcp/$RDFSOLVE_NOTEBOOK" \
  --output-dir "$OUTPUT_DIR" --output executed.ipynb \
  --ExecutePreprocessor.kernel_name=rdfsolve \
  --ExecutePreprocessor.timeout=-1 &
NOTEBOOK_PID=$!
wait "$NOTEBOOK_PID"
NOTEBOOK_PID=
stage=complete
