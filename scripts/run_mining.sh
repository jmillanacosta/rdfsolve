#!/bin/bash
# Run a prepared mining environment. Only prepare mode downloads. Grouped mode may
# build its provider index from prepared inputs: members are never mined one by one.
set -euo pipefail
trap 'status=$?; echo "Mining launcher failed (exit $status) at line $LINENO" >&2; exit "$status"' ERR

mode="${1:?Use remote, grouped, local, or prepare}"
shift
case "$mode" in remote|grouped|local|prepare) ;; *) echo "Unknown mode: $mode" >&2; exit 2 ;; esac
repo="${RDFSOLVE_REPO:?Set RDFSOLVE_REPO to the checkout}"
cd -- "$repo"
repo="$PWD"
python="${VENV_PATH:-$repo/.venv}/bin/python"
data="${DATA_DIR:-$(dirname -- "$repo")/data}"
registry="${SOURCES_FILE:-$repo/data/sources.yaml}"
output="${OUTPUT_DIR:-$(dirname -- "$repo")/runs/${mode}-${SLURM_JOB_ID:-manual}-$(date -u +%Y%m%dT%H%M%S)-$$}"
# Prepare is local mining that may download and index; the others use prepared inputs only.
if [ "$mode" = prepare ]; then select=--local-only; suffix=_local; cache=();
elif [ "$mode" = grouped ]; then select=--grouped-only; suffix=_grouped; cache=(--no-download);
else select="--$mode-only"; suffix="_$mode"; cache=(--no-download --no-index); fi
test -x "$python" || { echo "Prepare the Python environment: $python" >&2; exit 2; }
test -r "$registry" || { echo "Source registry not readable: $registry" >&2; exit 2; }

# Keep remote proxy settings, but connect to local QLever directly.
export no_proxy="localhost,127.0.0.1,::1${no_proxy:+,$no_proxy}"
export NO_PROXY="$no_proxy${NO_PROXY:+,$NO_PROXY}"
export RDFSOLVE_HTTP_LOCK_DIR="${RDFSOLVE_HTTP_LOCK_DIR:-$repo/../.rdfsolve-http-locks}"
export PYTHONUNBUFFERED=1
export PYTHONFAULTHANDLER=1
"$python" -c 'import pathlib, rdfsolve; p=pathlib.Path(rdfsolve.__file__).resolve(); print("Package:", p); assert p.is_relative_to(pathlib.Path.cwd() / "src"), "Install this checkout in the selected environment"'

preflight_only=false
for arg in "$@"; do
    case "$arg" in
        --preflight) preflight_only=true ;;
        --output-dir*|--data-dir*|--sources-file*|--*-only)
            echo "Choose the wrapper mode; use OUTPUT_DIR, DATA_DIR, SOURCES_FILE for paths" >&2
            exit 2 ;;
        --skip-completed) echo "Use a new output directory; resume is not validated" >&2; exit 2 ;;
    esac
done
args=(scripts/pipeline.py "$select" --sources-file "$registry"
      --output-dir "$output" --data-dir "$data" --output-suffix "$suffix"
      --extract-ontology --extract-metadata --ontology-as-data
      --skip-mappings --skip-inference --skip-analysis
      "${cache[@]}" "$@")
echo "Mode: $mode; job: ${SLURM_JOB_ID:-manual}; node: $(hostname)"
echo "Output: $output"
"$python" "${args[@]}" --preflight
if "$preflight_only"; then exit 0; fi

# Refuse an existing run directory, including an empty one.
mkdir -p -- "$(dirname -- "$output")"
mkdir -- "$output"
git rev-parse HEAD > "$output/code_commit.txt"
"$python" -c 'import importlib.metadata as m; print("\n".join(sorted({"%s==%s" % (d.name, d.version) for d in m.distributions()}, key=str.lower)))' > "$output/environment.txt"
cp -- "$registry" "$output/sources.yaml"
exec "$python" "${args[@]}"
