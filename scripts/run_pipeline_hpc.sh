#!/usr/bin/env bash
# =============================================================================
# run_pipeline_hpc.sh - RDFSolve pipeline for TGX-HPC (Singularity / SLURM)
# =============================================================================
#
# Differences from run_pipeline.sh (Docker version):
#   - No Docker / docker compose. The pipeline Python code runs NATIVELY.
#   - QLever runs inside a Singularity container (pulled from Docker Hub).
#   - Paths are host-side; /trinity/storage is used for data + output.
#   - Designed to be launched by a SLURM sbatch script.
#
# USAGE (direct):
#   bash scripts/run_pipeline_hpc.sh --dataset aopwikirdf --skip-remote
#
# USAGE (via SLURM):
#   sbatch scripts/slurm_pipeline.sh
#
# KEY ENV VARS (set by the SLURM wrapper or override here):
#   DATA_DIR    - where RDF dumps + QLever indexes are stored  (required)
#   OUTPUT_DIR  - where mined schemas + reports go             (required)
#   RESULTS_DIR - host dir to copy final output to             (optional)
#   SINGULARITY_IMAGE - path to qlever.sif (pulled if missing) (optional)
#
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ═══════════════════════════════════════════════════════════════════
# Defaults
# ═══════════════════════════════════════════════════════════════════
DATASETS=()
FILTER=""
SKIP_REMOTE=false
SKIP_LOCAL=false
BENCHMARK=true
BASE_PORT=7019
TIMEOUT=120
ONE_SHOT=true
CHUNK_SIZE=10000
CLASS_BATCH_SIZE=15
DISK_SPACE_FACTOR=12
DISK_SPACE_MIN_MB=500

# Prevent curl and Python requests from using proxy for QLever instances
export no_proxy="localhost,127.0.0.1,${no_proxy:-}"
export NO_PROXY="localhost,127.0.0.1,${NO_PROXY:-}"

# Directories — override via env vars or CLI flags
DATA_DIR="${DATA_DIR:-${REPO_ROOT}/data/rdf}"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_ROOT}/results/output}"
RESULTS_DIR="${RESULTS_DIR:-${REPO_ROOT}/results}"

# Singularity image for QLever
SINGULARITY_IMAGE="${SINGULARITY_IMAGE:-${DATA_DIR}/qlever.sif}"
QLEVER_DOCKER_IMAGE="docker://docker.io/adfreiburg/qlever:latest"

# ═══════════════════════════════════════════════════════════════════
# Colours & helpers
# ═══════════════════════════════════════════════════════════════════
# Terminal Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Helpers
banner()  { echo -e "\n${BLUE}════════════════════════════════════════\n  $1\n════════════════════════════════════════${NC}\n"; }
step()    { echo -e "${GREEN}▸ $1${NC}"; }
warn()    { 
    echo -e "${YELLOW}⚠ $1${NC}"
    if command -v _notify >/dev/null 2>&1; then
        _notify "rdfsolve warning ⚠️" "$1" "default"
    fi
}
fail()    { 
    echo -e "${RED}✗ $1${NC}" >&2
    if command -v _notify >/dev/null 2>&1; then
        _notify "rdfsolve error ❌" "$1" "high"
    fi
}
success() {
    echo -e "${GREEN}✓ $1${NC}"
    if command -v _notify >/dev/null 2>&1; then
        _notify "rdfsolve success ✅" "$1" "default"
    fi
}
elapsed() { printf '%02d:%02d:%02d' $(($1/3600)) $((($1%3600)/60)) $(($1%60)); }

# ═══════════════════════════════════════════════════════════════════
# Parse arguments
# ═══════════════════════════════════════════════════════════════════
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)            DATASETS+=("$2");        shift 2 ;;
        --filter)             FILTER="$2";             shift 2 ;;
        --skip-remote)        SKIP_REMOTE=true;        shift ;;
        --skip-local)         SKIP_LOCAL=true;         shift ;;
        --skip-mappings)      shift ;;  # mappings skipped by default on HPC
        --data-dir)           DATA_DIR="$2";           shift 2 ;;
        --output-dir)         OUTPUT_DIR="$2";         shift 2 ;;
        --results-dir)        RESULTS_DIR="$2";        shift 2 ;;
        --base-port)          BASE_PORT="$2";          shift 2 ;;
        --timeout)            TIMEOUT="$2";            shift 2 ;;
        --chunk-size)         CHUNK_SIZE="$2";         shift 2 ;;
        --singularity-image)  SINGULARITY_IMAGE="$2";  shift 2 ;;
        --help|-h)            head -30 "$0" | grep "^#" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                    fail "Unknown option: $1"; exit 1 ;;
    esac
done

# Build FILTER from --dataset args
if [[ ${#DATASETS[@]} -gt 0 && -z "${FILTER}" ]]; then
    FILTER="^($(IFS='|'; echo "${DATASETS[*]}"))$"
fi

mkdir -p "${DATA_DIR}" "${OUTPUT_DIR}" "${RESULTS_DIR}"

QLEVER_WORKDIRS="${DATA_DIR}/qlever_workdirs"
mkdir -p "${QLEVER_WORKDIRS}"

# ═══════════════════════════════════════════════════════════════════
# QLever via Singularity helpers
# ═══════════════════════════════════════════════════════════════════

# Pull the QLever Singularity image if not already present.
_ensure_singularity_image() {
    if [[ -f "${SINGULARITY_IMAGE}" ]]; then
        step "Singularity image already present: ${SINGULARITY_IMAGE}"
        return 0
    fi
    step "Pulling QLever Singularity image …"
    mkdir -p "$(dirname "${SINGULARITY_IMAGE}")"
    singularity pull --disable-cache "${SINGULARITY_IMAGE}" "${QLEVER_DOCKER_IMAGE}"
    step "Image saved: ${SINGULARITY_IMAGE}"
}

# Run a command inside the QLever Singularity container.
# Bind-mounts the workdir so QLever can read/write index files.
_qlever_run() {
    local workdir="$1"; shift
    singularity exec \
        --bind "${workdir}:${workdir}" \
        --bind "${DATA_DIR}:${DATA_DIR}" \
        --pwd "${workdir}" \
        "${SINGULARITY_IMAGE}" \
        "$@"
}

# Run qlever CLI command (get-data / index / start / stop) inside container.
# The qlever CLI binary is bundled in the adfreiburg/qlever image.
_qlever_cmd() {
    local workdir="$1"; shift
    _qlever_run "${workdir}" qlever "$@"
}

# Start QLever server in background (Singularity instance).
# Returns the instance name.
_qlever_start() {
    local name="$1"
    local workdir="$2"
    local port="$3"
    local instance_name="qlever_${name}"

    # Stop any stale instance
    singularity instance stop "${instance_name}" 2>/dev/null || true

    (cd "${workdir}" && singularity exec \
        --bind "${workdir}:${workdir}" \
        --bind "${DATA_DIR}:${DATA_DIR}" \
        "${SINGULARITY_IMAGE}" \
        qlever start --port "${port}") > "${workdir}/start.log" 2>&1

    # Wait for the SPARQL endpoint to come up (max 60s)
    local i=0
    until curl --noproxy '*' -sf "http://localhost:${port}/?query=ASK%7B%7D" >/dev/null 2>&1; do
        sleep 2; i=$((i+2))
        [[ $i -ge 60 ]] && { fail "[${name}] QLever did not start within 60s"; return 1; }
    done
    step "[${name}] QLever server ready on port ${port}."
    echo "${instance_name}"
}

# Stop a running QLever Singularity instance.
_qlever_stop() {
    local instance_name="$1"
    local port="$2"
    local workdir="$3"
    
    (cd "${workdir}" && singularity exec \
        --bind "${workdir}:${workdir}" \
        --bind "${DATA_DIR}:${DATA_DIR}" \
        "${SINGULARITY_IMAGE}" \
        qlever stop --port "${port}") > "${workdir}/stop.log" 2>&1 || true
}

# ═══════════════════════════════════════════════════════════════════
# Disk space check (native, no container needed)
# ═══════════════════════════════════════════════════════════════════
_check_disk_space() {
    local name="$1"
    local workdir="$2"
    local factor="${DISK_SPACE_FACTOR:-12}"
    local min_mb="${DISK_SPACE_MIN_MB:-500}"

    local url_info
    url_info=$(python3 - "${name}" "${REPO_ROOT}/data/sources.yaml" <<'PYEOF'
import sys, json, yaml
name, yaml_path = sys.argv[1], sys.argv[2]
with open(yaml_path) as f:
    sources = yaml.safe_load(f)
entry = next((s for s in sources if s['name'] == name), None)
if not entry:
    print(json.dumps({"urls": [], "tar_url": None}))
    sys.exit(0)
urls = []
for k, v in entry.items():
    if k.startswith('download_'):
        items = v if isinstance(v, list) else [v]
        urls.extend(u for u in items if u)
tar_url = entry.get('local_tar_url')
print(json.dumps({"urls": urls, "tar_url": tar_url}))
PYEOF
    )

    local check_urls=()
    local tar_url
    tar_url=$(echo "${url_info}" | python3 -c "import json,sys; print(json.load(sys.stdin)['tar_url'] or '')" 2>/dev/null || true)
    if [[ -n "${tar_url}" ]]; then
        check_urls=("${tar_url}")
    else
        while IFS= read -r u; do [[ -n "$u" ]] && check_urls+=("$u"); done < <(
            echo "${url_info}" | python3 -c "import json,sys; [print(u) for u in json.load(sys.stdin)['urls']]" 2>/dev/null || true
        )
    fi

    [[ ${#check_urls[@]} -eq 0 ]] && { warn "[${name}] No URLs for disk check."; return 0; }

    local total_compressed=0
    for url in "${check_urls[@]}"; do
        local cl
        cl=$(curl -sI --max-time 15 --location "${url}" \
            | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2; exit}')
        [[ "${cl}" =~ ^[0-9]+$ ]] && total_compressed=$(( total_compressed + cl ))
    done

    [[ "${total_compressed}" -eq 0 ]] && { warn "[${name}] Could not estimate size, skipping disk check."; return 0; }

    local required=$(( total_compressed * factor ))
    local required_mb=$(( required / 1048576 ))
    local compressed_mb=$(( total_compressed / 1048576 ))
    step "[${name}] ~${compressed_mb} MiB download → ~${required_mb} MiB estimated need (×${factor})"

    mkdir -p "${workdir}"
    local free_bytes
    free_bytes=$(df --output=avail -B1 "${workdir}" 2>/dev/null | tail -1 | tr -d ' ')
    free_bytes="${free_bytes//[^0-9]/}"
    free_bytes="${free_bytes:-0}"
    local free_mb=$(( free_bytes / 1048576 ))
    local min_bytes=$(( min_mb * 1048576 ))

    step "[${name}] Free: ${free_mb} MiB"

    if [[ "${free_bytes}" -lt "${required}" ]] || [[ "${free_bytes}" -lt "${min_bytes}" ]]; then
        fail "[${name}] Insufficient disk space (need ~${required_mb} MiB, have ${free_mb} MiB). Skipping."
        return 1
    fi
    return 0
}

# ═══════════════════════════════════════════════════════════════════
# Mining helpers (call Python mine_local.py directly — no container)
# ═══════════════════════════════════════════════════════════════════
_mine_local() {
    local name="$1" port="$2" strat="$3" one_shot="$4" cs="$5" cb="$6"
    local out="${OUTPUT_DIR}/${name}/qlever/${strat}/chunk${cs}_batch${cb}"
    [[ "${one_shot}" == true ]] && out="${OUTPUT_DIR}/${name}/qlever/${strat}/one_shot"

    local flags="--name ${name} --endpoint http://localhost:${port}"
    flags+=" --discover-first --output-dir ${out}"
    flags+=" --chunk-size ${cs} --class-batch-size ${cb}"
    [[ "${strat}" == "untyped" ]] && flags+=" --untyped-as-classes"
    [[ "${one_shot}" == true   ]] && flags+=" --one-shot"
    [[ "${BENCHMARK}" == true  ]] && flags+=" --benchmark"

    step "  [${name}] Mining qlever/${strat}/$(basename ${out}) …"
    eval "python3 ${REPO_ROOT}/scripts/mine_local.py local-mine ${flags}" \
        || warn "[${name}] Mining pass failed."
}

# ═══════════════════════════════════════════════════════════════════
# PREFLIGHT
# ═══════════════════════════════════════════════════════════════════
banner "RDFSolve HPC Pipeline"
echo -e "  Datasets:   ${BOLD}${FILTER:-<all>}${NC}"
echo -e "  DATA_DIR:   ${DATA_DIR}"
echo -e "  OUTPUT_DIR: ${OUTPUT_DIR}"
echo ""

# Check Python environment
python3 -c "import yaml, rdflib" 2>/dev/null \
    || { fail "Python deps missing. Run: pip install pyyaml rdflib"; exit 1; }

# Check singularity
command -v singularity >/dev/null 2>&1 \
    || { fail "singularity not found in PATH."; exit 1; }
step "Singularity: $(singularity --version)"

PIPELINE_START=$(date +%s)

# ═══════════════════════════════════════════════════════════════════
# STEP 0 - Pull QLever Singularity image
# ═══════════════════════════════════════════════════════════════════
banner "Step 0: Ensure QLever Singularity image"
_ensure_singularity_image

# ═══════════════════════════════════════════════════════════════════
# STEP 1 - Remote VoID discovery
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false ]]; then
    banner "Step 1: Remote VoID discovery"
    t0=$(date +%s)
    local_flags="--output-dir ${OUTPUT_DIR} --timeout ${TIMEOUT}"
    [[ -n "${FILTER}" ]] && local_flags+=" --filter '${FILTER}'"
    eval "python3 ${REPO_ROOT}/scripts/mine_local.py discover ${local_flags}" \
        || warn "Some discover tasks failed."
    step "Discovery done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 1: Remote VoID discovery - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 2 - Remote schema mining
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false ]]; then
    banner "Step 2: Remote schema mining"
    t0=$(date +%s)
    local_flags="--output-dir ${OUTPUT_DIR} --timeout ${TIMEOUT}"
    [[ -n "${FILTER}" ]] && local_flags+=" --filter '${FILTER}'"
    [[ "${BENCHMARK}" == true ]] && local_flags+=" --benchmark"
    eval "python3 ${REPO_ROOT}/scripts/mine_local.py mine ${local_flags}" \
        || warn "Remote mining had failures."
    step "Remote mining done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 2: Remote mining - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEPS 3-4 - Local mining (QLever via Singularity)
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_LOCAL}" == false ]]; then

    # Step 3: Generate Qleverfiles
    banner "Step 3: Generate Qleverfiles"
    qf_flags="--data-dir ${DATA_DIR} --base-port ${BASE_PORT} --output-dir ${OUTPUT_DIR}"
    [[ -n "${FILTER}" ]] && qf_flags+=" --filter '${FILTER}'"
    eval "python3 ${REPO_ROOT}/scripts/mine_local.py generate-qleverfile ${qf_flags}" \
        || { fail "Qleverfile generation failed."; exit 1; }

    # Override SYSTEM in all generated Qleverfiles: docker -> singularity
    # The qlever CLI uses SYSTEM = singularity and IMAGE = path/to/qlever.sif
    find "${QLEVER_WORKDIRS}" -name "Qleverfile" | while read -r qf; do
        sed -i "s|^SYSTEM *=.*|SYSTEM = native|" "${qf}"
        # Also point IMAGE to our local .sif
        sed -i "s|^IMAGE *=.*|IMAGE  = ${SINGULARITY_IMAGE}|" "${qf}"
    done
    step "Qleverfiles ready (SYSTEM=native, IMAGE=${SINGULARITY_IMAGE})."

    # Step 4: For each dataset: download → index → start → mine → stop → cleanup
    banner "Step 4: Download → Index → Mine"

    PORTS_JSON="${QLEVER_WORKDIRS}/ports.json"
    if [[ ! -f "${PORTS_JSON}" ]]; then
        warn "No ports.json found at ${PORTS_JSON} — nothing to process."
    else
        DS_LINES=$(python3 -c "
import json
with open('${PORTS_JSON}') as f:
    d = json.load(f)
for name, port in d.items():
    print(f'{name} {port}')
")
        TOTAL=$(echo "${DS_LINES}" | grep -c . || echo 0)
        IDX=0

        while read -r NAME PORT; do
            IDX=$((IDX + 1))
            echo ""
            echo -e "${BOLD}  [${IDX}/${TOTAL}] ${NAME}  (port ${PORT})${NC}"
            echo -e "${BOLD}──────────────────────────────────────────${NC}"

            WORKDIR="${QLEVER_WORKDIRS}/${NAME}"
            DONE_INDEX="${WORKDIR}/.index.done"
            mkdir -p "${WORKDIR}"

            # ── Download + Index ──────────────────────────────────
            if [[ -f "${DONE_INDEX}" ]]; then
                step "Index cached — skipping download+index."
            else
                _check_disk_space "${NAME}" "${WORKDIR}" || { continue; }

                step "Downloading …"
                _qlever_cmd "${WORKDIR}" get-data \
                    || { fail "[${NAME}] Download failed."; continue; }

                # Verify RDF input exists
                INPUT_GLOB=$(grep '^INPUT_FILES' "${WORKDIR}/Qleverfile" 2>/dev/null \
                    | head -1 | sed 's/.*=[ ]*//')
                INPUT_BYTES=0
                if [[ -n "${INPUT_GLOB}" ]]; then
                    for f in ${WORKDIR}/${INPUT_GLOB}; do
                        [[ -f "$f" ]] && INPUT_BYTES=$(( INPUT_BYTES + $(stat -c%s "$f" 2>/dev/null || echo 0) ))
                    done
                fi

                if [[ "${INPUT_BYTES}" -eq 0 ]]; then
                    fail "[${NAME}] Download produced 0 bytes of RDF input. Skipping."
                    continue
                fi

                step "Indexing …"
                _qlever_cmd "${WORKDIR}" index --overwrite-existing \
                    2>&1 | grep -v '^$' \
                    || { fail "[${NAME}] Index failed."; continue; }

                touch "${DONE_INDEX}"
            fi

            # ── Start QLever server ───────────────────────────────
            step "Starting QLever on port ${PORT} …"
            INSTANCE_NAME=$(_qlever_start "${NAME}" "${WORKDIR}" "${PORT}") \
                || { fail "[${NAME}] Server start failed."; continue; }

            # ── Mine ─────────────────────────────────────────────
            if [[ "${ONE_SHOT}" == true ]]; then
                _mine_local "${NAME}" "${PORT}" "typed" true \
                    "${CHUNK_SIZE}" "${CLASS_BATCH_SIZE}"
            fi

            # ── Stop server ───────────────────────────────────────
            step "Stopping QLever …"
            _qlever_stop "${INSTANCE_NAME}" "${PORT}" "${WORKDIR}"

            # ── Cleanup raw RDF files ─────────────────────────────
            # Index files stay (needed for final LSOLD step).
            step "Cleaning up raw RDF files for ${NAME} …"
            rdf_dir="${WORKDIR}/rdf"
            if [[ -d "${rdf_dir}" ]]; then
                du -sh "${rdf_dir}" 2>/dev/null && rm -rf "${rdf_dir}"
                step "  Deleted ${rdf_dir}"
            fi
            find "${WORKDIR}" -maxdepth 1 \
                \( -name '*.tar' -o -name '*.tar.gz' -o -name '*.tgz' \
                -o -name '*.zip' -o -name '*.gz' -o -name '*.xz' \) \
                -delete 2>/dev/null || true

            success "[${NAME}] pipeline steps completed."

        done <<< "${DS_LINES}"
    fi
else
    step "Local processing skipped (--skip-local)."
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 5-11 - Mappings (skipped — require LSLOD endpoint)
# ═══════════════════════════════════════════════════════════════════
banner "Steps 5-11: Mappings - SKIPPED (run separately with --lslod-endpoint)"

# ═══════════════════════════════════════════════════════════════════
# STEP 12 - Copy results
# ═══════════════════════════════════════════════════════════════════
banner "Step 12: Collect results → ${RESULTS_DIR}/"
rsync -a --info=progress2 "${OUTPUT_DIR}/" "${RESULTS_DIR}/" 2>/dev/null \
    || cp -r "${OUTPUT_DIR}/." "${RESULTS_DIR}/"
step "Results collected."

# ═══════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════
TOTAL_ELAPSED=$(( $(date +%s) - PIPELINE_START ))
banner "Pipeline complete"
echo -e "  Total time: ${BOLD}$(elapsed ${TOTAL_ELAPSED})${NC}"
echo -e "  Results in: ${BOLD}${RESULTS_DIR}/${NC}"
FILE_COUNT=$(find "${RESULTS_DIR}" -type f 2>/dev/null | wc -l)
echo -e "  Files:      ${BOLD}${FILE_COUNT}${NC}"
echo ""

if [[ "${FILE_COUNT}" -eq 0 ]]; then
    fail "0 files found in results. Pipeline failed to output data!"
    exit 1
fi

echo -e "${GREEN}${BOLD}Done.${NC}"
