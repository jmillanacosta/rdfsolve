#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh - End-to-end RDFSolve mining pipeline
# =============================================================================
#
# OVERVIEW
# --------
# Every step (schema mining, mapping seeding, graph building, notebook
# execution) runs INSIDE the Docker pipeline container.  The host only
# needs bash + docker compose.  At the end (Step 15) the output volume
# is extracted to a host-side results directory.
#
# STEPS
#   0.  Build Docker pipeline image
#   1.  Remote VoID discovery (skip with --skip-remote)
#   2.  Remote schema mining  (skip with --skip-remote)
#   3.  Generate Qleverfiles  (skip with --skip-local)
#   4.  Download -> QLever index -> mine (skip with --skip-local)
#   4b. Schema selection -> /output/paper_data/schemas/
#   5.  Seed SSSOM mappings (class + property)
#   6.  Seed SeMRA mappings
#   7.  Seed instance mappings
#   8.  Build class index (instance IRI -> rdf:type lookup via LSLOD)
#   9.  Derive class-level mappings from instance mappings
#   10. Enrich instance JSON-LD with class annotations
#   11. Inference expansion
#   12. Build connectivity graphs -> Parquet
#   14. Execute paper notebook -> HTML
#   15. Collect /output -> host results directory
#
# MINI MODE
#   --mini is a convenience preset for local testing:
#     - Default datasets: set from script
#     - Implies --skip-remote (no network SPARQL mining)
#     - Implies --one-shot QLever strategy
#
# USAGE
#   ./scripts/run_pipeline.sh --mini
#   ./scripts/run_pipeline.sh --mini --dataset aopwikirdf
#   ./scripts/run_pipeline.sh --data-dir /mnt/data
#   ./scripts/run_pipeline.sh --dataset drugbank --dataset chembl
#   ./scripts/run_pipeline.sh --skip-remote
#   ./scripts/run_pipeline.sh --skip-local
#   ./scripts/run_pipeline.sh --skip-mappings
#   ./scripts/run_pipeline.sh --skip-build-graphs
#   ./scripts/run_pipeline.sh --skip-notebook
#   ./scripts/run_pipeline.sh --skip-class-derivation
#   ./scripts/run_pipeline.sh --lslod-endpoint http://lslod.example.org/api/sparql
#   ./scripts/run_pipeline.sh --chunk-size 5000
#   ./scripts/run_pipeline.sh --chunk-sizes 1000,5000,10000
#   ./scripts/run_pipeline.sh --class-batch-size 10
#   ./scripts/run_pipeline.sh --class-batch-sizes 10,15,20
#   ./scripts/run_pipeline.sh --class-chunk-size 50000
#   ./scripts/run_pipeline.sh --remote-chunk-sizes 5000,10000
#   ./scripts/run_pipeline.sh --remote-batch-sizes 10,15
#   ./scripts/run_pipeline.sh --both-strategies
#   ./scripts/run_pipeline.sh --untyped-as-classes
#   ./scripts/run_pipeline.sh --one-shot
#   ./scripts/run_pipeline.sh --no-one-shot
#   ./scripts/run_pipeline.sh --no-benchmark
#   ./scripts/run_pipeline.sh --base-port 7100
#   ./scripts/run_pipeline.sh --timeout 180
#   ./scripts/run_pipeline.sh --results-dir /path/to/results
#   ./scripts/run_pipeline.sh --data-dir /path/to/rdf-data
#   ./scripts/run_pipeline.sh --output-dir /path/to/output
#   ./scripts/run_pipeline.sh --author "Javier"
#   ./scripts/run_pipeline.sh --help
#
# =============================================================================
set -euo pipefail

# Resolve repo root immediately (needed for DC= and docker-compose path).
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ═══════════════════════════════════════════════════════════════════
# Defaults
# ═══════════════════════════════════════════════════════════════════
MODE="all"
DATASETS=()
FILTER=""
SKIP_REMOTE=false
SKIP_LOCAL=false
RESULTS_DIR="./results"
DATA_DIR=""
OUTPUT_DIR=""
BENCHMARK=true
BASE_PORT=7019
TIMEOUT=120
UNTYPED_AS_CLASSES=false
BOTH_STRATEGIES=false
AUTHORS_FLAGS=""
ONE_SHOT=false
NO_ONE_SHOT=false
CHUNK_SIZE=10000
CHUNK_SIZES=""
CLASS_BATCH_SIZE=15
CLASS_BATCH_SIZES=""
CLASS_CHUNK_SIZE=""
REMOTE_CHUNK_SIZES=""
REMOTE_BATCH_SIZES=""
MINI=false
SKIP_MAPPINGS=false
SKIP_BUILD_GRAPHS=false
SKIP_NOTEBOOK=false
SKIP_CLASS_DERIVATION=false
LSLOD_ENDPOINT=""   # e.g. http://lslod.example.org/api/sparql

DC="docker compose -f ${REPO_ROOT}/docker-compose.pipeline.yml"

# ═══════════════════════════════════════════════════════════════════
# Colours & helpers
# ═══════════════════════════════════════════════════════════════════
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

banner()  {
    echo ""
    echo -e "${BLUE}${BOLD}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}════════════════════════════════════════════════════════════${NC}"
    echo ""
}
step()    { echo -e "${GREEN}▸ $1${NC}"; }
warn()    { echo -e "${YELLOW}⚠ $1${NC}"; }
fail()    { echo -e "${RED}✗ $1${NC}" >&2; }
elapsed() { printf '%02d:%02d:%02d' $(($1/3600)) $((($1%3600)/60)) $(($1%60)); }

# Run a command inside the pipeline container (stdin ← /dev/null so
# docker can never steal input from the outer shell loop).
run() { ${RUN} "$@" </dev/null; }

# _summarise_index_warnings <name> <workdir>
# Grep the QLever index log for WARN lines, count each distinct warning
# type (IRI stripped so all "IRI ref not standard-compliant" entries
# collapse into one bucket), print a brief inline summary, and append/
# merge the counts into  $CONTAINER_OUTPUT_DIR/index_warnings.json.
#
# JSON schema:
# {
#   "drugbank": {
#     "total_warnings": 340,
#     "generated_utc": "2026-03-09T17:00:00Z",
#     "breakdown": {
#       "WARN: IRI ref not standard-compliant:": 298,
#       "WARN: Some other warning:": 42
#     }
#   },
#   ...
# }
_summarise_index_warnings() {
    local name="$1"
    local workdir="$2"
    local logfile="${workdir}/${name}.index-log.txt"

    [[ -f "${logfile}" ]] || return 0

    local total_warns
    total_warns=$(grep -c 'WARN:' "${logfile}" 2>/dev/null || true)
    total_warns="${total_warns//[^0-9]/}"
    total_warns="${total_warns:-0}"

    [[ "${total_warns}" -eq 0 ]] && return 0

    warn "[${name}] ${total_warns} QLever index warnings"

    # Build breakdown: strip trailing IRI, count, sort
    local warn_summary
    warn_summary=$(
        grep -o 'WARN:.*' "${logfile}" \
        | sed 's/ <[^>]*>//' \
        | sort | uniq -c | sort -rn
    )

    # Print top-10 inline
    echo -e "${YELLOW}  Warning breakdown:${NC}"
    echo "${warn_summary}" | head -10 | while IFS= read -r line; do
        echo -e "${YELLOW}    ${line}${NC}"
    done
    local n_types
    n_types=$(echo "${warn_summary}" | wc -l)
    n_types="${n_types//[^0-9]/}"
    n_types="${n_types:-0}"
    [[ "${n_types}" -gt 10 ]] && \
        echo -e "${YELLOW}    … (${n_types} distinct types total - see index_warnings.json)${NC}"

    # ── Build / merge JSON ──────────────────────────────────────────
    local json_file="${CONTAINER_OUTPUT_DIR}/index_warnings.json"
    local ts
    ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

    # Build the breakdown JSON fragment
    local breakdown_json
    breakdown_json=$(
        echo "${warn_summary}" | awk '
        BEGIN { printf "{" }
        {
            # $1 = count, $2..$NF = message words
            count = $1
            msg = ""
            for (i = 2; i <= NF; i++) {
                msg = msg (i == 2 ? "" : " ") $i
            }
            # Escape backslashes and double-quotes for JSON
            gsub(/\\/, "\\\\", msg)
            gsub(/"/, "\\\"", msg)
            if (NR > 1) printf ","
            printf "\"%s\":%s", msg, count
        }
        END { printf "}" }
        '
    )

    # Build the entry for this dataset
    local entry
    entry=$(printf '{"total_warnings":%s,"generated_utc":"%s","breakdown":%s}' \
        "${total_warns}" "${ts}" "${breakdown_json}")

    # Merge into the existing JSON file (or create it).
    # Uses python3 (always available in the container) for robust JSON merge.
    run python3 - "${json_file}" "${name}" "${entry}" <<'PYEOF'
import sys, json, pathlib
path, name, entry_str = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
data = json.loads(path.read_text()) if path.exists() and path.stat().st_size else {}
data[name] = json.loads(entry_str)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(data, indent=2))
PYEOF

    echo -e "${GREEN}  ↳ index_warnings.json updated (${json_file})${NC}"
}

# ═══════════════════════════════════════════════════════════════════
# Parse arguments
# ═══════════════════════════════════════════════════════════════════
while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)                 MODE="all";              shift ;;
        --test)                MODE="test";             shift ;;
        --dataset)             DATASETS+=("$2");        shift 2 ;;
        --filter)              FILTER="$2";             shift 2 ;;
        --skip-remote)         SKIP_REMOTE=true;        shift ;;
        --skip-local)          SKIP_LOCAL=true;         shift ;;
        --results-dir)         RESULTS_DIR="$2";        shift 2 ;;
        --data-dir)            DATA_DIR="$2";           shift 2 ;;
        --output-dir)          OUTPUT_DIR="$2";         shift 2 ;;
        --no-benchmark)        BENCHMARK=false;         shift ;;
        --untyped-as-classes)  UNTYPED_AS_CLASSES=true; shift ;;
        --both-strategies)     BOTH_STRATEGIES=true;    shift ;;
        --author)              AUTHORS_FLAGS+=" --author '$2'"; shift 2 ;;
        --one-shot)            ONE_SHOT=true;           shift ;;
        --no-one-shot)         NO_ONE_SHOT=true;        shift ;;
        --chunk-size)          CHUNK_SIZE="$2";         shift 2 ;;
        --chunk-sizes)         CHUNK_SIZES="$2";        shift 2 ;;
        --class-batch-size)    CLASS_BATCH_SIZE="$2";   shift 2 ;;
        --class-batch-sizes)   CLASS_BATCH_SIZES="$2";  shift 2 ;;
        --class-chunk-size)    CLASS_CHUNK_SIZE="$2";   shift 2 ;;
        --remote-chunk-sizes)  REMOTE_CHUNK_SIZES="$2"; shift 2 ;;
        --remote-batch-sizes)  REMOTE_BATCH_SIZES="$2"; shift 2 ;;
        --base-port)           BASE_PORT="$2";          shift 2 ;;
        --timeout)             TIMEOUT="$2";            shift 2 ;;
        --mini)                MINI=true;               shift ;;
        --skip-mappings)       SKIP_MAPPINGS=true;      shift ;;
        --skip-build-graphs)   SKIP_BUILD_GRAPHS=true;  shift ;;
        --skip-notebook)       SKIP_NOTEBOOK=true;      shift ;;
        --skip-class-derivation) SKIP_CLASS_DERIVATION=true; shift ;;
        --lslod-endpoint)      LSLOD_ENDPOINT="$2";     shift 2 ;;
        --help|-h)             head -65 "$0" | grep -E "^#" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                     fail "Unknown option: $1"; echo "Use --help for usage." >&2; exit 1 ;;
    esac
done

# ═══════════════════════════════════════════════════════════════════
# Derived values
# ═══════════════════════════════════════════════════════════════════

# ── --mini preset ───────────────────────────────────────────────
if [[ "${MINI}" == true ]]; then
    [[ ${#DATASETS[@]} -eq 0 ]] && DATASETS=(drugbank aopwikirdf kegg mesh wikipathways)
    SKIP_REMOTE=true
    ONE_SHOT=true
    [[ -z "${DATA_DIR}" ]] && DATA_DIR="${REPO_ROOT}/data/rdf"
fi

# Build FILTER regex from --dataset args (--filter overrides explicit list).
if [[ ${#DATASETS[@]} -gt 0 && -z "${FILTER}" ]]; then
    FILTER="^($(IFS='|'; echo "${DATASETS[*]}"))$"
fi

# Do we have an explicit sweep?
HAS_SWEEP=false
[[ -n "${CHUNK_SIZES}" || -n "${CLASS_BATCH_SIZES}" ]] && HAS_SWEEP=true

# Auto one-shot: if no sweep sizes and user didn't --no-one-shot.
if [[ "${HAS_SWEEP}" == false && "${NO_ONE_SHOT}" == false ]]; then
    ONE_SHOT=true
fi

# Strategy list.
STRATEGIES=(typed)
[[ "${BOTH_STRATEGIES}" == true || "${UNTYPED_AS_CLASSES}" == true ]] && STRATEGIES+=(untyped)

# Resolve sweep lists.
IFS=',' read -ra LOCAL_CHUNK_LIST  <<< "${CHUNK_SIZES:-${CHUNK_SIZE}}"
IFS=',' read -ra LOCAL_BATCH_LIST  <<< "${CLASS_BATCH_SIZES:-${CLASS_BATCH_SIZE}}"
IFS=',' read -ra REMOTE_CHUNK_LIST <<< "${REMOTE_CHUNK_SIZES:-${CHUNK_SIZES:-${CHUNK_SIZE}}}"
IFS=',' read -ra REMOTE_BATCH_LIST <<< "${REMOTE_BATCH_SIZES:-${CLASS_BATCH_SIZES:-${CLASS_BATCH_SIZE}}}"

# ── Resolve data / output directories ───────────────────────────
# DATA_DIR:   host-side RDF dumps / QLever workdirs.
#             When set, bind-mounted at the same path inside the container.
if [[ -n "${DATA_DIR}" ]]; then
    mkdir -p "${DATA_DIR}"
    DATA_DIR="$(cd "${DATA_DIR}" && pwd)"
    export DATA_DIR
    CONTAINER_DATA_DIR="${DATA_DIR}"
else
    CONTAINER_DATA_DIR="/data"
fi

# OUTPUT_DIR: host-side bind-mount that overrides the pipeline-output volume.
if [[ -n "${OUTPUT_DIR}" ]]; then
    mkdir -p "${OUTPUT_DIR}"
    OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
    export OUTPUT_DIR
    CONTAINER_OUTPUT_DIR="${OUTPUT_DIR}"
else
    CONTAINER_OUTPUT_DIR="/output"
fi

# Mappings directory inside the container - lives under /output so it
# is always on the persistent volume (pipeline-output or bind-mounted OUTPUT_DIR).
CONTAINER_MAPPINGS_DIR="${CONTAINER_OUTPUT_DIR}/mappings"

# Schemas are written by the mining steps to /output/<dataset>/...
# build_graphs.py discovers them there.
CONTAINER_SCHEMAS_DIR="${CONTAINER_OUTPUT_DIR}"

# paper_data lives under /output so it is collected in step 15.
CONTAINER_PAPER_DATA_DIR="${CONTAINER_OUTPUT_DIR}/paper_data"

# Host results directory (step 15 target) - resolve to absolute path.
mkdir -p "${RESULTS_DIR}"
RESULTS_DIR="$(cd "${RESULTS_DIR}" && pwd)"

# ── Compose run prefix ──────────────────────────────────────────
EXTRA_VOLS=""
[[ -n "${DATA_DIR}"   ]] && EXTRA_VOLS+=" -v ${DATA_DIR}:${DATA_DIR}"
[[ -n "${OUTPUT_DIR}" ]] && EXTRA_VOLS+=" -v ${OUTPUT_DIR}:${OUTPUT_DIR}"
RUN="${DC} run --rm -T${EXTRA_VOLS} pipeline"

# ── DS_FILTER_ARGS for build_graphs.py and seed_instance_mappings ──
DS_FILTER_ARGS=()
[[ ${#DATASETS[@]} -gt 0 ]] && for _ds in "${DATASETS[@]}"; do DS_FILTER_ARGS+=(--datasets "$_ds"); done

# ── Common CLI flags for rdfsolve commands ───────────────────────
COMMON_FLAGS="--timeout ${TIMEOUT}"
[[ -n "${FILTER}" ]]         && COMMON_FLAGS+=" --filter '${FILTER}'"
[[ "${BENCHMARK}" == true ]] && COMMON_FLAGS+=" --benchmark"

# ═══════════════════════════════════════════════════════════════════
# Mining-pass helpers
# ═══════════════════════════════════════════════════════════════════

_cfg_label() {  # _cfg_label <one_shot:bool> <csize> <cbatch>
    if [[ "$1" == true ]]; then echo "one_shot"; return; fi
    local lbl="chunk${2}_batch${3}"
    [[ -n "${CLASS_CHUNK_SIZE}" ]] && lbl+="_cc${CLASS_CHUNK_SIZE}"
    echo "${lbl}"
}

_mine_remote() {  # _mine_remote DATASET STRATEGY CSIZE CBATCH
    local ds=$1 strat=$2 cs=$3 cb=$4
    local cfg; cfg=$(_cfg_label false "$cs" "$cb")
    local out="${CONTAINER_OUTPUT_DIR}/${ds}/remote/${strat}/${cfg}"

    local flags="${COMMON_FLAGS} --filter '^${ds}$'"
    flags+=" --output-dir ${out} --chunk-size ${cs} --class-batch-size ${cb}"
    [[ -n "${CLASS_CHUNK_SIZE}" ]] && flags+=" --class-chunk-size ${CLASS_CHUNK_SIZE}"
    [[ "${strat}" == "untyped"  ]] && flags+=" --untyped-as-classes"
    [[ -n "${AUTHORS_FLAGS}"    ]] && flags+="${AUTHORS_FLAGS}"

    step "  [${ds}] remote/${strat}/${cfg}"
    eval "run rdfsolve pipeline mine ${flags}" || warn "[${ds}] remote/${strat}/${cfg} failed."
}

_mine_local() {  # _mine_local NAME PORT STRATEGY ONE_SHOT CSIZE CBATCH
    local name=$1 port=$2 strat=$3 os=$4 cs=$5 cb=$6
    local cfg; cfg=$(_cfg_label "$os" "$cs" "$cb")
    local out="${CONTAINER_OUTPUT_DIR}/${name}/qlever/${strat}/${cfg}"

    local flags="--name ${name} --endpoint http://localhost:${port}"
    flags+=" --discover-first --output-dir ${out}"
    flags+=" --chunk-size ${cs} --class-batch-size ${cb}"
    [[ -n "${CLASS_CHUNK_SIZE}" ]] && flags+=" --class-chunk-size ${CLASS_CHUNK_SIZE}"
    [[ "${strat}" == "untyped"  ]] && flags+=" --untyped-as-classes"
    [[ "${os}" == true           ]] && flags+=" --one-shot"
    [[ "${BENCHMARK}" == true   ]] && flags+=" --benchmark"
    [[ -n "${AUTHORS_FLAGS}"    ]] && flags+="${AUTHORS_FLAGS}"

    step "  Pass -> qlever/${strat}/${cfg}"
    eval "run rdfsolve pipeline local-mine ${flags}" || warn "[${name}] qlever/${strat}/${cfg} failed."
}

# ═══════════════════════════════════════════════════════════════════
# Preflight checks
# ═══════════════════════════════════════════════════════════════════
banner "RDFSolve Pipeline - ${MODE} mode${MINI:+ (mini)}"

[[ -f "${REPO_ROOT}/docker-compose.pipeline.yml" ]] \
    || { fail "docker-compose.pipeline.yml not found at ${REPO_ROOT}."; exit 1; }
docker info >/dev/null 2>&1 \
    || { fail "Docker is not running."; exit 1; }

# Dataset / strategy labels for the summary banner
if   [[ ${#DATASETS[@]} -gt 0 ]]; then _DS_LABEL="${DATASETS[*]}"
elif [[ -n "${FILTER}"         ]]; then _DS_LABEL="(regex) ${FILTER}"
else                                    _DS_LABEL="<all sources>"; fi

if   [[ "${BOTH_STRATEGIES}" == true    ]]; then _STRAT_LBL="typed + untyped"
elif [[ "${UNTYPED_AS_CLASSES}" == true ]]; then _STRAT_LBL="untyped only"
else                                             _STRAT_LBL="typed only"; fi

echo -e "  Datasets:     ${BOLD}${_DS_LABEL}${NC}"
echo -e "  Strategy:     ${_STRAT_LBL}"
echo ""

echo -e "  Local mining"
if   [[ "${SKIP_LOCAL}" == true ]]; then
    echo -e "    (skipped)"
elif [[ "${ONE_SHOT}" == true && "${HAS_SWEEP}" == false ]]; then
    echo -e "    Mode:       one-shot"
elif [[ "${ONE_SHOT}" == true ]]; then
    echo -e "    Sweep:      chunks=[${LOCAL_CHUNK_LIST[*]}]  batches=[${LOCAL_BATCH_LIST[*]}]  + one-shot"
else
    echo -e "    Sweep:      chunks=[${LOCAL_CHUNK_LIST[*]}]  batches=[${LOCAL_BATCH_LIST[*]}]"
fi
echo ""

echo -e "  Remote mining"
if [[ "${SKIP_REMOTE}" == true ]]; then
    echo -e "    (skipped)"
else
    echo -e "    Sweep:      chunks=[${REMOTE_CHUNK_LIST[*]}]  batches=[${REMOTE_BATCH_LIST[*]}]"
fi
echo ""
echo -e "  Results dir:  ${RESULTS_DIR}"
echo ""

PIPELINE_START=$(date +%s)

# ═══════════════════════════════════════════════════════════════════
# STEP 0 - Build image
# ═══════════════════════════════════════════════════════════════════
banner "Step 0: Build pipeline image"
step "Building …"
${DC} build --quiet
step "Image ready."

# ═══════════════════════════════════════════════════════════════════
# STEP 1 - Remote VoID discovery
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false ]]; then
    banner "Step 1: Remote VoID discovery"
    t0=$(date +%s)
    eval "run rdfsolve pipeline discover ${COMMON_FLAGS} --output-dir ${CONTAINER_OUTPUT_DIR}" \
        || warn "Some discover tasks failed."
    step "Discovery done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 1: Remote VoID discovery - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 2 - Remote mining
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false ]]; then
    banner "Step 2: Remote schema mining"
    t0=$(date +%s)

    # Enumerate remote-endpoint datasets from sources.yaml (runs inside container).
    _REMOTE_DS=$(run python3 -c "
import yaml, re
with open('/app/data/sources.yaml') as f:
    sources = yaml.safe_load(f)
filt = '${FILTER}'
pat = re.compile(filt, re.IGNORECASE) if filt else None
for s in sources:
    name = s.get('name','')
    if not s.get('endpoint'): continue
    if pat and not pat.search(name): continue
    print(name)
" 2>/dev/null) || _REMOTE_DS=""

    if [[ -z "${_REMOTE_DS}" ]]; then
        warn "No remote-endpoint datasets found - skipping."
    else
        while IFS= read -r ds; do
            step "Dataset: ${ds}"
            for strat in "${STRATEGIES[@]}"; do
                for cs in "${REMOTE_CHUNK_LIST[@]}"; do
                    for cb in "${REMOTE_BATCH_LIST[@]}"; do
                        _mine_remote "${ds}" "${strat}" "${cs}" "${cb}"
                    done
                done
            done
        done <<< "${_REMOTE_DS}"
    fi
    step "Remote mining done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 2: Remote mining - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEPS 3-4 - Local mining (QLever)
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_LOCAL}" == false ]]; then

    [[ -z "${DATA_DIR}" ]] && {
        fail "Local mining requires --data-dir (host path for QLever Docker-in-Docker)."
        exit 1
    }

    # Step 3: Generate Qleverfiles
    banner "Step 3: Generate Qleverfiles"
    ql_flags="--data-dir ${CONTAINER_DATA_DIR} --base-port ${BASE_PORT}"
    ql_flags+=" --output-dir ${CONTAINER_OUTPUT_DIR}"
    [[ "${MODE}" == "test" ]] && ql_flags+=" --test"
    [[ -n "${FILTER}" ]]      && ql_flags+=" --filter '${FILTER}'"
    eval "run rdfsolve pipeline qleverfile ${ql_flags}" || { fail "Qleverfile generation failed."; exit 1; }
    step "Qleverfiles ready."

    # Step 4: Download -> Index -> Mine
    banner "Step 4: Download -> Index -> Mine"

    PORTS_JSON=$(run bash -c "cat ${CONTAINER_DATA_DIR}/qlever_workdirs/ports.json 2>/dev/null || echo '{}'")
    if [[ "${PORTS_JSON}" == "{}" ]]; then
        warn "No ports.json found- nothing to process."
    else
        DS_LINES=$(echo "${PORTS_JSON}" | python3 -c "
import json, sys
for name, port in json.load(sys.stdin).items():
    print(f'{name} {port}')
")
        TOTAL=$(echo "${DS_LINES}" | wc -l)
        IDX=0

        while read -r NAME PORT <&3; do
            IDX=$((IDX + 1))
            echo ""
            echo -e "${BOLD}  [${IDX}/${TOTAL}] ${NAME}  (port ${PORT})${NC}"
            echo -e "${BOLD}──────────────────────────────────────────────────────────${NC}"

            WORKDIR="${CONTAINER_DATA_DIR}/qlever_workdirs/${NAME}"
            DONE_INDEX="${WORKDIR}/.index.done"

            # Download + Index (skip if DONE_INDEX exists)
            if run bash -c "test -f ${DONE_INDEX}" 2>/dev/null; then
                step "Index cached - skipping download+index."
            else
                step "Downloading …"
                run bash -c "cd ${WORKDIR} && qlever get-data" \
                    || { fail "[${NAME}] Download failed."; continue; }

                step "Indexing …"
                run bash -c "cd ${WORKDIR} && qlever index --overwrite-existing 2>&1 | grep -v '^$'" \
                    || { fail "[${NAME}] Index failed - check ${WORKDIR}/${NAME}.index-log.txt"; continue; }

                # Summarise any WARN lines from the index log
                _summarise_index_warnings "${NAME}" "${WORKDIR}"

                run bash -c "touch ${DONE_INDEX}"
            fi

            # Start QLever server
            step "Starting QLever on port ${PORT} …"
            QLEVER_CTR="qlever.server.${NAME}"
            docker inspect "${QLEVER_CTR}" >/dev/null 2>&1 && {
                warn "Removing stale container ${QLEVER_CTR}"
                docker rm -f "${QLEVER_CTR}" >/dev/null 2>&1 || true
            }
            run bash -c "cd ${WORKDIR} && qlever start" \
                || { fail "[${NAME}] Server start failed."; continue; }

            # Mine
            for strat in "${STRATEGIES[@]}"; do
                if [[ "${HAS_SWEEP}" == true ]]; then
                    for cs in "${LOCAL_CHUNK_LIST[@]}"; do
                        for cb in "${LOCAL_BATCH_LIST[@]}"; do
                            _mine_local "${NAME}" "${PORT}" "${strat}" false "${cs}" "${cb}"
                        done
                    done
                fi
                if [[ "${ONE_SHOT}" == true ]]; then
                    _mine_local "${NAME}" "${PORT}" "${strat}" true \
                        "${LOCAL_CHUNK_LIST[0]}" "${LOCAL_BATCH_LIST[0]}"
                fi
            done

            # Stop server
            step "Stopping QLever …"
            run bash -c "cd ${WORKDIR} && qlever stop" 2>/dev/null || true
            step "[${NAME}] Done."

        done 3<<< "${DS_LINES}"
    fi
else
    banner "Steps 3-4: Local mining - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 4b - Schema selection
# ═══════════════════════════════════════════════════════════════════
# Selects the best schema per dataset (priority: qlever_oneshot > qlever >
# most-complete-remote) and copies chosen schemas to
# /output/paper_data/schemas/.  Runs inside the container.
# ───────────────────────────────────────────────────────────────────
banner "Step 4b: Schema selection -> paper_data/schemas/"
t0=$(date +%s)

_sel_args="python /app/scripts/build_graphs.py"
_sel_args+=" --schemas-dir ${CONTAINER_SCHEMAS_DIR}"
_sel_args+=" --output-dir  ${CONTAINER_PAPER_DATA_DIR}"
_sel_args+=" --schema-only"
for _ds in "${DATASETS[@]}"; do _sel_args+=" --datasets ${_ds}"; done

run bash -c "${_sel_args}" || warn "Schema selection had warnings."
step "Schema selection done in $(elapsed $(( $(date +%s) - t0 )))."

# ═══════════════════════════════════════════════════════════════════
# STEP 5 - Seed SSSOM mappings (class + property)
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_MAPPINGS}" == false ]]; then
    banner "Step 5: Seed SSSOM mappings"
    t0=$(date +%s)

    # Class mappings    -> /output/mappings/sssom/
    # Property mappings -> /output/mappings/property_mappings/
    run python /app/scripts/seed_sssom_mappings.py \
        --sources-yaml /app/data/sssom_sources.yaml \
        --output-dir   "${CONTAINER_MAPPINGS_DIR}/sssom" \
        --property-mappings-dir "${CONTAINER_MAPPINGS_DIR}/property_mappings" \
        || warn "SSSOM seeding had failures."

    step "SSSOM seeding done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 5: SSSOM mappings - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 6 - Seed SeMRA mappings
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_MAPPINGS}" == false ]]; then
    banner "Step 6: Seed SeMRA mappings"
    t0=$(date +%s)

    run python /app/scripts/seed_semra_mappings.py \
        --output-dir "${CONTAINER_MAPPINGS_DIR}/semra" \
        || warn "SeMRA seeding had failures."

    step "SeMRA seeding done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 6: SeMRA mappings - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 7 - Instance mappings
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_MAPPINGS}" == false ]]; then
    banner "Step 7: Instance mappings"
    t0=$(date +%s)

    _inst_args="python /app/scripts/seed_instance_mappings.py"
    _inst_args+=" --output-dir ${CONTAINER_MAPPINGS_DIR}/instance_matching"
    # --datasets accepts space-separated names (nargs="*")
    if [[ ${#DATASETS[@]} -gt 0 ]]; then
        _inst_args+=" --datasets"
        for _ds in "${DATASETS[@]}"; do _inst_args+=" ${_ds}"; done
    fi

    run bash -c "${_inst_args}" || warn "Instance mapping seeding had failures."
    step "Instance mappings done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 7: Instance mappings - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 8 - Build class index (instance IRI → rdf:type via LSLOD)
# ═══════════════════════════════════════════════════════════════════
# Queries the LSLOD QLever endpoint for every entity IRI that appears
# in the instance_matching/ JSON-LD files and records which rdf:type
# classes each entity belongs to in which named graphs.
# The result is a cached class-index JSON used by steps 9 and 10.
#
# Requires --lslod-endpoint <url> (or LSLOD_ENDPOINT env var).
# ───────────────────────────────────────────────────────────────────
if [[ "${SKIP_MAPPINGS}" == false && "${SKIP_CLASS_DERIVATION}" == false ]]; then
    if [[ -z "${LSLOD_ENDPOINT}" ]]; then
        warn "Step 8: --lslod-endpoint not set - skipping class index build."
        warn "  Re-run with: --lslod-endpoint <sparql-url>"
    else
        banner "Step 8: Build class index (instance IRIs → rdf:type)"
        t0=$(date +%s)

        # Discover all instance-mapping JSON-LD files produced in step 7
        _inst_dir="${CONTAINER_MAPPINGS_DIR}/instance_matching"

        # We call derive with --cache-index only; --enrich and the actual
        # derivation are done in steps 9/10 (or all at once in step 9).
        # The index cache lands next to each output file by convention.
        # Since there may be multiple input files, we loop over them.
        _inst_files=$(run bash -c "find ${_inst_dir} -name '*.jsonld' ! -name '*.enriched.jsonld' 2>/dev/null || true")

        if [[ -z "${_inst_files}" ]]; then
            warn "No instance-mapping JSON-LD files found in ${_inst_dir}."
        else
            while IFS= read -r _f; do
                [[ -z "${_f}" ]] && continue
                _stem="${_f%.jsonld}"
                _out="${_stem}.class_derived.jsonld"
                step "  Building class index for: $(basename "${_f}")"
                run rdfsolve instance-match derive \
                    --input  "${_f}" \
                    --output "${_out}" \
                    --endpoint "${LSLOD_ENDPOINT}" \
                    --cache-index \
                    --timeout "${TIMEOUT}" \
                    || warn "Class index build failed for $(basename "${_f}")."
            done <<< "${_inst_files}"
        fi

        step "Class index done in $(elapsed $(( $(date +%s) - t0 )))."
    fi
else
    banner "Step 8: Class index build - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 9 - Derive class-level mappings
# ═══════════════════════════════════════════════════════════════════
# Reads each instance-mapping JSON-LD together with its cached class
# index (produced in step 8) and outputs a class-derived mapping
# JSON-LD in docker/mappings/class_derived/.
# ───────────────────────────────────────────────────────────────────
if [[ "${SKIP_MAPPINGS}" == false && "${SKIP_CLASS_DERIVATION}" == false ]]; then
    if [[ -z "${LSLOD_ENDPOINT}" ]]; then
        banner "Step 9: Derive class mappings - SKIPPED (no --lslod-endpoint)"
    else
        banner "Step 9: Derive class-level mappings"
        t0=$(date +%s)

        _inst_dir="${CONTAINER_MAPPINGS_DIR}/instance_matching"
        _class_out_dir="${CONTAINER_MAPPINGS_DIR}/class_derived"

        _inst_files=$(run bash -c "find ${_inst_dir} -name '*.jsonld' ! -name '*.enriched.jsonld' ! -name '*.class_derived.jsonld' 2>/dev/null || true")

        if [[ -z "${_inst_files}" ]]; then
            warn "No instance-mapping JSON-LD files found - nothing to derive."
        else
            run bash -c "mkdir -p ${_class_out_dir}"
            while IFS= read -r _f; do
                [[ -z "${_f}" ]] && continue
                _base=$(basename "${_f}" .jsonld)
                _out="${_class_out_dir}/${_base}.class_derived.jsonld"
                step "  Deriving: $(basename "${_f}") → class_derived/"
                run rdfsolve instance-match derive \
                    --input    "${_f}" \
                    --output   "${_out}" \
                    --endpoint "${LSLOD_ENDPOINT}" \
                    --cache-index \
                    --timeout  "${TIMEOUT}" \
                    || warn "Class derivation failed for $(basename "${_f}")."
            done <<< "${_inst_files}"
        fi

        step "Class derivation done in $(elapsed $(( $(date +%s) - t0 )))."
    fi
else
    banner "Step 9: Derive class mappings - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 10 - Enrich instance JSON-LD with class annotations
# ═══════════════════════════════════════════════════════════════════
# Writes {stem}.enriched.jsonld alongside each instance-mapping file,
# annotating every entity node with @type and rdfsolve:classifiedIn
# provenance from the class index.
# ───────────────────────────────────────────────────────────────────
if [[ "${SKIP_MAPPINGS}" == false && "${SKIP_CLASS_DERIVATION}" == false ]]; then
    if [[ -z "${LSLOD_ENDPOINT}" ]]; then
        banner "Step 10: Enrich instance JSON-LD - SKIPPED (no --lslod-endpoint)"
    else
        banner "Step 10: Enrich instance JSON-LD with class annotations"
        t0=$(date +%s)

        _inst_dir="${CONTAINER_MAPPINGS_DIR}/instance_matching"

        _inst_files=$(run bash -c "find ${_inst_dir} -name '*.jsonld' ! -name '*.enriched.jsonld' ! -name '*.class_derived.jsonld' 2>/dev/null || true")

        if [[ -z "${_inst_files}" ]]; then
            warn "No instance-mapping JSON-LD files found - nothing to enrich."
        else
            while IFS= read -r _f; do
                [[ -z "${_f}" ]] && continue
                step "  Enriching: $(basename "${_f}")"
                run rdfsolve instance-match derive \
                    --input    "${_f}" \
                    --output   "${_f%.jsonld}.class_derived.jsonld" \
                    --endpoint "${LSLOD_ENDPOINT}" \
                    --cache-index \
                    --enrich \
                    --timeout  "${TIMEOUT}" \
                    || warn "Enrichment failed for $(basename "${_f}")."
            done <<< "${_inst_files}"
        fi

        step "Enrichment done in $(elapsed $(( $(date +%s) - t0 )))."
    fi
else
    banner "Step 10: Enrich instance JSON-LD - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 11 - Inference expansion  (always last mapping step)
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_MAPPINGS}" == false ]]; then
    banner "Step 11: Inference expansion"
    t0=$(date +%s)

    run python /app/scripts/seed_inferenced_mappings.py \
        --input-dir  "${CONTAINER_MAPPINGS_DIR}" \
        --output-dir "${CONTAINER_MAPPINGS_DIR}/inferenced" \
        || warn "Inference step had failures."

    step "Inference done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 11: Inference - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 12 - Build connectivity graphs -> Parquet
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_BUILD_GRAPHS}" == false ]]; then
    banner "Step 12: Build graphs -> Parquet"
    t0=$(date +%s)

    _bg_args="python /app/scripts/build_graphs.py"
    _bg_args+=" --schemas-dir  ${CONTAINER_SCHEMAS_DIR}"
    _bg_args+=" --mappings-dir ${CONTAINER_MAPPINGS_DIR}"
    _bg_args+=" --output-dir   ${CONTAINER_PAPER_DATA_DIR}"
    for _ds in "${DATASETS[@]}"; do _bg_args+=" --datasets ${_ds}"; done

    run bash -c "${_bg_args}" || warn "build_graphs.py had warnings."
    step "Graph build done in $(elapsed $(( $(date +%s) - t0 )))."
else
    banner "Step 12: Build graphs - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 14 - Execute paper notebook
# ═══════════════════════════════════════════════════════════════════
# The notebook is expected at /app/paper/notebooks/mapping_analysis_paper.ipynb
# inside the container.  Add 'COPY paper/ paper/' to Dockerfile.pipeline if needed.
# ───────────────────────────────────────────────────────────────────
if [[ "${SKIP_NOTEBOOK}" == false ]]; then
    banner "Step 14: Execute paper notebook"

    if run bash -c "test -f /app/paper/notebooks/mapping_analysis_paper.ipynb" 2>/dev/null; then
        t0=$(date +%s)

        run python -m jupyter nbconvert \
            --to notebook --execute \
            --ExecutePreprocessor.timeout=600 \
            --ExecutePreprocessor.kernel_name=python3 \
            --inplace \
            /app/paper/notebooks/mapping_analysis_paper.ipynb \
            || warn "Paper notebook execution had errors."

        # Also export as HTML for easy browsing - written to /output/paper_data/
        run python -m jupyter nbconvert \
            --to html \
            --ExecutePreprocessor.kernel_name=python3 \
            --output "${CONTAINER_PAPER_DATA_DIR}/mapping_analysis.html" \
            /app/paper/notebooks/mapping_analysis_paper.ipynb \
            2>/dev/null || true

        step "Notebook done in $(elapsed $(( $(date +%s) - t0 )))."
    else
        warn "Paper notebook not found inside container (/app/paper/notebooks/mapping_analysis_paper.ipynb)."
        warn "Add 'COPY paper/ paper/' to docker/Dockerfile.pipeline and rebuild (step 0)."
    fi
else
    banner "Step 14: Paper notebook - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 14b - Execute schema-extraction / performance-analysis notebook
# ═══════════════════════════════════════════════════════════════════
# Runs notebooks/schema_extraction/performance_analysis.ipynb inside the
# container.  Passes INDEX_WARNINGS so the notebook can load
# index_warnings.json from /output even when RESULTS_ROOT differs.
# HTML output lands in /output/paper_data/performance_analysis.html.
# ───────────────────────────────────────────────────────────────────
if [[ "${SKIP_NOTEBOOK}" == false ]]; then
    banner "Step 14b: Schema-extraction performance notebook"
    _PERF_NB="/app/notebooks/schema_extraction/performance_analysis.ipynb"

    if run bash -c "test -f ${_PERF_NB}" 2>/dev/null; then
        t0=$(date +%s)
        run bash -c "INDEX_WARNINGS=${CONTAINER_OUTPUT_DIR}/index_warnings.json \
            python -m jupyter nbconvert \
            --to notebook --execute \
            --ExecutePreprocessor.timeout=300 \
            --ExecutePreprocessor.kernel_name=python3 \
            --inplace \
            ${_PERF_NB}" \
            || warn "Performance notebook execution had errors."

        run bash -c "INDEX_WARNINGS=${CONTAINER_OUTPUT_DIR}/index_warnings.json \
            python -m jupyter nbconvert \
            --to html \
            --ExecutePreprocessor.kernel_name=python3 \
            --output ${CONTAINER_PAPER_DATA_DIR}/performance_analysis.html \
            ${_PERF_NB}" \
            2>/dev/null || true

        step "Performance notebook done in $(elapsed $(( $(date +%s) - t0 )))."
    else
        warn "Performance notebook not found inside container (${_PERF_NB})."
        warn "Ensure notebooks/ is COPYed into the Docker image."
    fi
else
    banner "Step 14b: Performance notebook - SKIPPED"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 15 - Collect /output -> host results directory
# ═══════════════════════════════════════════════════════════════════
banner "Step 15: Collect results -> ${RESULTS_DIR}/"
step "Copying ${CONTAINER_OUTPUT_DIR}/ -> ${RESULTS_DIR}/ …"

run bash -c "ls -la ${CONTAINER_OUTPUT_DIR}/" || true
run bash -c "cd ${CONTAINER_OUTPUT_DIR} && tar cf - . 2>/dev/null" \
    | tar xf - -C "${RESULTS_DIR}/" 2>/dev/null || {
    warn "tar copy failed - falling back to docker cp."
    ${DC} run -d --name rdfsolve-copy pipeline sleep 30 >/dev/null 2>&1
    docker cp "rdfsolve-copy:${CONTAINER_OUTPUT_DIR}/." "${RESULTS_DIR}/" 2>/dev/null || true
    docker rm -f rdfsolve-copy >/dev/null 2>&1 || true
}
step "Results collected."

# ═══════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════
TOTAL_ELAPSED=$(( $(date +%s) - PIPELINE_START ))
banner "Pipeline complete"

echo -e "  Total time:   ${BOLD}$(elapsed ${TOTAL_ELAPSED})${NC}"
echo -e "  Results in:   ${BOLD}${RESULTS_DIR}/${NC}"

if [[ -d "${RESULTS_DIR}" ]]; then
    echo ""
    FILE_COUNT=$(find "${RESULTS_DIR}" -type f 2>/dev/null | wc -l)
    echo -e "  Files:        ${BOLD}${FILE_COUNT}${NC}"
    for ext in jsonld ttl json csv jsonl parquet; do
        cnt=$(find "${RESULTS_DIR}" -name "*.${ext}" 2>/dev/null | wc -l)
        (( cnt > 0 )) && echo "    *.${ext}: ${cnt}"
    done
    echo ""
    echo -e "  Size:         $(du -sh "${RESULTS_DIR}" 2>/dev/null | cut -f1)"
fi

echo ""
echo -e "${GREEN}${BOLD}Done.${NC}"
