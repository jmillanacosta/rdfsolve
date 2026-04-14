#!/usr/bin/env bash
# run_pipeline_hpc.sh - RDFSolve pipeline for TGX-HPC (Singularity / SLURM)
#
# Usage:
#   bash scripts/run_pipeline_hpc.sh --dataset aopwikirdf --skip-remote
#   sbatch scripts/slurm_pipeline.sh
#
# Env vars (set by SLURM wrapper or override):
#   DATA_DIR           - RDF dumps + QLever indexes  (required)
#   OUTPUT_DIR         - mined schemas + reports      (required)
#   RESULTS_DIR        - final output copy target     (optional)
#   SINGULARITY_IMAGE  - path to qlever.sif           (optional)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────────
DATASETS=()
FILTER=""
SKIP_REMOTE=false
SKIP_DISCOVERY=false
SKIP_LOCAL=false
SKIP_MAPPINGS=false
SKIP_MINING=false
BENCHMARK=true
BASE_PORT=7019
TIMEOUT=1000
ONE_SHOT=true
CHUNK_SIZE=50000
CLASS_BATCH_SIZE=50
DISK_SPACE_FACTOR=12
DISK_SPACE_MIN_MB=500

export no_proxy="localhost,127.0.0.1,${no_proxy:-}"
export NO_PROXY="localhost,127.0.0.1,${NO_PROXY:-}"

DATA_DIR="${DATA_DIR:-${REPO_ROOT}/data/rdf}"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_ROOT}/results/output}"
RESULTS_DIR="${RESULTS_DIR:-${REPO_ROOT}/results}"
SINGULARITY_IMAGE="${SINGULARITY_IMAGE:-${DATA_DIR}/qlever.sif}"
QLEVER_DOCKER_IMAGE="docker://docker.io/adfreiburg/qlever:latest"

# ── Logging ───────────────────────────────────────────────────────
log()  { echo "[$(date +%H:%M:%S)] $*"; }
warn() { echo "[$(date +%H:%M:%S)] WARN: $*" >&2; }
die()  { echo "[$(date +%H:%M:%S)] ERROR: $*" >&2; exit 1; }
elapsed() { printf '%02d:%02d:%02d' $(($1/3600)) $((($1%3600)/60)) $(($1%60)); }

# _notify: expected to be exported by the SLURM wrapper; no-op fallback.
if ! declare -f _notify >/dev/null 2>&1; then
    _notify() { :; }
fi

# ── Parse arguments ───────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)            DATASETS+=("$2");        shift 2 ;;
        --filter)             FILTER="$2";             shift 2 ;;
        --skip-remote)        SKIP_REMOTE=true;        shift ;;
        --skip-discovery)     SKIP_DISCOVERY=true;     shift ;;
        --skip-local)         SKIP_LOCAL=true;         shift ;;
        --skip-mining)        SKIP_MINING=true;        shift ;;
        --skip-mappings)      SKIP_MAPPINGS=true;      shift ;;
        --data-dir)           DATA_DIR="$2";           shift 2 ;;
        --output-dir)         OUTPUT_DIR="$2";         shift 2 ;;
        --results-dir)        RESULTS_DIR="$2";        shift 2 ;;
        --base-port)          BASE_PORT="$2";          shift 2 ;;
        --timeout)            TIMEOUT="$2";            shift 2 ;;
        --chunk-size)         CHUNK_SIZE="$2";         shift 2 ;;
        --singularity-image)  SINGULARITY_IMAGE="$2";  shift 2 ;;
        --help|-h)            head -14 "$0" | grep "^#" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                    die "Unknown option: $1" ;;
    esac
done

if [[ ${#DATASETS[@]} -gt 0 && -z "${FILTER}" ]]; then
    FILTER="^($(IFS='|'; echo "${DATASETS[*]}"))$"
fi

mkdir -p "${DATA_DIR}" "${OUTPUT_DIR}" "${RESULTS_DIR}"
QLEVER_WORKDIRS="${DATA_DIR}/qlever_workdirs"
mkdir -p "${QLEVER_WORKDIRS}"

# ── QLever Singularity helpers ────────────────────────────────────

_ensure_singularity_image() {
    if [[ -f "${SINGULARITY_IMAGE}" ]]; then
        log "Singularity image present: ${SINGULARITY_IMAGE}"
        return 0
    fi
    log "Pulling QLever Singularity image …"
    mkdir -p "$(dirname "${SINGULARITY_IMAGE}")"
    singularity pull --disable-cache "${SINGULARITY_IMAGE}" "${QLEVER_DOCKER_IMAGE}"
}

_qlever_run() {
    local workdir="$1"; shift
    (cd "${workdir}" && singularity exec \
        --bind "${workdir}:${workdir}" \
        --bind "${DATA_DIR}:${DATA_DIR}" \
        "${SINGULARITY_IMAGE}" "$@")
}

_qlever_get_data() {
    local workdir="$1"
    local get_data_cmd
    get_data_cmd=$(grep '^GET_DATA_CMD' "${workdir}/Qleverfile" 2>/dev/null \
        | head -1 | sed 's/^[^=]*=[ ]*//')
    if [[ -z "${get_data_cmd}" ]]; then
        echo "ERROR: No GET_DATA_CMD in ${workdir}/Qleverfile" >&2
        return 1
    fi
    (cd "${workdir}" && eval "${get_data_cmd}")
}

_qlever_index() {
    local name="$1" workdir="$2"
    local settings_json="${workdir}/${name}.settings.json"

    # Parse CAT_INPUT_FILES (handles continuation lines)
    local cat_cmd
    cat_cmd=$(python3 - "${workdir}/Qleverfile" <<'PYEOF'
import sys, re
with open(sys.argv[1]) as f:
    content = f.read()
content = re.sub(r'\n([ \t]+)', r' ', content)
m = re.search(r'^CAT_INPUT_FILES\s*=\s*(.+)', content, re.MULTILINE)
print(m.group(1).strip() if m else '')
PYEOF
)
    local settings_raw
    settings_raw=$(grep '^SETTINGS_JSON' "${workdir}/Qleverfile" 2>/dev/null \
        | head -1 | sed 's/.*=[ ]*//')

    local input_files_raw
    input_files_raw=$(grep '^INPUT_FILES' "${workdir}/Qleverfile" 2>/dev/null \
        | head -1 | sed 's/.*=[ ]*//')
    export INPUT_FILES="${input_files_raw}"

    local rdf_format
    rdf_format=$(grep '^FORMAT' "${workdir}/Qleverfile" 2>/dev/null \
        | head -1 | sed 's/.*=[ ]*//' | tr -d '[:space:]')
    rdf_format="${rdf_format:-ttl}"

    local mem_for_queries
    mem_for_queries=$(grep '^MEMORY_FOR_QUERIES' "${workdir}/Qleverfile" 2>/dev/null \
        | head -1 | sed 's/.*=[ ]*//' | tr -d '[:space:]')
    mem_for_queries="${mem_for_queries:-300G}"

    echo "${settings_raw}" > "${settings_json}"

    # Direct file mode when CAT_INPUT_FILES is plain "cat ${INPUT_FILES}"
    local use_direct_files=false
    local cat_cmd_trimmed
    cat_cmd_trimmed="$(echo "${cat_cmd}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ "${cat_cmd_trimmed}" == 'cat ${INPUT_FILES}' ]] && use_direct_files=true

    # Workaround: rewrite bare integers that overflow int64 as xsd:double
    log "Checking for int64-overflowing integers …"
    local _overflow_patched=0
    pushd "${workdir}" > /dev/null
    for f in ${INPUT_FILES}; do
        [ -f "$f" ] || continue
        if grep -qP '\t[0-9]{20,}\s' "$f"; then
            log "  fixing overflows in $(basename "$f")"
            sed -i -E 's/\t([0-9]{20,})(\s)/\t"\1"^^<http:\/\/www.w3.org\/2001\/XMLSchema#double>\2/g' "$f"
            _overflow_patched=$(( _overflow_patched + 1 ))
        fi
    done
    popd > /dev/null
    [[ ${_overflow_patched} -gt 0 ]] && log "Patched overflowing integers in ${_overflow_patched} file(s)"

    # Workaround: strip control chars (except \t, \n, \r) that break
    # QLever's IRI / N-Quads parser (e.g. \x01 and \x7F in bio2rdf data).
    log "Checking for illegal control characters …"
    local _sanitised=0
    pushd "${workdir}" > /dev/null
    for f in ${INPUT_FILES}; do
        [ -f "$f" ] || continue
        if grep -qP '[\x00-\x08\x0e-\x1f\x7f]' "$f" 2>/dev/null; then
            log "  stripping control chars from $(basename "$f")"
            perl -pi -e 's/[\x00-\x08\x0e-\x1f\x7f]//g' "$f"
            _sanitised=$(( _sanitised + 1 ))
        fi
    done
    popd > /dev/null
    [[ ${_sanitised} -gt 0 ]] && log "Sanitised ${_sanitised} file(s)"

    if [[ "${use_direct_files}" == true ]]; then
        local file_flags=()
        (cd "${workdir}" && \
            for f in ${INPUT_FILES}; do
                [ -f "$f" ] && echo "${workdir}/$f"
            done) | sort > "${workdir}/.input_file_list.tmp"

        while IFS= read -r fpath; do
            file_flags+=( -f "${fpath}" )
        done < "${workdir}/.input_file_list.tmp"
        rm -f "${workdir}/.input_file_list.tmp"

        [[ ${#file_flags[@]} -eq 0 ]] && { echo "ERROR: No input files for ${name}" >&2; return 1; }
        log "Direct file input: ${#file_flags[@]} files"

        (cd "${workdir}" && singularity exec \
            --bind "${workdir}:${workdir}" \
            --bind "${DATA_DIR}:${DATA_DIR}" \
            "${SINGULARITY_IMAGE}" \
            qlever-index -i "${name}" \
                -s "${settings_json}" \
                --vocabulary-type on-disk-compressed \
                -m "${mem_for_queries}" \
                -F "${rdf_format}" "${file_flags[@]}" -p false \
                2>&1 | tee "${workdir}/${name}.index-log.txt")
    else
        (cd "${workdir}" && eval "${cat_cmd}" | singularity exec \
            --bind "${workdir}:${workdir}" \
            --bind "${DATA_DIR}:${DATA_DIR}" \
            "${SINGULARITY_IMAGE}" \
            qlever-index -i "${name}" \
                -s "${settings_json}" \
                --vocabulary-type on-disk-compressed \
                -m "${mem_for_queries}" \
                -F "${rdf_format}" -f - -p false \
                2>&1 | tee "${workdir}/${name}.index-log.txt")
    fi
}

_qlever_start() {
    local name="$1" workdir="$2" port="$3"
    local instance_name="qlever_${name}"

    # --- Ensure port is free before starting ---
    local _port_pid
    _port_pid=$(ss -tlnp "sport = :${port}" 2>/dev/null \
        | awk 'NR>1{match($0,/pid=([0-9]+)/,a); if(a[1]) print a[1]}' | head -1)
    if [[ -n "${_port_pid}" ]]; then
        warn "[${name}] Port ${port} occupied by PID ${_port_pid} – killing"
        kill -9 "${_port_pid}" 2>/dev/null || true
        sleep 2
    fi

    singularity instance stop "${instance_name}" 2>/dev/null || true
    sleep 1   # let the OS reclaim resources from the old instance

    singularity instance start \
        --bind "${workdir}:${workdir}" \
        --bind "${DATA_DIR}:${DATA_DIR}" \
        -W "${workdir}" \
        "${SINGULARITY_IMAGE}" \
        "${instance_name}" \
        > "${workdir}/start.log" 2>&1

    # Clear old server log so we can detect fresh errors
    : > "${workdir}/server.log"

    singularity exec "instance://${instance_name}" \
        bash -c "cd '${workdir}' && exec qlever-server -i '${name}' -j 8 -p '${port}' -m 40G -c 8G -e 4G -k 200 -s 1000s -a '${name}'" \
        > "${workdir}/server.log" 2>&1 &

    local i=0
    until env http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= \
          curl --noproxy '*' -sf "http://localhost:${port}/?query=ASK%7B%7D" >/dev/null 2>&1; do
        sleep 2; i=$((i+2))
        # Detect early fatal errors (e.g. "Address already in use") to fail fast
        if [[ -s "${workdir}/server.log" ]] \
            && grep -qi 'Address already in use\|cannot bind\|FATAL' "${workdir}/server.log" 2>/dev/null; then
            warn "[${name}] QLever failed: $(head -5 "${workdir}/server.log")"
            singularity instance stop "${instance_name}" 2>/dev/null || true
            return 1
        fi
        [[ $i -ge 120 ]] && { warn "[${name}] QLever did not start within 120s"; return 1; }
    done
    log "[${name}] QLever ready on port ${port}"
    echo "${instance_name}"
}

_qlever_stop() {
    local instance_name="$1" port="$2" workdir="$3"
    singularity instance stop "${instance_name}" > "${workdir}/stop.log" 2>&1 || true
}

# ── Disk space check ──────────────────────────────────────────────

_check_disk_space() {
    local name="$1" workdir="$2"
    local factor="${DISK_SPACE_FACTOR}" min_mb="${DISK_SPACE_MIN_MB}"

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
print(json.dumps({"urls": urls, "tar_url": entry.get('local_tar_url')}))
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

    [[ ${#check_urls[@]} -eq 0 ]] && { warn "[${name}] No URLs for disk check"; return 0; }

    local total_compressed=0
    for url in "${check_urls[@]}"; do
        local cl
        cl=$(curl -sI --max-time 15 --location "${url}" \
            | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2; exit}')
        [[ "${cl}" =~ ^[0-9]+$ ]] && total_compressed=$(( total_compressed + cl ))
    done

    [[ "${total_compressed}" -eq 0 ]] && { warn "[${name}] Could not estimate size"; return 0; }

    local required=$(( total_compressed * factor ))
    local required_mb=$(( required / 1048576 ))
    log "[${name}] ~$(( total_compressed / 1048576 )) MiB download > ~${required_mb} MiB needed (×${factor})"

    mkdir -p "${workdir}"
    local free_bytes
    free_bytes=$(df --output=avail -B1 "${workdir}" 2>/dev/null | tail -1 | tr -d ' ')
    free_bytes="${free_bytes//[^0-9]/}"
    free_bytes="${free_bytes:-0}"
    local min_bytes=$(( min_mb * 1048576 ))

    if [[ "${free_bytes}" -lt "${required}" ]] || [[ "${free_bytes}" -lt "${min_bytes}" ]]; then
        warn "[${name}] Insufficient disk space (need ~${required_mb} MiB, have $(( free_bytes / 1048576 )) MiB)"
        return 1
    fi
    return 0
}

# ── Mining helper ─────────────────────────────────────────────────

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

    log "[${name}] Mining qlever/${strat}/$(basename ${out})"
    eval "rdfsolve local-mine ${flags}" || warn "[${name}] Mining pass failed"
}

# ── Dataset name list helper ──────────────────────────────────────

_list_dataset_names() {
    python3 - "${REPO_ROOT}/data/sources.yaml" "${FILTER}" <<'PYEOF'
import sys, re, yaml
yaml_path, filt = sys.argv[1], sys.argv[2]
with open(yaml_path) as f:
    sources = yaml.safe_load(f) or []
rx = re.compile(filt) if filt else None
for s in sources:
    name = s.get("name", "")
    if rx is None or rx.search(name):
        print(name)
PYEOF
}

# ═══════════════════════════════════════════════════════════════════
# PREFLIGHT
# ═══════════════════════════════════════════════════════════════════
log "=== RDFSolve HPC Pipeline ==="
log "Datasets: ${FILTER:-<all>}"
log "DATA_DIR: ${DATA_DIR}"
log "OUTPUT_DIR: ${OUTPUT_DIR}"

python3 -c "import yaml, rdflib" 2>/dev/null \
    || die "Python deps missing. Run: pip install pyyaml rdflib"
command -v singularity >/dev/null 2>&1 \
    || die "singularity not found in PATH"
log "Singularity: $(singularity --version)"

PIPELINE_START=$(date +%s)

# ═══════════════════════════════════════════════════════════════════
# STEP 0 - Pull QLever Singularity image
# ═══════════════════════════════════════════════════════════════════
log "--- Step 0: Ensure QLever Singularity image ---"
_ensure_singularity_image

# ═══════════════════════════════════════════════════════════════════
# STEP 1 - Remote VoID discovery
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false && "${SKIP_DISCOVERY}" == false ]]; then
    log "--- Step 1: Remote VoID discovery ---"
    t0=$(date +%s)

    _discover_names=()
    while IFS= read -r _n; do
        [[ -n "${_n}" ]] && _discover_names+=("${_n}")
    done < <(_list_dataset_names)

    for _ds_name in "${_discover_names[@]}"; do
        log "[${_ds_name}] Discovering VoID …"
        _ds_flags="--output-dir ${OUTPUT_DIR} --filter '^${_ds_name}$'"
        [[ "${TIMEOUT}" -gt 0 ]] 2>/dev/null && _ds_flags+=" --timeout ${TIMEOUT}"
        eval "rdfsolve discover ${_ds_flags}" \
            || warn "[${_ds_name}] discover had failures"
    done
    log "Discovery done in $(elapsed $(( $(date +%s) - t0 )))"
else
    log "--- Step 1: Remote VoID discovery - SKIPPED ---"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 2 - Remote schema mining
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false ]]; then
    log "--- Step 2: Remote schema mining ---"
    t0=$(date +%s)

    if [[ -z "${_discover_names+x}" || ${#_discover_names[@]} -eq 0 ]]; then
        _discover_names=()
        while IFS= read -r _n; do
            [[ -n "${_n}" ]] && _discover_names+=("${_n}")
        done < <(_list_dataset_names)
    fi

    for _ds_name in "${_discover_names[@]}"; do
        log "[${_ds_name}] Mining remote schema …"
        _ds_flags="--output-dir ${OUTPUT_DIR} --filter '^${_ds_name}$'"
        [[ "${TIMEOUT}" -gt 0 ]] 2>/dev/null && _ds_flags+=" --timeout ${TIMEOUT}"
        [[ "${BENCHMARK}" == true ]] && _ds_flags+=" --benchmark"
        eval "rdfsolve mine ${_ds_flags}" \
            || warn "[${_ds_name}] remote mining had failures"
    done
    log "Remote mining done in $(elapsed $(( $(date +%s) - t0 )))"
else
    log "--- Step 2: Remote mining - SKIPPED ---"
fi

# ═══════════════════════════════════════════════════════════════════
# STEPS 3–4 - Local mining (QLever via Singularity)
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_LOCAL}" == false ]]; then

    # Step 3: Generate Qleverfiles
    log "--- Step 3: Generate Qleverfiles ---"
    qf_flags="--data-dir ${DATA_DIR} --base-port ${BASE_PORT} --output-dir ${OUTPUT_DIR}"
    [[ -n "${FILTER}" ]] && qf_flags+=" --filter '${FILTER}'"
    eval "rdfsolve qleverfile ${qf_flags}" || die "Qleverfile generation failed"

    find "${QLEVER_WORKDIRS}" -name "Qleverfile" | while read -r qf; do
        sed -i "s|^SYSTEM *=.*|SYSTEM = native|" "${qf}"
        sed -i "s|^IMAGE *=.*|IMAGE  = ${SINGULARITY_IMAGE}|" "${qf}"
    done
    log "Qleverfiles ready (SYSTEM=native)"

    # Step 4: For each dataset - download > index > start > mine > stop > cleanup
    if [[ "${SKIP_MINING}" == true ]]; then
        log "--- Step 4: Mining — SKIPPED (--skip-mining) ---"
    else
    log "--- Step 4: Download > Index > Mine ---"

    PORTS_JSON="${QLEVER_WORKDIRS}/ports.json"
    if [[ ! -f "${PORTS_JSON}" ]]; then
        warn "No ports.json at ${PORTS_JSON} - nothing to process"
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
            log "[${IDX}/${TOTAL}] ${NAME} (port ${PORT})"
            WORKDIR="${QLEVER_WORKDIRS}/${NAME}"
            DONE_INDEX="${WORKDIR}/.index.done"
            mkdir -p "${WORKDIR}"

            # Download + Index
            if [[ -f "${DONE_INDEX}" ]]; then
                log "  Index cached - skipping download+index"
            else
                _check_disk_space "${NAME}" "${WORKDIR}" || { continue; }

                log "  Downloading …"
                _qlever_get_data "${WORKDIR}" \
                    || { warn "[${NAME}] Download failed"; continue; }

                INPUT_GLOB=$(grep '^INPUT_FILES' "${WORKDIR}/Qleverfile" 2>/dev/null \
                    | head -1 | sed 's/.*=[ ]*//')
                INPUT_BYTES=0
                if [[ -n "${INPUT_GLOB}" ]]; then
                    for f in ${WORKDIR}/${INPUT_GLOB}; do
                        [[ -f "$f" ]] && INPUT_BYTES=$(( INPUT_BYTES + $(stat -c%s "$f" 2>/dev/null || echo 0) ))
                    done
                fi
                if [[ "${INPUT_BYTES}" -eq 0 ]]; then
                    warn "[${NAME}] Download produced 0 bytes - skipping"
                    continue
                fi

                log "  Indexing …"
                _qlever_index "${NAME}" "${WORKDIR}" \
                    || { warn "[${NAME}] Index failed"; continue; }
                touch "${DONE_INDEX}"
            fi

            # Start > Mine > Stop
            log "  Starting QLever on port ${PORT} …"
            INSTANCE_NAME=$(_qlever_start "${NAME}" "${WORKDIR}" "${PORT}") \
                || { warn "[${NAME}] Server start failed"; continue; }

            if [[ "${ONE_SHOT}" == true ]]; then
                _mine_local "${NAME}" "${PORT}" "typed" true "${CHUNK_SIZE}" "${CLASS_BATCH_SIZE}"
            fi

            log "  Stopping QLever …"
            _qlever_stop "${INSTANCE_NAME}" "${PORT}" "${WORKDIR}"

            # Cleanup raw RDF (keep index files for LSLOD)
            rdf_dir="${WORKDIR}/rdf"
            if [[ -d "${rdf_dir}" ]]; then
                du -sh "${rdf_dir}" 2>/dev/null && rm -rf "${rdf_dir}"
            fi
            find "${WORKDIR}" -maxdepth 1 \
                \( -name '*.tar' -o -name '*.tar.gz' -o -name '*.tgz' \
                -o -name '*.zip' -o -name '*.gz' -o -name '*.xz' \) \
                -delete 2>/dev/null || true

            log "[${NAME}] done"
        done <<< "${DS_LINES}"
    fi
    fi  # end SKIP_MINING
else
    log "--- Steps 3–4: Local processing - SKIPPED (--skip-local) ---"
fi

# ═══════════════════════════════════════════════════════════════════
# STEPS 5–10 - Mappings & graph build (requires --skip-mappings=false)
# ═══════════════════════════════════════════════════════════════════
if [[ "${SKIP_MAPPINGS}" == false ]]; then

# Step 5: Schema selection
log "--- Step 5: Schema selection ---"
t0=$(date +%s)

SCHEMAS_DIR="${OUTPUT_DIR}/schemas"
MAPPINGS_DIR="${OUTPUT_DIR}/mappings"
PAPER_DATA_DIR="${OUTPUT_DIR}/paper_data"
mkdir -p "${SCHEMAS_DIR}" "${MAPPINGS_DIR}" "${PAPER_DATA_DIR}"

_sel_args="rdfsolve build-graphs"
_sel_args+=" --schemas-dir ${OUTPUT_DIR}"
_sel_args+=" --output-dir  ${PAPER_DATA_DIR}"
_sel_args+=" --schema-only"
if [[ ${#DATASETS[@]} -gt 0 ]]; then
    for _ds in "${DATASETS[@]}"; do _sel_args+=" --datasets ${_ds}"; done
fi
eval "${_sel_args}" || warn "Schema selection had warnings"
log "Schema selection done in $(elapsed $(( $(date +%s) - t0 )))"

# Step 6: Seed SSSOM mappings
log "--- Step 6: Seed SSSOM mappings ---"
t0=$(date +%s)

rdfsolve sssom seed \
    --sources-yaml "${REPO_ROOT}/data/sssom_sources.yaml" \
    --output-dir   "${MAPPINGS_DIR}/sssom" \
    --property-mappings-dir "${MAPPINGS_DIR}/property_mappings" \
    || warn "SSSOM seeding had failures"
log "SSSOM seeding done in $(elapsed $(( $(date +%s) - t0 )))"

# Step 7: Seed SeMRA mappings
log "--- Step 7: Seed SeMRA mappings ---"
t0=$(date +%s)

rdfsolve semra seed \
    --sources all \
    --exclude clo --exclude wikidata \
    --output-dir "${MAPPINGS_DIR}/semra" \
    || warn "SeMRA seeding had failures"
log "SeMRA seeding done in $(elapsed $(( $(date +%s) - t0 )))"

# ═══════════════════════════════════════════════════════════════════
# STEPS 8–10 - LSLOD block: start ALL QLever > instance mappings >
#              class derivation > stop ALL QLever
#
# Instance mapping (step 8) queries LOCAL QLever endpoints only.
# Class derivation (step 9) uses the same running instances.
# All instances are stopped after step 10.
# ═══════════════════════════════════════════════════════════════════
LSLOD_ENDPOINT=""
declare -a LSLOD_INSTANCES=()
declare -a LSLOD_PORTS=()

if [[ "${SKIP_LOCAL}" == false ]]; then
    log "--- Steps 8–10: LSLOD block (start QLever > instance match > derive > stop) ---"
    PORTS_JSON="${QLEVER_WORKDIRS}/ports.json"

    # Reinforce proxy bypass for localhost QLever endpoints.
    # (HPC module loads can reset no_proxy after our initial export.)
    export no_proxy="localhost,127.0.0.1,${no_proxy:-}"
    export NO_PROXY="localhost,127.0.0.1,${NO_PROXY:-}"

    if [[ -f "${PORTS_JSON}" ]]; then
        DS_LINES_LSLOD=$(python3 -c "
import json
with open('${PORTS_JSON}') as f:
    d = json.load(f)
for name, port in d.items():
    print(f'{name} {port}')
")
        while read -r _LNAME _LPORT; do
            _LWORKDIR="${QLEVER_WORKDIRS}/${_LNAME}"
            if [[ ! -f "${_LWORKDIR}/.index.done" ]]; then
                log "  [${_LNAME}] No index - skipping"
                continue
            fi
            log "  Starting ${_LNAME} on port ${_LPORT} …"
            _LINST=$(_qlever_start "${_LNAME}" "${_LWORKDIR}" "${_LPORT}") \
                && {
                    LSLOD_INSTANCES+=("${_LINST}")
                    LSLOD_PORTS+=("${_LPORT}")
                    [[ -z "${LSLOD_ENDPOINT}" ]] && LSLOD_ENDPOINT="http://localhost:${_LPORT}"
                } || warn "  [${_LNAME}] Failed to start"
            sleep 1   # brief cooldown between instance starts
        done <<< "${DS_LINES_LSLOD}"
    fi

    if [[ ${#LSLOD_INSTANCES[@]} -eq 0 ]]; then
        warn "No QLever instances started - skipping steps 8–10"
    else
        log "LSLOD: ${#LSLOD_INSTANCES[@]} instances running"

        # Step 8: Instance mappings (LOCAL QLever only via ports.json)
        log "--- Step 8: Instance mappings (local endpoints) ---"
        t0=$(date +%s)

        # Dynamically discover entity prefixes from all mapping files
        _pfx_file="${MAPPINGS_DIR}/.discovered_prefixes.txt"
        _discover_args="rdfsolve instance-match discover-prefixes"
        _discover_args+=" -d ${MAPPINGS_DIR}/sssom"
        _discover_args+=" -d ${MAPPINGS_DIR}/semra"
        _discover_args+=" -o ${_pfx_file}"
        eval "${_discover_args}" || warn "Prefix discovery had warnings"

        _inst_prefixes=()
        if [[ -s "${_pfx_file}" ]]; then
            while IFS= read -r _pfx_line; do
                [[ -n "${_pfx_line}" ]] && _inst_prefixes+=("${_pfx_line}")
            done < "${_pfx_file}"
        fi
        if [[ ${#_inst_prefixes[@]} -eq 0 ]]; then
            _inst_prefixes=(chebi ensembl faldo uniprot)
            warn "Prefix discovery returned 0 prefixes; using defaults: ${_inst_prefixes[*]}"
        fi
        log "Instance mapping prefixes (${#_inst_prefixes[@]}): ${_inst_prefixes[*]}"

        _inst_args="rdfsolve instance-match seed"
        for _pfx in "${_inst_prefixes[@]}"; do _inst_args+=" --prefixes ${_pfx}"; done
        _inst_args+=" --output-dir ${MAPPINGS_DIR}/instance_matching"
        _inst_args+=" --timeout ${TIMEOUT}"
        _inst_args+=" --ports-json ${PORTS_JSON}"
        if [[ ${#DATASETS[@]} -gt 0 ]]; then
            for _ds in "${DATASETS[@]}"; do _inst_args+=" --dataset ${_ds}"; done
        fi
        eval "${_inst_args}" || warn "Instance mapping seeding had failures"
        log "Instance mappings done in $(elapsed $(( $(date +%s) - t0 )))"

        # Step 9: Class derivation + enrichment
        log "--- Step 9: Class derivation + enrichment ---"
        t0=$(date +%s)

        _inst_dir="${MAPPINGS_DIR}/instance_matching"
        _class_out_dir="${MAPPINGS_DIR}/class_derived"
        mkdir -p "${_class_out_dir}"

        _inst_files=$(find "${_inst_dir}" -maxdepth 1 -name '*.jsonld' \
            ! -name '*.enriched.jsonld' \
            ! -name '*.class_derived.jsonld' \
            | sort 2>/dev/null || true)

        if [[ -z "${_inst_files}" ]]; then
            warn "No instance-mapping JSON-LD files in ${_inst_dir}"
        else
            while IFS= read -r _f; do
                [[ -z "${_f}" ]] && continue
                _base=$(basename "${_f}" .jsonld)
                _out="${_class_out_dir}/${_base}.class_derived.jsonld"
                log "  Deriving: $(basename "${_f}")"
                rdfsolve instance-match derive \
                    --input    "${_f}" \
                    --output   "${_out}" \
                    --ports-json "${PORTS_JSON}" \
                    --cache-index \
                    --enrich \
                    --timeout  "${TIMEOUT}" \
                    || warn "Derivation failed for $(basename "${_f}")"
            done <<< "${_inst_files}"
        fi
        log "Class derivation done in $(elapsed $(( $(date +%s) - t0 )))"
    fi
else
    log "--- Steps 8–10: LSLOD - SKIPPED (--skip-local) ---"
fi

# Step 10: Stop all LSLOD instances
if [[ ${#LSLOD_INSTANCES[@]} -gt 0 ]]; then
    log "--- Step 10: Stopping LSLOD instances ---"
    for _i in "${!LSLOD_INSTANCES[@]}"; do
        _inst="${LSLOD_INSTANCES[${_i}]}"
        _port="${LSLOD_PORTS[${_i}]}"
        _iname="${_inst#qlever_}"
        _qlever_stop "${_inst}" "${_port}" "${QLEVER_WORKDIRS}/${_iname}"
    done
    log "All LSLOD instances stopped"
fi

# Step 11: Inference expansion
log "--- Step 11: Inference expansion ---"
t0=$(date +%s)

rdfsolve inference seed \
    --input-dir  "${MAPPINGS_DIR}" \
    --output-dir "${MAPPINGS_DIR}/inferenced" \
    || warn "Inference step had failures"
log "Inference done in $(elapsed $(( $(date +%s) - t0 )))"

# Step 12: Build connectivity graphs > Parquet
log "--- Step 12: Build graphs > Parquet ---"
t0=$(date +%s)

_bg_args="rdfsolve build-graphs"
_bg_args+=" --schemas-dir  ${OUTPUT_DIR}"
_bg_args+=" --mappings-dir ${MAPPINGS_DIR}"
_bg_args+=" --output-dir   ${PAPER_DATA_DIR}"
if [[ ${#DATASETS[@]} -gt 0 ]]; then
    for _ds in "${DATASETS[@]}"; do _bg_args+=" --datasets ${_ds}"; done
fi
eval "${_bg_args}" || warn "build-graphs had warnings"
log "Graph build done in $(elapsed $(( $(date +%s) - t0 )))"

else
    log "--- Steps 5–12: Mappings & graphs - SKIPPED (--skip-mappings) ---"
fi  # end SKIP_MAPPINGS

# ═══════════════════════════════════════════════════════════════════
# STEP 13 - Collect results
# ═══════════════════════════════════════════════════════════════════
log "--- Step 13: Collect results > ${RESULTS_DIR}/ ---"
rsync -a --info=progress2 "${OUTPUT_DIR}/" "${RESULTS_DIR}/" 2>/dev/null \
    || cp -r "${OUTPUT_DIR}/." "${RESULTS_DIR}/"

# ── Summary ───────────────────────────────────────────────────────
TOTAL_ELAPSED=$(( $(date +%s) - PIPELINE_START ))
FILE_COUNT=$(find "${RESULTS_DIR}" -type f 2>/dev/null | wc -l)
log "=== Pipeline complete ==="
log "Total time: $(elapsed ${TOTAL_ELAPSED})"
log "Results:    ${RESULTS_DIR}/ (${FILE_COUNT} files)"

if [[ "${FILE_COUNT}" -eq 0 ]]; then
    die "0 files in results - pipeline produced no output"
fi

_notify "Pipeline done" "$(elapsed ${TOTAL_ELAPSED}) - ${FILE_COUNT} files in ${RESULTS_DIR}" "default"
log "Done."
