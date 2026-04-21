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

# ── QLever server PID tracking ────────────────────────────────────────────────
declare -A SERVER_PID_MAP=()   # instance_name → PID
SERVER_PIDS=()                 # flat list for bulk cleanup

cleanup() {
    local _pids=()
    for _p in "${SERVER_PIDS[@]:-}"; do
        [[ -n "${_p}" ]] && kill -0 "${_p}" 2>/dev/null && _pids+=("${_p}")
    done
    [[ ${#_pids[@]} -eq 0 ]] && return 0
    echo "[$(date +%H:%M:%S)] cleanup: TERM → ${_pids[*]}"
    kill -TERM "${_pids[@]}" 2>/dev/null || true
    sleep 10
    kill -KILL "${_pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT TERM INT USR1

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────────
DATASETS=()
FILTER=""
EXCLUDE_ENGINES=()
SKIP_REMOTE=false
SKIP_DISCOVERY=false
SKIP_LOCAL=false
SKIP_MAPPINGS=false
SKIP_MINING=false
SKIP_MINE=false
SKIP_SEEDING=false
REMOTE_MAPPINGS=false
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
        --exclude-engine)     EXCLUDE_ENGINES+=("$2"); shift 2 ;;
        --skip-remote)        SKIP_REMOTE=true;        shift ;;
        --skip-discovery)     SKIP_DISCOVERY=true;     shift ;;
        --skip-local)         SKIP_LOCAL=true;         shift ;;
        --skip-mining)        SKIP_MINING=true;        shift ;;
        --skip-mine)          SKIP_MINE=true;          shift ;;
        --skip-mappings)      SKIP_MAPPINGS=true;      shift ;;
        --skip-seeding)       SKIP_SEEDING=true;       shift ;;
        --remote-mappings)    REMOTE_MAPPINGS=true;    shift ;;
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
    local name="$1" workdir="$2" port="$3" srv_mem="${4:-40G}"
    local instance_name="qlever_${name}"

    # Derive cache/extra budgets proportional to server mem (floor at 1G).
    # Formula: cache = srv_mem/5, extra = srv_mem/10, each min 1G.
    local _mem_mb
    _mem_mb=$(python3 -c "
import re, sys
s='${srv_mem}'.upper()
m=re.match(r'([0-9]+(?:\.[0-9]+)?)\s*([GMK]?)B?$',s)
if not m: sys.exit(1)
v,u=float(m.group(1)),m.group(2)
mb=int(v*(1024 if u=='G' else (1 if u=='M' else 1024*1024 if u=='K' else 1)))
print(mb)
" 2>/dev/null || echo "40960")
    local _cache_mb=$(( _mem_mb / 5 ));  [[ "${_cache_mb}" -lt 1024 ]] && _cache_mb=1024
    local _extra_mb=$(( _mem_mb / 10 )); [[ "${_extra_mb}" -lt 1024 ]] && _extra_mb=1024
    local srv_cache="${_cache_mb}M" srv_extra="${_extra_mb}M"

    # --- Ensure port is free before starting ---
    local _port_pid
    _port_pid=$(ss -tlnp "sport = :${port}" 2>/dev/null \
        | awk 'NR>1{match($0,/pid=([0-9]+)/,a); if(a[1]) print a[1]}' | head -1)
    if [[ -n "${_port_pid}" ]]; then
        warn "[${name}] Port ${port} occupied by PID ${_port_pid} – killing"
        kill -9 "${_port_pid}" 2>/dev/null || true
        sleep 2
    fi

    # --- Kill any leftover server for this name ---
    if [[ -n "${SERVER_PID_MAP[${instance_name}]+x}" ]]; then
        local _old_pid="${SERVER_PID_MAP[${instance_name}]}"
        kill -TERM "${_old_pid}" 2>/dev/null || true
        sleep 2
        kill -KILL "${_old_pid}" 2>/dev/null || true
        unset "SERVER_PID_MAP[${instance_name}]"
    fi

    # Clear old server log so we can detect fresh errors
    : > "${workdir}/server.log"

    # Run QLever in the foreground of a backgrounded singularity exec so
    # the process stays inside Slurm's cgroup and can be cleanly reaped.
    singularity exec \
        --bind "${workdir}:${workdir}" \
        --bind "${DATA_DIR}:${DATA_DIR}" \
        -W "${workdir}" \
        "${SINGULARITY_IMAGE}" \
        bash -c "cd '${workdir}' && exec qlever-server -i '${name}' -j 8 -p '${port}' -m ${srv_mem} -c ${srv_cache} -e ${srv_extra} -k 200 -s 1000s -a '${name}'" \
        > "${workdir}/server.log" 2>&1 &

    local srv_pid=$!
    SERVER_PIDS+=("${srv_pid}")
    SERVER_PID_MAP["${instance_name}"]="${srv_pid}"

    local i=0
    until env http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= \
          curl --noproxy '*' -sf "http://localhost:${port}/?query=ASK%7B%7D" >/dev/null 2>&1; do
        sleep 2; i=$((i+2))
        # Detect early fatal errors (e.g. "Address already in use") to fail fast
        if [[ -s "${workdir}/server.log" ]] \
            && grep -qi 'Address already in use\|cannot bind\|FATAL' "${workdir}/server.log" 2>/dev/null; then
            warn "[${name}] QLever failed: $(head -5 "${workdir}/server.log")"
            kill -TERM "${srv_pid}" 2>/dev/null || true
            return 1
        fi
        # Detect if the process already died
        if ! kill -0 "${srv_pid}" 2>/dev/null; then
            warn "[${name}] QLever process died unexpectedly"
            return 1
        fi
        [[ $i -ge 120 ]] && { warn "[${name}] QLever did not start within 120s"; return 1; }
    done
    log "[${name}] QLever ready on port ${port} (PID ${srv_pid}, mem ${srv_mem})"
    echo "${instance_name}"
}

_qlever_stop() {
    local instance_name="$1" port="$2" workdir="$3"
    local srv_pid="${SERVER_PID_MAP[${instance_name}]:-}"
    if [[ -n "${srv_pid}" ]]; then
        log "  Stopping QLever ${instance_name} (PID ${srv_pid}) …"
        kill -TERM "${srv_pid}" 2>/dev/null || true
        local i=0
        while kill -0 "${srv_pid}" 2>/dev/null && [[ $i -lt 30 ]]; do
            sleep 1; i=$((i+1))
        done
        kill -KILL "${srv_pid}" 2>/dev/null || true
        unset "SERVER_PID_MAP[${instance_name}]"
        # Remove from the flat SERVER_PIDS array
        local _new=()
        for _p in "${SERVER_PIDS[@]:-}"; do
            [[ "${_p}" != "${srv_pid}" ]] && _new+=("${_p}")
        done
        SERVER_PIDS=("${_new[@]:-}")
    else
        warn "  [${instance_name}] No tracked PID – server may already be gone"
    fi
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
    local _excl_engines="${EXCLUDE_ENGINES[*]:-}"
    python3 - "${REPO_ROOT}/data/sources.yaml" "${FILTER}" "${_excl_engines}" <<'PYEOF'
import sys, re, yaml
yaml_path, filt, excl_engines_str = sys.argv[1], sys.argv[2], sys.argv[3]
with open(yaml_path) as f:
    sources = yaml.safe_load(f) or []
rx = re.compile(filt) if filt else None
excl_engines = set(e.strip() for e in excl_engines_str.split() if e.strip())
for s in sources:
    name = s.get("name", "")
    if excl_engines:
        engine = s.get("sparql_engine", "") or s.get("local_provider", "")
        if engine in excl_engines:
            continue
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
# STEP 2b - Remote instance mapping + class derivation
#   Queries remote SPARQL endpoints directly (no local QLever).
#   Enabled via --remote-mappings.
# ═══════════════════════════════════════════════════════════════════
if [[ "${REMOTE_MAPPINGS}" == true && "${SKIP_MAPPINGS}" == false ]]; then
    log "--- Step 2b: Remote instance mapping ---"
    t0=$(date +%s)

    _remote_names=()
    while IFS= read -r _n; do
        [[ -n "${_n}" ]] && _remote_names+=("${_n}")
    done < <(_list_dataset_names)

    _rem_inst_out="${MAPPINGS_DIR}/instance_matching"
    mkdir -p "${_rem_inst_out}"

    # Discover prefixes from any existing sssom/semra files
    _rem_pfx_file="${_rem_inst_out}/.remote_prefixes.txt"
    _rem_disc="rdfsolve instance-match discover-prefixes"
    [[ -d "${MAPPINGS_DIR}/sssom" ]] && _rem_disc+=" -d ${MAPPINGS_DIR}/sssom"
    [[ -d "${MAPPINGS_DIR}/semra" ]] && _rem_disc+=" -d ${MAPPINGS_DIR}/semra"
    _rem_disc+=" -o ${_rem_pfx_file}"
    eval "${_rem_disc}" || warn "Remote prefix discovery had warnings"

    _rem_prefixes=()
    if [[ -s "${_rem_pfx_file}" ]]; then
        while IFS= read -r _p; do
            [[ -n "${_p}" ]] && _rem_prefixes+=("${_p}")
        done < "${_rem_pfx_file}"
    fi
    if [[ ${#_rem_prefixes[@]} -eq 0 ]]; then
        _rem_prefixes=(chebi ensembl faldo uniprot)
        warn "Remote prefix discovery empty; using defaults"
    fi
    log "Remote instance mapping prefixes (${#_rem_prefixes[@]}): ${_rem_prefixes[*]}"

    # Seed: query remote endpoints for each prefix; use --delay to throttle
    _rem_seed_args="rdfsolve instance-match seed"
    for _p in "${_rem_prefixes[@]}"; do _rem_seed_args+=" --prefixes ${_p}"; done
    _rem_seed_args+=" --output-dir ${_rem_inst_out}"
    _rem_seed_args+=" --timeout    ${TIMEOUT}"
    _rem_seed_args+=" --delay      2.0"
    if [[ ${#_remote_names[@]} -gt 0 ]]; then
        for _rn in "${_remote_names[@]}"; do _rem_seed_args+=" --dataset ${_rn}"; done
    fi
    eval "${_rem_seed_args}" || warn "Remote instance-match seed had failures"

    # Class derivation using each source's remote endpoint
    log "  Remote class derivation …"
    _rem_class_out="${MAPPINGS_DIR}/class_derived"
    mkdir -p "${_rem_class_out}"
    _rem_to_derive=$(find "${MAPPINGS_DIR}/sssom" "${MAPPINGS_DIR}/semra" \
        -maxdepth 1 -name '*.jsonld' \
        ! -name '*.enriched.jsonld' \
        ! -name '*.class_derived.jsonld' \
        2>/dev/null | sort || true)

    if [[ -n "${_rem_to_derive}" ]]; then
        # Build a combined remote endpoint list from sources.yaml
        _rem_endpoints=$(python3 - "${REPO_ROOT}/data/sources.yaml" <<'PYEOF'
import sys, yaml
with open(sys.argv[1]) as f:
    sources = yaml.safe_load(f) or []
seen = set()
for s in sources:
    ep = s.get("endpoint", "")
    if ep and ep not in seen:
        seen.add(ep)
        print(ep)
PYEOF
)
        # Use first available remote endpoint for class lookup
        _first_remote_ep=$(echo "${_rem_endpoints}" | head -1)
        if [[ -n "${_first_remote_ep}" ]]; then
            while IFS= read -r _f; do
                [[ -z "${_f}" ]] && continue
                _base=$(basename "${_f}" .jsonld)
                _out="${_rem_class_out}/${_base}.class_derived.jsonld"
                rdfsolve instance-match derive \
                    --input     "${_f}" \
                    --output    "${_out}" \
                    --endpoint  "${_first_remote_ep}" \
                    --cache-index \
                    --enrich \
                    --timeout   "${TIMEOUT}" \
                    || warn "Remote derivation failed for $(basename "${_f}")"
            done <<< "${_rem_to_derive}"
        fi
    fi
    log "Remote instance mapping done in $(elapsed $(( $(date +%s) - t0 )))"
else
    log "--- Step 2b: Remote instance mapping - SKIPPED (pass --remote-mappings to enable) ---"
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

            if [[ "${ONE_SHOT}" == true && "${SKIP_MINE}" == false ]]; then
                _mine_local "${NAME}" "${PORT}" "typed" true "${CHUNK_SIZE}" "${CLASS_BATCH_SIZE}"
            fi

            # ── Instance matching + class derivation ──────────────
            if [[ "${SKIP_MAPPINGS}" == false ]]; then
                _inst_out="${MAPPINGS_DIR}/instance_matching"
                mkdir -p "${_inst_out}"

                # Discover prefixes from sssom+semra for this dataset
                _pfx_file_ds="${_inst_out}/.prefixes_${NAME}.txt"
                _disc_args="rdfsolve instance-match discover-prefixes"
                [[ -d "${MAPPINGS_DIR}/sssom" ]]  && _disc_args+=" -d ${MAPPINGS_DIR}/sssom"
                [[ -d "${MAPPINGS_DIR}/semra" ]]  && _disc_args+=" -d ${MAPPINGS_DIR}/semra"
                _disc_args+=" -o ${_pfx_file_ds}"
                eval "${_disc_args}" || warn "[${NAME}] Prefix discovery had warnings"

                _ds_prefixes=()
                if [[ -s "${_pfx_file_ds}" ]]; then
                    while IFS= read -r _p; do
                        [[ -n "${_p}" ]] && _ds_prefixes+=("${_p}")
                    done < "${_pfx_file_ds}"
                fi
                if [[ ${#_ds_prefixes[@]} -eq 0 ]]; then
                    _ds_prefixes=(chebi ensembl faldo uniprot)
                    warn "[${NAME}] Prefix discovery empty; using defaults"
                fi

                # Build a single-entry ports JSON for this dataset
                _ds_ports_json="${_inst_out}/.ports_${NAME}.json"
                python3 -c "import json; print(json.dumps({'${NAME}': ${PORT}}))" \
                    > "${_ds_ports_json}"

                log "  [${NAME}] Instance-match seed (local, port ${PORT}) …"
                _seed_args="rdfsolve instance-match seed"
                for _p in "${_ds_prefixes[@]}"; do _seed_args+=" --prefixes ${_p}"; done
                _seed_args+=" --output-dir ${_inst_out}"
                _seed_args+=" --timeout    ${TIMEOUT}"
                _seed_args+=" --ports-json ${_ds_ports_json}"
                if [[ ${#DATASETS[@]} -gt 0 ]]; then
                    for _ds in "${DATASETS[@]}"; do _seed_args+=" --dataset ${_ds}"; done
                fi
                eval "${_seed_args}" || warn "[${NAME}] Instance-match seed had failures"

                # Class derivation for entity-level mapping files
                log "  [${NAME}] Class derivation …"
                _class_out="${MAPPINGS_DIR}/class_derived"
                mkdir -p "${_class_out}"
                _to_derive=$(find "${MAPPINGS_DIR}/sssom" "${MAPPINGS_DIR}/semra" \
                    -maxdepth 1 -name '*.jsonld' \
                    ! -name '*.enriched.jsonld' \
                    ! -name '*.class_derived.jsonld' \
                    2>/dev/null | sort || true)
                if [[ -n "${_to_derive}" ]]; then
                    while IFS= read -r _f; do
                        [[ -z "${_f}" ]] && continue
                        _base=$(basename "${_f}" .jsonld)
                        _out="${_class_out}/${_base}.class_derived.jsonld"
                        rdfsolve instance-match derive \
                            --input      "${_f}" \
                            --output     "${_out}" \
                            --endpoint   "http://localhost:${PORT}" \
                            --cache-index \
                            --enrich \
                            --timeout    "${TIMEOUT}" \
                            || warn "[${NAME}] Derivation failed for $(basename "${_f}")"
                    done <<< "${_to_derive}"
                fi
            fi

            log "  Stopping QLever …"
            _qlever_stop "${INSTANCE_NAME}" "${PORT}" "${WORKDIR}"

            # Remove raw RDF dumps and archives; index files are preserved.
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

SCHEMAS_DIR="${OUTPUT_DIR}/schemas"
MAPPINGS_DIR="${OUTPUT_DIR}/mappings"
PAPER_DATA_DIR="${OUTPUT_DIR}/paper_data"
mkdir -p "${SCHEMAS_DIR}" "${MAPPINGS_DIR}" "${PAPER_DATA_DIR}"

if [[ "${SKIP_SEEDING}" == false ]]; then

# Step 5: Schema selection
log "--- Step 5: Schema selection ---"
t0=$(date +%s)

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

else
    log "--- Steps 5–7: Seeding - SKIPPED (--skip-seeding) ---"
fi  # end SKIP_SEEDING

# Step 8: Inference expansion
log "--- Step 8: Inference expansion ---"
t0=$(date +%s)

rdfsolve inference seed \
    --input-dir  "${MAPPINGS_DIR}" \
    --output-dir "${MAPPINGS_DIR}/inferenced" \
    || warn "Inference step had failures"
log "Inference done in $(elapsed $(( $(date +%s) - t0 )))"

# Step 9: Build connectivity graphs > Parquet
log "--- Step 9: Build graphs > Parquet ---"
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
    log "--- Steps 5–9: Mappings & graphs - SKIPPED (--skip-mappings) ---"
fi  # end SKIP_MAPPINGS

# ═══════════════════════════════════════════════════════════════════
# STEP 10 - Collect results
# ═══════════════════════════════════════════════════════════════════
log "--- Step 10: Collect results > ${RESULTS_DIR}/ ---"
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
