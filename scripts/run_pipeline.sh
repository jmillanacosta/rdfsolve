#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — End-to-end RDFSolve mining pipeline
# =============================================================================
#
# Runs the full pipeline inside the Docker container:
#   1. Build the pipeline image (if needed)
#   2. Discover VoID from remote endpoints
#   3. Mine schemas from remote endpoints
#   4. Generate Qleverfiles for local mining
#   5. For each dataset: download → index → start → mine → stop
#   6. Copy results out of the Docker volume to ./results/
#
# Usage:
#   ./scripts/run_pipeline.sh              # test mode (3 smallest datasets)
#   ./scripts/run_pipeline.sh --all        # all 31 downloadable datasets
#   ./scripts/run_pipeline.sh --filter "drugbank|chembl"  # specific datasets
#   ./scripts/run_pipeline.sh --skip-remote              # local-mine only
#   ./scripts/run_pipeline.sh --skip-local               # remote-only
#   ./scripts/run_pipeline.sh --results-dir /mnt/data/results
#
# Prerequisites:
#   - Docker + Docker Compose installed
#   - Run from the repo root (where docker-compose.pipeline.yml lives)
#
# =============================================================================
set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────
MODE="test"              # "test" (3 datasets) or "all"
FILTER=""                # regex filter for dataset names
SKIP_REMOTE=false        # skip remote discover + mine
SKIP_LOCAL=false         # skip local qleverfile + download + mine
RESULTS_DIR="./results"  # where to copy final output on the host
DATA_DIR=""              # bind-mount host dir for /data (optional)
OUTPUT_DIR=""            # bind-mount host dir for /output (optional)
BENCHMARK=true           # collect benchmarks by default
BASE_PORT=7019           # first QLever port
TIMEOUT=120              # SPARQL timeout (seconds)
UNTYPED_AS_CLASSES=false # treat untyped URIs as owl:Class

# ── Compose shorthand ────────────────────────────────────────────
DC="docker compose -f docker-compose.pipeline.yml"
# RUN is built later, after DATA_DIR / OUTPUT_DIR are resolved.

# ── Colours ──────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'  # No Colour

# ── Parse arguments ──────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)          MODE="all"; shift ;;
        --test)         MODE="test"; shift ;;
        --filter)       FILTER="$2"; shift 2 ;;
        --skip-remote)  SKIP_REMOTE=true; shift ;;
        --skip-local)   SKIP_LOCAL=true; shift ;;
        --results-dir)  RESULTS_DIR="$2"; shift 2 ;;
        --data-dir)     DATA_DIR="$2"; shift 2 ;;
        --output-dir)   OUTPUT_DIR="$2"; shift 2 ;;
        --no-benchmark) BENCHMARK=false; shift ;;
        --untyped-as-classes) UNTYPED_AS_CLASSES=true; shift ;;
        --base-port)    BASE_PORT="$2"; shift 2 ;;
        --timeout)      TIMEOUT="$2"; shift 2 ;;
        --help|-h)
            head -30 "$0" | grep -E "^#" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}" >&2
            echo "Use --help for usage." >&2
            exit 1
            ;;
    esac
done

# ── Build common flags ───────────────────────────────────────────
COMMON_FLAGS="--timeout ${TIMEOUT}"
if [[ -n "${FILTER}" ]]; then
    COMMON_FLAGS="${COMMON_FLAGS} --filter '${FILTER}'"
fi
if [[ "${BENCHMARK}" == true ]]; then
    COMMON_FLAGS="${COMMON_FLAGS} --benchmark"
fi

MINE_FLAGS="${COMMON_FLAGS}"
if [[ "${UNTYPED_AS_CLASSES}" == true ]]; then
    MINE_FLAGS="${MINE_FLAGS} --untyped-as-classes"
fi

TEST_FLAG=""
if [[ "${MODE}" == "test" ]]; then
    TEST_FLAG="--test"
fi

# ── Export env vars for docker compose volume overrides ───────────
# When DATA_DIR / OUTPUT_DIR point to host directories we mount them
# at the *same* path inside the container.  This is critical for
# Docker-in-Docker: QLever's `index` and `start` commands spawn a
# nested Docker container and bind-mount $(pwd).  If the container path
# differs from the host path the nested container can't find the data.

if [[ -n "${DATA_DIR}" ]]; then
    mkdir -p "${DATA_DIR}"
    DATA_DIR="$(cd "${DATA_DIR}" && pwd)"
    export DATA_DIR
    CONTAINER_DATA_DIR="${DATA_DIR}"        # same as host
else
    CONTAINER_DATA_DIR="/data"             # default Docker volume
fi

if [[ -n "${OUTPUT_DIR}" ]]; then
    mkdir -p "${OUTPUT_DIR}"
    OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
    export OUTPUT_DIR
    CONTAINER_OUTPUT_DIR="${OUTPUT_DIR}"
else
    CONTAINER_OUTPUT_DIR="/output"
fi

# Append output-dir now that CONTAINER_OUTPUT_DIR is known.
DISCOVER_FLAGS="${COMMON_FLAGS} --output-dir ${CONTAINER_OUTPUT_DIR}"
MINE_FLAGS="${MINE_FLAGS} --output-dir ${CONTAINER_OUTPUT_DIR}"

# Build the RUN command.  When using host bind-mounts, override the
# compose-file volumes so the container path equals the host path.
EXTRA_VOLS=""
if [[ -n "${DATA_DIR}" ]]; then
    EXTRA_VOLS="${EXTRA_VOLS} -v ${DATA_DIR}:${DATA_DIR}"
fi
if [[ -n "${OUTPUT_DIR}" ]]; then
    EXTRA_VOLS="${EXTRA_VOLS} -v ${OUTPUT_DIR}:${OUTPUT_DIR}"
fi
RUN="${DC} run --rm -T${EXTRA_VOLS} pipeline"

# ── Helper functions ─────────────────────────────────────────────

banner() {
    echo ""
    echo -e "${BLUE}${BOLD}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

step() {
    echo -e "${GREEN}▸ $1${NC}"
}

warn() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

fail() {
    echo -e "${RED}✗ $1${NC}" >&2
}

elapsed() {
    local t=$1
    printf '%02d:%02d:%02d' $((t/3600)) $(( (t%3600)/60 )) $((t%60))
}

# ── Preflight checks ─────────────────────────────────────────────
banner "RDFSolve Pipeline — ${MODE} mode"

if [[ ! -f docker-compose.pipeline.yml ]]; then
    fail "docker-compose.pipeline.yml not found. Run from the repo root."
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    fail "Docker is not running."
    exit 1
fi

echo -e "  Mode:         ${BOLD}${MODE}${NC}"
echo -e "  Filter:       ${FILTER:-<all sources>}"
echo -e "  Benchmark:    ${BENCHMARK}"
echo -e "  Untyped→cls:  ${UNTYPED_AS_CLASSES}"
echo -e "  Results dir:  ${RESULTS_DIR}"
echo -e "  Skip remote:  ${SKIP_REMOTE}"
echo -e "  Skip local:   ${SKIP_LOCAL}"
echo ""

PIPELINE_START=$(date +%s)

# ══════════════════════════════════════════════════════════════════
# STEP 0: Build the pipeline image
# ══════════════════════════════════════════════════════════════════
banner "Step 0: Build pipeline image"
step "Building docker image …"
${DC} build --quiet
step "Image built successfully."

# ══════════════════════════════════════════════════════════════════
# STEP 1: Remote VoID discovery
# ══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false ]]; then
    banner "Step 1: Remote VoID discovery"
    step "Discovering VoID descriptions from remote SPARQL endpoints …"
    DISC_START=$(date +%s)

    eval "${RUN} rdfsolve pipeline discover ${DISCOVER_FLAGS}" || {
        warn "Some discover tasks failed (continuing)."
    }

    DISC_END=$(date +%s)
    step "Discovery completed in $(elapsed $((DISC_END - DISC_START)))."
else
    banner "Step 1: Remote VoID discovery — SKIPPED"
fi

# ══════════════════════════════════════════════════════════════════
# STEP 2: Remote mining
# ══════════════════════════════════════════════════════════════════
if [[ "${SKIP_REMOTE}" == false ]]; then
    banner "Step 2: Remote schema mining"
    step "Mining schemas from remote SPARQL endpoints …"
    MINE_START=$(date +%s)

    eval "${RUN} rdfsolve pipeline mine ${MINE_FLAGS}" || {
        warn "Some mining tasks failed (continuing)."
    }

    MINE_END=$(date +%s)
    step "Mining completed in $(elapsed $((MINE_END - MINE_START)))."
else
    banner "Step 2: Remote schema mining — SKIPPED"
fi

# ══════════════════════════════════════════════════════════════════
# STEP 3: Generate Qleverfiles
# ══════════════════════════════════════════════════════════════════
if [[ "${SKIP_LOCAL}" == false ]]; then
    # QLever spawns nested Docker containers via the Docker socket.
    # Those containers bind-mount $(pwd) from the host.  When using a
    # named Docker volume the container-internal path doesn't exist on
    # the host, so we require a host bind-mount (--data-dir).
    if [[ -z "${DATA_DIR}" ]]; then
        fail "Local mining requires --data-dir (a host directory)."
        fail "Named Docker volumes don't work with QLever Docker-in-Docker."
        fail "Example: --data-dir /tmp/rdfsolve-data"
        exit 1
    fi

    banner "Step 3: Generate Qleverfiles"
    step "Generating Qleverfiles for downloadable sources …"

    QLEVER_GEN_FLAGS="--data-dir ${CONTAINER_DATA_DIR} --base-port ${BASE_PORT}"
    if [[ -n "${TEST_FLAG}" ]]; then
        QLEVER_GEN_FLAGS="${QLEVER_GEN_FLAGS} ${TEST_FLAG}"
    fi
    if [[ -n "${FILTER}" ]]; then
        QLEVER_GEN_FLAGS="${QLEVER_GEN_FLAGS} --filter '${FILTER}'"
    fi

    eval "${RUN} rdfsolve pipeline qleverfile ${QLEVER_GEN_FLAGS} --output-dir ${CONTAINER_OUTPUT_DIR}" || {
        fail "Qleverfile generation failed."
        exit 1
    }

    step "Qleverfiles generated."

    # ══════════════════════════════════════════════════════════════
    # STEP 4: Download, index, and mine each dataset
    # ══════════════════════════════════════════════════════════════
    banner "Step 4: Download → Index → Mine (local QLever)"

    # Read the port manifest to know which datasets to process.
    # Run inside the container since the volume is there.
    PORTS_JSON=$(${RUN} bash -c "cat ${CONTAINER_DATA_DIR}/qlever_workdirs/ports.json 2>/dev/null || echo '{}'")

    if [[ "${PORTS_JSON}" == "{}" ]]; then
        warn "No ports.json found — no datasets to process."
    else
        # Parse dataset names and ports from the JSON manifest.
        DATASETS=$(echo "${PORTS_JSON}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
for name, port in d.items():
    print(f'{name} {port}')
")

        TOTAL=$(echo "${DATASETS}" | wc -l)
        IDX=0

        echo "${DATASETS}" | while read -r NAME PORT; do
            IDX=$((IDX + 1))
            echo ""
            echo -e "${BOLD}──────────────────────────────────────────────────────────${NC}"
            echo -e "${BOLD}  [${IDX}/${TOTAL}] ${NAME}  (port ${PORT})${NC}"
            echo -e "${BOLD}──────────────────────────────────────────────────────────${NC}"

            WORKDIR="${CONTAINER_DATA_DIR}/qlever_workdirs/${NAME}"

            # 4a. Download RDF data
            step "Downloading RDF data …"
            ${RUN} bash -c "cd ${WORKDIR} && qlever get-data" || {
                fail "[${NAME}] Download failed — skipping."
                continue
            }

            # 4b. Build QLever index
            step "Building QLever index …"
            ${RUN} bash -c "cd ${WORKDIR} && qlever index" || {
                fail "[${NAME}] Indexing failed — skipping."
                continue
            }

            # 4c. Start QLever SPARQL server
            step "Starting QLever server on port ${PORT} …"
            ${RUN} bash -c "cd ${WORKDIR} && qlever start" || {
                fail "[${NAME}] Server start failed — skipping."
                continue
            }

            # 4d. Mine schema from local endpoint
            step "Mining schema from http://localhost:${PORT} …"
            LOCAL_MINE_FLAGS="--name ${NAME} --endpoint http://localhost:${PORT}"
            LOCAL_MINE_FLAGS="${LOCAL_MINE_FLAGS} --discover-first"
            LOCAL_MINE_FLAGS="${LOCAL_MINE_FLAGS} --output-dir ${CONTAINER_OUTPUT_DIR}"
            if [[ "${BENCHMARK}" == true ]]; then
                LOCAL_MINE_FLAGS="${LOCAL_MINE_FLAGS} --benchmark"
            fi
            if [[ "${UNTYPED_AS_CLASSES}" == true ]]; then
                LOCAL_MINE_FLAGS="${LOCAL_MINE_FLAGS} --untyped-as-classes"
            fi

            eval "${RUN} rdfsolve pipeline local-mine ${LOCAL_MINE_FLAGS}" || {
                warn "[${NAME}] Mining failed (server may have stopped)."
            }

            # 4e. Stop QLever server
            step "Stopping QLever server …"
            ${RUN} bash -c "cd ${WORKDIR} && qlever stop" 2>/dev/null || true

            step "[${NAME}] Done."
        done
    fi
else
    banner "Steps 3–4: Local mining — SKIPPED"
fi

# ══════════════════════════════════════════════════════════════════
# STEP 5: Collect results
# ══════════════════════════════════════════════════════════════════
banner "Step 5: Collect results"

mkdir -p "${RESULTS_DIR}"

step "Copying output files from Docker volume to ${RESULTS_DIR}/ …"

# Copy /output contents to host
${RUN} bash -c "ls -la ${CONTAINER_OUTPUT_DIR}/" || true
CONTAINER_ID=$(docker create rdfsolve-2_pipeline:latest 2>/dev/null || \
               docker create rdfsolve-2-pipeline:latest 2>/dev/null || \
               echo "")

if [[ -n "${CONTAINER_ID}" ]]; then
    # Use a temporary container with the same volumes to copy files out
    docker rm -f "${CONTAINER_ID}" >/dev/null 2>&1 || true
fi

# Simpler: use docker compose to tar and extract
${RUN} bash -c "cd ${CONTAINER_OUTPUT_DIR} && tar cf - . 2>/dev/null" | tar xf - -C "${RESULTS_DIR}/" 2>/dev/null || {
    warn "Could not copy files via tar. Trying docker cp …"
    # Fallback: run a named container and docker cp
    ${DC} run -d --name rdfsolve-pipeline-copy pipeline sleep 30 >/dev/null 2>&1
    docker cp rdfsolve-pipeline-copy:${CONTAINER_OUTPUT_DIR}/. "${RESULTS_DIR}/" 2>/dev/null || true
    docker rm -f rdfsolve-pipeline-copy >/dev/null 2>&1 || true
}

# Also copy benchmark files if they exist
${RUN} bash -c "cd ${CONTAINER_OUTPUT_DIR} && ls benchmarks*.* 2>/dev/null" && \
    step "Benchmarks found in output." || true

# ── Summary ──────────────────────────────────────────────────────
PIPELINE_END=$(date +%s)
TOTAL_ELAPSED=$((PIPELINE_END - PIPELINE_START))

banner "Pipeline complete"

echo -e "  Mode:           ${BOLD}${MODE}${NC}"
echo -e "  Total time:     ${BOLD}$(elapsed ${TOTAL_ELAPSED})${NC}"
echo -e "  Results in:     ${BOLD}${RESULTS_DIR}/${NC}"
echo ""

# List what was produced
if [[ -d "${RESULTS_DIR}" ]]; then
    FILE_COUNT=$(find "${RESULTS_DIR}" -type f 2>/dev/null | wc -l)
    echo -e "  Files produced: ${BOLD}${FILE_COUNT}${NC}"
    echo ""

    # Breakdown by type
    for ext in jsonld ttl json csv jsonl; do
        cnt=$(find "${RESULTS_DIR}" -name "*.${ext}" 2>/dev/null | wc -l)
        if [[ ${cnt} -gt 0 ]]; then
            echo "    *.${ext}:  ${cnt} files"
        fi
    done

    echo ""
    TOTAL_SIZE=$(du -sh "${RESULTS_DIR}" 2>/dev/null | cut -f1)
    echo -e "  Total size:     ${BOLD}${TOTAL_SIZE}${NC}"
fi

echo ""
echo -e "${GREEN}${BOLD}Done. Results are in ${RESULTS_DIR}/${NC}"
echo ""
