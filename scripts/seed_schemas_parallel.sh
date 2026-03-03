#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────
# Mine schemas for ALL sources in parallel.
#
# Each source runs as a background process with its own log file:
#   docker/schemas/<name>/output.log
#
# Per source, produces:
#   <name>_schema.jsonld   — primary JSON-LD schema (mined)
#   <name>_void.ttl        — VoID Turtle description
#   <name>_linkml.yaml     — LinkML YAML schema
#   <name>_shacl.ttl       — SHACL shapes (Turtle)
#   <name>_config/         — RDF-config YAMLs (model, prefix, endpoint)
#   <name>_coverage.csv    — coverage table (class × property × type)
#   report.json            — mining report / diagnostics
#
# Usage:
#   bash scripts/seed_schemas_parallel.sh                        # all sources
#   bash scripts/seed_schemas_parallel.sh -j 4                   # max 4 jobs
#   bash scripts/seed_schemas_parallel.sh -j 4 -s data/sources_all.yaml
#   bash scripts/seed_schemas_parallel.sh --no-filter-service    # keep service ns
# ──────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCES="${REPO_ROOT}/data/sources.yaml"
OUTPUT_DIR="${REPO_ROOT}/docker/schemas"
MAX_JOBS=0  # 0 = unlimited
FILTER_SERVICE="True"

# ── parse args ────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -j|--jobs)    MAX_JOBS="$2"; shift 2 ;;
    -s|--sources) SOURCES="$2";  shift 2 ;;
    -o|--output)  OUTPUT_DIR="$2"; shift 2 ;;
    --no-filter-service) FILTER_SERVICE="False"; shift ;;
    -h|--help)
      echo "Usage: $0 [-j MAX_JOBS] [-s SOURCES_YAML] [-o OUTPUT_DIR] [--no-filter-service]"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

mkdir -p "${OUTPUT_DIR}"

# ── extract source names from YAML ───────────────────────────────
NAMES=$(python3 -c "
import yaml, sys
with open('${SOURCES}') as f:
    sources = yaml.safe_load(f)
for s in sources:
    ep = s.get('endpoint', '')
    if ep:
        print(s['name'])
")

TOTAL=$(echo "${NAMES}" | wc -l | tr -d ' ')
echo "═══════════════════════════════════════════════════════════"
echo "  Parallel schema mining — ${TOTAL} sources"
echo "  Sources : ${SOURCES}"
echo "  Output  : ${OUTPUT_DIR}"
echo "  Max jobs: $([ "${MAX_JOBS}" -eq 0 ] && echo 'unlimited' || echo "${MAX_JOBS}")"
echo "  Filter service ns: ${FILTER_SERVICE}"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ── mine one source (function used by background jobs) ────────────
mine_one() {
  local name="$1"
  local src_dir="${OUTPUT_DIR}/${name}"
  local logfile="${src_dir}/output.log"
  local schema_file="${src_dir}/${name}_schema.jsonld"

  mkdir -p "${src_dir}"

  # Skip if schema already exists
  if [[ -f "${schema_file}" ]]; then
    echo "[SKIP] ${name} — already exists" | tee -a "${logfile}"
    return 0
  fi

  echo "[START] ${name}" | tee -a "${logfile}"

  python3 -c "
import json, sys, time, traceback
sys.path.insert(0, '${REPO_ROOT}/src')

from rdfsolve.sources import load_sources
from rdfsolve.api import mine_schema, load_parser_from_jsonld

# Find this source entry
entries = load_sources('${SOURCES}')
entry = next((e for e in entries if e['name'] == '${name}'), None)
if entry is None:
    print('ERROR: source ${name} not found', file=sys.stderr)
    sys.exit(1)

endpoint = entry.get('endpoint', '')
if not endpoint:
    print('SKIP: no endpoint')
    sys.exit(0)

use_graph = entry.get('use_graph', False)
graph_uris = entry.get('graph_uris', []) if use_graph else None
two_phase = entry.get('two_phase', True)

t0 = time.monotonic()
try:
    result = mine_schema(
        endpoint_url=endpoint,
        dataset_name='${name}',
        graph_uris=graph_uris if graph_uris else None,
        two_phase=two_phase,
        chunk_size=entry.get('chunk_size', 500),
        class_batch_size=entry.get('class_batch_size', 15),
        delay=entry.get('delay', 10),
        timeout=entry.get('timeout', 99999),
        counts=entry.get('counts', True),
        filter_service_namespaces=${FILTER_SERVICE},
        report_path = '${src_dir}/report.json'
    )
    # ── save JSON-LD (primary output) ────────────────────────────
    with open('${schema_file}', 'w') as f:
        json.dump(result, f, indent=2)
    elapsed_mine = time.monotonic() - t0
    print(f'MINED {elapsed_mine:.1f}s — exporting derivatives...')

    # ── export derivatives from JSON-LD ──────────────────────────
    parser = load_parser_from_jsonld('${schema_file}')

    # VoID Turtle
    try:
        void_graph = parser.graph
        void_graph.serialize('${src_dir}/${name}_void.ttl', format='turtle')
        print('  VoID       OK')
    except Exception as e:
        print(f'  VoID       FAIL: {e}', file=sys.stderr)

    # LinkML YAML
    try:
        linkml_yaml = parser.to_linkml_yaml(
            schema_name='${name}',
            schema_base_uri='https://w3id.org/rdfsolve/${name}/',
        )
        with open('${src_dir}/${name}_linkml.yaml', 'w') as f:
            f.write(linkml_yaml)
        print('  LinkML     OK')
    except Exception as e:
        print(f'  LinkML     FAIL: {e}', file=sys.stderr)

    # SHACL Turtle
    try:
        shacl_ttl = parser.to_shacl(
            schema_name='${name}',
            schema_base_uri='https://w3id.org/rdfsolve/${name}/',
        )
        with open('${src_dir}/${name}_shacl.ttl', 'w') as f:
            f.write(shacl_ttl)
        print('  SHACL      OK')
    except Exception as e:
        print(f'  SHACL      FAIL: {e}', file=sys.stderr)

    # RDF-config YAMLs
    try:
        rdfconfig = parser.to_rdfconfig(
            endpoint_url=endpoint,
            endpoint_name='${name}',
            graph_uri=(graph_uris[0] if graph_uris else None),
        )
        import os
        cfg_dir = '${src_dir}/${name}_config'
        os.makedirs(cfg_dir, exist_ok=True)
        for fname, content in rdfconfig.items():
            with open(os.path.join(cfg_dir, fname + '.yaml'), 'w') as f:
                f.write(content)
        print('  RDF-config OK')
    except Exception as e:
        print(f'  RDF-config FAIL: {e}', file=sys.stderr)

    # Coverage table (CSV)
    try:
        schema_df = parser.to_schema()
        if not schema_df.empty:
            schema_df.to_csv('${src_dir}/${name}_coverage.csv', index=False)
            print(f'  Coverage   OK ({len(schema_df)} rows)')
        else:
            print('  Coverage   SKIP (empty)')
    except Exception as e:
        print(f'  Coverage   FAIL: {e}', file=sys.stderr)

    elapsed = time.monotonic() - t0
    print(f'SUCCESS {elapsed:.1f}s (mine {elapsed_mine:.1f}s + exports {elapsed - elapsed_mine:.1f}s)')
except Exception:
    elapsed = time.monotonic() - t0
    traceback.print_exc()
    print(f'FAIL after {elapsed:.1f}s', file=sys.stderr)
    sys.exit(1)
" >>"${logfile}" 2>&1

  local rc=$?
  if [[ ${rc} -eq 0 ]]; then
    echo "[  OK ] ${name}  (log: ${logfile})"
  else
    echo "[ FAIL] ${name}  (log: ${logfile})"
  fi
  return ${rc}
}

export -f mine_one  # needed if using xargs/parallel (not used here)

# ── launch jobs ───────────────────────────────────────────────────
RUNNING=0
PIDS=()
NAMES_ARR=()
SUCCEEDED=0
FAILED=0
START_TIME=$(date +%s)

while IFS= read -r name; do
  # Throttle: wait for a slot if MAX_JOBS is set
  if [[ ${MAX_JOBS} -gt 0 ]]; then
    while [[ ${RUNNING} -ge ${MAX_JOBS} ]]; do
      # Wait for any one child to finish
      wait -n 2>/dev/null || true
      RUNNING=$((RUNNING - 1))
    done
  fi

  mine_one "${name}" &
  PIDS+=($!)
  NAMES_ARR+=("${name}")
  RUNNING=$((RUNNING + 1))

done <<< "${NAMES}"

# ── wait for all and collect exit codes ───────────────────────────
for i in "${!PIDS[@]}"; do
  if wait "${PIDS[$i]}"; then
    SUCCEEDED=$((SUCCEEDED + 1))
  else
    FAILED=$((FAILED + 1))
  fi
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  DONE in ${ELAPSED}s"
echo "  ✓ Succeeded: ${SUCCEEDED}"
echo "  ✗ Failed:    ${FAILED}"
echo "  Logs in:     ${OUTPUT_DIR}/<name>/output.log"
echo "═══════════════════════════════════════════════════════════"

# Exit non-zero if anything failed
[[ ${FAILED} -eq 0 ]]
