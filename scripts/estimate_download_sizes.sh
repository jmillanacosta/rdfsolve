#!/usr/bin/env bash
# estimate_download_sizes.sh — check Content-Length for all download URLs in sources.yaml
#
# Usage:
#   bash scripts/estimate_download_sizes.sh
#   bash scripts/estimate_download_sizes.sh --source glycosmos
set -euo pipefail
export LC_ALL=C

YAML="${YAML:-data/sources.yaml}"
FILTER_SOURCE="${FILTER_SOURCE:-}"
TSV_OUT="/tmp/download_size_estimate.tsv"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --yaml)    YAML="$2";          shift 2 ;;
        --source)  FILTER_SOURCE="$2"; shift 2 ;;
        *)         echo "Unknown: $1"; exit 1 ;;
    esac
done

# Extract URLs from YAML → lines: SOURCE\tURL
URLS_TSV=$(python3 - "${YAML}" "${FILTER_SOURCE}" <<'PYEOF'
import sys, yaml

yaml_path = sys.argv[1]
filt = sys.argv[2] if len(sys.argv) > 2 else ""
with open(yaml_path) as f:
    sources = yaml.safe_load(f)

seen_tars = {}
tar_members = {}

for s in sources:
    name = s.get("name", "?")
    if filt and name != filt:
        continue
    emitted = False
    for k, v in s.items():
        if not k.startswith("download_"):
            continue
        items = v if isinstance(v, list) else [v]
        for url in items:
            if url:
                print(f"{name}\t{url}")
                emitted = True
    tar = s.get("local_tar_url")
    if tar:
        provider = s.get("local_provider", name)
        tar_members.setdefault(tar, []).append(name)
        if not emitted and tar not in seen_tars:
            seen_tars[tar] = provider
            print(f"{provider}\t{tar}")

for tar_url, members in tar_members.items():
    provider = seen_tars.get(tar_url, "?")
    print(f"__tar_info__\t{provider}\t{len(members)}\t{tar_url}")
PYEOF
)

[[ -z "${URLS_TSV}" ]] && { echo "No URLs found (filter='${FILTER_SOURCE}')." >&2; exit 1; }

REAL_URLS=$(echo "${URLS_TSV}" | grep -v '^__tar_info__' || true)
TAR_INFO_LINES=$(echo "${URLS_TSV}" | grep '^__tar_info__' || true)
TOTAL_URLS=$(echo "${REAL_URLS}" | grep -c . || echo 0)

echo "Checking ${TOTAL_URLS} URLs (20 parallel) …"
printf "source\turl\tcontent_length_bytes\tnote\n" > "${TSV_OUT}"
[[ -n "${TAR_INFO_LINES}" ]] && echo "${TAR_INFO_LINES}" >> "${TSV_OUT}"

_head_one() {
    local src="$1" url="$2"
    if [[ "${url}" =~ ^ftp:// ]] || [[ "${url}" =~ /download-data$ ]] || [[ "${url}" =~ /about/download$ ]]; then
        printf "%s\t%s\t0\tskipped\n" "${src}" "${url}"; return
    fi
    local cl
    cl=$(curl -sI --max-time 8 --location "${url}" 2>/dev/null \
        | grep -i "^content-length:" | tail -1 \
        | tr -d '[:space:]' | cut -d: -f2 || true)
    if [[ ! "${cl}" =~ ^[0-9]+$ ]]; then
        local range_hdr
        range_hdr=$(curl -s --max-time 8 --location -r 0-0 -I "${url}" 2>/dev/null \
            | grep -i "^content-range:" | tail -1 || true)
        cl=$(echo "${range_hdr}" | grep -oP '(?<=/)\d+' || true)
    fi
    if [[ "${cl}" =~ ^[0-9]+$ ]]; then
        local note="ok"; [[ "${url}" == *".tar"* ]] && note="ok (shared tar)"
        printf "%s\t%s\t%s\t%s\n" "${src}" "${url}" "${cl}" "${note}"
    else
        printf "%s\t%s\t0\tno Content-Length\n" "${src}" "${url}"
    fi
}
export -f _head_one

echo "${REAL_URLS}" \
    | awk -F'\t' '{print $1"\t"$2}' \
    | xargs -P 20 -d '\n' -I {} bash -c 'src="${@%%	*}"; url="${@#*	}"; _head_one "$src" "$url"' _ {} \
    >> "${TSV_OUT}"

echo "Done — ${TOTAL_URLS} URLs checked."

# Accumulate per-source totals
declare -A SRC_BYTES SRC_UNKNOWN TAR_INFO
GRAND_BYTES=0; GRAND_UNKNOWN=0

while IFS=$'\t' read -r SRC _URL BYTES NOTE; do
    [[ "${SRC}" == "source" ]] && continue
    if [[ "${SRC}" == "__tar_info__" ]]; then
        TAR_INFO["${_URL}"]="${BYTES} members"
        continue
    fi
    if [[ "${NOTE}" == ok* ]] && [[ "${BYTES}" =~ ^[0-9]+$ ]] && [[ "${BYTES}" -gt 0 ]]; then
        GRAND_BYTES=$(( GRAND_BYTES + BYTES ))
        SRC_BYTES["${SRC}"]=$(( ${SRC_BYTES["${SRC}"]:-0} + BYTES ))
    else
        GRAND_UNKNOWN=$(( GRAND_UNKNOWN + 1 ))
        SRC_UNKNOWN["${SRC}"]=$(( ${SRC_UNKNOWN["${SRC}"]:-0} + 1 ))
    fi
done < "${TSV_OUT}"

human() {
    local b=$1
    if   (( b >= 1099511627776 )); then printf "%.1f TiB" "$(echo "scale=1; ${b}/1099511627776" | bc)"
    elif (( b >=    1073741824 )); then printf "%.1f GiB" "$(echo "scale=1; ${b}/1073741824"    | bc)"
    elif (( b >=       1048576 )); then printf "%.1f MiB" "$(echo "scale=1; ${b}/1048576"        | bc)"
    else                               printf "%d B" "${b}"
    fi
}

echo ""
printf "  %-40s %12s  %s\n" "SOURCE" "BYTES" "HUMAN"
printf "  %-40s %12s  %s\n" "------" "-----" "-----"
for SRC in $(for k in "${!SRC_BYTES[@]}"; do echo "${SRC_BYTES[$k]} ${k}"; done | sort -rn | awk '{print $2}'); do
    B="${SRC_BYTES[$SRC]}"; UNK="${SRC_UNKNOWN[$SRC]:-0}"; EXTRA=""
    [[ "${UNK}" -gt 0 ]] && EXTRA=" (+${UNK} unknown)"
    [[ -v TAR_INFO["${SRC}"] ]] && EXTRA+=" [${TAR_INFO[$SRC]}]"
    printf "  %-40s %12d  %s%s\n" "${SRC}" "${B}" "$(human "${B}")" "${EXTRA}"
done
for SRC in "${!SRC_UNKNOWN[@]}"; do
    [[ -v SRC_BYTES["${SRC}"] ]] && continue
    printf "  %-40s %12s  (all %d unknown)\n" "${SRC}" "?" "${SRC_UNKNOWN[$SRC]}"
done

echo ""
printf "  %-40s %12d  %s\n" "TOTAL (known)" "${GRAND_BYTES}" "$(human "${GRAND_BYTES}")"
[[ "${GRAND_UNKNOWN}" -gt 0 ]] && printf "  %-40s %12s  (%d unknown)\n" "" "?" "${GRAND_UNKNOWN}"
echo "TSV: ${TSV_OUT}"
