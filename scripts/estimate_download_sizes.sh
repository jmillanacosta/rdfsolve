#!/usr/bin/env bash
# estimate_download_sizes.sh
#
# For every download_* URL in data/sources.yaml, fetch the
# Content-Length header with curl --head (no actual download).
# Reports per-source totals and a grand total, in bytes and human-readable.
#
# Usage:
#   bash scripts/estimate_download_sizes.sh [--yaml data/sources.yaml]
#   bash scripts/estimate_download_sizes.sh --source glycosmos
#
# Output goes to stdout; a TSV summary is also written to
# /tmp/download_size_estimate.tsv

set -euo pipefail

# YAML can be set as env var or first positional arg; env var wins if set.
YAML="${YAML:-${1:-data/sources.yaml}}"
FILTER_SOURCE="${FILTER_SOURCE:-}"   # set to a source name to restrict
TSV_OUT="/tmp/download_size_estimate.tsv"

# Force C locale so bc/printf always use '.' as decimal separator.
export LC_ALL=C

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# ── Extract URLs from YAML ────────────────────────────────────────
# Produces lines: SOURCE_NAME\tURL
URLS_TSV=$(python3 - "${YAML}" "${FILTER_SOURCE}" <<'PYEOF'
import sys, yaml

yaml_path = sys.argv[1]
filt      = sys.argv[2] if len(sys.argv) > 2 else ""

with open(yaml_path) as f:
    sources = yaml.safe_load(f)

seen_tars = {}   # tar_url -> provider_name, to emit each tar once
tar_members = {} # tar_url -> [member_names], to report breakdown

for s in sources:
    name = s.get("name", "?")
    if filt and name != filt:
        continue
    emitted = False
    # download_* URLs (individual files)
    for k, v in s.items():
        if not k.startswith("download_"):
            continue
        items = v if isinstance(v, list) else [v]
        for url in items:
            if url:
                print(f"{name}\t{url}")
                emitted = True
    # local_tar_url: shared provider tar streamed per-subdir, downloaded ONCE.
    # Emit once under the provider name; track members for the size report.
    tar = s.get("local_tar_url")
    if tar:
        provider = s.get("local_provider", name)
        tar_members.setdefault(tar, []).append(name)
        if not emitted and tar not in seen_tars:
            seen_tars[tar] = provider
            print(f"{provider}\t{tar}")

# Emit member-count metadata lines for reporting (parsed below, not sent to curl)
for tar_url, members in tar_members.items():
    provider = seen_tars.get(tar_url, "?")
    print(f"__tar_info__\t{provider}\t{len(members)}\t{tar_url}")
PYEOF
)

if [[ -z "${URLS_TSV}" ]]; then
    echo "No download URLs found (filter='${FILTER_SOURCE}')." >&2
    exit 1
fi

# Split: real URLs vs __tar_info__ metadata lines (must not be curled)
REAL_URLS=$(echo "${URLS_TSV}" | grep -v '^__tar_info__' || true)
TAR_INFO_LINES=$(echo "${URLS_TSV}" | grep '^__tar_info__' || true)

TOTAL_URLS=$(echo "${REAL_URLS}" | grep -c . || echo 0)
echo "Checking ${TOTAL_URLS} URLs (parallel, up to 20 at once) …"
echo ""

# ── TSV header ────────────────────────────────────────────────────
printf "source\turl\tcontent_length_bytes\tnote\n" > "${TSV_OUT}"

# Append __tar_info__ lines verbatim so the accumulator can read them
if [[ -n "${TAR_INFO_LINES}" ]]; then
    echo "${TAR_INFO_LINES}" >> "${TSV_OUT}"
fi

# ── Parallel HEAD requests via xargs ─────────────────────────────
# Each line: SOURCE\tURL -> SOURCE\tURL\tBYTES\tNOTE
_head_one() {
    local src="$1" url="$2"
    local cl note

    # Skip FTP directory listings and known HTML index pages – no size available.
    if [[ "${url}" =~ ^ftp:// ]] || [[ "${url}" =~ /download-data$ ]] || [[ "${url}" =~ /about/download$ ]]; then
        printf "%s\t%s\t0\tskipped (FTP/index page)\n" "${src}" "${url}"
        return
    fi

    # 1st attempt: HEAD request
    cl=$(curl -sI --max-time 8 --location "${url}" 2>/dev/null \
        | grep -i "^content-length:" | tail -1 \
        | tr -d '[:space:]' | cut -d: -f2 || true)

    # 2nd attempt: GET with Range header (servers that ignore HEAD)
    if [[ ! "${cl}" =~ ^[0-9]+$ ]]; then
        local range_hdr
        range_hdr=$(curl -s --max-time 8 --location -r 0-0 -I "${url}" 2>/dev/null \
            | grep -i "^content-range:" | tail -1 || true)
        # Content-Range: bytes 0-0/TOTAL  or  bytes */TOTAL
        cl=$(echo "${range_hdr}" | grep -oP '(?<=/)\d+' || true)
    fi

    if [[ "${cl}" =~ ^[0-9]+$ ]]; then
        note="ok"
        [[ "${url}" == *".tar"* ]] && note="ok (shared tar)"
        printf "%s\t%s\t%s\t%s\n" "${src}" "${url}" "${cl}" "${note}"
    else
        printf "%s\t%s\t0\tno Content-Length\n" "${src}" "${url}"
    fi
}
export -f _head_one

# Feed "SRC URL" pairs to xargs, run _head_one in parallel (20 workers).
# Progress printed to stderr every 50 lines.
echo "${REAL_URLS}" \
| awk -F'\t' '{print $1"\t"$2}' \
| xargs -P 20 -d '\n' -I {} bash -c '
    src="${@%%	*}"; url="${@#*	}"
    _head_one "$src" "$url"
' _ {} \
>> "${TSV_OUT}"

echo "  Done — ${TOTAL_URLS} URLs checked."

# ── Per-source accumulators ───────────────────────────────────────
declare -A SRC_BYTES
declare -A SRC_UNKNOWN
declare -A TAR_INFO   # provider -> "N members"
GRAND_BYTES=0
GRAND_UNKNOWN=0

while IFS=$'\t' read -r SRC _URL BYTES NOTE; do
    [[ "${SRC}" == "source" ]] && continue  # skip TSV header
    # __tar_info__ lines: provider \t N \t tar_url  (from Python extractor)
    if [[ "${SRC}" == "__tar_info__" ]]; then
        # fields: __tar_info__ \t provider \t N \t tar_url
        PROV="${_URL}"; NMEMBERS="${BYTES}"
        TAR_INFO["${PROV}"]="${NMEMBERS} members (streamed per-subdir, tar downloaded once)"
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

# ── Human-readable helper ─────────────────────────────────────────
human() {
    local b=$1
    if   (( b >= 1099511627776 )); then printf "%.1f TiB" "$(echo "scale=1; ${b}/1099511627776" | bc)"
    elif (( b >=    1073741824 )); then printf "%.1f GiB" "$(echo "scale=1; ${b}/1073741824"    | bc)"
    elif (( b >=       1048576 )); then printf "%.1f MiB" "$(echo "scale=1; ${b}/1048576"        | bc)"
    elif (( b >=          1024 )); then printf "%.1f KiB" "$(echo "scale=1; ${b}/1024"           | bc)"
    else                               printf "%d B" "${b}"
    fi
}

# ── Per-source report ─────────────────────────────────────────────
echo ""
echo "Per-source totals (compressed / as downloaded):"
printf "  %-40s %12s  %s\n" "SOURCE" "BYTES" "HUMAN"
printf "  %-40s %12s  %s\n" "──────" "─────" "─────"

# Sort by bytes descending
for SRC in $(for k in "${!SRC_BYTES[@]}"; do echo "${SRC_BYTES[$k]} ${k}"; done \
            | sort -rn | awk '{print $2}'); do
    B="${SRC_BYTES[$SRC]}"
    UNK="${SRC_UNKNOWN[$SRC]:-0}"
    EXTRA=""
    [[ "${UNK}" -gt 0 ]] && EXTRA=" (+${UNK} unknown)"
    [[ -v TAR_INFO["${SRC}"] ]] && EXTRA+=" [${TAR_INFO[$SRC]}]"
    printf "  %-40s %12d  %s%s\n" "${SRC}" "${B}" "$(human "${B}")" "${EXTRA}"
done

# Sources with only unknown sizes
for SRC in "${!SRC_UNKNOWN[@]}"; do
    [[ -v SRC_BYTES["${SRC}"] ]] && continue
    UNK="${SRC_UNKNOWN[$SRC]}"
    printf "  %-40s %12s  (all %d unknown)\n" "${SRC}" "?" "${UNK}"
done

# ── Grand total ───────────────────────────────────────────────────
echo ""
echo "────────────────────────────────────────────────────────────"
printf "  %-40s %12d  %s\n" "GRAND TOTAL (known)" "${GRAND_BYTES}" "$(human "${GRAND_BYTES}")"
[[ "${GRAND_UNKNOWN}" -gt 0 ]] && \
    printf "  %-40s %12s  (%d URLs returned no Content-Length)\n" \
           "UNKNOWN" "?" "${GRAND_UNKNOWN}"
echo ""
echo "Full TSV written to: ${TSV_OUT}"
