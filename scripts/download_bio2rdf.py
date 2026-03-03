#!/usr/bin/env python3
"""
Download Bio2RDF dataset files (.nq.gz and .schema.owl) for local loading.

For each of the 35 Bio2RDF Release 3 datasets, this script:
  1. Fetches the JSON file listing from
     https://download.bio2rdf.org/files/release/3/<dataset>/
  2. Identifies the .nq.gz (N-Quads, compressed) and .schema.owl files
  3. Records the download URLs in data/sources.yaml
  4. Optionally downloads them to a local directory

Usage
-----
    # Dry-run: only update sources.yaml with download paths
    python scripts/download_bio2rdf.py --dry-run

    # Download all files to data/bio2rdf_local/
    python scripts/download_bio2rdf.py --output-dir data/bio2rdf_local

    # Download a single dataset
    python scripts/download_bio2rdf.py --datasets drugbank --output-dir data/bio2rdf_local

    # Only update sources.yaml (no download)
    python scripts/download_bio2rdf.py --update-yaml-only
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://download.bio2rdf.org/files/release/3"

BIO2RDF_DATASETS = [
    "affymetrix",
    "biomodels",
    "bioportal",
    "chembl",
    "clinicaltrials",
    "ctd",
    "dbsnp",
    "drugbank",
    "genage",
    "gendr",
    "goa",
    "hgnc",
    "homologene",
    "interpro",
    "iproclass",
    "irefindex",
    "kegg",
    "linkedspl",
    "lsr",
    "mesh",
    "mgi",
    "ncbigene",
    "ndc",
    "omim",
    "orphanet",
    "pathwaycommons",
    "pharmgkb",
    "pubmed",
    "reactome",
    "sabiork",
    "sgd",
    "sider",
    "taxonomy",
    "wikipathways",
    "wormbase",
]


# ── helpers ──────────────────────────────────────────────────────────────────


def fetch_file_listing(dataset: str) -> list[dict[str, Any]]:
    """Fetch the JSON directory listing for a Bio2RDF dataset."""
    url = f"{BASE_URL}/{dataset}/"
    log.info("Fetching file listing: %s", url)
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        log.warning("  ⚠  Could not fetch listing for %s: %s", dataset, exc)
        return []


def identify_files(
    dataset: str, listing: list[dict[str, Any]]
) -> dict[str, str | None]:
    """From a directory listing, pick out .nq.gz and .schema.owl files.

    Returns a dict with keys ``nq_files`` (list of URLs) and ``owl_url``
    (single URL or None).
    """
    nq_urls: list[str] = []
    owl_url: str | None = None

    for entry in listing:
        name: str = entry.get("name", "")
        if name.endswith(".nq.gz"):
            nq_urls.append(f"{BASE_URL}/{dataset}/{name}")
        elif name.endswith(".schema.owl"):
            owl_url = f"{BASE_URL}/{dataset}/{name}"

    return {"nq_files": nq_urls, "owl_url": owl_url}


def download_file(url: str, dest: Path) -> bool:
    """Download a file from *url* to *dest*.  Returns True on success."""
    if dest.exists():
        log.info("  ✓ Already exists: %s", dest.name)
        return True
    log.info("  ⬇  Downloading %s …", dest.name)
    try:
        urllib.request.urlretrieve(url, str(dest))
        size_mb = dest.stat().st_size / (1024 * 1024)
        log.info("  ✓ Done (%.1f MB)", size_mb)
        return True
    except urllib.error.URLError as exc:
        log.error("  ✗ Failed: %s", exc)
        return False


# ── sources.yaml integration ────────────────────────────────────────────────


def load_sources_yaml(path: Path) -> tuple[list[dict], str]:
    """Load sources.yaml preserving comments via raw text."""
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    return data, text


def update_sources_yaml(
    yaml_path: Path,
    download_info: dict[str, dict],
) -> None:
    """Add ``download_nq`` and ``download_owl`` fields to Bio2RDF entries.

    Reads the raw YAML text, finds each Bio2RDF entry by ``- name:`` line,
    and inserts download fields right after the ``notes:`` or ``use_graph:``
    line (whichever comes last for that entry).
    """
    text = yaml_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    # Build a map:  name → set of Bio2RDF dataset names that match it.
    # Bio2RDF central entries have name == dataset (e.g. "drugbank")
    # Bio2RDF mirror entries have name == "bio2rdf.<dataset>" (e.g. "bio2rdf.drugbank")
    name_to_dataset: dict[str, str] = {}
    for ds in download_info:
        name_to_dataset[ds] = ds                     # central: name == ds
        name_to_dataset[f"bio2rdf.{ds}"] = ds         # mirror:  bio2rdf.ds

    # Parse the YAML line-by-line to find entry boundaries and inject fields
    new_lines: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect "- name: <something>"
        stripped = line.lstrip()
        if stripped.startswith("- name:"):
            entry_name = stripped.split(":", 1)[1].strip()
            ds = name_to_dataset.get(entry_name)
            if ds and ds in download_info:
                info = download_info[ds]
                # Collect all lines of this entry (until next "- name:" or section comment or EOF)
                entry_lines = [line]
                j = i + 1
                while j < len(lines):
                    next_stripped = lines[j].lstrip()
                    # Next entry starts with "- name:" or a section header "# ═"
                    if next_stripped.startswith("- name:") or next_stripped.startswith("# ═"):
                        break
                    entry_lines.append(lines[j])
                    j += 1

                # Check if download fields already exist
                entry_text = "".join(entry_lines)
                if "download_nq:" in entry_text or "download_owl:" in entry_text:
                    # Already has download fields — skip
                    new_lines.extend(entry_lines)
                    i = j
                    continue

                # Find insertion point: after the last field line of the entry
                # (before any blank line or next entry)
                insert_idx = len(entry_lines) - 1
                # Walk backwards to skip trailing blank lines
                while insert_idx > 0 and entry_lines[insert_idx].strip() == "":
                    insert_idx -= 1
                insert_idx += 1  # insert after last non-blank line

                # Determine indentation (usually 2 spaces for sub-fields)
                indent = "  "

                # Build the new lines to insert
                inject: list[str] = []
                nq_files = info.get("nq_files", [])
                owl_url = info.get("owl_url")

                if nq_files:
                    if len(nq_files) == 1:
                        inject.append(f"{indent}download_nq: {nq_files[0]}\n")
                    else:
                        inject.append(f"{indent}download_nq:\n")
                        for nq in nq_files:
                            inject.append(f"{indent}  - {nq}\n")

                if owl_url:
                    inject.append(f"{indent}download_owl: {owl_url}\n")

                # Insert into entry_lines
                for idx, inj_line in enumerate(inject):
                    entry_lines.insert(insert_idx + idx, inj_line)

                new_lines.extend(entry_lines)
                i = j
                continue

        new_lines.append(line)
        i += 1

    yaml_path.write_text("".join(new_lines), encoding="utf-8")
    log.info("Updated %s with download URLs", yaml_path)


# ── report ───────────────────────────────────────────────────────────────────


def print_report(download_info: dict[str, dict]) -> None:
    """Print a summary table of discovered files."""
    print("\n" + "=" * 78)
    print(f"{'Dataset':<20} {'NQ files':>8}  {'OWL':>5}  {'NQ URLs'}")
    print("-" * 78)
    for ds in sorted(download_info):
        info = download_info[ds]
        nq_count = len(info.get("nq_files", []))
        has_owl = "✓" if info.get("owl_url") else "✗"
        nq_names = ", ".join(
            url.rsplit("/", 1)[-1] for url in info.get("nq_files", [])
        )
        if len(nq_names) > 40:
            nq_names = nq_names[:37] + "…"
        print(f"{ds:<20} {nq_count:>8}  {has_owl:>5}  {nq_names}")
    print("=" * 78)
    total_nq = sum(len(v.get("nq_files", [])) for v in download_info.values())
    total_owl = sum(1 for v in download_info.values() if v.get("owl_url"))
    print(f"Total: {len(download_info)} datasets, {total_nq} NQ files, {total_owl} OWL files\n")


# ── main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Bio2RDF Release 3 data files (.nq.gz + .schema.owl)."
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="Specific datasets to process (default: all 35).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to download files into. Each dataset gets a sub-directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only probe the server and print the report; don't download or update YAML.",
    )
    parser.add_argument(
        "--update-yaml-only",
        action="store_true",
        help="Probe the server and update sources.yaml with download URLs, but don't download files.",
    )
    parser.add_argument(
        "--sources-yaml",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "sources.yaml",
        help="Path to sources.yaml (default: data/sources.yaml).",
    )
    parser.add_argument(
        "--skip-download-nq",
        action="store_true",
        help="Skip downloading .nq.gz files (only download .schema.owl).",
    )
    args = parser.parse_args()

    datasets = args.datasets or BIO2RDF_DATASETS

    # ── Phase 1: probe each dataset for available files ──────────────
    download_info: dict[str, dict] = {}
    for ds in datasets:
        listing = fetch_file_listing(ds)
        if not listing:
            continue
        info = identify_files(ds, listing)
        if info["nq_files"] or info["owl_url"]:
            download_info[ds] = info

    print_report(download_info)

    if args.dry_run:
        log.info("Dry-run mode — nothing written.")
        return

    # ── Phase 2: update sources.yaml ─────────────────────────────────
    if args.sources_yaml.exists():
        update_sources_yaml(args.sources_yaml, download_info)
    else:
        log.warning("sources.yaml not found at %s — skipping YAML update.", args.sources_yaml)

    if args.update_yaml_only:
        log.info("YAML-only mode — skipping downloads.")
        return

    # ── Phase 3: download files ──────────────────────────────────────
    if args.output_dir is None:
        log.info("No --output-dir specified; skipping downloads.")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)

    for ds in sorted(download_info):
        info = download_info[ds]
        ds_dir = args.output_dir / ds
        ds_dir.mkdir(parents=True, exist_ok=True)

        # Download OWL schema (always — small file)
        owl_url = info.get("owl_url")
        if owl_url:
            fname = owl_url.rsplit("/", 1)[-1]
            download_file(owl_url, ds_dir / fname)

        # Download NQ files
        if not args.skip_download_nq:
            for nq_url in info.get("nq_files", []):
                fname = nq_url.rsplit("/", 1)[-1]
                download_file(nq_url, ds_dir / fname)

    log.info("All downloads complete.")


if __name__ == "__main__":
    main()
