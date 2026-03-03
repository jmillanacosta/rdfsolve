#!/usr/bin/env python3
"""
Download UniProt RDF data files from the UniProt FTP site.

The UniProt RDF release directory is at:
    https://ftp.uniprot.org/pub/databases/uniprot/current_release/rdf/

Files are compressed with **xz** (not gzip).  The script categorises them
into five groups so you can download selectively:

    reference   – small vocabulary/ontology files (core.owl, void.rdf,
                  diseases.rdf.xz, taxonomy.rdf.xz, …)
    uniparc     – UniParc partitions  (200 × ~5 GB each → ~1 TB)
    reviewed    – UniProtKB reviewed  (Swiss-Prot, ~33 files)
    unreviewed  – UniProtKB unreviewed (TrEMBL, ~290 files)
    obsolete    – UniProtKB obsolete entries (~28 files)
    uniref      – UniRef50/90/100 clusters (~67 files)

Usage
-----
    # Dry-run: probe the FTP and print what would be downloaded
    python scripts/download_uniprot.py --dry-run

    # Only reference / vocabulary files
    python scripts/download_uniprot.py --categories reference --output-dir data/uniprot_local

    # Reference + reviewed (Swiss-Prot)
    python scripts/download_uniprot.py --categories reference reviewed --output-dir data/uniprot_local

    # All categories
    python scripts/download_uniprot.py --output-dir data/uniprot_local

    # Only update sources.yaml with download URLs (no download)
    python scripts/download_uniprot.py --update-yaml-only
"""

from __future__ import annotations

import argparse
import logging
import re
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/rdf"

# Categories and the regex that identifies which files belong to each.
CATEGORIES: dict[str, re.Pattern] = {
    "reference": re.compile(
        r"^(core\.owl|void\.rdf|databases|diseases|enzyme|go|journals|"
        r"keywords|locations|pathways|proteomes|taxonomy|tissues|"
        r"citation)"
    ),
    "uniparc": re.compile(r"^uniparc"),
    "reviewed": re.compile(r"^uniprotkb_reviewed"),
    "unreviewed": re.compile(r"^uniprotkb_unreviewed"),
    "obsolete": re.compile(r"^uniprotkb_obsolete"),
    "uniref": re.compile(r"^uniref"),
}

ALL_CATEGORIES = list(CATEGORIES.keys())


# ── helpers ──────────────────────────────────────────────────────────────────


def fetch_file_listing() -> list[str]:
    """Fetch the HTML directory listing from UniProt FTP and return file names.

    The FTP mirror exposes an Apache-style HTML index.  We parse ``href``
    attributes and filter out navigation links.
    """
    url = f"{BASE_URL}/"
    log.info("Fetching directory listing from %s", url)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=60) as resp:
            html = resp.read().decode()
    except urllib.error.URLError as exc:
        log.error("Could not fetch listing: %s", exc)
        return []

    # Extract href values from the Apache index HTML
    hrefs = re.findall(r'href="([^"]+)"', html)
    files: list[str] = []
    for href in hrefs:
        # Skip sorting links, parent directory, and subdirectories
        if "?" in href or href == "../" or href.endswith("/"):
            continue
        # We only care about RDF-related files
        if any(ext in href for ext in (".rdf", ".owl", ".rdf.xz", ".owl.xz")):
            files.append(href)
    return sorted(files)


def categorise_file(filename: str) -> str | None:
    """Return the category name for a file, or None if it doesn't match."""
    for cat, pattern in CATEGORIES.items():
        if pattern.match(filename):
            return cat
    return None


def classify_files(files: list[str]) -> dict[str, list[str]]:
    """Bin all files into categories.  Returns ``{category: [filenames]}``."""
    result: dict[str, list[str]] = {cat: [] for cat in ALL_CATEGORIES}
    unclassified: list[str] = []
    for f in files:
        cat = categorise_file(f)
        if cat:
            result[cat].append(f)
        else:
            unclassified.append(f)
    if unclassified:
        log.warning("Unclassified files: %s", unclassified)
    return result


def file_url(filename: str) -> str:
    """Full download URL for a file."""
    return f"{BASE_URL}/{filename}"


# ── sources.yaml integration ────────────────────────────────────────────────


def update_sources_yaml(
    yaml_path: Path,
    classified: dict[str, list[str]],
    categories: list[str],
) -> None:
    """Add ``download_rdf`` field to the ``uniprot`` entry in sources.yaml.

    The download_rdf field will contain a list of all download URLs for
    the selected categories.
    """
    text = yaml_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    # Build the URL list for selected categories
    all_urls: list[str] = []
    for cat in categories:
        for f in classified.get(cat, []):
            all_urls.append(file_url(f))

    if not all_urls:
        log.warning("No files to record in sources.yaml")
        return

    # Find the "- name: uniprot" entry (not bio2rdf.uniprot)
    new_lines: list[str] = []
    i = 0
    updated = False
    while i < len(lines):
        line = lines[i]
        stripped = line.lstrip()
        if stripped.startswith("- name:"):
            entry_name = stripped.split(":", 1)[1].strip()
            if entry_name == "uniprot":
                # Collect all lines of this entry
                entry_lines = [line]
                j = i + 1
                while j < len(lines):
                    next_stripped = lines[j].lstrip()
                    if next_stripped.startswith("- name:") or next_stripped.startswith("# ═"):
                        break
                    entry_lines.append(lines[j])
                    j += 1

                # Check if download_rdf already present
                entry_text = "".join(entry_lines)
                if "download_rdf:" in entry_text:
                    log.info("download_rdf already present for uniprot — skipping")
                    new_lines.extend(entry_lines)
                    i = j
                    continue

                # Find insertion point: after last non-blank line
                insert_idx = len(entry_lines) - 1
                while insert_idx > 0 and entry_lines[insert_idx].strip() == "":
                    insert_idx -= 1
                insert_idx += 1

                indent = "  "
                inject: list[str] = []
                if len(all_urls) == 1:
                    inject.append(f"{indent}download_rdf: {all_urls[0]}\n")
                else:
                    inject.append(f"{indent}download_rdf:\n")
                    for url in all_urls:
                        inject.append(f"{indent}  - {url}\n")

                for idx, inj_line in enumerate(inject):
                    entry_lines.insert(insert_idx + idx, inj_line)

                new_lines.extend(entry_lines)
                i = j
                updated = True
                continue

        new_lines.append(line)
        i += 1

    if updated:
        yaml_path.write_text("".join(new_lines), encoding="utf-8")
        log.info("Updated %s — added %d download URLs for uniprot", yaml_path, len(all_urls))
    else:
        log.warning("Could not find '- name: uniprot' entry in %s", yaml_path)


# ── download ─────────────────────────────────────────────────────────────────


def download_file(url: str, dest: Path) -> bool:
    """Download a single file.  Skips if it already exists."""
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


# ── report ───────────────────────────────────────────────────────────────────


def print_report(classified: dict[str, list[str]], categories: list[str]) -> None:
    """Print a summary table of files per category."""
    print("\n" + "=" * 70)
    print(f"{'Category':<15} {'Files':>6}  {'Sample files'}")
    print("-" * 70)
    total = 0
    for cat in ALL_CATEGORIES:
        files = classified.get(cat, [])
        count = len(files)
        selected = "✓" if cat in categories else " "
        sample = ", ".join(files[:3])
        if len(files) > 3:
            sample += f" … (+{len(files) - 3} more)"
        print(f"[{selected}] {cat:<12} {count:>6}  {sample}")
        if cat in categories:
            total += count
    print("=" * 70)
    print(f"Selected: {total} files across {len(categories)} categories")
    print(f"Compression: .xz (use rdfsolve.tools.decompress to unpack)\n")


# ── main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download UniProt RDF data files (.rdf.xz, .owl.xz, .owl, .rdf).",
    )
    parser.add_argument(
        "--categories",
        nargs="*",
        default=None,
        choices=ALL_CATEGORIES,
        help=(
            "Which file categories to include (default: all).  "
            "Choose from: reference, uniparc, reviewed, unreviewed, obsolete, uniref."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to download files into.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only probe the FTP and print the report; don't download or update YAML.",
    )
    parser.add_argument(
        "--update-yaml-only",
        action="store_true",
        help="Probe and update sources.yaml with download URLs, but don't download files.",
    )
    parser.add_argument(
        "--sources-yaml",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "sources.yaml",
        help="Path to sources.yaml (default: data/sources.yaml).",
    )
    args = parser.parse_args()

    categories = args.categories or ALL_CATEGORIES

    # ── Phase 1: probe FTP ───────────────────────────────────────────
    files = fetch_file_listing()
    if not files:
        log.error("No files found — aborting.")
        return

    classified = classify_files(files)
    print_report(classified, categories)

    if args.dry_run:
        log.info("Dry-run mode — nothing written.")
        return

    # ── Phase 2: update sources.yaml ─────────────────────────────────
    if args.sources_yaml.exists():
        update_sources_yaml(args.sources_yaml, classified, categories)
    else:
        log.warning("sources.yaml not found at %s — skipping.", args.sources_yaml)

    if args.update_yaml_only:
        log.info("YAML-only mode — skipping downloads.")
        return

    # ── Phase 3: download files ──────────────────────────────────────
    if args.output_dir is None:
        log.info("No --output-dir specified; skipping downloads.")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)

    success = 0
    total = 0
    for cat in categories:
        cat_files = classified.get(cat, [])
        if not cat_files:
            continue
        cat_dir = args.output_dir / cat
        cat_dir.mkdir(parents=True, exist_ok=True)
        for f in cat_files:
            total += 1
            url = file_url(f)
            dest = cat_dir / f
            if download_file(url, dest):
                success += 1

    log.info("Downloaded %d / %d files.", success, total)


if __name__ == "__main__":
    main()
