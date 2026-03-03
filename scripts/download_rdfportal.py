#!/usr/bin/env python3
"""
Download RDFPortal dataset files (``*.ttl.gz``) for local loading.

RDFPortal (https://rdfportal.org) hosts 75+ life-science RDF datasets.
Each dataset has a ``latest`` version at::

    https://rdfportal.org/download/<dataset>/latest/

The directory listing is a plain nginx HTML index.  Files are gzip-compressed
Turtle (``.ttl.gz``).  Some datasets also contain ``.owl.gz`` files or
subdirectories (which this script can optionally recurse into).

Usage
-----
    # Dry-run: probe the server and print the report
    python scripts/download_rdfportal.py --dry-run

    # Download specific datasets
    python scripts/download_rdfportal.py --datasets medgen chebi --output-dir data/rdfportal_local

    # Only update sources.yaml with download URLs (no download)
    python scripts/download_rdfportal.py --update-yaml-only

    # Download all datasets (warning: very large!)
    python scripts/download_rdfportal.py --output-dir data/rdfportal_local
"""

from __future__ import annotations

import argparse
import logging
import re
import urllib.request
import urllib.error
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://rdfportal.org/download"

# All 75 datasets discovered from the RDFPortal download page.
RDFPORTAL_DATASETS = [
    "amrportal",
    "bacdive",
    "bgee",
    "biomodels",
    "bioportal",
    "biosample",
    "biosampleplus",
    "bmrb",
    "brenda",
    "cellosaurus",
    "chebi",
    "chembl",
    "clinvar",
    "dbcatalog",
    "dbnsfp",
    "dbscsnv",
    "ddbj",
    "ensembl",
    "ensembl_grch37",
    "ensembl_grch38",
    "expressionatlas",
    "famsbase",
    "ggdonto",
    "glycoepitope",
    "glycosmos",
    "glytoucan",
    "gtdb",
    "gwas-catalog",
    "hgnc",
    "homologene",
    "icgc",
    "jcm",
    "jpostdb",
    "kero",
    "kg-covid-19",
    "knapsack",
    "ligandbox",
    "massbank",
    "mbgd",
    "medgen",
    "mediadive",
    "mesh",
    "microbedbjp",
    "nadd",
    "nando",
    "naro_genebank",
    "nbrc",
    "ncbigene",
    "nextprot",
    "nikkaji",
    "nlm-catalog",
    "oma",
    "ontology",
    "opentggates",
    "paconto",
    "pbo",
    "pdb",
    "pgdbj",
    "pheknowlator",
    "polyinfo",
    "proteinatlas",
    "pubcasefinder",
    "pubchem",
    "pubmed",
    "pubtator",
    "quanto",
    "reactome",
    "refex",
    "rhea",
    "ssbd",
    "togoid",
    "uniprot",
    "uniprot-covid",
    "wikidata",
    "wurcs",
]

# ── helpers ──────────────────────────────────────────────────────────────────

# Regex to match file links in nginx autoindex HTML
_HREF_RE = re.compile(r'href="([^"]+)"')


def fetch_file_listing(
    dataset: str,
    *,
    recurse: bool = False,
    subpath: str = "",
) -> list[str]:
    """Fetch the nginx HTML listing for a dataset and return file URLs.

    Parameters
    ----------
    dataset
        RDFPortal dataset name (e.g. "medgen").
    recurse
        If *True*, follow subdirectory links (one level).
    subpath
        Used internally for recursive calls.

    Returns
    -------
    list[str]
        Relative file paths (e.g. ``MGCONSO.ttl.gz``, ``pdb/00/1abc.ttl.gz``).
    """
    if subpath:
        url = f"{BASE_URL}/{dataset}/latest/{subpath}"
    else:
        url = f"{BASE_URL}/{dataset}/latest/"
    log.info("Fetching listing: %s", url)

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode(errors="replace")
    except urllib.error.URLError as exc:
        log.warning("  ⚠  Could not fetch listing for %s: %s", dataset, exc)
        return []

    hrefs = _HREF_RE.findall(html)
    files: list[str] = []

    for href in hrefs:
        if href.startswith("..") or href.startswith("?"):
            continue
        if href.endswith("/"):
            if recurse:
                sub = f"{subpath}{href}" if subpath else href
                files.extend(
                    fetch_file_listing(dataset, recurse=True, subpath=sub)
                )
            continue
        full_path = f"{subpath}{href}" if subpath else href
        files.append(full_path)

    return files


def identify_ttl_files(
    dataset: str, files: list[str]
) -> dict[str, list[str] | str | None]:
    """Pick out TTL and OWL files from a listing.

    Returns a dict with:
      ``ttl_files``: list of full URLs for ``.ttl.gz`` / ``.ttl`` files
      ``owl_url``:   first ``.owl`` / ``.owl.gz`` URL found, or None
    """
    ttl_urls: list[str] = []
    owl_url: str | None = None

    for f in files:
        url = f"{BASE_URL}/{dataset}/latest/{f}"
        lower = f.lower()
        if ".ttl" in lower:
            ttl_urls.append(url)
        elif ".owl" in lower and owl_url is None:
            owl_url = url

    return {"ttl_files": ttl_urls, "owl_url": owl_url}


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


# ── sources.yaml integration ────────────────────────────────────────────────


def _yaml_entry_name_to_dataset(name: str) -> str | None:
    """Map a sources.yaml entry name to an RDFPortal dataset name.

    Mappings:
      ``rdfportal.pdb``     → ``pdb``
      ``rdfportal.primary`` → None (no direct download dataset)
      ``medgen``            → ``medgen``  (if it's in our list)
    """
    if name.startswith("rdfportal."):
        ds = name[len("rdfportal."):]
        if ds in RDFPORTAL_DATASETS:
            return ds
    # Direct match (some entries may use the dataset name directly)
    if name in RDFPORTAL_DATASETS:
        return name
    return None


def update_sources_yaml(
    yaml_path: Path,
    download_info: dict[str, dict],
) -> None:
    """Add ``download_ttl`` and ``download_owl`` fields to matching entries."""
    text = yaml_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    # Build a lookup from entry-name → dataset
    # We'll match both "rdfportal.<ds>" and bare "<ds>" entries
    name_to_dataset: dict[str, str] = {}
    for ds in download_info:
        name_to_dataset[ds] = ds
        name_to_dataset[f"rdfportal.{ds}"] = ds

    new_lines: list[str] = []
    i = 0
    updates = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.lstrip()
        if stripped.startswith("- name:"):
            entry_name = stripped.split(":", 1)[1].strip()
            ds = name_to_dataset.get(entry_name)
            if ds and ds in download_info:
                info = download_info[ds]
                # Collect entry lines
                entry_lines = [line]
                j = i + 1
                while j < len(lines):
                    ns = lines[j].lstrip()
                    if ns.startswith("- name:") or ns.startswith("# ═"):
                        break
                    entry_lines.append(lines[j])
                    j += 1

                entry_text = "".join(entry_lines)
                if "download_ttl:" in entry_text or "download_owl:" in entry_text:
                    new_lines.extend(entry_lines)
                    i = j
                    continue

                # Find insertion point
                insert_idx = len(entry_lines) - 1
                while insert_idx > 0 and entry_lines[insert_idx].strip() == "":
                    insert_idx -= 1
                insert_idx += 1

                indent = "  "
                inject: list[str] = []

                ttl_files = info.get("ttl_files", [])
                owl_url = info.get("owl_url")

                if ttl_files:
                    if len(ttl_files) == 1:
                        inject.append(f"{indent}download_ttl: {ttl_files[0]}\n")
                    else:
                        inject.append(f"{indent}download_ttl:\n")
                        for u in ttl_files:
                            inject.append(f"{indent}  - {u}\n")

                if owl_url:
                    inject.append(f"{indent}download_owl: {owl_url}\n")

                for idx, inj_line in enumerate(inject):
                    entry_lines.insert(insert_idx + idx, inj_line)

                new_lines.extend(entry_lines)
                i = j
                updates += 1
                continue

        new_lines.append(line)
        i += 1

    yaml_path.write_text("".join(new_lines), encoding="utf-8")
    log.info("Updated %s — %d entries modified", yaml_path, updates)


# ── report ───────────────────────────────────────────────────────────────────


def print_report(download_info: dict[str, dict]) -> None:
    """Print a summary table of discovered files."""
    print("\n" + "=" * 78)
    print(f"{'Dataset':<20} {'TTL files':>9}  {'OWL':>5}  {'Sample TTL'}")
    print("-" * 78)
    for ds in sorted(download_info):
        info = download_info[ds]
        ttl_count = len(info.get("ttl_files", []))
        has_owl = "✓" if info.get("owl_url") else "✗"
        ttl_names = ", ".join(
            url.rsplit("/", 1)[-1]
            for url in info.get("ttl_files", [])[:3]
        )
        if ttl_count > 3:
            ttl_names += f" … (+{ttl_count - 3} more)"
        print(f"{ds:<20} {ttl_count:>9}  {has_owl:>5}  {ttl_names}")
    print("=" * 78)
    total_ttl = sum(len(v.get("ttl_files", [])) for v in download_info.values())
    total_owl = sum(1 for v in download_info.values() if v.get("owl_url"))
    print(
        f"Total: {len(download_info)} datasets, "
        f"{total_ttl} TTL files, {total_owl} OWL files"
    )
    print("Compression: .gz (use rdfsolve.tools.decompress to unpack)\n")


# ── main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download RDFPortal dataset files (*.ttl.gz).",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="Specific datasets to process (default: all 75).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to download files into.  Each dataset gets a subdirectory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only probe the server and print the report; no downloads or YAML changes.",
    )
    parser.add_argument(
        "--update-yaml-only",
        action="store_true",
        help="Probe and update sources.yaml with download URLs, but don't download.",
    )
    parser.add_argument(
        "--sources-yaml",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "sources.yaml",
        help="Path to sources.yaml (default: data/sources.yaml).",
    )
    parser.add_argument(
        "--recurse",
        action="store_true",
        help="Follow subdirectory links in the dataset listings (e.g. PDB).",
    )
    args = parser.parse_args()

    datasets = args.datasets or RDFPORTAL_DATASETS

    # ── Phase 1: probe each dataset ──────────────────────────────────
    download_info: dict[str, dict] = {}
    for ds in datasets:
        files = fetch_file_listing(ds, recurse=args.recurse)
        if not files:
            continue
        info = identify_ttl_files(ds, files)
        if info["ttl_files"] or info["owl_url"]:
            download_info[ds] = info

    print_report(download_info)

    if args.dry_run:
        log.info("Dry-run mode — nothing written.")
        return

    # ── Phase 2: update sources.yaml ─────────────────────────────────
    if args.sources_yaml.exists():
        update_sources_yaml(args.sources_yaml, download_info)
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
    for ds in sorted(download_info):
        info = download_info[ds]
        ds_dir = args.output_dir / ds
        ds_dir.mkdir(parents=True, exist_ok=True)

        for url in info.get("ttl_files", []):
            total += 1
            filename = url.rsplit("/", 1)[-1]
            dest = ds_dir / filename
            if download_file(url, dest):
                success += 1

        owl_url = info.get("owl_url")
        if owl_url:
            total += 1
            filename = owl_url.rsplit("/", 1)[-1]
            dest = ds_dir / filename
            if download_file(owl_url, dest):
                success += 1

    log.info("Downloaded %d / %d files.", success, total)


if __name__ == "__main__":
    main()
