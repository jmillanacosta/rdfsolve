#!/usr/bin/env python3
"""
Download Bgee RDF data (TTL files) from the remote zip archive.

Bgee distributes its RDF data as a single large zip file:
    https://www.bgee.org/ftp/current/rdf_easybgee.zip

This script uses **HTTP range requests** to read the zip central
directory, enumerate the ``.ttl`` members, and extract them one-by-one
*without* downloading the entire archive (~40 GB).

Usage
-----
    # Dry-run: probe the zip and list the TTL members
    python scripts/download_bgee.py --dry-run

    # Extract all TTL files into data/bgee_local/
    python scripts/download_bgee.py --output-dir data/bgee_local

    # Only update sources.yaml with the download_zip URL
    python scripts/download_bgee.py --update-yaml-only
"""

from __future__ import annotations

import argparse
import logging
import struct
import zlib
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────

ZIP_URL = (
    "https://www.bgee.org/ftp/current/rdf_easybgee.zip"
)

# How many bytes to fetch from the end of the zip to find the
# End-Of-Central-Directory record.  20 MiB is generous.
EOCD_TAIL_SIZE = 20 * 1024 * 1024

# YAML entry name for Bgee
BGEE_ENTRY_NAME = "bgee"


# ── low-level zip helpers ────────────────────────────────────────────


class RemoteZip:
    """Read the central directory of a remote zip via range requests.

    Only the central directory + per-member data ranges are fetched.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()
        self.size = self._content_length()

    # ── private helpers ──────────────────────────────────────────

    def _content_length(self) -> int:
        r = self.session.head(self.url, allow_redirects=True)
        r.raise_for_status()
        return int(r.headers["Content-Length"])

    def _fetch_range(self, start: int, end: int) -> bytes:
        """Fetch bytes [start, end] inclusive."""
        headers = {"Range": f"bytes={start}-{end}"}
        r = self.session.get(
            self.url, headers=headers, stream=True,
        )
        r.raise_for_status()
        return r.content

    # ── public API ───────────────────────────────────────────────

    @staticmethod
    def _parse_zip64_extra(
        extra: bytes,
        uncomp_size: int,
        comp_size: int,
        local_offset: int,
    ) -> tuple[int, int, int]:
        """Extract real sizes / offset from a Zip64 extra field.

        The Zip64 extra field (tag 0x0001) stores 64-bit values only
        for fields that were set to 0xFFFFFFFF in the central header.
        The values appear **in order**: uncompressed size, compressed
        size, local-header offset, disk start — but only the ones
        that overflowed are present.
        """
        MARKER32 = 0xFFFFFFFF
        j = 0
        while j + 4 <= len(extra):
            tag = struct.unpack_from("<H", extra, j)[0]
            sz = struct.unpack_from("<H", extra, j + 2)[0]
            if tag == 0x0001:
                data = extra[j + 4 : j + 4 + sz]
                off = 0
                if uncomp_size == MARKER32:
                    uncomp_size = struct.unpack_from(
                        "<Q", data, off,
                    )[0]
                    off += 8
                if comp_size == MARKER32:
                    comp_size = struct.unpack_from(
                        "<Q", data, off,
                    )[0]
                    off += 8
                if local_offset == MARKER32:
                    local_offset = struct.unpack_from(
                        "<Q", data, off,
                    )[0]
                break
            j += 4 + sz
        return uncomp_size, comp_size, local_offset

    def _find_cd(self) -> tuple[int, int]:
        """Return (cd_offset, cd_size) handling Zip64."""
        tail_start = max(0, self.size - EOCD_TAIL_SIZE)
        tail = self._fetch_range(tail_start, self.size - 1)

        eocd_sig = b"\x50\x4b\x05\x06"
        eocd_idx = tail.rfind(eocd_sig)
        if eocd_idx == -1:
            raise RuntimeError(
                "End-Of-Central-Directory signature not found"
            )

        eocd = tail[eocd_idx : eocd_idx + 22]
        (
            _sig, _disk_no, _disk_cd, _disk_entries,
            _total_entries, cd_size, cd_offset, _comment_len,
        ) = struct.unpack("<IHHHHIIH", eocd)

        # If cd_offset == 0xFFFFFFFF we need the Zip64 EOCD record
        if cd_offset == 0xFFFFFFFF or cd_size == 0xFFFFFFFF:
            z64_sig = b"\x50\x4b\x06\x06"
            z64_idx = tail.rfind(z64_sig, 0, eocd_idx)
            if z64_idx == -1:
                raise RuntimeError(
                    "Zip64 EOCD record not found"
                )
            cd_size = struct.unpack_from(
                "<Q", tail, z64_idx + 40,
            )[0]
            cd_offset = struct.unpack_from(
                "<Q", tail, z64_idx + 48,
            )[0]
            log.debug(
                "Zip64: cd_offset=%d, cd_size=%d",
                cd_offset, cd_size,
            )

        return cd_offset, cd_size

    def list_entries(self) -> list[dict]:
        """Return metadata dicts for every member in the archive."""
        cd_offset, cd_size = self._find_cd()
        cd_data = self._fetch_range(
            cd_offset, cd_offset + cd_size - 1,
        )

        entries: list[dict] = []
        i = 0
        while i < len(cd_data):
            if cd_data[i : i + 4] != b"\x50\x4b\x01\x02":
                break

            hdr = struct.unpack(
                "<IHHHHHHIIIHHHHHII", cd_data[i : i + 46],
            )
            (
                _sig, _ver_made, _ver_needed, _flags,
                compression, _mod_time, _mod_date, _crc,
                comp_size, uncomp_size,
                fname_len, extra_len, comment_len,
                _disk_start, _int_attr, _ext_attr,
                local_offset,
            ) = hdr

            name = cd_data[
                i + 46 : i + 46 + fname_len
            ].decode()

            extra = cd_data[
                i + 46 + fname_len
                : i + 46 + fname_len + extra_len
            ]

            # Resolve Zip64 overflow values
            uncomp_size, comp_size, local_offset = (
                self._parse_zip64_extra(
                    extra, uncomp_size, comp_size, local_offset,
                )
            )

            entries.append({
                "filename": name,
                "compression": compression,
                "comp_size": comp_size,
                "uncomp_size": uncomp_size,
                "local_offset": local_offset,
            })

            i += 46 + fname_len + extra_len + comment_len

        return entries

    def extract_member(
        self, entry: dict, output_dir: Path,
    ) -> Path:
        """Download and decompress a single zip member to *output_dir*."""
        offset = entry["local_offset"]

        # Local file header is at least 30 bytes
        lh = self._fetch_range(offset, offset + 29)
        if lh[:4] != b"\x50\x4b\x03\x04":
            raise RuntimeError(
                f"Invalid local header for {entry['filename']}"
            )

        (
            _sig, _ver, _flags, compression,
            _mt, _md, _crc,
            comp_size, _uncomp_size,
            fname_len, extra_len,
        ) = struct.unpack("<IHHHHHIIIHH", lh[:30])

        data_start = offset + 30 + fname_len + extra_len
        data_end = data_start + entry["comp_size"] - 1
        raw = self._fetch_range(data_start, data_end)

        if compression == 0:
            data = raw
        elif compression == 8:
            d = zlib.decompressobj(-zlib.MAX_WBITS)
            data = d.decompress(raw)
        else:
            raise RuntimeError(
                f"Unsupported compression method {compression}"
            )

        out_path = output_dir / entry["filename"]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(data)
        return out_path


# ── sources.yaml integration ────────────────────────────────────────


def update_sources_yaml(yaml_path: Path) -> None:
    """Add ``download_zip`` field to the ``bgee`` entry."""
    text = yaml_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    new_lines: list[str] = []
    i = 0
    updated = False
    while i < len(lines):
        line = lines[i]
        stripped = line.lstrip()
        if stripped.startswith("- name:"):
            entry_name = stripped.split(":", 1)[1].strip()
            if entry_name == BGEE_ENTRY_NAME:
                entry_lines = [line]
                j = i + 1
                while j < len(lines):
                    ns = lines[j].lstrip()
                    if ns.startswith("- name:") or ns.startswith("# ═"):
                        break
                    entry_lines.append(lines[j])
                    j += 1

                entry_text = "".join(entry_lines)
                if "download_zip:" in entry_text:
                    log.info(
                        "download_zip already present for %s",
                        BGEE_ENTRY_NAME,
                    )
                    new_lines.extend(entry_lines)
                    i = j
                    continue

                # Insert after last non-blank line of the entry
                ins = len(entry_lines) - 1
                while ins > 0 and entry_lines[ins].strip() == "":
                    ins -= 1
                ins += 1

                entry_lines.insert(
                    ins, f"  download_zip: {ZIP_URL}\n",
                )

                new_lines.extend(entry_lines)
                i = j
                updated = True
                continue

        new_lines.append(line)
        i += 1

    if updated:
        yaml_path.write_text("".join(new_lines), encoding="utf-8")
        log.info("Updated %s with download_zip for bgee", yaml_path)
    else:
        log.warning(
            "Could not find '- name: %s' in %s",
            BGEE_ENTRY_NAME, yaml_path,
        )


# ── report ───────────────────────────────────────────────────────────


def print_report(
    entries: list[dict],
    ttl_entries: list[dict],
    archive_mb: float,
) -> None:
    """Print a human-readable summary."""
    print()
    print("=" * 60)
    print(f"Bgee RDF zip: {ZIP_URL}")
    print(f"Archive size:  {archive_mb:,.1f} MB")
    print(f"Total members: {len(entries)}")
    print(f"TTL members:   {len(ttl_entries)}")
    if ttl_entries:
        total_uncomp = sum(e["uncomp_size"] for e in ttl_entries)
        total_comp = sum(e["comp_size"] for e in ttl_entries)
        print(
            f"TTL compressed: {total_comp / 1024 / 1024:,.1f} MB  "
            f"→  uncompressed: {total_uncomp / 1024 / 1024:,.1f} MB"
        )
        print(f"\nFirst 10 TTL files:")
        for e in ttl_entries[:10]:
            print(
                f"  {e['filename']:<60s} "
                f"{e['uncomp_size'] / 1024:>8.0f} KB"
            )
        if len(ttl_entries) > 10:
            print(f"  … and {len(ttl_entries) - 10} more")
    print("=" * 60)
    print()


# ── main ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download Bgee RDF TTL files from the remote zip "
            "archive via HTTP range requests."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to extract TTL files into.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Probe the zip and print a report; "
            "don't extract or update YAML."
        ),
    )
    parser.add_argument(
        "--update-yaml-only",
        action="store_true",
        help=(
            "Add download_zip to sources.yaml but "
            "don't extract files."
        ),
    )
    parser.add_argument(
        "--sources-yaml",
        type=Path,
        default=(
            Path(__file__).resolve().parent.parent
            / "data" / "sources.yaml"
        ),
        help="Path to sources.yaml (default: data/sources.yaml).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Only extract the first N TTL members "
            "(useful for testing)."
        ),
    )
    args = parser.parse_args()

    # ── Phase 1: probe the remote zip ────────────────────────────
    log.info("Probing %s …", ZIP_URL)
    rz = RemoteZip(ZIP_URL)
    archive_mb = rz.size / (1024 * 1024)
    log.info("Archive size: %.1f MB", archive_mb)

    log.info("Reading central directory …")
    all_entries = rz.list_entries()

    ttl_entries = [
        e for e in all_entries
        if e["filename"].endswith(".ttl")
        and not e["filename"].endswith("/")
    ]

    print_report(all_entries, ttl_entries, archive_mb)

    if args.dry_run:
        log.info("Dry-run mode — nothing written.")
        return

    # ── Phase 2: update sources.yaml ─────────────────────────────
    if args.sources_yaml.exists():
        update_sources_yaml(args.sources_yaml)
    else:
        log.warning(
            "sources.yaml not found at %s", args.sources_yaml,
        )

    if args.update_yaml_only:
        log.info("YAML-only mode — skipping extraction.")
        return

    # ── Phase 3: extract TTL files ───────────────────────────────
    if args.output_dir is None:
        log.info("No --output-dir specified; skipping extraction.")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)

    to_extract = ttl_entries
    if args.limit is not None:
        to_extract = ttl_entries[: args.limit]

    log.info(
        "Extracting %d TTL files into %s …",
        len(to_extract), args.output_dir,
    )
    for idx, entry in enumerate(to_extract, 1):
        dest = args.output_dir / entry["filename"]
        if dest.exists():
            log.info(
                "  [%d/%d] ✓ Already exists: %s",
                idx, len(to_extract), entry["filename"],
            )
            continue
        log.info(
            "  [%d/%d] ⬇  %s (%.0f KB) …",
            idx, len(to_extract),
            entry["filename"],
            entry["uncomp_size"] / 1024,
        )
        rz.extract_member(entry, args.output_dir)

    log.info("Done — extracted %d files.", len(to_extract))


if __name__ == "__main__":
    main()
