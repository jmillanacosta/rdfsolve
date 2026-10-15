"""Select external SSSOM assertions relevant to the mined classes."""

from __future__ import annotations

import gzip
import logging
import shutil
import tarfile
import tempfile
import zipfile
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import yaml

log = logging.getLogger(__name__)
from sssom import MappingSetDataFrame, parse_sssom_table, write_tsv

from rdfsolve.mappings.confidence import ConfidencePolicy, edge_type

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema


def extract_classes_from_schema(schema: MinedSchema) -> set[str]:
    """Extract all class URIs from a mined schema.

    Args:
        schema: MinedSchema with patterns

    Returns:
        Set of class URIs used in the schema
    """
    return set(schema.get_classes())


def download_sssom_source(
    url: str,
    output_dir: Path,
    name: str,
) -> list[Path]:
    """Download and extract SSSOM files from a URL.

    Supports .tgz, .tar.gz, .zip archives and plain or gzipped .sssom.tsv files.

    Args:
        url: URL to download from
        output_dir: Directory to extract files to
        name: Source name for logging

    Returns:
        List of paths to extracted .sssom.tsv files
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    sssom_files: list[Path] = []

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes():
                tmp.write(chunk)
        tmp_path = Path(tmp.name)

    try:
        if url.endswith((".tgz", ".tar.gz")):
            with tarfile.open(tmp_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".sssom.tsv"):
                        tar.extract(member, output_dir, filter="data")
                        sssom_files.append(output_dir / member.name)

        elif url.endswith(".zip"):
            with zipfile.ZipFile(tmp_path, "r") as zf:
                for member_name in zf.namelist():
                    if member_name.endswith(".sssom.tsv"):
                        zf.extract(member_name, output_dir)
                        sssom_files.append(output_dir / member_name)

        elif url.endswith((".sssom.tsv", ".sssom.tsv.gz")):
            # A gzipped mapping set is decompressed; either is copied in parts, not read whole.
            dest = output_dir / f"{name}.sssom.tsv"
            opener = gzip.open if url.endswith(".gz") else open
            with opener(tmp_path, "rb") as source, dest.open("wb") as out:
                shutil.copyfileobj(source, out)
            sssom_files.append(dest)

    finally:
        tmp_path.unlink()

    return sssom_files


def enrich_sssom_file(
    sssom_file: Path,
    class_to_dataset: dict[str, str],
    dataset_void_uris: dict[str, str],
    creator_id: str | None = None,
    creator_label: str | None = None,
    policy: ConfidencePolicy | None = None,
    set_confidence: float | None = None,
    ignore_confidence: bool = False,
    skipped: Counter[str] | None = None,
) -> MappingSetDataFrame | None:
    """Retain mappings touching observed classes and preserve their provenance.

    Each kept row gets its effective confidence (*policy*, with the set's *set_confidence* and
    *ignore_confidence*); the row's own value, when it differs, is kept in its other column
    (source_confidence=...). Rows whose predicate states a hierarchy within a vocabulary, and
    rows without a justification, are left out and counted in *skipped*.

    The rows are read as a stream and only those whose subject or object is an observed class
    are parsed as SSSOM, so that a mapping set of any size is read within bounded memory. A
    CURIE is expanded with the set's own prefix map, else with Bioregistry's URI prefix. Where
    the prefix map names one namespace under several prefixes, the first is kept and the kept
    rows are written with it (sssom-py refuses a map with duplicate namespaces).
    """
    import csv

    import bioregistry

    header, columns, kept = _filtered_rows(sssom_file)
    if columns is None:
        return None
    prefix_map, renamed = _prefix_map(header)
    known: dict[str, str | None] = {}

    def local(value: str) -> bool:
        """Check whether a CURIE or IRI names an observed class."""
        prefix, separator, identifier = value.partition(":")
        if not separator or value.startswith(("http://", "https://")):
            return value in class_to_dataset
        if prefix not in known:
            known[prefix] = prefix_map.get(prefix) or bioregistry.get_uri_prefix(prefix)
        base = known[prefix]
        return base is not None and base + identifier in class_to_dataset

    subject, obj = columns.index("subject_id"), columns.index("object_id")
    rows = [r for r in kept(csv.reader) if local(r[subject]) or local(r[obj])]
    rows = _with_confidence(
        columns, rows, policy or ConfidencePolicy(), set_confidence, ignore_confidence, skipped
    )
    if not rows:
        return None
    selected = sssom_file.with_name(sssom_file.stem + ".selected.sssom.tsv")
    with selected.open("w", encoding="utf-8", newline="") as out:
        out.write(_header_text(header, renamed))
        writer = csv.writer(out, delimiter="\t", lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([_rename(cell, renamed) for cell in row])
    return parse_sssom_table(selected)


def _with_confidence(
    columns: list[str],
    rows: list[list[str]],
    policy: ConfidencePolicy,
    set_confidence: float | None,
    ignore_confidence: bool,
    skipped: Counter[str] | None,
) -> list[list[str]]:
    """Give each row its effective confidence, and leave out the rows that make no mapping edge.

    The confidence and other columns are added when the set has none.
    """
    for name in ("confidence", "other"):
        if name not in columns:
            columns.append(name)
    index = {name: columns.index(name) for name in columns}
    out = []
    for row in rows:
        row = row + [""] * (len(columns) - len(row))
        justification = (
            row[index["mapping_justification"]] if "mapping_justification" in index else ""
        )
        predicate = row[index["predicate_id"]]
        if not justification:
            if skipped is not None:
                skipped["no justification"] += 1
            continue
        if edge_type(predicate) is None:
            if skipped is not None:
                skipped["hierarchy predicate"] += 1
            continue
        given = row[index["confidence"]]
        try:
            stated = float(given) if given else None
        except ValueError:
            stated = None
        effective = policy.of(stated, justification, set_confidence, ignore_confidence)
        row[index["confidence"]] = f"{effective:g}"
        if stated is not None and stated != effective:
            other = row[index["other"]]
            row[index["other"]] = "|".join(filter(None, [other, f"source_confidence={given}"]))
        out.append(row)
    return out


def _filtered_rows(sssom_file: Path) -> tuple[list[str], list[str] | None, Any]:
    """Return the metadata lines (#), the column names, and a reader of the data rows."""
    header: list[str] = []
    columns: list[str] | None = None
    start: int | None = None
    with sssom_file.open(encoding="utf-8") as stream:
        while line := stream.readline():  # readline, so that the position can be told
            if line.startswith("#"):
                header.append(line)
                continue
            columns = line.rstrip("\n").split("\t")
            start = stream.tell()
            break

    def kept(reader: Any) -> Any:
        """Yield the data rows after the column names."""
        if start is None:
            return
        with sssom_file.open(encoding="utf-8", newline="") as stream:
            stream.seek(start)
            yield from reader(stream, delimiter="\t")

    return header, columns, kept


def _prefix_map(header: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """Return the set's prefix map without duplicate namespaces, and the prefixes renamed."""
    metadata = yaml.safe_load("".join(line[1:] for line in header)) or {}
    prefix_map: dict[str, str] = {}
    renamed: dict[str, str] = {}
    first: dict[str, str] = {}
    for prefix, namespace in (metadata.get("curie_map") or {}).items():
        if namespace in first:
            renamed[str(prefix)] = first[namespace]
            continue
        first[namespace] = str(prefix)
        prefix_map[str(prefix)] = str(namespace)
    return prefix_map, renamed


def _header_text(header: list[str], renamed: dict[str, str]) -> str:
    """Return the metadata lines with the renamed prefixes left out of the prefix map."""
    if not renamed:
        return "".join(header)
    metadata = yaml.safe_load("".join(line[1:] for line in header)) or {}
    curie_map = metadata.get("curie_map") or {}
    metadata["curie_map"] = {k: v for k, v in curie_map.items() if str(k) not in renamed}
    text = yaml.safe_dump(metadata, sort_keys=False, allow_unicode=True)
    return "".join(f"#{line}\n" for line in text.splitlines())


def _rename(cell: str, renamed: dict[str, str]) -> str:
    """Write a CURIE with the kept prefix of its namespace."""
    prefix, separator, rest = cell.partition(":")
    return f"{renamed[prefix]}:{rest}" if separator and prefix in renamed else cell


def enrich_external_sssom_sources(
    sssom_sources_file: Path,
    schemas: list[tuple[str, MinedSchema]],
    dataset_void_uris: dict[str, str],
    output_dir: Path,
    creator_id: str | None = None,
    creator_label: str | None = None,
    policy: ConfidencePolicy | None = None,
) -> dict[str, int]:
    """Download and select external SSSOM assertions from source configuration.

    *policy* gives the confidence of rows without one (DEFAULTS when not given). A source's
    confidence field is the default of its rows of unspecified justification, and its
    ignore_confidence field leaves its stated confidences out (a set that states one value for
    every row says nothing).

    Args:
        sssom_sources_file: Path to sssom_sources.yaml config file
        schemas: List of (dataset_name, MinedSchema) tuples
        dataset_void_uris: Mapping of dataset name -> VoID URI
        output_dir: Directory for output files
        creator_id: Optional creator ORCID
        creator_label: Optional creator name

    Returns:
        Dict mapping source name -> number of enriched mappings
    """
    class_to_dataset: dict[str, str] = {}
    for ds_name, schema in schemas:
        for cls_uri in extract_classes_from_schema(schema):
            class_to_dataset[cls_uri] = ds_name

    sources = yaml.safe_load(sssom_sources_file.read_text())

    downloads_dir = output_dir / "sssom_downloads"
    enriched_dir = output_dir / "mappings" / "enriched"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    enriched_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, int] = {}

    for source in sources:
        name = source["name"]
        url = source["url"]
        source_type = source.get("type", "class_mappings")

        if source_type == "property_mappings":
            continue

        try:
            sssom_files = download_sssom_source(url, downloads_dir / name, name)

            total_enriched = 0
            for sssom_file in sssom_files:
                skipped: Counter[str] = Counter()
                enriched_msdf = enrich_sssom_file(
                    sssom_file,
                    class_to_dataset,
                    dataset_void_uris,
                    creator_id=creator_id,
                    creator_label=creator_label,
                    policy=policy,
                    set_confidence=source.get("confidence"),
                    ignore_confidence=bool(source.get("ignore_confidence", False)),
                    skipped=skipped,
                )
                if skipped:
                    log.info("%s: rows left out: %s", sssom_file.name, dict(skipped))

                if enriched_msdf is not None:
                    output_path = enriched_dir / f"enriched-{sssom_file.stem}.sssom.tsv"
                    write_tsv(enriched_msdf, output_path, embedded_mode=True)
                    total_enriched += len(enriched_msdf.df)

            results[name] = total_enriched

        except Exception as e:
            results[name] = -1  # Error indicator
            log.warning(f"Error processing {name}: {e}")

    return results
