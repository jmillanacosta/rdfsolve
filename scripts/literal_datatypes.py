"""Count the numeric literal datatypes of the sources of QLever indexes that were built without.

QLever returns every integer type as xsd:int and a decimal as xsd:double, so an index cannot tell
the datatype of its source. For an index built before the census (rdfsolve.qlever.datatypes),
the source files are fetched again with the GET_DATA_CMD of its Qleverfile, the input files of
INPUT_FILES are counted, the census is written beside the index, and the fetched files are
deleted. A fetch can give a newer release than the index; the census records its date.

A source without a GET_DATA_CMD (built from the download_* URLs of its registry entry, such as
the OWL files of the Disease Ontology) is fetched from those URLs, given with --sources; the
members of a zip archive are read from it (Bgee: rdf_easybgee.zip, 28.5 GB).

Run: python scripts/literal_datatypes.py WORKDIR [WORKDIR ...] [--sources SOURCES_YAML] [--processes N]
"""

import argparse
import configparser
import glob
import subprocess
import urllib.parse
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from rdfsolve.qlever.datatypes import (
    CENSUS_FILE,
    RDF_SUFFIXES as SUFFIXES,
    count_literal_datatypes,
    input_format,
    merge_counts,
    write_census,
    zip_members,
)


def inputs(workdir: Path, pattern: str) -> list[Path]:
    """Return the input files of the Qleverfile, or every RDF file under the work directory."""
    found = sorted(Path(p) for p in glob.glob(str(workdir / pattern)) if Path(p).is_file())
    if not found:
        found = sorted(
            p
            for p in workdir.rglob("*")
            if p.is_file() and any(p.name.endswith(s) or p.name.endswith(s + ".gz") for s in SUFFIXES)
        )
    return found


def main() -> None:
    """Fetch, count and delete the sources of each work directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workdirs", nargs="+", type=Path)
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--sources", type=Path, help="sources.yaml with download_* URLs")
    args = parser.parse_args()
    registry = {}
    if args.sources:
        import yaml

        registry = {entry["name"]: entry for entry in yaml.safe_load(args.sources.read_text())}
    for workdir in (w.resolve() for w in args.workdirs):
        config = configparser.ConfigParser(interpolation=None)
        config.read(workdir / "Qleverfile")
        before = {p for p in workdir.rglob("*") if p.is_file()}
        fetched = datetime.now(timezone.utc).isoformat()
        try:
            if config.has_option("data", "GET_DATA_CMD"):
                command = config.get("data", "GET_DATA_CMD")
                subprocess.run(["bash"], input=command, text=True, check=True, cwd=workdir)
                files = inputs(workdir, config.get("index", "INPUT_FILES", fallback="rdf/*"))
            else:
                entry = registry.get(workdir.name, {})
                urls = [u for key, value in entry.items() if key.startswith("download_") for u in value]
                if not urls:
                    print(f"{workdir.name}: no GET_DATA_CMD and no download_* URLs; skipped", flush=True)
                    continue
                (workdir / "rdf").mkdir(exist_ok=True)
                files = []
                for number, url in enumerate(urls):
                    name = Path(urllib.parse.urlparse(url).path).name or f"input{number}.ttl"
                    target = workdir / "rdf" / f"census-{number}-{name}"
                    subprocess.run(["curl", "-sSfL", "--retry", "3", "-o", str(target), url], check=True)
                    # A zip archive (Bgee) is read member by member, without extraction.
                    files.extend(zip_members(target) if name.endswith(".zip") else [target])
            with ProcessPoolExecutor(args.processes) as pool:
                parts = pool.map(count_literal_datatypes, [[(p, input_format(p))] for p in files])
                counts = merge_counts(parts)
            write_census(workdir / CENSUS_FILE, counts, files, fetched=fetched)
            numeric = sum(sum(found.values()) for found in counts.values())
            print(f"{workdir.name}: {len(files)} files, {len(counts)} properties, {numeric} numeric literals", flush=True)
        finally:
            # Only fetched inputs: a server of the index may write its log beside them meanwhile.
            for path in {p for p in workdir.rglob("*") if p.is_file()} - before:
                fetched_input = (workdir / "rdf") in path.parents or any(
                    path.name.endswith(s) or path.name.endswith(s + ".gz") for s in SUFFIXES
                )
                if fetched_input:
                    path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
