#!/usr/bin/env python
"""Write the identities that local sources declare, with the checks of each statement.

For each source, its QLever index is served, every declared identity is read
(rdfsolve.mappings.declared), and <source>_declared_identities.sssom.tsv and
<source>_declared_identities.json (the result of the checks) are written in OUTPUT/<source>/.

The table carries the licence of its source, read from a licence table (TSV with the columns
name, spdx and evidence_url). A source without a licence in the table is refused.
"""

import argparse
import csv
import json
from pathlib import Path

import requests

from rdfsolve.mappings.declared import declared_identities, declared_query, write_declared_identities
from rdfsolve.qlever.lifecycle import image_for_index, index_name, start_server, stop_server


NOT_SPDX = {"", "unknown", "custom", "various", "public-domain"}


def licence(row):
    """Return the licence IRI of a source: its SPDX page, or the page that states its terms."""
    spdx = row.get("spdx", "")
    if spdx not in NOT_SPDX:
        return f"https://spdx.org/licenses/{spdx}"
    return row.get("evidence_url") or None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", nargs="+")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=20680)
    parser.add_argument("--licences", type=Path, required=True, help="Licence table (TSV)")
    args = parser.parse_args()
    with args.licences.open(newline="") as handle:
        licences = {row["name"]: licence(row) for row in csv.DictReader(handle, delimiter="\t")}
    missing = [s for s in args.sources if not licences.get(s)]
    if missing:
        parser.error(f"No licence in {args.licences} for: {', '.join(missing)}")
    for source in args.sources:
        workdir = args.data_dir / "qlever_workdirs" / source
        name = index_name(workdir, source)
        server = start_server(image_for_index(args.data_dir, workdir, name), workdir, name, args.port)
        try:
            answer = requests.post(
                f"http://localhost:{args.port}",
                data={"query": declared_query()},
                headers={"Accept": "application/sparql-results+json"},
                timeout=3600,
            )
            answer.raise_for_status()
            bindings = answer.json()["results"]["bindings"]
        finally:
            stop_server(server)
        rows = declared_identities(bindings, source)
        summary = write_declared_identities(
            rows, args.output / source, source, license_uri=licences[source]
        )
        print(json.dumps(summary))


if __name__ == "__main__":
    main()
