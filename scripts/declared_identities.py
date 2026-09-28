#!/usr/bin/env python
"""Write the identities that local sources declare, with the checks of each statement.

For each source, its QLever index is served, every declared identity is read
(rdfsolve.mappings.declared), and <source>_declared_identities.sssom.tsv and
<source>_declared_identities.json (the verdict) are written in OUTPUT/<source>/.
"""

import argparse
import json
from pathlib import Path

import requests

from rdfsolve.mappings.declared import declared_identities, declared_query, write_declared_identities
from rdfsolve.qlever.lifecycle import image_for_index, index_name, start_server, stop_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", nargs="+")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=20680)
    args = parser.parse_args()
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
        summary = write_declared_identities(rows, args.output / source, source)
        print(json.dumps(summary))


if __name__ == "__main__":
    main()
