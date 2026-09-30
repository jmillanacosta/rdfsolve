#!/usr/bin/env python
"""Infer the links between the datasets of runs or releases, verify each one, and write the
class associations of the verified joins.

Schemas are read from RUN/<dataset>/<dataset>_{local,remote}_schema.json, one per dataset:
the local one when a dataset has both, because its shares can be exact. Links are named by
dataset, as in the release. A local schema is served from its QLever index in DATA_DIR (at
most --servers at a time); a remote schema is read at its endpoint.

Verification (rdfsolve.mappings.verify): between two local indexes every value is read, so the
share is exact, and the terms of the target are read once and matched (verify(read_target=True));
otherwise --sample values are looked up and the share has a Wilson interval.
Identifiers in --replacements (SSSOM sets of term replaced by) are looked up by their current
identifier. For a join between two local indexes with a share of at least --min-share, the
class association (entity pairs and the coverage of each class) is read as well.

Writes OUTPUT/replacement_sets.json (the replacement sets with their checksums), and
OUTPUT/links.tsv, OUTPUT/class_associations.tsv and OUTPUT/failed.json after each link;
a stopped run continues with the first link that is in none of them.
"""

import argparse
import csv
import json
from pathlib import Path

from rdfsolve import MinedSchema
from rdfsolve.api import Client
from rdfsolve.mappings import infer_links, read_links, read_replacements, verify, write_links
from rdfsolve.mappings.signatures import (
    ASSOCIATION_FIELDS,
    association_row,
    class_association,
    describe_replacement_sets,
)
from rdfsolve.qlever.lifecycle import ServerPool


def schemas_of(runs):
    """Return {dataset: (channel, schema path)}, the local schema when a dataset has both."""
    found = {}
    for channel in ("remote", "local"):  # local last, so that it replaces remote
        for run in runs:
            for path in sorted(run.glob(f"*/*_{channel}_schema.json")):
                found[path.parent.name] = (channel, path)
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample", type=int, default=50)
    parser.add_argument("--min-share", type=float, default=0.5)
    parser.add_argument("--replacements", nargs="*", type=Path, default=[])
    parser.add_argument("--servers", type=int, default=2)
    parser.add_argument("--base-port", type=int, default=19700)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    found = schemas_of(args.runs)
    schemas = {name: MinedSchema.from_json(path) for name, (_, path) in found.items()}
    local = {name for name, (channel, _) in found.items() if channel == "local"}
    print({name: channel for name, (channel, _) in found.items()}, flush=True)
    replacements = {}
    for path in args.replacements:
        replacements.update(read_replacements(path))

    out = args.output
    out.mkdir(parents=True, exist_ok=True)
    # The replacement sets (pysec2pri) with their checksums; the citation is a placeholder.
    (out / "replacement_sets.json").write_text(
        json.dumps(describe_replacement_sets(args.replacements), indent=1)
    )
    links_path, failed_path = out / "links.tsv", out / "failed.json"
    associations_path = out / "class_associations.tsv"
    done = read_links(links_path) if links_path.exists() else []
    failed = json.loads(failed_path.read_text()) if failed_path.exists() else []
    seen = {repr(e.link) for e in done} | {f["link"] for f in failed if f["stage"] == "verify"}
    if not associations_path.exists():
        with associations_path.open("w", newline="") as handle:
            csv.DictWriter(handle, ASSOCIATION_FIELDS, delimiter="\t").writeheader()

    links = list(infer_links(schemas))
    links.sort(key=lambda l: (l.source, l.target))
    print(len(links), "candidate links;", len(seen), "already done", flush=True)
    pool = ServerPool(args.data_dir, size=args.servers, base_port=args.base_port)

    def client(name):
        """Return a client of a schema at its endpoint, starting its local server when needed."""
        endpoint = pool.endpoint(name) if name in local else schemas[name].about.endpoint
        return Client(schemas[name], endpoint, timeout=args.timeout)

    def fail(link, stage, error):
        """Record a failed link with its stage and error."""
        message = f"{type(error).__name__}: {error}"[:300]
        failed.append({"link": repr(link), "stage": stage, "error": message})
        failed_path.write_text(json.dumps(failed, indent=1))
        return message

    try:
        for i, link in enumerate(links):
            if repr(link) in seen:
                continue
            both_local = link.source in local and link.target in local
            try:
                source, target = client(link.source), client(link.target)
                evidence = verify(
                    link,
                    source,
                    target,
                    sample=None if both_local else args.sample,
                    replacements=replacements,
                    read_target=both_local,
                )
            except Exception as error:  # noqa: BLE001 - a failed link is reported, not hidden
                print(i + 1, len(links), link.source, link.target, fail(link, "verify", error))
                continue
            done.append(evidence)
            write_links(links_path, done)
            result = f"{evidence.found} / {evidence.sampled}"
            share = evidence.share
            if link.kind == "join" and both_local and share is not None and share >= args.min_share:
                try:
                    association = class_association(link, source, target, replacements=replacements)
                    with associations_path.open("a", newline="") as handle:
                        writer = csv.DictWriter(handle, ASSOCIATION_FIELDS, delimiter="\t")
                        writer.writerow(association_row(association))
                    result += f"; {len(association.pairs)} pairs"
                except Exception as error:  # noqa: BLE001
                    result += "; association failed: " + fail(link, "association", error)[:60]
            print(i + 1, len(links), link.kind, link.source, link.target, result, flush=True)
    finally:
        pool.close()
    print("written", out)


if __name__ == "__main__":
    main()
