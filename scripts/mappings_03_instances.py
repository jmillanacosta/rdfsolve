#!/usr/bin/env python3
"""Dump instances from local QLever indices with normalized IRIs."""
import logging
import sys
from pathlib import Path
import gzip
import curies

from rdfsolve.sparql_helper import SparqlHelper
from mappings_qlever_utils import get_qlever_workdirs, start_qlever_server, stop_qlever_server

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "output" / "mappings" / "instances"
DATA_DIR = Path(__file__).parent.parent.parent / "data"
BASE_PORT = 7000

try:
    CONVERTER = curies.get_bioregistry_converter()
except:
    CONVERTER = None
    log.warning("Could not load bioregistry converter")


def normalize_iri(iri):
    """Normalize IRI to CURIE if possible."""
    if not CONVERTER:
        return iri
    try:
        curie = CONVERTER.compress(iri)
        return curie if curie else iri
    except:
        return iri


def dump_instances(source_name, port):
    """Dump all instances from source."""
    output_file = OUTPUT_DIR / f"{source_name}_instances.tsv.gz"

    # Skip if exists and non-empty
    if output_file.exists() and output_file.stat().st_size > 100:
        sys.stderr.write(f"  Skip {source_name}: output exists\n")
        sys.stderr.flush()
        return

    workdir = DATA_DIR / "qlever_workdirs" / source_name

    sys.stderr.write(f"Starting {source_name} on port {port}...\n")
    sys.stderr.flush()
    pid = start_qlever_server(workdir, source_name, port, DATA_DIR)

    if not pid:
        sys.stderr.write(f"  {source_name}: failed to start server\n")
        sys.stderr.flush()
        return

    try:
        endpoint = f"http://localhost:{port}"
        helper = SparqlHelper(endpoint, timeout=300.0)

        sys.stderr.write(f"  Querying instances...\n")
        sys.stderr.flush()

        query = """
        SELECT DISTINCT ?s ?c WHERE {
            ?s a ?c .
            FILTER(isIRI(?s) && isIRI(?c))
        }
        """

        results = helper.select(query)

        sys.stderr.write(f"  Writing output...\n")
        sys.stderr.flush()

        # Write compressed output
        with gzip.open(output_file, 'wt') as f:
            f.write("instance_iri\tclass_iri\tnormalized_iri\n")

            count = 0
            for binding in results.get('results', {}).get('bindings', []):
                instance = binding['s']['value']
                cls = binding['c']['value']
                normalized = normalize_iri(instance)

                f.write(f"{instance}\t{cls}\t{normalized}\n")
                count += 1

        helper.close()
        sys.stderr.write(f"  {source_name}: {count} instances\n")
        sys.stderr.flush()

    except Exception as e:
        sys.stderr.write(f"  {source_name}: Error: {e}\n")
        sys.stderr.flush()
        if output_file.exists():
            output_file.unlink()

    finally:
        sys.stderr.write(f"  Stopping server...\n")
        sys.stderr.flush()
        stop_qlever_server(pid)
        sys.stderr.write(f"  Server stopped\n")
        sys.stderr.flush()


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    sources = get_qlever_workdirs(DATA_DIR)
    sys.stderr.write(f"Processing {len(sources)} sources with local QLever indices\n")
    sys.stderr.flush()

    port = BASE_PORT

    for idx, source_name in enumerate(sources, 1):
        sys.stderr.write(f"\n[{idx}/{len(sources)}] {source_name}\n")
        sys.stderr.flush()
        dump_instances(source_name, port)
        port += 1

    sys.stderr.write("\nDone\n")
    sys.stderr.flush()


if __name__ == "__main__":
    main()
