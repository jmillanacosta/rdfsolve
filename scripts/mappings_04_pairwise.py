#!/usr/bin/env python3
"""Compute pairwise class mappings from shared instances."""
import logging
from pathlib import Path
from collections import defaultdict
import gzip
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

INSTANCES_DIR = Path(__file__).parent.parent / "output" / "mappings" / "instances"
OUTPUT_FILE = Path(__file__).parent.parent / "output" / "mappings" / "04_pairwise.sssom.tsv"

MIN_SHARED_INSTANCES = 5  # Minimum overlap to emit mapping


def load_all_instances():
    """Load all instance dumps into memory."""
    # normalized_iri -> list of (source, class_iri)
    iri_to_sources = defaultdict(list)

    instance_files = list(INSTANCES_DIR.glob("*_instances.tsv.gz"))
    log.info(f"Loading {len(instance_files)} instance files...")

    for inst_file in instance_files:
        source_name = inst_file.stem.replace("_instances", "")

        try:
            with gzip.open(inst_file, 'rt') as f:
                # Skip header
                next(f)

                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) != 3:
                        continue

                    instance_iri, class_iri, normalized_iri = parts

                    # Skip if normalization failed
                    if normalized_iri == instance_iri:
                        continue

                    iri_to_sources[normalized_iri].append((source_name, class_iri))

            log.info(f"  Loaded {source_name}")

        except Exception as e:
            log.warning(f"  Could not load {inst_file}: {e}")

    return iri_to_sources


def compute_class_mappings(iri_to_sources):
    """Compute class mappings from shared instances."""
    # (source1, class1, source2, class2) -> count
    mapping_counts = defaultdict(int)

    log.info("Computing pairwise overlaps...")

    for normalized_iri, occurrences in iri_to_sources.items():
        if len(occurrences) < 2:
            continue

        # For each pair of sources sharing this IRI
        for i in range(len(occurrences)):
            for j in range(i + 1, len(occurrences)):
                source1, class1 = occurrences[i]
                source2, class2 = occurrences[j]

                # Create canonical ordering
                if (source1, class1) > (source2, class2):
                    source1, class1, source2, class2 = source2, class2, source1, class1

                key = (source1, class1, source2, class2)
                mapping_counts[key] += 1

    return mapping_counts


def main():
    if OUTPUT_FILE.exists():
        log.info(f"Output exists: {OUTPUT_FILE}")
        return

    if not INSTANCES_DIR.exists():
        log.error(f"Instances directory not found: {INSTANCES_DIR}")
        return

    # Load all instances
    iri_to_sources = load_all_instances()
    log.info(f"Total unique normalized IRIs: {len(iri_to_sources)}")

    # Compute mappings
    mapping_counts = compute_class_mappings(iri_to_sources)
    log.info(f"Total class mappings: {len(mapping_counts)}")

    # Filter by minimum count and convert to DataFrame
    rows = []
    for (source1, class1, source2, class2), count in mapping_counts.items():
        if count >= MIN_SHARED_INSTANCES:
            rows.append({
                'subject_id': class1,
                'predicate_id': 'skos:relatedMatch',
                'object_id': class2,
                'mapping_justification': 'semapv:SharedInstances',
                'confidence': min(count / 100.0, 1.0),  # Normalize to 0-1
                'comment': f'{count} shared instances between {source1} and {source2}'
            })

    if not rows:
        log.warning("No mappings above threshold")
        OUTPUT_FILE.write_text("# No pairwise mappings\n")
        return

    df = pd.DataFrame(rows)
    log.info(f"Mappings above threshold: {len(df)}")

    # Write SSSOM output
    df.to_csv(OUTPUT_FILE, sep='\t', index=False)
    log.info(f"Wrote: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
