#!/usr/bin/env python3
"""Consolidate all mappings via semra inference."""
import logging
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

MAPPINGS_DIR = Path(__file__).parent.parent / "output" / "mappings"
OUTPUT_FILE = MAPPINGS_DIR / "lslod_mappings.sssom.tsv"
OUTPUT_INFERRED = MAPPINGS_DIR / "lslod_mappings_inferred.sssom.tsv"


def load_sssom_file(path):
    """Load SSSOM file, handling comments."""
    if not path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_csv(path, sep='\t', comment='#')
        log.info(f"Loaded {len(df)} mappings from {path.name}")
        return df
    except Exception as e:
        log.warning(f"Could not load {path}: {e}")
        return pd.DataFrame()


def sssom_to_semra(df):
    """Convert SSSOM DataFrame to semra Mappings."""
    from semra.struct import Mapping, Reference, SimpleEvidence

    mappings = []

    for _, row in df.iterrows():
        # Parse subject and object CURIEs
        subj = str(row.get('subject_id', ''))
        obj = str(row.get('object_id', ''))

        if not subj or not obj:
            continue

        # Split CURIE
        s_parts = subj.split(':', 1)
        o_parts = obj.split(':', 1)

        if len(s_parts) < 2 or len(o_parts) < 2:
            continue

        s = Reference(prefix=s_parts[0], identifier=s_parts[1])
        o = Reference(prefix=o_parts[0], identifier=o_parts[1])

        # Get predicate
        pred = row.get('predicate_id', 'skos:relatedMatch')

        # Create evidence
        evidence_kwargs = {
            'justification': row.get('mapping_justification', 'unspecified'),
            'mapping_set': row.get('source', 'lslod'),
        }
        if pd.notna(row.get('confidence')):
            evidence_kwargs['confidence'] = row.get('confidence')

        evidence = [SimpleEvidence(**evidence_kwargs)]

        mappings.append(Mapping(s=s, p=pred, o=o, evidence=evidence))

    return mappings


def semra_to_sssom(mappings):
    """Convert semra Mappings to SSSOM DataFrame."""
    rows = []

    for m in mappings:
        row = {
            'subject_id': f"{m.s.prefix}:{m.s.identifier}",
            'predicate_id': m.p,
            'object_id': f"{m.o.prefix}:{m.o.identifier}",
        }

        # Extract evidence fields
        if m.evidence:
            ev = m.evidence[0]
            if hasattr(ev, 'mapping_set'):
                row['source'] = ev.mapping_set
            if hasattr(ev, 'justification'):
                row['mapping_justification'] = ev.justification
            if hasattr(ev, 'confidence'):
                row['confidence'] = ev.confidence

        rows.append(row)

    return pd.DataFrame(rows)


def main():
    if OUTPUT_INFERRED.exists():
        log.info(f"Output exists: {OUTPUT_INFERRED}")
        return

    # Load all input files
    input_files = [
        MAPPINGS_DIR / "01_external.sssom.tsv",
        MAPPINGS_DIR / "02_crossrefs.sssom.tsv",
        MAPPINGS_DIR / "04_pairwise.sssom.tsv",
    ]

    all_dfs = []
    for input_file in input_files:
        df = load_sssom_file(input_file)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        log.error("No input mappings found")
        return

    # Concatenate all mappings
    df_all = pd.concat(all_dfs, ignore_index=True)
    log.info(f"Total input mappings: {len(df_all)}")

    # Deduplicate
    key_cols = ['subject_id', 'predicate_id', 'object_id']
    df_all = df_all.drop_duplicates(subset=key_cols)
    log.info(f"After deduplication: {len(df_all)}")

    # Write consolidated file (no inference)
    df_all.to_csv(OUTPUT_FILE, sep='\t', index=False)
    log.info(f"Wrote: {OUTPUT_FILE}")

    # Convert to semra and run inference
    try:
        from semra.api import assemble_evidences
        from semra.inference import infer_chains, infer_reversible

        semra_mappings = sssom_to_semra(df_all)
        log.info(f"Converted to {len(semra_mappings)} semra mappings")

        # Run inference
        semra_mappings = infer_reversible(semra_mappings)
        log.info(f"After inversion: {len(semra_mappings)}")

        semra_mappings = infer_chains(semra_mappings, cutoff=3)
        log.info(f"After transitivity: {len(semra_mappings)}")

        semra_mappings = assemble_evidences(semra_mappings)
        log.info(f"After evidence assembly: {len(semra_mappings)}")

        # Convert back to SSSOM
        df_inferred = semra_to_sssom(semra_mappings)
        df_inferred.to_csv(OUTPUT_INFERRED, sep='\t', index=False)
        log.info(f"Wrote: {OUTPUT_INFERRED}")

    except Exception as e:
        log.error(f"Inference failed: {e}")
        log.info("Using consolidated mappings without inference")


if __name__ == "__main__":
    main()
