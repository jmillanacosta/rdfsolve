#!/usr/bin/env python3
"""Infer class mappings from cross-references with classes."""
import sys
from pathlib import Path
from collections import Counter
import pandas as pd

INPUT_FILE = Path(__file__).parent.parent / "output" / "mappings" / "02_crossrefs.sssom.tsv"
OUTPUT_FILE = Path(__file__).parent.parent / "output" / "mappings" / "03_class_mappings.sssom.tsv"
MIN_EVIDENCE = 5

def main():
    if OUTPUT_FILE.exists():
        sys.stderr.write(f"Output exists: {OUTPUT_FILE}\n")
        sys.stderr.flush()
        return

    if not INPUT_FILE.exists():
        sys.stderr.write(f"Input missing: {INPUT_FILE}\n")
        sys.stderr.flush()
        return

    sys.stderr.write(f"Reading {INPUT_FILE}\n")
    sys.stderr.flush()
    df = pd.read_csv(INPUT_FILE, sep='\t')

    # Filter rows with both classes present
    df_with_classes = df[df['subject_class'].notna() & df['object_class'].notna()].copy()
    sys.stderr.write(f"Rows with both classes: {len(df_with_classes)}\n")
    sys.stderr.flush()

    if df_with_classes.empty:
        sys.stderr.write("No class pairs found\n")
        sys.stderr.flush()
        OUTPUT_FILE.write_text("# No class pairs\n")
        return

    # Count class pairs
    class_pairs = []
    for _, row in df_with_classes.iterrows():
        class_pairs.append((
            row['subject_class'],
            row['object_class'],
            row['predicate_id']
        ))

    pair_counts = Counter(class_pairs)
    sys.stderr.write(f"Unique class pairs: {len(pair_counts)}\n")
    sys.stderr.flush()

    # Generate class mappings
    mappings = []
    for (subj_class, obj_class, pred), count in pair_counts.items():
        if count >= MIN_EVIDENCE:
            mappings.append({
                'subject_id': subj_class,
                'predicate_id': pred,
                'object_id': obj_class,
                'confidence': min(0.99, count / 100.0),
                'mapping_justification': 'semapv:InferredFromEntityMappings',
                'comment': f'Inferred from {count} entity cross-references'
            })

    if not mappings:
        sys.stderr.write(f"No class mappings with >={MIN_EVIDENCE} evidence\n")
        sys.stderr.flush()
        OUTPUT_FILE.write_text("# No class mappings\n")
        return

    df_out = pd.DataFrame(mappings)
    df_out = df_out.sort_values('confidence', ascending=False)

    sys.stderr.write(f"Class mappings: {len(df_out)}\n")
    sys.stderr.flush()

    df_out.to_csv(OUTPUT_FILE, sep='\t', index=False)
    sys.stderr.write(f"Wrote: {OUTPUT_FILE}\n")
    sys.stderr.flush()

if __name__ == "__main__":
    main()
