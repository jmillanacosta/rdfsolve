#!/usr/bin/env python3
"""Consolidate all mappings and run semra inference."""
import sys
from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path(__file__).parent.parent / "output" / "mappings"
EXTERNAL_FILE = OUTPUT_DIR / "01_external.sssom.tsv"
CLASS_FILE = OUTPUT_DIR / "03_class_mappings.sssom.tsv"
OUTPUT_FILE = OUTPUT_DIR / "04_consolidated.sssom.tsv"

def main():
    if OUTPUT_FILE.exists():
        sys.stderr.write(f"Output exists: {OUTPUT_FILE}\n")
        sys.stderr.flush()
        return

    dfs = []

    # Load external mappings
    if EXTERNAL_FILE.exists():
        df_ext = pd.read_csv(EXTERNAL_FILE, sep='\t', comment='#')
        sys.stderr.write(f"External mappings: {len(df_ext)}\n")
        sys.stderr.flush()
        dfs.append(df_ext)

    # Load class mappings
    if CLASS_FILE.exists():
        df_class = pd.read_csv(CLASS_FILE, sep='\t', comment='#')
        sys.stderr.write(f"Class mappings: {len(df_class)}\n")
        sys.stderr.flush()
        dfs.append(df_class)

    if not dfs:
        sys.stderr.write("No mappings to consolidate\n")
        sys.stderr.flush()
        OUTPUT_FILE.write_text("# No mappings\n")
        return

    # Concatenate
    df_all = pd.concat(dfs, ignore_index=True)

    # Deduplicate
    key_cols = ['subject_id', 'predicate_id', 'object_id']
    df_all = df_all.drop_duplicates(subset=key_cols, keep='first')

    sys.stderr.write(f"Total before semra: {len(df_all)}\n")
    sys.stderr.flush()

    # Try semra inference
    try:
        from semra import Mapping, MappingSet
        from semra.rules import BROAD_MATCH_RULES

        sys.stderr.write("Running semra inference...\n")
        sys.stderr.flush()

        # Convert to semra Mapping objects
        mappings = []
        for _, row in df_all.iterrows():
            try:
                mapping = Mapping(
                    s=str(row['subject_id']),
                    p=str(row['predicate_id']),
                    o=str(row['object_id']),
                    evidence=[],
                    confidence=float(row.get('confidence', 0.95)) if pd.notna(row.get('confidence')) else 0.95
                )
                mappings.append(mapping)
            except Exception as e:
                sys.stderr.write(f"  Skip row: {e}\n")
                sys.stderr.flush()
                continue

        mapping_set = MappingSet(name='lslod', mappings=mappings)

        # Apply inference rules
        inferred_set = mapping_set.apply_rules(BROAD_MATCH_RULES)

        sys.stderr.write(f"After semra inference: {len(inferred_set.mappings)}\n")
        sys.stderr.flush()

        # Convert back to DataFrame
        rows = []
        for m in inferred_set.mappings:
            rows.append({
                'subject_id': m.s,
                'predicate_id': m.p,
                'object_id': m.o,
                'confidence': m.confidence
            })

        df_final = pd.DataFrame(rows)

    except Exception as e:
        sys.stderr.write(f"Semra failed: {e}\n")
        sys.stderr.write("Falling back to non-inferred mappings\n")
        sys.stderr.flush()
        df_final = df_all

    # Deduplicate again
    df_final = df_final.drop_duplicates(subset=key_cols, keep='first')
    df_final = df_final.sort_values('confidence', ascending=False) if 'confidence' in df_final.columns else df_final

    sys.stderr.write(f"Final consolidated mappings: {len(df_final)}\n")
    sys.stderr.flush()

    df_final.to_csv(OUTPUT_FILE, sep='\t', index=False)
    sys.stderr.write(f"Wrote: {OUTPUT_FILE}\n")
    sys.stderr.flush()

if __name__ == "__main__":
    main()
