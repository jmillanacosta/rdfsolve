#!/usr/bin/env python3
"""Load external mappings from bioregistry and biomappings packages."""
import logging
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "output" / "mappings"
OUTPUT_FILE = OUTPUT_DIR / "01_external.sssom.tsv"


def load_bioregistry_mappings():
    """Load bioregistry curated mappings."""
    try:
        import bioregistry
        mappings_path = Path(bioregistry.__file__).parent / "data" / "curated_mappings.sssom.tsv"
        if mappings_path.exists():
            df = pd.read_csv(mappings_path, sep='\t', comment='#')
            log.info(f"Loaded {len(df)} bioregistry mappings")
            return df
    except Exception as e:
        log.warning(f"Could not load bioregistry mappings: {e}")
    return pd.DataFrame()


def load_biomappings():
    """Load biomappings positive predictions."""
    try:
        import biomappings
        mappings_path = Path(biomappings.__file__).parent / "resources" / "positive.sssom.tsv"
        if mappings_path.exists():
            df = pd.read_csv(mappings_path, sep='\t', comment='#')
            log.info(f"Loaded {len(df)} biomappings")
            return df
    except Exception as e:
        log.warning(f"Could not load biomappings: {e}")
    return pd.DataFrame()


def derive_namespace_mappings(entity_df, min_shared=5):
    """Derive namespace-to-namespace mappings from entity-entity mappings.

    If many entities from namespace A map to namespace B, infer A and B are related.
    """
    if entity_df.empty or 'subject_id' not in entity_df.columns or 'object_id' not in entity_df.columns:
        return pd.DataFrame()

    # Extract prefix pairs
    prefix_pairs = []
    for _, row in entity_df.iterrows():
        subj = str(row['subject_id'])
        obj = str(row['object_id'])

        # Extract prefix (before colon)
        if ':' in subj and ':' in obj:
            subj_prefix = subj.split(':', 1)[0].lower()
            obj_prefix = obj.split(':', 1)[0].lower()

            if subj_prefix != obj_prefix:  # Don't map prefix to itself
                prefix_pairs.append((subj_prefix, obj_prefix, row.get('predicate_id', 'skos:relatedMatch')))

    if not prefix_pairs:
        return pd.DataFrame()

    # Count prefix pairs
    from collections import Counter
    pair_counts = Counter((p[0], p[1], p[2]) for p in prefix_pairs)

    # Create mappings for pairs with enough evidence
    mappings = []
    for (subj_pfx, obj_pfx, pred), count in pair_counts.items():
        if count >= min_shared:
            mappings.append({
                'subject_id': subj_pfx,
                'predicate_id': pred,
                'object_id': obj_pfx,
                'confidence': min(0.99, count / 100.0),  # Simple confidence scoring
                'mapping_justification': f'semapv:InferredFromEntityMappings',
                'comment': f'Inferred from {count} entity-entity mappings'
            })

    if mappings:
        log.info(f"Derived {len(mappings)} namespace mappings from entity mappings")
        return pd.DataFrame(mappings)

    return pd.DataFrame()


def main():
    if OUTPUT_FILE.exists():
        log.info(f"Output exists: {OUTPUT_FILE}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    dfs = []

    # Load bioregistry mappings
    df_bio = load_bioregistry_mappings()
    if not df_bio.empty:
        dfs.append(df_bio)

    # Load biomappings
    df_biomappings = load_biomappings()
    if not df_biomappings.empty:
        dfs.append(df_biomappings)

        # Derive namespace-level mappings from entity mappings
        df_namespace = derive_namespace_mappings(df_biomappings)
        if not df_namespace.empty:
            dfs.append(df_namespace)

    if not dfs:
        log.warning("No external mappings found")
        # Create empty file
        OUTPUT_FILE.write_text("# No external mappings\n")
        return

    # Concatenate and deduplicate
    df_all = pd.concat(dfs, ignore_index=True)

    # Deduplicate by subject_id, predicate_id, object_id
    key_cols = ['subject_id', 'predicate_id', 'object_id']
    existing_cols = [c for c in key_cols if c in df_all.columns]
    if existing_cols:
        df_all = df_all.drop_duplicates(subset=existing_cols)

    log.info(f"Total mappings: {len(df_all)}")

    # Write output
    df_all.to_csv(OUTPUT_FILE, sep='\t', index=False)
    log.info(f"Wrote: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
