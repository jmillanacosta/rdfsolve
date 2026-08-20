#!/usr/bin/env python3
"""Analyze SSSOM mapping files to show statistics and insights."""

import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


def extract_namespace(uri: str) -> str:
    """Extract namespace from URI."""
    if "#" in uri:
        return uri.rsplit("#", 1)[0] + "#"
    elif "/" in uri:
        return uri.rsplit("/", 1)[0] + "/"
    return uri


def get_prefix(uri: str) -> str:
    """Get a readable prefix for a URI."""
    ns = extract_namespace(uri)

    prefixes = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
        "http://www.w3.org/2000/01/rdf-schema#": "rdfs",
        "http://www.w3.org/2002/07/owl#": "owl",
        "http://www.w3.org/2004/02/skos/core#": "skos",
        "http://purl.org/dc/terms/": "dcterms",
        "http://purl.org/dc/elements/1.1/": "dc",
        "http://xmlns.com/foaf/0.1/": "foaf",
        "http://purl.org/pav/": "pav",
        "http://aopkb.org/aop_ontology#": "aopo",
        "http://vocabularies.wikipathways.org/gpml#": "gpml",
        "http://vocabularies.wikipathways.org/wp#": "wp",
        "http://purl.bioontology.org/ontology/NCBITAXON/": "ncbitaxon",
        "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#": "ncit",
        "http://purl.obolibrary.org/obo/": "obo",
        "http://semanticscience.org/resource/": "sio",
        "http://edamontology.org/": "edam",
        "https://w3id.org/semapv/vocab/": "semapv",
    }

    if ns in prefixes:
        return prefixes[ns]

    parsed = urlparse(ns.rstrip("/#"))
    if parsed.netloc:
        parts = parsed.netloc.split(".")
        if len(parts) >= 2:
            return parts[-2]

    return ns


def parse_sssom_tsv(filepath: Path) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Parse SSSOM TSV file returning metadata and mappings."""
    metadata: dict[str, str] = {}
    mappings: list[dict[str, str]] = []
    headers: list[str] = []

    with filepath.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.startswith("#"):
                # Metadata line (YAML-like)
                content = line[1:].strip()
                if ":" in content:
                    key, _, value = content.partition(":")
                    metadata[key.strip()] = value.strip()
            elif not headers:
                # Header row
                headers = line.split("\t")
            else:
                # Data row
                values = line.split("\t")
                row = dict(zip(headers, values, strict=False))
                mappings.append(row)

    return metadata, mappings


def analyze_sssom(sssom_file: Path) -> None:
    """Analyze a single SSSOM file."""
    print(f"Analyzing: {sssom_file}")
    print("=" * 80)

    metadata, mappings = parse_sssom_tsv(sssom_file)

    # Metadata summary
    print("\nMETADATA")
    print("-" * 40)
    for key, value in metadata.items():
        print(f"  {key}: {value}")

    # Mapping counts
    print(f"\nTotal mappings: {len(mappings)}")

    if not mappings:
        print("No mappings found.")
        return

    # Predicate distribution
    predicate_counter = Counter()
    for m in mappings:
        pred = m.get("predicate_id", "")
        predicate_counter[pred] += 1

    print("\n" + "=" * 80)
    print("PREDICATES")
    print("=" * 80)
    for pred, count in predicate_counter.most_common():
        pct = (count / len(mappings)) * 100
        prefix = get_prefix(pred)
        local = pred.replace(extract_namespace(pred), "")
        print(f"{count:5d} ({pct:5.1f}%)  {prefix}:{local}")

    # Mapping justification distribution
    justification_counter = Counter()
    for m in mappings:
        just = m.get("mapping_justification", "")
        justification_counter[just] += 1

    print("\n" + "=" * 80)
    print("MAPPING JUSTIFICATIONS")
    print("=" * 80)
    for just, count in justification_counter.most_common():
        pct = (count / len(mappings)) * 100
        prefix = get_prefix(just) if just else "none"
        local = just.replace(extract_namespace(just), "") if just else ""
        print(f"{count:5d} ({pct:5.1f}%)  {prefix}:{local}")

    # Confidence distribution
    confidences = [float(m["confidence"]) for m in mappings if m.get("confidence")]
    if confidences:
        print("\n" + "=" * 80)
        print("CONFIDENCE STATISTICS")
        print("=" * 80)
        print(f"  Min: {min(confidences):.2f}")
        print(f"  Max: {max(confidences):.2f}")
        print(f"  Avg: {sum(confidences) / len(confidences):.2f}")

    # Subject/Object namespace distribution
    subject_ns_counter = Counter()
    object_ns_counter = Counter()
    for m in mappings:
        subj = m.get("subject_id", "")
        obj = m.get("object_id", "")
        if subj:
            subject_ns_counter[get_prefix(subj)] += 1
        if obj:
            object_ns_counter[get_prefix(obj)] += 1

    print("\n" + "=" * 80)
    print("SUBJECT NAMESPACES")
    print("=" * 80)
    for ns, count in subject_ns_counter.most_common(15):
        pct = (count / len(mappings)) * 100
        print(f"{count:5d} ({pct:5.1f}%)  {ns}")

    print("\n" + "=" * 80)
    print("OBJECT NAMESPACES")
    print("=" * 80)
    for ns, count in object_ns_counter.most_common(15):
        pct = (count / len(mappings)) * 100
        print(f"{count:5d} ({pct:5.1f}%)  {ns}")

    print("\n" + "=" * 80)


def analyze_directory(mappings_dir: Path) -> None:
    """Analyze all SSSOM files in a directory."""
    sssom_files = sorted(mappings_dir.glob("*.sssom.tsv"))

    if not sssom_files:
        print(f"No SSSOM files found in {mappings_dir}")
        return

    print(f"Found {len(sssom_files)} SSSOM files in {mappings_dir}")
    print("=" * 80)

    total_mappings = 0
    file_counts: dict[str, int] = {}

    for sssom_file in sssom_files:
        _, mappings = parse_sssom_tsv(sssom_file)
        count = len(mappings)
        total_mappings += count
        file_counts[sssom_file.name] = count

    print("\nFILE SUMMARY")
    print("-" * 40)
    for filename, count in sorted(file_counts.items(), key=lambda x: -x[1]):
        print(f"  {count:6d} mappings  {filename}")

    print(f"\nTotal: {total_mappings} mappings across {len(sssom_files)} files")
    print("=" * 80)

    # Analyze each file
    for sssom_file in sssom_files:
        print("\n")
        analyze_sssom(sssom_file)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
    else:
        target = Path("/home/javier.millanacosta/rdfsolve/output/mappings")

    if not target.exists():
        print(f"Error: Path not found: {target}")
        sys.exit(1)

    if target.is_dir():
        analyze_directory(target)
    else:
        analyze_sssom(target)
