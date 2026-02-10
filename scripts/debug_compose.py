#!/usr/bin/env python3
"""Reproduce the compose bug reported from the WikiPathways frontend.

The user drew a 3-hop chain in the diagram:

  Interaction --[isPartOf]--> Pathway --[isPartOf]--> Protein --[bdbUniprot]--> ???

Expected SPARQL:
  ?interaction dcterms:isPartOf ?pathway .
  ?pathway dcterms:isPartOf ?protein .
  ?protein wp:bdbUniprot ?protein_1 .

Actual SPARQL (broken — from the frontend legacy composer):
  ?interaction dcterms:isPartOf ?pathway .
  ?pathway wp:bdbUniprot ?protein .

The middle hop is missing entirely!

This script uses `rdfsolve.compose.compose_query_from_paths` (package-
level, no Flask) to reproduce and verify the fix.
"""

from rdfsolve.compose import compose_query_from_paths

WP = "http://vocabularies.wikipathways.org/wp#"
DCTERMS = "http://purl.org/dc/terms/"

prefixes = {
    "wp": WP,
    "dcterms": DCTERMS,
    "dc": "http://purl.org/dc/elements/1.1/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
}


# ══════════════════════════════════════════════════════════════════
# SCENARIO 1 — Correct chain: 3 hops, each edge has distinct
#              source/target pairs (how path-finder SHOULD emit)
# ══════════════════════════════════════════════════════════════════

chain_path = {
    "edges": [
        {
            "source": f"{WP}Interaction",
            "target": f"{WP}Pathway",
            "predicate": f"{DCTERMS}isPartOf",
            "is_forward": True,
        },
        {
            "source": f"{WP}Pathway",
            "target": f"{WP}Protein",
            "predicate": f"{DCTERMS}isPartOf",
            "is_forward": True,
        },
        {
            "source": f"{WP}Protein",
            "target": f"{WP}Protein",   # reflexive
            "predicate": f"{WP}bdbUniprot",
            "is_forward": True,
        },
    ],
}

result1 = compose_query_from_paths(
    paths=[chain_path],
    prefixes=prefixes,
    options={
        "include_types": True,
        "include_labels": True,
        "limit": 100,
    },
)


# ══════════════════════════════════════════════════════════════════
# SCENARIO 2 — Fan/star: same ?interaction and ?pathway, multiple
#              predicates between them (NOT a chain)
# ══════════════════════════════════════════════════════════════════

# If we model this as separate single-edge paths we get fresh vars
# each time — that's the current (broken) behavior for fan patterns
fan_paths = [
    {"edges": [{
        "source": f"{WP}Interaction",
        "target": f"{WP}Pathway",
        "predicate": f"{DCTERMS}isPartOf",
        "is_forward": True,
    }]},
    {"edges": [{
        "source": f"{WP}Interaction",
        "target": f"{WP}Pathway",
        "predicate": f"{WP}participants",
        "is_forward": True,
    }]},
]

result2 = compose_query_from_paths(
    paths=fan_paths,
    prefixes=prefixes,
    options={
        "include_types": True,
        "include_labels": False,
        "limit": 100,
    },
)

# ══════════════════════════════════════════════════════════════════
# ANALYSIS
# ══════════════════════════════════════════════════════════════════

for _label, result in [("1 (chain)", result1), ("2 (fan)", result2)]:
    lines = result["query"].split("\n")
    pats = [
        ln for ln in lines
        if ln.strip().startswith("?") and " a " not in ln
    ]
    types = [
        ln for ln in lines
        if " a " in ln and ln.strip().startswith("?")
    ]

