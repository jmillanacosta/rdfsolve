"""Analysis-facing imports for ontology discovery and usage evidence.

The evidence models live outside :mod:`rdfsolve.analysis` so mining and release
code can use the same records without depending on the analysis package.
"""

from rdfsolve.evidence.ontology import (
    VERSION_PREDICATES,
    ObservedOntologyTerms,
    OntologyArtifact,
    OntologyGraphCandidate,
    OntologyTermUsage,
    OntologyUsage,
    OntologyVersionEvidence,
    UsageOverlap,
    assess_ontology_usage,
    compare_ontology_usage,
    discover_ontology_graphs,
    inspect_ontology_graph,
    observed_terms_from_patterns,
    ontology_usage_slice,
)

__all__ = [
    "VERSION_PREDICATES",
    "ObservedOntologyTerms",
    "OntologyArtifact",
    "OntologyGraphCandidate",
    "OntologyTermUsage",
    "OntologyUsage",
    "OntologyVersionEvidence",
    "UsageOverlap",
    "assess_ontology_usage",
    "compare_ontology_usage",
    "discover_ontology_graphs",
    "inspect_ontology_graph",
    "observed_terms_from_patterns",
    "ontology_usage_slice",
]
