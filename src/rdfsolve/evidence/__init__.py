"""Scientific evidence records produced or consumed by rdfsolve."""

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
from rdfsolve.evidence.ontology_artifacts import (
    archive_ontology_bytes,
    archive_ontology_file,
    fetch_and_archive_ontology,
    parse_ontology_bytes,
)
from rdfsolve.evidence.ontology_registry import OntologyRecord, OntologyRegistry
from rdfsolve.evidence.ontology_sources import (
    INFRASTRUCTURE_NAMESPACES,
    OntologySourceCandidate,
    OntologySourceResolution,
    UnresolvedOntologyNamespace,
    resolve_ontology_sources,
    term_namespace,
)

__all__ = [
    "INFRASTRUCTURE_NAMESPACES",
    "VERSION_PREDICATES",
    "ObservedOntologyTerms",
    "OntologyArtifact",
    "OntologyGraphCandidate",
    "OntologyRecord",
    "OntologyRegistry",
    "OntologySourceCandidate",
    "OntologySourceResolution",
    "OntologyTermUsage",
    "OntologyUsage",
    "OntologyVersionEvidence",
    "UnresolvedOntologyNamespace",
    "UsageOverlap",
    "archive_ontology_bytes",
    "archive_ontology_file",
    "assess_ontology_usage",
    "compare_ontology_usage",
    "discover_ontology_graphs",
    "fetch_and_archive_ontology",
    "inspect_ontology_graph",
    "observed_terms_from_patterns",
    "ontology_usage_slice",
    "parse_ontology_bytes",
    "resolve_ontology_sources",
    "term_namespace",
]
