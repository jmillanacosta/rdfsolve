"""Ontologies in rdfsolve: one place for the ontology model and the choices about it.

rdfsolve mines data graphs, including how they use ontology terms; an ontology itself is read,
not mined. This package holds:

- vocabulary: the IRIs that ontologies are written in (class and property types, structure,
  annotation and infrastructure namespaces, versions);
- terms: the namespace of a term (one rule, in Python and SPARQL) and the record of a term;
- service: Ontologies, answers about terms (OLS or Ontobee; UberGraph for ancestors,
  descendants, relations and Biolink categories), cached with provenance;
- ubergraph: UberGraph, the reasoned OBO ontologies as a source;
- hierarchy: named parents and ancestors, from an ontology graph, an endpoint or files, and
  the rule that fills parents from hierarchy files;
- discovery: which graphs of a dataset hold ontology material, and their versions;
- usage: how a data graph uses the terms of an ontology artifact;
- sources: which ontology a namespace of the data comes from, and the plan to acquire it;
- artifacts: retrieved ontology files, their versions and registry, and local copies;
- reference: acquiring reference ontologies after mining and assessing usage against them.

Mining strategies (rdfsolve.mining) use these for the ontology terms of data graphs.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rdfsolve.ontology.service import Ontologies
    from rdfsolve.ontology.terms import Term, namespace
    from rdfsolve.ontology.ubergraph import UberGraph

# Loaded on first use: schema_models imports rdfsolve.ontology.structure, and the service
# imports schema models.
_EXPORTS = {
    "Ontologies": "rdfsolve.ontology.service",
    "Term": "rdfsolve.ontology.terms",
    "UberGraph": "rdfsolve.ontology.ubergraph",
    "namespace": "rdfsolve.ontology.terms",
}


def __getattr__(name: str) -> Any:
    """Return a public name of the package, importing its module on first use."""
    if name not in _EXPORTS:
        raise AttributeError(f"module 'rdfsolve.ontology' has no attribute {name!r}")
    return getattr(import_module(_EXPORTS[name]), name)


__all__ = ["Ontologies", "Term", "UberGraph", "namespace"]
