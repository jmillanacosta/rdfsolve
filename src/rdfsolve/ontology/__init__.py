"""Ontologies in rdfsolve: one place for the ontology model and the choices about it.

rdfsolve mines data graphs, including how they use ontology terms; an ontology itself is read,
not mined (the owner, 2026-10-02). This package holds:

- vocabulary: the IRIs that ontologies are written in (class and property types, structure,
  annotation and infrastructure namespaces, versions);
- terms: the namespace of a term (one rule, in Python and SPARQL) and the record of a term;
- service: Ontologies, answers about terms from OLS or Ontobee, cached with provenance;
- hierarchy: named parents and ancestors, from an ontology graph, an endpoint or files;
- discovery: which graphs of a dataset hold ontology material, and their versions;
- usage: how a data graph uses the terms of an ontology artifact;
- sources: which ontology a namespace of the data comes from, and the plan to acquire it;
- artifacts: retrieved ontology files, their versions and registry, and local copies;
- reference: acquiring reference ontologies after mining and assessing usage against them.

Mining strategies (rdfsolve.mining) use these for the ontology terms of data graphs.
"""

from rdfsolve.ontology.service import Ontologies
from rdfsolve.ontology.terms import Term, namespace, term_key

__all__ = ["Ontologies", "Term", "namespace", "term_key"]
