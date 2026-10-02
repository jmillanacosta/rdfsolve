Ontologies
==========

rdfsolve mines data graphs, including how they use ontology terms. An ontology itself is
read with ontology services and tools, not mined: ``rdfsolve.ontology`` holds the ontology
model and the choices about it, and mining strategies use it.

``Ontologies`` answers questions about terms and says which source answered: OLS (or
Ontobee) for terms, names and parents; UberGraph for the ancestors, descendants, relations
and Biolink categories of OBO terms. A term that no source knows gets ``None``, never an
empty list.

.. automodule:: rdfsolve.ontology

vocabulary
----------

.. automodule:: rdfsolve.ontology.vocabulary
   :members:
   :undoc-members:
   :show-inheritance:

terms
-----

.. automodule:: rdfsolve.ontology.terms
   :members:
   :undoc-members:
   :show-inheritance:

service
-------

.. automodule:: rdfsolve.ontology.service
   :members:
   :undoc-members:
   :show-inheritance:

ubergraph
---------

.. automodule:: rdfsolve.ontology.ubergraph
   :members:
   :undoc-members:
   :show-inheritance:

hierarchy
---------

.. automodule:: rdfsolve.ontology.hierarchy
   :members:
   :undoc-members:
   :show-inheritance:

discovery
---------

.. automodule:: rdfsolve.ontology.discovery
   :members:
   :undoc-members:
   :show-inheritance:

usage
-----

.. automodule:: rdfsolve.ontology.usage
   :members:
   :undoc-members:
   :show-inheritance:

sources
-------

.. automodule:: rdfsolve.ontology.sources
   :members:
   :undoc-members:
   :show-inheritance:

artifacts
---------

.. automodule:: rdfsolve.ontology.artifacts
   :members:
   :undoc-members:
   :show-inheritance:

reference
---------

.. automodule:: rdfsolve.ontology.reference
   :members:
   :undoc-members:
   :show-inheritance:

structure
---------

.. automodule:: rdfsolve.ontology.structure
   :members:
   :undoc-members:
   :show-inheritance:
