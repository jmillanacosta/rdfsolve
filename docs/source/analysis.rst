Schema connectivity
===================

Package layout
--------------

* ``mining`` discovers schemas and observed navigation paths.
* ``mappings`` imports mapping assertions, indexes entity types, and counts
  entity-supported class associations.
* ``analysis`` compares canonical schemas and builds dataset-scoped graphs.
* ``client`` retrieves data and composes grounded queries.
* ``mcp`` contains model interaction and transport.

The pipeline command delegates source preparation and stage orchestration to
``scripts/pipeline_stages``. Analysis experiments remain in scripts and notebooks.

Compare snapshots
-----------------

Select one canonical ``*_schema.json`` or ``*.schema.json`` per dataset. Duplicate
snapshots are rejected so a run cannot silently select a different schema.

.. code-block:: python

   from rdfsolve.analysis import load_schemas, compare_schemas, build_connectivity

   schemas = load_schemas("schemas")
   overlaps = compare_schemas(schemas)
   graph = build_connectivity(schemas)

Nodes are identified by dataset and class IRI. Graph edges distinguish observed
class predicates, shared class vocabulary, explicit class mappings, and
entity-supported associations. Shared entities provide association counts and
coverage among indexed entities. These counts do not establish class equivalence.

Add mapping evidence
--------------------

.. code-block:: python

   from rdfsolve.analysis import read_class_mappings

   mappings, report = read_class_mappings("classes.sssom.tsv", schemas)
   graph = build_connectivity(schemas, class_mappings=mappings)

SSSOM predicates, justification, source identifiers and supplied confidence are
retained. Entity mappings must be supplied separately with dataset-scoped entity
type evidence. Class coverage is relative to the supplied index; incomplete
indexes cannot establish population coverage.

Run an analysis
---------------

.. code-block:: bash

   python scripts/analyze_mappings.py schemas \
     --instances instance-dumps \
     --class-mappings classes.sssom.tsv \
     --entity-mappings entities.sssom.tsv \
     --output ../results/connectivity

Instance files use ``<dataset>_instances.tsv.gz`` with ``instance_iri`` and
``class_iri`` columns. Exact RDF identities are compared. The command writes the
connectivity graph, vocabulary overlaps, class associations, and evidence counts.
The mapping SLURM scripts run this command against prepared inputs; set
``RDFSOLVE_SCHEMAS`` and optionally ``RDFSOLVE_INSTANCES`` before submission.
Download and instance-extraction scripts remain separate preparation steps.

Boundaries
----------

The connectivity graph currently uses direct class-level patterns. Retained SHACL
paths remain available to mining and the client; compound paths are not expanded
into cross-dataset analysis edges. Property correspondence and transitive mapping
inference are separate work. The optional inference script is not part of this
validated analysis workflow.

External class links can improve retrieval vocabulary without authorizing remote
query predicates or proving entity identity. Explicit assertions and inferred
associations remain separately measurable, consistent with the
`SSSOM data model <https://mapping-commons.github.io/sssom/1.0/spec-model/>`_.
