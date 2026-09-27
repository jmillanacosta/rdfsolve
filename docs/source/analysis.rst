Schema connectivity
===================

Package layout
--------------

* ``mining`` discovers schemas and observed navigation paths.
* ``mappings`` imports mapping assertions, and infers and verifies links between
  datasets from the identifier types of the schemas.
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
class predicates, shared class vocabulary, explicit class mappings, and verified
links. A verified link keeps its sample size, the share of sampled identifiers that
the target has, and the forms in which the target writes them. A link does not
establish class equivalence.

Add mapping evidence
--------------------

.. code-block:: python

   from rdfsolve.analysis import read_class_mappings

   mappings, report = read_class_mappings("classes.sssom.tsv", schemas)
   graph = build_connectivity(schemas, class_mappings=mappings)

SSSOM predicates, justification, source identifiers and supplied confidence are
retained.

Add verified links
------------------

.. code-block:: python

   from rdfsolve.mappings import infer_links, verify, write_links, read_links

   candidates = infer_links(schemas)
   evidence = [verify(link, clients[link.source], clients[link.target]) for link in candidates]
   write_links("links.tsv", evidence)
   graph = build_connectivity(schemas, links=read_links("links.tsv"))

A candidate link comes from the schema examples: values of one dataset carry the
identifier type (a Bioregistry prefix) of subjects of another dataset (a join), or
of values of another dataset (a shared reference). ``verify`` looks up a sample of
the values in the target. The share is relative to the sample only.

Run an analysis
---------------

.. code-block:: bash

   python scripts/analyze_mappings.py schemas \
     --links links.tsv \
     --min-share 0.5 \
     --class-mappings classes.sssom.tsv \
     --output ../results/connectivity

Links with a share below ``--min-share`` are left out. The command writes the
connectivity graph, vocabulary overlaps, and evidence counts. The mapping SLURM
script runs this command; set ``RDFSOLVE_SCHEMAS`` and ``RDFSOLVE_LINKS`` before
submission.

Boundaries
----------

The connectivity graph currently uses direct class-level patterns. Retained SHACL
paths remain available to mining and the client; compound paths are not expanded
into cross-dataset analysis edges. Property correspondence and transitive mapping
inference are separate work. The optional inference script is not part of this
validated analysis workflow.

External class links can improve retrieval vocabulary without authorizing remote
query predicates or proving entity identity. Explicit assertions and verified
links remain separately measurable, consistent with the
`SSSOM data model <https://mapping-commons.github.io/sssom/1.0/spec-model/>`_.
