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

Identifiers can be resolved before the lookup. ``read_replacements`` reads the
"term replaced by" rows (``IAO:0100001``) of an SSSOM mapping set, such as the
secondary-to-primary sets of pysec2pri; withdrawn identifiers and splits are left
out. ``verify(..., replacements=...)`` then looks up each secondary identifier by
its primary identifier, and ``replaced`` counts how many sampled identifiers were
rewritten.

.. code-block:: python

   replacements = read_replacements("uniprot.sssom.tsv")
   evidence = verify(link, source, target, replacements=replacements)

Run an analysis
---------------

.. code-block:: bash

   python scripts/analyze_mappings.py schemas \
     --links links.tsv \
     --min-share 0.5 \
     --class-mappings classes.sssom.tsv \
     --output ../results/connectivity

Links with a share below ``--min-share`` are left out. The command writes the
connectivity graph, vocabulary overlaps, evidence counts, and the links kept as an
SSSOM mapping set (``verified_links.sssom.tsv``). In the mapping set, a link maps
the source class to the target class with ``skos:relatedMatch``; the source
property is the ``subject_match_field``, the share is the ``similarity_score``,
and ``other`` holds the sample and the target forms as JSON. The mapping SLURM
script runs this command; set ``RDFSOLVE_SCHEMAS`` and ``RDFSOLVE_LINKS`` before
submission.

Boundaries
----------

Each edge of the connectivity graph has a level of evidence:

- ``confirmed``: seen in full on the data. A class-level pattern with its count, a path
  over several steps that instances follow, and a link of which every value was read and the
  share is at least the threshold.
- ``tested``: checked on part of the data, or with a weak result. A link verified on a sample,
  and a link read in full with a share under the threshold.
- ``plausible``: stated or composed, and not tested on the data. The same class in two
  datasets, an external mapping assertion, a proposed link, and a path composed from the
  schema.

A link of which no value was found is not an edge. A tested path is one edge (kind ``path``)
from its first class to its last class, within one dataset. A route of several edges is a
composition: each part can be confirmed, but no instance is known to follow the whole route,
also across datasets. ``rdfsolve.analysis.best_route`` therefore gives such a route the
evidence ``plausible`` with the level of its weakest edge (``weakest_segment``). It takes the
strongest edges first, then the fewest edges.

A route across two datasets can be tested on the data (``scripts/check_routes.py``,
``rdfsolve.mappings.routes``). A route is a path in the first dataset, a link, and a path in
the second dataset. The start instances and their link values are read, the values are looked
up in the second dataset together with its path, and the start instances that reach the end
are counted. Between two local indexes every start instance is read and a matched route is
``confirmed``; when a dataset is read at its endpoint a sample is read and a matched route is
``tested``. A tested route is one edge (kind ``route``). The file ``routes.json`` holds the
matched routes, each with a federated query that is generated from the record and is not
executed. Routes over more than one link are not tested.

A link can reach an OWL construct that holds the identifier as a value: an
``owl:Restriction`` or an ``owl:Axiom``. Such a route is resolved to the term that the
construct describes (the subclass of the restriction, the annotated source of the axiom), so
that it ends at an entity; ``resolved_construct`` names the construct. A path with an
``rdf:type`` step is not tested.

A link over an identity property (``owl:sameAs``, ``skos:exactMatch``) is checked as a
declared identity is: an identifier that does not match the Bioregistry pattern of its
namespace (an entry name such as ``uniprot:P53_HUMAN`` where the namespace has accessions) is
flagged. The join exists on the data, so the link keeps its level of evidence, and it records
the failed checks (``flags``). The kind of entity of an identifier is not checked: no
registry holds it. Routes and edges that use the link carry the flags, and
``best_route`` takes a route without flagged edges first (``flagged``). Property correspondence and transitive mapping
inference are separate work. The optional inference script is not part of this validated
analysis workflow.

External class links can improve retrieval vocabulary without authorizing remote
query predicates or proving entity identity. Explicit assertions and verified
links remain separately measurable, consistent with the
`SSSOM data model <https://mapping-commons.github.io/sssom/1.0/spec-model/>`_.
