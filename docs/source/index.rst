RDFSolve |release| Documentation
=================================

**RDFSolve** is a Python toolkit for mining, analyzing, and exporting
RDF schemas from SPARQL endpoints.

Features
--------

Schema Discovery
^^^^^^^^^^^^^^^^

* **Schema Mining** (:mod:`rdfsolve.miner`):
  Extract schema patterns (class–property–class triples) directly from
  any SPARQL endpoint using lightweight ``SELECT DISTINCT`` queries.
  Supports **single-pass** and **two-phase mining** for large endpoints
  (QLever, PubChem, UniProt).

* **VoID Parsing** (:mod:`rdfsolve.parser`):
  Parse existing VoID (Vocabulary of Interlinked Datasets) descriptions
  to extract the same schema patterns.

* **Batch Mining** (:func:`rdfsolve.api.mine_all_sources`):
  Mine all sources listed in a CSV file (``data/sources.csv``,
  ``data/qlever.csv``), with progress callbacks and per-source
  two-phase control via the ``two_phase`` column.

Query Composition & Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Path-based Query Composition** (:mod:`rdfsolve.compose`):
  Build SPARQL queries from *diagram paths* — sequences of
  class → property → class edges selected interactively.
  Generates fresh variables, deduplicates triple patterns, adds
  optional ``rdf:type`` assertions and ``OPTIONAL`` label clauses
  (``rdfs:label``, ``dc:title``), and supports ``VALUES`` bindings.

* **SPARQL Query Execution** (:mod:`rdfsolve.query`):
  Execute queries against any endpoint and get back structured
  :class:`~rdfsolve.query.QueryResult` objects with typed
  :class:`~rdfsolve.query.ResultCell` values, timing, and
  error handling.

* **Query Export as JSON-LD** (:mod:`rdfsolve.codegen`):
  Every composed or executed query can be exported as a
  ``sh:SPARQLExecutable`` JSON-LD document for sharing and
  reproducibility.

IRI Resolution
^^^^^^^^^^^^^^

* **IRI Resolution** (:mod:`rdfsolve.iri`):
  Resolve IRIs against SPARQL endpoints to discover their
  ``rdf:type``, ``rdfs:label``, and ``dc:title``.
  Supports batch resolution and label-based search.

SPARQL Infrastructure
^^^^^^^^^^^^^^^^^^^^^

* **Robust SPARQL Client** (:mod:`rdfsolve.sparql_helper`):
  Resilient HTTP client with GET→POST fallback, exponential
  back-off with jitter, adaptive pagination (auto-shrinks page
  size on timeouts), HTTP 414→POST recovery, and per-query
  ``purpose`` logging.

Export Formats
^^^^^^^^^^^^^^

* **Multiple Export Formats** from :class:`~rdfsolve.parser.VoidParser`:
  JSON-LD, VoID (Turtle), CSV / DataFrame, LinkML YAML,
  SHACL shapes (open or closed), RDF-config (model + prefix +
  endpoint YAML), and coverage analysis.

* **Code Generation** (:mod:`rdfsolve.codegen`):
  Generate ready-to-run Python snippets for compose, execute,
  resolve, and export operations — included in every API
  response for learn-by-example workflows.

* **Schema Analysis** (:mod:`rdfsolve.schema_utils`):
  Helpers for LinkML schema comparison, semantic richness
  metrics, and Plotly visualisations.

Quick Examples
--------------

**Mine a schema**:

.. code-block:: python

   from rdfsolve.miner import mine_schema

   schema = mine_schema(
       endpoint_url="https://sparql.rhea-db.org/sparql",
       dataset_name="rhea",
   )
   schema.to_jsonld()      # → JSON-LD dict
   schema.to_void_graph()  # → rdflib Graph

**Two-phase mining (large endpoints)**:

.. code-block:: python

   schema = mine_schema(
       endpoint_url="https://qlever.cs.uni-freiburg.de/api/pubchem",
       dataset_name="pubchem.qlever",
       two_phase=True,
       counts=False,
   )

**Compose a SPARQL query from paths**:

.. code-block:: python

   from rdfsolve import compose_query_from_paths

   result = compose_query_from_paths(
       paths=[{
           "edges": [{
               "source": "http://rdf.rhea-db.org/Reaction",
               "target": "http://rdf.rhea-db.org/ReactionSide",
               "predicate": "http://rdf.rhea-db.org/side",
               "is_forward": True,
           }],
       }],
       prefixes={"rh": "http://rdf.rhea-db.org/"},
   )
   print(result["query"])    # SPARQL SELECT with variables
   print(result["jsonld"])   # sh:SPARQLExecutable JSON-LD

**Execute a SPARQL query**:

.. code-block:: python

   from rdfsolve import execute_sparql

   result = execute_sparql(
       query="SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10",
       endpoint="https://sparql.rhea-db.org/sparql",
   )
   for row in result.rows:
       print(row)

**Resolve IRIs**:

.. code-block:: python

   from rdfsolve import resolve_iris

   result = resolve_iris(
       iris=["http://rdf.rhea-db.org/10000"],
       endpoints=[{
           "name": "rhea",
           "endpoint": "https://sparql.rhea-db.org/sparql",
       }],
   )

**CLI**:

.. code-block:: bash

   # Mine a single endpoint
   rdfsolve mine --endpoint https://sparql.rhea-db.org/sparql \
     --dataset-name rhea --output-dir ./schemas

   # Mine all sources from CSV
   rdfsolve mine-all --sources data/sources.csv \
     --output-dir ./schemas

   # VoID-based workflow
   rdfsolve extract --endpoint https://sparql.rhea-db.org/sparql \
     --output-dir ./output
   rdfsolve export --void-file ./output/void_description.ttl \
     --format all

Table of Contents
-----------------
.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   installation
   usage
   cli

.. toctree::
   :maxdepth: 2
   :caption: Core Modules

   miner
   sparql_helper
   parser
   models

.. toctree::
   :maxdepth: 2
   :caption: Query & IRI

   query
   iri
   compose
   codegen

.. toctree::
   :maxdepth: 2
   :caption: Utilities

   schema_utils
   utils

Indices and Tables
------------------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
