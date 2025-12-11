RDF solve |release| Documentation
=================================

**RDFSolve** is a Python toolkit for extracting, analyzing, and converting RDF schemas from SPARQL endpoints.

Features
--------

* **VoID Generation**: Extract VoID (Vocabulary of Interlinked Datasets) descriptions from SPARQL endpoints
* **Schema Discovery**: Automatically discover existing VoID metadata in endpoints
* **Multiple Export Formats**: Convert schemas to CSV, JSON-LD, LinkML, and coverage analysis
* **Bioregistry Integration**: Automatic prefix resolution using Bioregistry for proper namespace handling
* **Custom Configuration**: Support for custom dataset naming, VoID partition URIs, and LinkML schema URIs
* **Service Graph Filtering**: Exclude Virtuoso system graphs and well-known URIs by default
* **Pagination Support**: Handle large endpoints with configurable pagination
* **Interactive CLI**: User-friendly command-line interface with confirmation prompts

Typical Workflow
----------------

1. **Discover**: Check if endpoint has existing VoID metadata
2. **Extract**: Generate fresh VoID description or use discovered metadata
3. **Export**: Convert schema to desired format (CSV, JSON-LD, LinkML)
4. **Count**: Analyze class instance counts for dataset composition

Quick Example
-------------

.. code-block:: bash

   # Discover existing VoID metadata
   rdfsolve discover --endpoint https://sparql.example.org/sparql

   # Extract schema with custom naming
   rdfsolve -v extract \\
     --endpoint https://sparql.example.org/sparql \\
     --dataset-name mydata \\
     --void-base-uri "http://example.org/void" \\
     --output-dir ./output

   # Export to multiple formats
   rdfsolve export \\
     --void-file ./output/mydata_void.ttl \\
     --format all \\
     --schema-uri "http://example.org/schemas/mydata"

Table of Contents
-----------------
.. toctree::
   :maxdepth: 2
   :caption: Getting Started
   :name: start

   installation
   usage
   cli
   parser
   utils
   models
   schema_utils

Indices and Tables
------------------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
