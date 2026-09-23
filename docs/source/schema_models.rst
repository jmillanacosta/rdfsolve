Schema Models
=============

.. automodule:: rdfsolve.schema_models
   :no-members:

Pattern views
-------------

Canonical JSON retains three pattern collections:

* **patterns** contains typed patterns and any hierarchy-derived summaries.
  Class counts, navigation and class-schema exports use this collection.
* **raw_patterns** retains typed observations before hierarchy grouping when
  ontology-as-data mining is enabled.
* **term_patterns** retains exact ontology-term observations before grouping.
  **subject_binding** and **object_binding** distinguish **type** (instance
  membership) from **term** (the IRI itself). Object roles apply to IRI objects;
  literal and blank-node kinds retain their usual meaning.

A term record and an instance of that term can have the same property.
Their observations remain separate. Exact term patterns retain their original
IRIs and measured counts; class-schema exports do not express these records.
Scientific validation and example queries bind exact term IRIs with VALUES.

None means a collection was not collected; an empty list means its queries
found no patterns. Older files have no term-role evidence. Re-mine their source
snapshot to obtain it; roles cannot be recovered from ambiguous IRIs alone.

Core
----

.. automodule:: rdfsolve.schema_models.core
   :members:
   :undoc-members:
   :show-inheritance:

Metadata Models
---------------

.. automodule:: rdfsolve.schema_models.metadata
   :members:
   :undoc-members:
   :show-inheritance:

Ontology Models
---------------

.. automodule:: rdfsolve.schema_models.ontology
   :members:
   :undoc-members:
   :show-inheritance:

LinkML Converter
----------------

.. automodule:: rdfsolve.schema_models.exporters.linkml
   :members:
   :undoc-members:
   :show-inheritance:

SHACL Models
------------

.. automodule:: rdfsolve.schema_models.shacl_model
   :members:
   :undoc-members:
   :show-inheritance:

SHACL Converter
---------------

.. automodule:: rdfsolve.schema_models.exporters.shacl
   :members:
   :undoc-members:
   :show-inheritance:

VoID Models
-----------

.. automodule:: rdfsolve.schema_models.void_model
   :members:
   :undoc-members:
   :show-inheritance:

VoID Converter
--------------

.. automodule:: rdfsolve.schema_models.readers.void
   :members:
   :undoc-members:
   :show-inheritance:

RDF-Config Converter
--------------------

.. automodule:: rdfsolve.schema_models.exporters.rdfconfig
   :members:
   :undoc-members:
   :show-inheritance:

Report
------

.. automodule:: rdfsolve.schema_models.report
   :members:
   :undoc-members:
   :show-inheritance:
