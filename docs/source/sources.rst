Sources
=======

.. automodule:: rdfsolve.sources
   :members:
   :undoc-members:
   :show-inheritance:


Dataset graph inputs
--------------------

A dataset can load several input bundles into one index. ``graph_sources`` maps each
named graph IRI to its download fields. Use ``graph_uris`` for data edges,
``type_context_graph_uris`` for extra linked-object types, and ``ontology_graph_uris``
for ontology interpretation. Every selected local graph must have an input mapping. A
graph can serve more than one scope.

.. code-block:: yaml

   - name: example
     graph_uris: [urn:example:records, urn:example:targets]
     type_context_graph_uris: []
     ontology_graph_uris: [urn:example:ontology]
     graph_sources:
       urn:example:records:
         download_ttl: [https://example.org/records.ttl.gz]
       urn:example:targets:
         download_nt: [https://example.org/targets.nt.gz]
       urn:example:ontology:
         download_owl: [https://example.org/ontology.owl]

Run this entry with the ordinary local pipeline stage. It yields one schema and one
extraction report. Subject typing spans the data graphs. Linked-object typing also uses
the type-context graphs. Ontology context supports interpretation and ontology
extraction. Its edges enter empirical patterns only when that graph is also explicitly
selected as data.

Mapped inputs accept Turtle, N-Triples and RDF/XML (``download_rdf``,
``download_rdfxml``, ``download_owl``). XML conversion requires ``rapper``. Files use
SHA-256 names derived from their URLs inside graph-specific directories. This keeps
downloads with equal basenames separate. RDF/XML conversion writes N-Triples. A failed
download or conversion stops preparation. The pipeline checks that each graph has the
declared number of prepared triple files before indexing. A prepared index remains
subject to named-graph verification when mining starts.

The generated Qleverfile uses ``MULTI_INPUT_JSON``, with one command, format and graph
per input. The pipeline passes the same mapping through ``-f``, ``-F`` and ``-g``.
QLever control requires exactly one of
``MULTI_INPUT_JSON`` and ``CAT_INPUT_FILES``.

Explicit ``local_provider`` groups remain available for existing registries. Host names
and download URL prefixes do not define membership. A dataset with ``graph_sources``
uses the local stage.

PubChem
-------

``pubchem.ftp`` specifies 2,240 data inputs across the 26 data graphs in the pinned
QLever recipe. Its ruleset graph contains the 23 companion ontology inputs. The registry
records retrieval locations; each build must retain exact input hashes, ontology
versions, conversion details and engine identity. BAO and NDF-RT need provider access or
prepared inputs. The live QLever endpoint is an access route; its current contents
require their own observation.

``pubchem.ftp.schema`` records publisher VoID and SHACL documents as access artifacts.
The IDSM entries retain their graph scopes. Local FTP reproduction does not imply that
the IDSM and QLever provider snapshots are equal.

Counts and release inputs
-------------------------

``SchemaPattern.count_semantics`` identifies ``triples_in_graph``, ``quad_occurrences``,
``endpoint_default``, ``upper_bound``, or ``unknown``. Counts across named graphs add
edge occurrences. Distinct subjects and objects stay unset when their per-graph values
cannot be added safely. Hierarchy merges carry upper bounds. Blank-node patterns carry
edge counts and graph attribution.

Each release extraction records its snapshot, canonical schema artifact, report, and
graph scopes. A dataset with several extractions has no single snapshot ID. Analysis
reads ``release.json``, checks schema hashes and requires an extraction selection when a
dataset has several snapshots. The analysis scripts accept ``--extraction-mode
local|remote|grouped|unknown``. Build the release manifest before running those scripts.
Pipeline analysis creates a manifest checkpoint first.

The canonical schema is saved before optional evidence and exports. Pipeline output work
leaves the report unfinished until it ends. Failed output work marks the attempt partial
and retains the saved schema. Remote health skips retain a reason. Endpoint health
applies to remote access independently of local inputs.
