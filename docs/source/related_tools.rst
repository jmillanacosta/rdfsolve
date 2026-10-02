Standards and related tools
===========================

rdfsolve reuses standards rather than re-implementing the tools built on them. This page says
what rdfsolve takes from each, what it leaves to them, and what it adds.

Standards rdfsolve writes and reads
-----------------------------------

**SSSOM** (Simple Standard for Sharing Ontological Mappings) [SSSOM]_ is the contract between
rdfsolve and other mapping tools. The mappings that sources state, and rdfsolve's resolution of
them, are SSSOM records (sssom-py's ``Mapping``; :mod:`rdfsolve.mappings.claims`):

- a stated cross-reference is ``oboInOwl:hasDbXref`` (a declared identity, ``skos:exactMatch``)
  with justification ``semapv:UnspecifiedMatching``, the stating source as
  ``mapping_provider``, and the IRIs and property as the source wrote them in ``other``;
- an accepted mapping is ``skos:exactMatch`` with justification ``semapv:MappingReview`` and
  the rule that decided it (``curation_rule_text``);
- a mapping overruled by the source that issues the target identifiers is a negative mapping
  (``predicate_modifier: Not``).

**Bioregistry** [Bioregistry]_ (with ``curies``) reads identifiers: their namespace, local
identifier and validity (:mod:`rdfsolve.identifiers`).

Tools that can use rdfsolve's output
------------------------------------

**SeMRA** (Semantic Mapping Reasoning Assembler) [SeMRA]_ assembles, infers and prioritizes
mappings at scale. rdfsolve's resolution follows its assembly and prioritization; SeMRA can be
run on rdfsolve's SSSOM files, and removes the negative mappings before grouping, so its groups
follow rdfsolve's decisions. rdfsolve does not import SeMRA.

**Babel and Node Normalization** [Babel]_ give curated groups of equivalent identifiers with a
preferred identifier, and conflate genes with the proteins they encode on request. rdfsolve
uses them to validate its joins; they do not cover every namespace that SPARQL sources use
(LIPID MAPS, for example).

**KGX** (Biolink knowledge graph exchange) merges equivalent nodes (clique merge) and summarizes
graphs (``kgx graph-summary``); **neosemantics** imports RDF into Neo4j. **S3PG** [S3PG]_
(KG2PG) transforms RDF into property graphs from SHACL shapes, without loss, and describes the
result in **PG-Schema** [PGSchema]_. rdfsolve keeps its own RDF to property graph conversion,
because it is built from the mined schema, keeps literals as typed properties, folds n-ary
nodes into edges and merges the nodes of one entity from SSSOM decisions, and gives the RDF back
without loss; it reuses the others' conventions, summaries and schema language. A comparison
with S3PG on WP4726 is in the article's experiments (``s3pg-comparison-20261002``).

What rdfsolve adds
------------------

- Which properties of a source are cross-references, and which source issues a namespace, read
  from the mined schema and the records of live SPARQL endpoints.
- The authority of the issuing source over its namespace: when ChEBI cross-references one ChEBI
  class and another source gives two, the other is overruled, recorded as a negative mapping,
  and reported; several targets that are not forms of one entity stay ambiguous.
- A property graph that holds one node per entity and gives back every RDF statement.

A comparison on WikiPathways WP4726 (SeMRA's prioritization and Node Normalization next to
rdfsolve's resolution) is in the article's experiments (``semra-comparison-20261002``).

References
----------

.. [SSSOM] Matentzoglu N, Balhoff JP, Bello SM, et al. A Simple Standard for Sharing Ontological
   Mappings (SSSOM). *Database* 2022: baac035. https://doi.org/10.1093/database/baac035

.. [SeMRA] Hoyt CT, Karis K, Gyori BM. Assembly and reasoning over semantic mappings at scale for
   biomedical data integration. *Bioinformatics* 2025; 41: btaf542.
   https://doi.org/10.1093/bioinformatics/btaf542

.. [Bioregistry] Hoyt CT, Balk M, Callahan TJ, et al. Unifying the identification of biomedical
   entities with the Bioregistry. *Scientific Data* 2022; 9: 714.
   https://doi.org/10.1038/s41597-022-01807-3

.. [S3PG] Rabbani K, Lissandrini M, Bonifati A, Hose K. Transforming RDF Graphs to Property Graphs
   using Standardized Schemas. *Proceedings of the ACM on Management of Data* 2024; 2(6).
   https://doi.org/10.1145/3698817

.. [PGSchema] Angles R, Bonifati A, Dumbrava S, Fletcher G, et al. PG-Schema: Schemas for Property
   Graphs. *Proceedings of the ACM on Management of Data* 2023; 1(2).
   https://doi.org/10.1145/3589778

.. [Babel] Morris E, Vaidya G, Owen P, et al. The "I" in FAIR: Translating from Interoperability
   in Principle to Interoperation in Practice. arXiv:2601.10008 (2026).
   https://github.com/TranslatorSRI/Babel
