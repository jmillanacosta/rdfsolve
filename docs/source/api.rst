API
===

.. automodule:: rdfsolve.api
   :members:
   :undoc-members:
   :show-inheritance:


Author RDF records
------------------

Generated models accept RDFLib literals and RdfTerm values. They retain
language tags, datatypes and lexical forms when written to RDF. Plain values
remain convenient when the field has one unambiguous datatype.

.. code-block:: python

   from rdflib import Literal, XSD

   part = client.create("Part", label=Literal("Teil", lang="de"))
   item = client.create(
       "Item",
       uri="https://example.org/item/1",
       label=Literal("One", lang="en"),
       date=Literal("2026", datatype=XSD.gYear, normalize=False),
       part=part,
   )
   item.to_graph().serialize("item.ttl", format="turtle")

Use full class and predicate IRIs when local names overlap. Ambiguous class
names raise an error listing the matching IRIs. Generated hash suffixes are
implementation names; resolve them with client.field_name(model, predicate).

.. code-block:: python

   from rdflib.namespace import DCTERMS

   item = client.create(
       "https://example.org/Item",
       **{DCTERMS.date: Literal("2026", datatype=XSD.gYear, normalize=False)},
   )

RDFLib can normalize lexical forms when Literal is constructed. Use
normalize=False or RdfTerm when the original spelling matters; rdfsolve
preserves the supplied term but cannot recover text already normalized.

Client.create and Client.from_table resolve plain values against the field
definition. A date field allowing gYear, gYearMonth and date accepts
"2026", "2026-09" and "2026-09-24" with their respective datatypes and
unchanged lexical forms. Calendar dates and timezone offsets are checked.
Invalid or ambiguous values raise an error naming the field. Custom datatypes
whose lexical forms cannot be checked require an explicit Literal or RdfTerm.

Pass language="en" to either method as a default for language-tagged fields
and collection members. It does not add language tags to IRIs or typed
numbers and dates. Explicit terms and per-column defaults take precedence.
Where a field permits both an IRI and text, pass URIRef or Literal explicitly.

Pass extra_types=["https://example.org/Other"] to create to add RDF type
assertions on the same node. The primary model class is retained. Extra
types do not combine generated models or validate the additional classes.

Use client.save("records.ttl", record, table_results) to save individual
records and table results together.

Omitting uri creates a fresh blank node. Reuse the same record to share
an anonymous resource. For externally supplied blank-node labels, pass a
shared blank_node_scope to create or from_table. Different scopes
keep equal labels distinct.

The from_table method uses the same record construction. Omit id_column for
anonymous rows. Missing identifiers also create blank nodes; missing cells
leave optional fields unset. The languages and datatypes mappings provide
defaults keyed by model field name; explicit RDF terms in cells retain their own metadata.

.. code-block:: python

   records = client.from_table(
       "Item", table, id_column="id",
       label="title", date="year",
       languages={"label": "en"},
       datatypes={"date": str(XSD.gYear)},
   )
   client.save("items.ttl", records)

Generated classes also accept explicit RDF terms directly. Fields can be
addressed by their generated Python names or full predicate IRI aliases.
These records retain observed field definitions; optional fields do not
establish required values or closed-world constraints.


Ordered RDF collections
-----------------------

Use RDFList for an ordered collection. Ordinary Python lists still write
multiple predicate values. Collections preserve order and duplicates, and
an empty collection writes rdf:nil. Plain members use the owning field's
collection profile: a string IRI becomes a reference in a resource-only
collection; numbers use the permitted literal datatype. If both a reference
and text are valid, use URIRef or Literal to state which you mean. Explicit
terms keep their metadata. A reference does not establish its target's class.

.. code-block:: python

   from rdfsolve.api import RDFList

   record = client.create(
       "Record",
       members=RDFList(items=[first_agent, second_agent, first_agent]),
   )
   record.to_graph().serialize("record.ttl", format="turtle")

Local snapshot mining inspects collections within each selected graph.
Canonical schema JSON and generated models retain member types, literal
datatypes and observed lengths. A profile describes one owner class,
predicate and graph; it does not create a pattern for each list position.
Malformed lists are counted separately. Lengths are observations, not
required or maximum counts for future records.

To inspect a local snapshot against an existing schema, call
schema.discover_collections(graph). Pass graph_uris=[] for the default
graph, or supply a Dataset and named graph IRIs. Remote mining does not
automatically fetch list contents. VoID exports omit collection profiles;
keep canonical JSON for the full model. Access profiles as schema.collections,
or document["schema"]["collections"] in a canonical document.

Client.links, field_name, type_name and link_name accept generated model
classes, class names or full IRIs. Client.diagram() draws all models;
pass class names or IRIs to select a smaller view. Use
client.diagram(fenced=False) for raw Mermaid suitable for embedding.
This also applies to paths tables; partial-view notices become Mermaid comments.


Use an approved model as a contract
-----------------------------------

Keep the reviewed canonical schema JSON under version control. Generate
contract models from that snapshot with
schema.to_pydantic_classes(contract=True), or export Python with
schema.to_pydantic(contract=True). Client.open also accepts contract=True.

.. code-block:: python

   from rdflib import Graph, Literal
   from rdfsolve.api import Client
   from rdfsolve import MinedSchema

   approved = MinedSchema.from_json("approved-schema.json")
   with Client.open(approved, Graph(), contract=True) as client:
       item = client.create(
           "Item", label=Literal("One", lang="en")
       )
       item.to_graph().serialize("item.ttl", format="turtle")

Contract models reject extra fields and check RDF kinds, datatypes and
known linked-record classes at construction and serialization. Partial
field exports cannot establish contract conformance and are rejected.
A bare IRI remains a reference; it does not prove the target's class.

Explicit active constraints in the snapshot's shapes are checked with
pySHACL. Install the optional dependency with:

.. code-block:: console

   pip install 'rdfsolve[validation]'

Required values and cardinalities come from these declared constraints.
Mining does not activate observed shapes or invent required fields.
Deactivated shapes stay inactive. Validation uses the supplied graph
without imports, inference or endpoint requests. Include referenced records
when a declared class constraint needs their type statements.

Keep the approved snapshot separate from later mining output and review
changes before generating a new contract. Generated model metadata retains
the snapshot's schema version and source provenance. Canonical schema JSON
is the shared definition for models and declared constraints.

Observed SHACL exports
----------------------

schema.to_shacl() exports mined value-type templates as deactivated shapes
and reports their number. They impose no validation requirements. To enforce
the observed one-hop value types explicitly, use
schema.to_shacl(activate_observed=True). This does not infer required fields
or per-record cardinalities. Retained source constraints keep their activation
state. Review the shapes before treating a conformance result as validation
of an intended contract.

Provider SHACL evidence
-----------------------

``MinedSchema.from_shacl(text)`` retains the supplied RDF in canonical JSON.
Use ``schema.get_metadata().to_rdf_graph()`` for the original constraints,
including predicates outside the supported shape model. ``schema.shapes``
and ``schema.to_shacl()`` expose the supported projection. Validate against
the retained provider graph when full source constraints are required.

Class shapes without an explicit ``sh:targetClass`` use their implicit
class target when the supplied graph identifies them as RDFS classes.
Their composed paths remain unchecked until probed against instance data.

Selected path observations
--------------------------

Call ``schema.discover_paths(probe_limit=0)`` to retain candidate routes.
Select entries from ``schema.navigation.paths``, then call
``schema.probe_paths(selected, helper=helper)`` to measure only those routes.
The helper must address the intended data source. Data and typing graph scopes
come from ``schema.about`` and are retained on each observation. Repeated
selections update retained routes; unselected observations remain unchanged.
``probe_selection`` and ``probe_limit`` describe the latest selection.
Canonical JSON preserves observations. A matched route establishes joined
support in the queried scope; it does not establish SHACL or OWL validity.

A path probe reports no_sources when its starting class has no instances in the
queried data scope. It reports no_match when starting instances exist but none
completes the route with the requested typing constraints. Missing context can
cause either outcome; these statuses do not identify its cause. Timeout and error
carry no support counts. Old saved no_match results retain their original status.

Published query recipes
-----------------------

QueryCollection.add accepts source= with the IRI of a published query or recipe.
The exported SHACL executable retains it as prov:wasDerivedFrom, independently
of the execution endpoint and optional schema hash. Exporting and reloading the
collection preserves this attribution and the executable query. A supplied
conversion remains explicit SPARQL; attribution does not certify its correctness
or assert equivalence between the input and output identifiers.

Identifier resolution policy
----------------------------

resolve_identifiers(terms, source, target_iris=observed_iris) reports exact IRI
matches. Set mode="namespace" to also try the source's recorded alternative
namespaces and prefix synonyms. Supply RdfTerm values from selected identifier
fields and target IRIs observed in your chosen graph and type scope.

The report preserves original terms, namespace candidates, matching targets,
source rules and a hash of the supplied target set. Multiple target alternatives
are ambiguous; no winner is selected. Language-tagged text and non-string typed
literals are not expanded. An exact target IRI takes precedence over expansion.
No network requests, RDF edits, registry writes or equivalence assertions occur.
A match establishes membership in the supplied set, not identifier validity,
a further graph connection or ontology consistency. Preserve the target snapshot
and its scope alongside the report; the hash alone cannot reconstruct it.

Use report.to_sparql_values() inside a SELECT query to follow accepted matches.
It binds ?resolution (the zero-based input position), ?input (the original RDF
term), and ?target (the accepted IRI). Only exact or uniquely resolved inputs
are included; inspect report.results for ambiguous and unresolved inputs.
An empty accepted set yields FILTER(1 = 0). Keep the input rows and the
report so returned resolution positions identify the original records.
This prepares bindings only; callers retain query scope and execution budgets.


Select source-backed fields
---------------------------

Use schema.select(paths=[route], fields=[(class_iri, predicate_iri)]) to keep
fields needed for an application. Routes must already occur in schema.navigation.
The result retains an independent copy of the complete source schema. Save it
with model_dump_json() and reload with SchemaSelection.model_validate_json()
from rdfsolve.schema_models.selection.

selection.patterns contains every range observed for the selected fields and
path steps. selection.collections retains their list profiles. Counts and partial
population states keep their original meaning and denominators. selection.paths
keeps the source's joined-support evidence. No queries run during selection.

Provider shapes, original RDF, structural evidence and other source fields remain
under selection.source as context. They are not projected or adopted constraints.
This view does not extract instance data, prove a path works, or close a SHACL
shape. Selection of untyped structural paths is not yet supported.


Retrieve across selected graphs
-------------------------------

Client queries can join across the union of their selected data graphs. A model
belongs to its schema's selected scope, which may contain one or several graphs.
Its JSON Schema retains that scope and the graph evidence for each mined field.
Models from another declared data or typing scope are rejected before retrieval.
An explicit client graph override changes retrieval scope; it does not rewrite
the model's original evidence. Mine that scope to obtain its observed model.

For a retained selection, query = selection.path_query(selection.paths[0])
builds a SELECT with n0 as the source and n1, n2, etc. as successive nodes.
Run it through client.select(query) or a SPARQL helper in the same source.
Intermediate identities, alternative values, languages and datatypes remain
available in the returned bindings. By default, sources lacking the complete
path remain as rows with unbound intermediate/target values. Set
include_unmatched=False to retrieve only complete matches.

Selected-path queries reuse the mining scope: data edges join across selected
data graphs; companion graphs supply subject and object types. Generated client
fragments, samples and link retrieval also use companion types.
Handwritten SPARQL retains its explicit type clauses.
A path needs to have been discovered or supplied before it can be selected.
Mining includes fields on data subjects typed in companion context. Companion-only
subjects without outgoing data edges are excluded from class discovery and
populations. A failed match is not an ontology inconsistency.

Cross-graph results retain their query and selected scope. A missing single
graph value is not evidence that the path came from the default graph.
Selected extraction retains each statement's graph. Full SHACL shape-fragment
semantics remain separate work. Query generation adopts no new SHACL constraints.


Extract and assess selected RDF
--------------------------------

client.extract(selection, root_class=class_iri, roots=None) returns the
selected connected records. Omit roots for all matching data subjects, or
supply exact resource IRIs. Paths must start at the root class; extra selected
fields must belong to a class reached by those paths. Unmatched roots remain.
Every selected field retains all its actual values. Intermediate identities,
type statements and selected RDF list cells remain available.

result.quads records the graph of each returned statement. result.roots
contains the starting records; result.selection retains the source schema.
result.save("selected.trig") preserves named graphs. Blank nodes remain
within one response and receive a response-specific scope when restored.
Extraction uses one SELECT for local or remote sources. Exceeding max_rows
raises an error; a successful response cannot certify an endpoint's hidden limits.

result.assess(shapes_graph, ontology=ontology_graph, inference="none")
checks the extracted RDF union with pySHACL from the validation extra.
The supplied shapes and ontology remain unchanged. The report keeps declarations,
individual violations, inactive-targeted-shape counts, retained path support,
and comparisons between selected observations and simple declared constraints.
Requirements outside the selected fields produce scope warnings. Closed shapes
apply to the extracted view; they cannot certify absent properties in the source.

Set inference="rdfs", "owlrl" or "both" explicitly for external
inference. OWL-RL modes also retain contradictions reported by owlrl.
No reported contradiction is not a proof of full OWL consistency.
The report's source_conforms remains unknown. With no active targeted shapes,
the result is not_checked. Validator failures remain error.
Opening or closing a supplied shape is an explicit change to that shape's
sh:closed value; extraction and probing do not make that decision.

For example, selecting cell-line references does not imply that every cell line
has one. A minimum count of one can expose missing references; a minimum count
of zero permits them. Keep that application decision separate from mined counts.
