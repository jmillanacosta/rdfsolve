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
