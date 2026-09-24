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
an empty collection writes rdf:nil.

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
keep canonical JSON for the full model.


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
