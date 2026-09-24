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

Omitting uri creates a fresh blank node. Reuse the same record to share
an anonymous resource. For externally supplied blank-node labels, pass a
shared blank_node_scope to create or from_table. Different scopes
keep equal labels distinct.

The from_table method uses the same record construction. Omit id_column for
anonymous rows. The languages and datatypes mappings provide defaults keyed
by model field name; explicit RDF terms in cells retain their own metadata.

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
