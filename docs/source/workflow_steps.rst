Workflow steps
==============

Each step below was first written by hand in a notebook (the WikiPathways to Biolink
notebook, ``notebooks/conversion/01_wikipathways_to_biolink.ipynb``, 2026-10-03), and is now
part of rdfsolve. An existing method is extended where one fits. The reason each
step exists is kept here, so that a later rewrite keeps what the step is for.

Steps
-----

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * - Step
     - Why it was needed
     - Where in rdfsolve
   * - Open a source from the registry
     - The notebook downloaded each RDF dump of a release by URL and passed the files to
       ``Client.open``; the registry already lists the downloads.
     - ``Client.open(..., data_file=<entry name>)``: the entry's RDF downloads are fetched
       once into ``$RDFSOLVE_DOWNLOADS/<name>`` and loaded together.
   * - Follow links to a depth
     - A pathway draws other pathways as nodes; they are taken whole, level by level
       (``Has version`` gives the pathway a node stands for). The loop also kept each
       node and the record it stands for, which become identity pairs.
     - ``Results.related(kind, via=[...], depth=n)``: *via* a path of links (``^name``
       backwards); ``Results.links(via=)`` gives the pairs of one link.
   * - Links of a step
     - Each ``related`` step had to be grouped back by source record (a protein's
       activities, a reaction's sides); the evidence held it, unread.
     - ``Results.links()``: source to targets, from the evidence.
   * - What a scope holds
     - Every record type of the scope with its count, to see what the rules must cover.
     - Existing: ``Results.related(via=, incoming=).types()``; ``related`` now takes a link
       without a kind.
   * - Records that name an identifier
     - The proteins of a gene: records whose cross-reference names the gene in any
       registered IRI form (``rdfs:seeAlso`` in UniProt), also through the gene a
       source links to the node (BridgeDb).
     - ``Client.naming(identifiers, kind=, via=)``, returning each identifier's records.
   * - Kind conflicts
     - One node with two Biolink kinds of which neither is an ancestor of the other
       (protein and RNA), after the identity groups; decided by evidence, before the
       graph is built.
     - ``conversion.kind_conflicts(client, statements, biolink, same=)`` and
       ``conversion.keep_kinds(client, statements, biolink, kept=)``. Both are CONSTRUCTs run
       with ``Client.construct(query, data=statements)``, recorded as named steps in the
       client's session; each conflict is logged as a warning.
   * - Compounds that meet
     - A compound named by one source meets another source's when it is the same term,
       another form (acid or base, tautomer) or a more general term.
     - ``Ontologies.meets(named, others)``: exact, form, narrower, or None; answers kept
       across calls.
   * - Views of a network
     - The direct edges without complex, reaction and association nodes; qualifiers
       shown with the predicate; nodes without edges left out unless an edge
       attribute names them.
     - ``PropertyGraph.to_networkx(without=, qualifiers=, keep=)``.
   * - Draw a network
     - Breadth-first layers from chosen nodes, one colour per category and edge type
       for the whole run, dotted edges of unknown type, edge attributes drawn to the
       edge's middle, an image a browser can show.
     - Stays in the notebook: plotting is simple enough not to wrap.
   * - Edge evidence
     - What another source says about an edge (a reaction or transport that confirms
       it) is kept on the edge, not as a new edge type.
     - Edge attributes of the network, written by the step that checks them (no new API).
