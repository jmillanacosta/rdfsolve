Workflow steps
==============

The steps below are common to workflows that convert and reconcile RDF sources. Each is
part of rdfsolve, as an extension of an existing method where one fits. The reason for
each step is given with it, so that a later change keeps what the step is for.

Steps
-----

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * - Step
     - Why it is needed
     - Where in rdfsolve
   * - Open a source from the registry
     - The RDF downloads of a release are listed in the registry entry, so they are not
       given again by URL.
     - ``Client.open(..., data_file=<entry name>)``: the entry's RDF downloads are fetched
       once into ``$RDFSOLVE_DOWNLOADS/<name>`` and loaded together.
   * - Follow links to a depth
     - Records that stand for other records (a node that stands for a whole pathway, a
       version of a record) are followed level by level; the pairs of a link become
       identity pairs.
     - ``Results.related(kind, via=[...], depth=n)``: *via* is a path of links (``^name``
       is followed backwards), *kind* one class or one per link. ``Results.links(via=)``
       gives the pairs of one link as a table.
   * - Links of a step
     - The records a step reached are read back by the record it started from.
     - ``Results.links()``: a table of each start record and the records it reached; with
       *via* naming a field, each record and its values.
   * - What a scope holds
     - Every record type of a scope, with its count, shows what the rules must cover.
     - ``Results.related(via=, incoming=).types()``: ``related`` takes a link without a kind.
   * - Records that name an identifier
     - Records are found by any registered IRI form of an identifier: the record that is
       the identifier, and the records whose cross-reference names it.
     - ``Client.naming(identifiers, kind=, via=)``; ``links()`` gives each identifier its
       records.
   * - One question for a set of records
     - Several links and fields of many records are read in one query, as one table,
       instead of one query per step.
     - ``Client.prepare_network(patterns, outputs=, values={role: results}, resolve=True)``
       and ``Client.select(query).table()``. Plain class and field names are accepted.
   * - Kind conflicts
     - A node with two target-model kinds, neither under the other, after identity groups
       are formed, is decided before the graph is built.
     - ``conversion.kind_conflicts(client, statements, biolink, same=)`` and
       ``conversion.keep_kinds(client, statements, biolink, kept=)``. Both are CONSTRUCTs run
       with ``Client.construct(query, data=statements)`` and recorded as named steps in the
       client's session. Each conflict is logged as a warning.
   * - Conversions
     - A process node implies direct edges (each input to each output), with its catalysts
       as qualifiers.
     - ``conversion.derive_conversions(client, statements, biolink)``: a recorded CONSTRUCT
       that states the target model's associations; ``derive_associations`` makes each
       association an edge.
   * - Compounds that meet
     - A term of one source meets a term of another when it is the same term, another form
       of it, or a more general term.
     - ``Ontologies.meets(named, others)``: exact, form, narrower, or None; answers are kept
       across calls.
   * - Views of a network
     - Direct edges are shown without process and association nodes, with qualifiers next
       to the predicate; nodes without edges are left out unless an edge attribute names
       them.
     - ``PropertyGraph.to_networkx(without=, qualifiers=, keep=)``.
   * - Draw a network
     - Plotting depends on the analysis and its figure.
     - Left to the workflow; not part of rdfsolve.
   * - Edge evidence
     - What another source says about an edge is kept on the edge, not as a new edge type.
     - Edge attributes of the network, written by the step that checks them.
