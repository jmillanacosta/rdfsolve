Usage
=====

RDFSolve re-exports every public function from :mod:`rdfsolve.api` at the
top-level package, so you can import directly::

    import rdfsolve

Mining a schema
---------------

Mine an RDF schema from a live SPARQL endpoint::

    jsonld = rdfsolve.mine_schema(
        "https://sparql.uniprot.org/sparql",
        dataset_name="uniprot",
    )

Loading an existing VoID file
-----------------------------

::

    parser = rdfsolve.load_parser_from_file("dataset_void.ttl")

Or from a previously mined JSON-LD::

    parser = rdfsolve.load_parser_from_jsonld("dataset_schema.jsonld")

Converting between formats
---------------------------

VoID to JSON-LD::

    jsonld = rdfsolve.to_jsonld_from_file("dataset_void.ttl")

VoID / JSON-LD to SHACL shapes::

    shacl_ttl = rdfsolve.to_shacl_from_file(
        "dataset_void.ttl",
        schema_name="my_dataset",
        closed=True,
        suffix="Shape",
    )

VoID to LinkML YAML::

    linkml_yaml = rdfsolve.to_linkml_from_file("dataset_void.ttl")

VoID to RDF-config::

    files = rdfsolve.to_rdfconfig_from_file(
        "dataset_void.ttl",
        endpoint_url="https://sparql.example.org",
    )

JSON-LD back to VoID graph::

    void_graph = rdfsolve.to_void_from_file("dataset_schema.jsonld")

See :doc:`api` for the full function reference and :doc:`cli` for the
command-line interface.
