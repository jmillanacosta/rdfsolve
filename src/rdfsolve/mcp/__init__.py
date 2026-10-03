"""Answer questions about one RDF source with a language model and the rdfsolve tools.

ask_rdf starts the tool server of the source. The model writes SPARQL; the tools show
the schema, find resources, check and run queries, and run the final query on all data.
max_response_tokens limits each model reply, reasoning included; None removes the limit.
"""

from rdfsolve.mcp.workflow import ask_rdf

__all__ = ["ask_rdf"]
