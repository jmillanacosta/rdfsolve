"""Queries of local data with SERVICE are refused: both local engines would call the host
the pattern names, so a user query passed to a local client could reach internal servers."""

import pyoxigraph as ox
import pytest
from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.schema_models import MinedSchema

DATA = "<urn:a> <urn:p> <urn:b> ."
INTERNAL = "SERVICE <http://127.0.0.1:9/internal> { ?s ?p ?o }"


@pytest.mark.parametrize("backend", ["oxigraph", "rdflib"])
def test_service_is_refused_before_any_request(backend):
    data = Dataset().parse(data=DATA, format="nt")
    with Client(MinedSchema(), data, local_backend=backend) as client:
        assert len(client.select("SELECT ?s WHERE { ?s ?p ?o }").rows) == 1
        with pytest.raises(ValueError, match="SERVICE is not run"):
            client.select(f"SELECT ?s WHERE {{ {INTERNAL} }}")
        with pytest.raises(ValueError, match="SERVICE is not run"):
            client.construct(f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ {INTERNAL} }}")
        given = ox.Dataset([ox.Quad(ox.NamedNode("urn:a"), ox.NamedNode("urn:p"), ox.NamedNode("urn:b"))])
        with pytest.raises(ValueError, match="SERVICE is not run"):
            client.construct(f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ {INTERNAL} }}", data=given)


def test_service_can_be_allowed_explicitly():
    from rdfsolve.local_rdf import LocalRdf

    backend = LocalRdf(Dataset().parse(data=DATA, format="nt"), allow_service=True)
    assert backend.allow_service
