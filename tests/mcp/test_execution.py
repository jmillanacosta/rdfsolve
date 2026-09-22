import re
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from conftest import prepare
from rdflib import RDF, Graph, Literal, Namespace
from rdfsolve.sparql_helper import SparqlHelper

E = Namespace("http://fixture.example/")
PAGE = re.compile("\\nOFFSET (\\d+)\\nLIMIT (\\d+)")


@contextmanager
def endpoint(*, fail=False, capped=False):
    g = Graph()
    for i in range(5):
        g.add((E[f"x{i}"], RDF.type, E.A))
        g.add((E[f"x{i}"], E.n, Literal(i)))
        g.add((E[f"x{i}"], E.same, E.value))
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            q = parse_qs(urlsplit(self.path).query)["query"][0]
            calls.append(q)
            page = PAGE.search(q)
            if capped:
                code = 200
                q = q[: page.start(2)] + "2" + q[page.end(2) :] if page else q
                if "LIMIT" not in q:
                    q += " LIMIT 2"
                body = g.query(q).serialize(format="json")
            elif fail or not page or int(page[2]) > 2:
                code = 500
                body = b"Virtuoso query cost limit exceeded"
            else:
                code = 200
                body = g.query(q).serialize(format="json")
            self.send_response(code)
            self.send_header(
                "Content-Type", "application/sparql-results+json" if code == 200 else "text/plain"
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    helper = SparqlHelper(
        f"http://127.0.0.1:{server.server_port}",
        timeout=2,
        select_page_size=4,
        select_page_cooldown=0,
        inter_request_delay=0.01,
    )
    helper.enable_query_collection(include_results=True)
    try:
        yield (helper, calls)
    finally:
        helper.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_client_execution_recovers_and_replays(session):
    capped = False
    from conftest import declare
    from rdfsolve.client.api import Client
    from rdfsolve.mcp.session import Session
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern

    schema = MinedSchema(
        about={"dataset_name": "http"},
        patterns=[
            SchemaPattern(subject_class=str(E.A), property_uri=str(E.n), object_class="Literal")
        ],
    )
    with endpoint(capped=capped) as (helper, calls):
        s = Session(Client(schema, helper, graph_uris=[]))
        declare(s, "List A resources", concept="A")
        prepared = prepare(s, {"g1": {"pattern": f"?a a <{E.A}> .", "project": ["a"]}})
        assert not calls
        probe = s.probe(prepared["query_ref"], limit=1)
        assert probe["state"] == "probed" and (not s.executions)
        done = s.finish(prepared["query_ref"])
        assert done["rows"] == 5
        assert done["execution"]["completeness_basis"] == "empty_page"
        assert {r["a"]["value"] for r in s.export(done["result_ref"])["bindings"]} == {
            str(E[f"x{i}"]) for i in range(5)
        }
        count = len(calls)
        assert s.diagnostics()["endpoint_requests"] == count
        assert s.finish(prepared["query_ref"]) == done and len(calls) == count
