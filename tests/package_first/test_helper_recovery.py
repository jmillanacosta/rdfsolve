"""Shared-helper regression oracles retained independently of any agent protocol."""
import json,re,threading
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import parse_qs,urlsplit
import pytest
from rdflib import Graph,Namespace,RDF,Literal
from rdfsolve.sparql_helper import SparqlHelper,PaginationTruncatedError
E=Namespace('http://fixture.example/')
PAGE=re.compile(r'\nOFFSET (\d+)\nLIMIT (\d+)')

@contextmanager
def endpoint(*,fail=False):
    g=Graph()
    for i in range(5):
        g.add((E[f'x{i}'], RDF.type, E.A));g.add((E[f'x{i}'],E.n,Literal(i)))
        g.add((E[f'x{i}'],E.same,E.value))
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_GET(self):
            q=parse_qs(urlsplit(self.path).query)['query'][0];calls.append(q)
            page=PAGE.search(q)
            if fail or not page or int(page[2])>2:
                code=500;body=b'Virtuoso query cost limit exceeded'
            else:
                code=200;body=g.query(q).serialize(format='json')
            self.send_response(code);self.send_header('Content-Type','application/sparql-results+json' if code==200 else 'text/plain')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    helper=SparqlHelper(f'http://127.0.0.1:{server.server_port}',timeout=2,select_page_size=4,select_page_cooldown=0,inter_request_delay=0.01)
    helper.enable_query_collection(include_results=True)
    try:yield helper,calls
    finally:helper.close();server.shutdown();server.server_close();thread.join(timeout=2)


def test_duplicate_bag_rows_survive_identical_pages():
    with endpoint() as (helper,calls):
        rows=helper.select_with_fallback(f'SELECT ?v WHERE {{ ?s <{E.same}> ?v }}')['results']['bindings']
        assert Counter(r['v']['value'] for r in rows)==Counter({str(E.value):5})


def test_original_limit_offset_order_and_values_survive():
    with endpoint() as (helper,calls):
        q=f'SELECT ?s ?n WHERE {{ ?s <{E.n}> ?n }} ORDER BY DESC(?n) LIMIT 3 OFFSET 1 VALUES ?n {{0 1 2 3 4}}'
        rows=helper.select_with_fallback(q)['results']['bindings']
        assert [r['n']['value'] for r in rows]==['3','2','1']
        assert all('VALUES ?n' in q for q in calls)


def test_page_budget_cannot_return_partial_success():
    with endpoint() as (helper,calls):
        with pytest.raises(PaginationTruncatedError) as exc:
            helper.select_with_fallback(f'SELECT DISTINCT ?s WHERE {{ ?s a <{E.A}> }}',max_pages=1)
        assert len(exc.value.partial_rows)==2
        assert helper.last_select_execution['status']=='failed'


