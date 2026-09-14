"""Real dispatch/Client tests plus optional REAL SDK tests. No live model required."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import re
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs,urlsplit

import pytest
from rdflib import Graph,RDF

from rdfsolve.client_api import Client
from rdfsolve.mcp.agent import Bridge,NoProgressError
from rdfsolve.mcp.server import CONTRACTS,dispatch
from rdfsolve.openai import resolve_schema,launch_config,generation_settings,NotebookAnswer
from rdfsolve.sparql_helper import SparqlHelper
from .test_workspace import fixture,insert,typ,E


class LocalTransport:
    """Only replace the wire. Dispatch, clients and query execution are real."""
    def __init__(self,workspace):self.workspace=workspace
    async def list_tools(self):
        return SimpleNamespace(tools=[SimpleNamespace(name=n,description=d,input_schema=m.model_json_schema()) for n,(m,_,d) in CONTRACTS.items()])
    async def call_tool(self,name,args):
        return SimpleNamespace(structured_content=dispatch(self.workspace,name,args),content=[])


@pytest.mark.asyncio
async def test_bridge_does_not_finish_on_prepare_or_probe_and_stops_after_explicit_final():
    w=fixture();bridge=Bridge(LocalTransport(w))
    prepared=await bridge.call('rdf_prepare',{'sparql':'SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' }'})
    assert bridge.final is None and not w.client.queries
    probe=await bridge.call('rdf_probe',{'query_ref':prepared['query_ref'],'limit':1})
    assert bridge.final is None and probe['state']=='probed'
    done=await bridge.call('rdf_finish',{'query_ref':prepared['query_ref']})
    assert bridge.final==done and done['rows']==3
    n=len(w.client.queries)
    assert (await bridge.call('rdf_schema',{'concepts':['other']}))['state']=='stopped'
    assert len(w.client.queries)==n


@pytest.mark.asyncio
async def test_repeated_bad_call_reports_error_but_new_arguments_can_repair():
    w=fixture();b=Bridge(LocalTransport(w))
    args={'sparql':'SELECT ?a WHERE {?a a <urn:invented>}' }
    for i in range(3):assert 'error' in await b.call('rdf_prepare',args)
    with pytest.raises(NoProgressError):await b.call('rdf_prepare',args)
    repaired=await b.call('rdf_prepare',{'sparql':'SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' }'})
    assert repaired['state']=='prepared' and not w.client.queries


@pytest.mark.asyncio
async def test_lost_final_response_replays_one_execution():
    w=fixture();p=w.prepare('SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' }')
    class Loss(LocalTransport):
        lost=False
        async def call_tool(self,name,args):
            result=await super().call_tool(name,args)
            if not self.lost:
                self.lost=True
                raise TimeoutError('simulated lost response after actual execution')
            return result
    b=Bridge(Loss(w))
    with pytest.raises(TimeoutError):await b.call('rdf_finish',{'query_ref':p['query_ref']})
    done=await b.call('rdf_finish',{'query_ref':p['query_ref']})
    assert done['rows']==3 and len(w.client.queries)==1


@contextmanager
def http_endpoint():
    local=fixture();requests=[]
    page=re.compile(r'\nOFFSET (\d+)\nLIMIT (\d+)')
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            query=parse_qs(urlsplit(self.path).query)['query'][0]
            requests.append(query)
            m=page.search(query)
            if not m or int(m[2])>2:
                code,body=500,b'Virtuoso query cost limit exceeded'
            else:
                code,body=200,local.client.source.query(query).serialize(format='json')
            self.send_response(code)
            self.send_header('Content-Type','application/sparql-results+json' if code==200 else 'text/plain')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    http=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=Thread(target=http.serve_forever,daemon=True);thread.start()
    helper=SparqlHelper(f'http://127.0.0.1:{http.server_port}',timeout=2,inter_request_delay=0.01,
                        select_page_size=4,select_page_cooldown=0)
    try:yield Client(local.client._schema,helper,graph_uris=[]).workspace(),requests
    finally:helper.close();http.shutdown();http.server_close();thread.join(timeout=3)


def test_real_http_final_uses_shared_pagination_fallback_not_new_transport():
    with http_endpoint() as (w,requests):
        p=w.prepare('SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' } ORDER BY ?a')
        done=w.finish(p['query_ref'])
        assert done['state']=='complete' and done['rows']==3
        assert done['execution']['strategy']=='adaptive_offset'
        assert len(requests)>2
        assert {r['a']['value'] for r in w.resource(done['result_ref'])['bindings']}=={str(E.humanAOP),str(E.mouseAOP),str(E.noChemicalAOP)}
        assert requests[0]==w.prepared[p['query_ref']].sparql
        before=len(requests)
        assert w.finish(p['query_ref'])==done and len(requests)==before


def test_notebook_paths_stay_on_checkout_and_exports_win(monkeypatch,tmp_path):
    root=tmp_path/'repo';schema=root/'notebooks/data/aopwikirdf.schema.json'
    schema.parent.mkdir(parents=True);schema.write_text('{}')
    monkeypatch.setenv('RDFSOLVE_ROOT',str(root));monkeypatch.delenv('RDFSOLVE_SCHEMA',raising=False)
    monkeypatch.chdir(tmp_path)
    assert resolve_schema()==schema
    config=launch_config()
    assert '--max-paths' in config['args'] and str(schema) in config['args']
    assert 'rdfsolve.mcp' in config['args']
    monkeypatch.setenv('RDFSOLVE_MAX_TOKENS','65536')
    assert generation_settings()['max_tokens']==65536
    assert generation_settings({'max_tokens':1024})['max_tokens']==1024
    monkeypatch.setenv('RDFSOLVE_SCHEMA',str(tmp_path/'missing'))
    with pytest.raises(FileNotFoundError):resolve_schema()


def test_notebook_saves_failure_and_raw_rdf_terms(tmp_path):
    a=NotebookAnswer('failed','failed',None,[],{'requests':4},[], 'schema',error={'code':'test'})
    with pytest.raises(ValueError):a.table()
    a.save(tmp_path/'answer.json')
    assert json.loads((tmp_path/'answer.json').read_text())['error']=={'code':'test'}


def test_public_contracts_have_no_intent_dsl_or_operation_ids():
    text=json.dumps({k:m.model_json_schema() for k,(m,_,_) in CONTRACTS.items()})
    assert 'operation_id' not in text and 'WhereClause' not in text and 'Intent' not in text
    assert len(text)<14000


@pytest.mark.skipif(importlib.util.find_spec('pydantic_ai') is None,reason='Pinned PydanticAI SDK not installed in this environment')
@pytest.mark.asyncio
async def test_real_pydanticai_runs_one_agent_without_review_or_post_execution_request():
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.messages import ModelResponse,ToolCallPart
    from rdfsolve.mcp.agent import ask
    w=fixture();calls=[];n=0
    def step(messages,info):
        nonlocal n
        n+=1
        assert all('operation_id' not in json.dumps(x.parameters_json_schema) for x in info.function_tools)
        if n==1:name,args='rdf_find',{'text':'Human','kind':str(E.Taxon)}
        elif n==2:name,args='rdf_paths',{'source':str(E.AOP),'target':calls[-1]['result']['records'][0]['ref'],'max_hops':1}
        elif n==3:name,args='rdf_prepare',{'sparql':'SELECT ?a WHERE { '+insert(calls[-1]['result']['paths'][0]['ref'],'a','tax')+' }'}
        elif n==4:name,args='rdf_probe',{'query_ref':calls[-1]['result']['query_ref'],'limit':1}
        elif n==5:name,args='rdf_finish',{'query_ref':calls[-1]['result']['query_ref']}
        else:raise AssertionError('There must be no critic or post-execution model request')
        return ModelResponse(parts=[ToolCallPart(name,args,tool_call_id=str(n))])
    run=await ask(LocalTransport(w),'Find pathways applicable to humans',model=FunctionModel(step),calls=calls)
    assert run.state=='complete' and n==5
    rows=w.resource(run.terminal['result_ref'])['bindings']
    assert {r['a']['value'] for r in rows}=={str(E.humanAOP),str(E.noChemicalAOP)}


@pytest.mark.skipif(importlib.util.find_spec('mcp') is None,reason='Pinned MCP SDK not installed in this environment')
@pytest.mark.asyncio
async def test_real_mcp_stdio_registered_tools_and_result_resources(tmp_path):
    import sys
    from mcp import Client as MCPClient,StdioServerParameters
    from rdfsolve.mcp.agent import tool_json,read_resource
    w=fixture();schema=tmp_path/'schema.json';data=tmp_path/'data.ttl'
    schema.write_text(json.dumps(w.client._schema.to_dict()))
    w.client.source.serialize(destination=data,format='turtle')
    params=StdioServerParameters(command=sys.executable,args=['-m','rdfsolve.mcp','--schema',str(schema),'--data-file',str(data)],
        env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[2]/'src')})
    async with MCPClient(params,read_timeout_seconds=30) as server:
        assert {t.name for t in (await server.list_tools()).tools}==set(CONTRACTS)
        query='SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' }'
        p=tool_json(await server.call_tool('rdf_prepare',{'sparql':query}))
        assert p['state']=='prepared'
        done=tool_json(await server.call_tool('rdf_finish',{'query_ref':p['query_ref']}))
        assert done['rows']==3
        full=await read_resource(server,done['result_ref'])
        assert len(full['bindings'])==3 and full['query']==(await read_resource(server,p['query_ref']))['sparql']
        assert (await read_resource(server,'diagnostics'))['package_calls']['Client.select']==1


@pytest.mark.skipif(importlib.util.find_spec('mcp') is None or importlib.util.find_spec('pydantic_ai') is None,
                    reason='Pinned MCP/PydanticAI SDKs not installed')
@pytest.mark.asyncio
async def test_real_notebook_helper_with_real_stdio_and_function_model(tmp_path,monkeypatch):
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.messages import ModelResponse,ToolCallPart
    from rdfsolve.openai import ask_aopwiki
    w=fixture();schema=tmp_path/'schema.json';data=tmp_path/'data.ttl'
    schema.write_text(json.dumps(w.client._schema.to_dict()))
    w.client.source.serialize(destination=data,format='turtle')
    monkeypatch.setenv('RDFSOLVE_QWEN_OUTPUT',str(tmp_path/'output'))
    n=0
    def step(messages,info):
        nonlocal n
        n+=1
        if n==1:
            return ModelResponse(parts=[ToolCallPart('rdf_prepare',{'sparql':f'SELECT ?a WHERE {{ ?a a <{E.AOP}> }}'},tool_call_id='1')])
        if n==2:
            results=[p.content for m in messages for p in m.parts if getattr(p,'tool_name','')=='rdf_prepare' and hasattr(p,'content')]
            content=results[-1];content=json.loads(content) if isinstance(content,str) else content
            return ModelResponse(parts=[ToolCallPart('rdf_finish',{'query_ref':content['query_ref']},tool_call_id='2')])
        raise AssertionError('No extra review or completion generation')
    answer=await ask_aopwiki('List pathways',schema=schema,data_file=data,model=FunctionModel(step))
    assert answer.state=='complete',answer.error
    assert n==2 and len(answer.bindings)==3
    assert answer.package['package_calls']['Client.select']==1
    assert len(answer.table())==3 and len(list((tmp_path/'output').glob('*.answer.json')))==1
