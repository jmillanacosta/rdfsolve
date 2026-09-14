"""MCP transport and typed contracts only. RDF work lives in Client.workspace()."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_core import to_jsonable_python

from rdfsolve.hydration import HydrationLimitError
from rdfsolve.sparql_helper import EndpointError
from rdfsolve.workspace import Workspace


class Args(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


class SchemaArgs(Args):
    concepts: list[str] = Field(default_factory=list, max_length=8, description='Separate concepts; each searched independently over classes AND fields.')
    owner: str | None = Field(default=None, description='Optional type reference or exact class label, to inspect its fields.')
    offset: int = Field(default=0, ge=0)


class FindArgs(Args):
    text: str = Field(min_length=1, max_length=200)
    kind: str | None = Field(default=None, description='Type reference or exact class label. Prefer a known type to broad search.')
    fields: list[str] | None = Field(default=None, max_length=12)
    descriptions: bool = Field(default=False, description='False searches entity names/IDs with Client.find; True uses Client.search over descriptive fields.')
    offset: int = Field(default=0, ge=0)


class PathsArgs(Args):
    source: str
    target: str | None = None
    target_text: str | None = Field(default=None, description='Alternative to target: core value-grounded path search. Contacts the source.')
    max_hops: int = Field(default=2, ge=1, le=6)
    offset: int = Field(default=0, ge=0)


class InspectArgs(Args):
    ref: str
    fields: list[str] | None = Field(default=None, max_length=12, description='Hydrate these generated fields on a retained record.')
    text: str = Field(default='', description='For a field reference, optionally filter its sampled actual values.')
    offset: int = Field(default=0, ge=0)


class FollowArgs(Args):
    record: str
    field: str
    target: str
    fields: list[str] | None = Field(default=None, max_length=12)


class PrepareArgs(Args):
    sparql: str = Field(min_length=1, description='SELECT, optionally inserting grounded {{ref ?s ?o}} fragments or {{term_ref}} values. Scope is added by Client. Never executed automatically.')


class ProbeArgs(Args):
    query_ref: str
    limit: int = Field(default=5, ge=1, le=20)


class FinishArgs(Args):
    query_ref: str


CONTRACTS = {
    'rdf_schema': (SchemaArgs, 'schema', 'Discover actual classes and full field paths in the saved schema. Multiple concepts are separate searches. Use owner to browse opaque fields. No endpoint query.'),
    'rdf_find': (FindArgs, 'find', 'Ground entity names/IDs using Client.find or Client.search. Retains generated typed records and exact RDF terms. A candidate search does not impose a final substring filter.'),
    'rdf_paths': (PathsArgs, 'paths', 'Use Client.paths_between / connections, plus retained compound SHACL fields. Returns grounded reusable fragments, direction, anchors and hop complexity. Paths are alternatives, not automatically correct meanings.'),
    'rdf_inspect': (InspectArgs, 'inspect', 'Type: browse fields. Field: probe actual values using its full path. Record: hydrate selected fields using Client.get_many. Query/result: inspect retained artifact. Truncated text is an excerpt, never the RDF value.'),
    'rdf_follow': (FollowArgs, 'follow', 'Follow a generated field from a retained typed record using Client.follow; retain typed targets and package evidence.'),
    'rdf_prepare': (PrepareArgs, 'prepare', 'Compose grounded fragments with normal SPARQL OPTIONAL/UNION/FILTER. Package expands paths, allocates internal variables, parses, and applies Client graph scope. Returns a query artifact, NOT an answer.'),
    'rdf_probe': (ProbeArgs, 'probe', 'Execute a bounded sample of a prepared SELECT through the same client/helper. Retains full sampled RDF values. Never finalizes the investigation.'),
    'rdf_finish': (FinishArgs, 'finish', 'Explicitly execute the final prepared artifact once through Client.select and its helper fallbacks. Finish only when all requested outputs and restrictions are present. Zero rows are valid. Full rows remain in the result resource.'),
}


def dispatch(workspace: Workspace, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if name not in CONTRACTS:
        return {'error': {'code':'unknown_tool', 'message':name}}
    model, method, _ = CONTRACTS[name]
    try:
        args = model.model_validate(arguments).model_dump(exclude_none=True)
        with workspace.lock:
            return getattr(workspace,method)(**args)
    except ValidationError as exc:
        return {'error': {'code':'invalid_arguments','details':exc.errors(include_input=False,include_url=False)}}
    except HydrationLimitError as exc:
        return {'error': {'code':'search_budget','message':str(exc),'not_absence':True}}
    except EndpointError as exc:
        return {'state':'failed','error':{'code':'endpoint_error','message':str(exc)},
                'execution':workspace.client.last_query_execution}
    except (ValueError,KeyError,TypeError,LookupError) as exc:
        return {'error': {'code':'invalid_request','message':str(exc)}}
    except Exception as exc:
        logging.getLogger(__name__).exception('Tool %s failed',name)
        return {'error':{'code':'package_error','message':f'{type(exc).__name__}: {exc}'}}


def create_server(client, source_id='rdf', *, max_paths=200):
    from mcp.server import Server
    from mcp.types import (CallToolResult, ListToolsResult, ReadResourceResult, ListResourcesResult, Resource,
                           TextContent, TextResourceContents, Tool)
    workspace = client.workspace(source_id=source_id,max_paths=max_paths)

    async def list_tools(ctx, params):
        return ListToolsResult(tools=[Tool(name=name,description=desc,input_schema=model.model_json_schema())
            for name,(model,_,desc) in CONTRACTS.items()])

    async def call_tool(ctx,params):
        value = await asyncio.to_thread(dispatch,workspace,params.name,params.arguments or {})
        value = to_jsonable_python(value)
        # IDs are workspace-local; the resource URI is supplied by this adapter.
        for key in ('query_ref','result_ref'):
            if value.get(key):
                value[key.replace('_ref','_uri')] = 'rdfsolve://artifacts/' + value[key]
        return CallToolResult(content=[TextContent(type='text',text=json.dumps(value,ensure_ascii=False))],
                              structured_content=value,is_error='error' in value)

    async def list_resources(ctx,params):
        return ListResourcesResult(resources=[Resource(uri='rdfsolve://artifacts/diagnostics',
            name='package-diagnostics',mime_type='application/json')])

    async def read_resource(ctx,params):
        prefix='rdfsolve://artifacts/'
        uri=str(params.uri)
        if not uri.startswith(prefix):
            raise ValueError('Unknown resource URI')
        ref=uri[len(prefix):]
        with workspace.lock:
            value = workspace.diagnostics() if ref=='diagnostics' else workspace.resource(ref)
        text=json.dumps(to_jsonable_python(value),ensure_ascii=False)
        return ReadResourceResult(contents=[TextResourceContents(uri=uri,mime_type='application/json',text=text)])

    return Server(name='rdfsolve',on_list_tools=list_tools,on_call_tool=call_tool,
                  on_list_resources=list_resources,on_read_resource=read_resource)


async def run_server(client,source_id='rdf',*,max_paths=200):
    from mcp.server.stdio import stdio_server
    server=create_server(client,source_id,max_paths=max_paths)
    async with stdio_server() as (read,write):
        await server.run(read,write,server.create_initialization_options())
