"""Notebook convenience for the package-backed MCP agent. No planning logic."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4
import json
import os
import sys

SCHEMA_RELATIVE = Path("notebooks/data/aopwikirdf.schema.json")


def _search_roots():
    # The imported checkout is preferred to accidentally finding a second checkout.
    starts = [Path(__file__).resolve().parent, Path.cwd()]
    if os.getenv("SLURM_SUBMIT_DIR"):
        starts.append(Path(os.environ["SLURM_SUBMIT_DIR"]).expanduser().resolve())
    seen = set()
    for start in starts:
        for root in (start, *start.parents):
            if root not in seen:
                seen.add(root)
                yield root


def resolve_schema(schema: str | Path | None = None) -> Path:
    """Find the existing saved schema without depending on notebook cwd.

    Explicit schema/root overrides are strict: a typo raises instead of silently
    choosing a different dataset. No network access or mining occurs.
    """
    explicit = schema if schema is not None else os.getenv("RDFSOLVE_SCHEMA")
    if explicit is not None:
        path = Path(os.path.expandvars(str(explicit))).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Schema does not exist: {path}")
        return path
    if os.getenv("RDFSOLVE_ROOT"):
        path = Path(os.environ["RDFSOLVE_ROOT"]).expanduser().resolve() / SCHEMA_RELATIVE
        if not path.is_file():
            raise FileNotFoundError(f"No saved AOPWiki schema under RDFSOLVE_ROOT: {path}")
        return path
    for root in _search_roots():
        candidate = root / SCHEMA_RELATIVE
        if candidate.is_file():
            return candidate.resolve()
    raise FileNotFoundError("Cannot find notebooks/data/aopwikirdf.schema.json. Set RDFSOLVE_SCHEMA to its absolute path.")


def load_notebook_env() -> None:
    """Match the Qwen notebook's notebooks/.env, without overriding job exports."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return  # Exported environment variables work without python-dotenv.
    root_override = os.getenv("RDFSOLVE_ROOT")
    roots = [Path(root_override).expanduser().resolve()] if root_override else _search_roots()
    for root in roots:
        if (root / "pyproject.toml").is_file() and (root / "src/rdfsolve").is_dir():
            load_dotenv(root / "notebooks/.env", override=False)
            load_dotenv(root / ".env", override=False)
            return


def model_config(*, base_url: str | None = None, model_name: str | None = None) -> tuple[str, str]:
    """Keep QWEN_* defaults while allowing model-neutral settings."""
    return (base_url or os.getenv("RDFSOLVE_MODEL_BASE_URL") or os.getenv("QWEN_BASE_URL") or "http://127.0.0.1:8080/v1",
            model_name or os.getenv("RDFSOLVE_MODEL") or os.getenv("QWEN_MODEL") or "qwen36-35b-a3b")


def launch_config(schema: str | Path | None = None, *, timeout: float = 900, total_ceiling: int = 200) -> dict[str, Any]:
    """Use this kernel's Python and this exact package checkout in the subprocess."""
    path = resolve_schema(schema)
    env = dict(os.environ)
    package_parent = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [package_parent, env.get("PYTHONPATH")]))
    env["PYTHONUNBUFFERED"] = "1"
    return dict(command=sys.executable,
        args=["-m", "rdfsolve.mcp", "--schema", str(path), "--source-id", "aopwikirdf",
              "--timeout", str(timeout), "--max-paths", str(total_ceiling)], env=env)


def server_parameters(schema: str | Path | None = None, *, timeout: float = 900, total_ceiling: int = 200):
    from mcp import StdioServerParameters
    return StdioServerParameters(**launch_config(schema, timeout=timeout, total_ceiling=total_ceiling))


@dataclass
class NotebookAnswer:
    text: str
    state: str
    query: str | None
    bindings: list[dict[str, Any]]
    usage: Any
    calls: list[dict]
    schema: str
    run: Any = None
    error: dict | None = None
    query_ref: str | None = None
    result_ref: str | None = None
    execution: dict = field(default_factory=dict)
    package: dict = field(default_factory=dict)
    messages: list = field(default_factory=list)
    diagnostic_files: dict = field(default_factory=dict)
    elapsed_seconds: float = 0

    def table(self):
        if self.state != "complete":
            raise ValueError(f"No executed result: {self.state}. Inspect .error and .calls.")
        import pandas as pd
        return pd.DataFrame([{name:term['value'] for name,term in row.items()} for row in self.bindings])

    def diagnostics(self):
        from pydantic_core import to_jsonable_python
        usage=self.usage() if callable(self.usage) else self.usage
        return {'state':self.state,'usage':to_jsonable_python(usage),
                'elapsed_seconds':self.elapsed_seconds,'tool_calls':len(self.calls),
                'tool_response_bytes':sum(c.get('result_bytes',0) for c in self.calls),
                'package':self.package,'execution':self.execution,'error':self.error,
                'files':self.diagnostic_files}

    def save(self,path):
        from pydantic_core import to_jsonable_python
        data={k:v for k,v in vars(self).items() if k!='run'}
        if callable(data['usage']):
            data['usage']=data['usage']()
        path=Path(path)
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(to_jsonable_python(data),ensure_ascii=False,indent=2),encoding='utf-8')


def generation_settings(overrides=None):
    settings={'temperature':0, 'max_tokens':int(os.getenv('RDFSOLVE_MAX_TOKENS','16384')),
              'timeout':float(os.getenv('RDFSOLVE_MODEL_TIMEOUT','900'))}
    settings.update(overrides or {})
    return settings


async def ask_aopwiki(question: str, *, schema=None, model=None, base_url=None,
                     model_name=None, model_settings=None, usage_limits=None,
                     timeout=900, total_ceiling=200, data_file=None):
    """Use the existing schema and local-model environment, with one MCP investigation.

    Passing schema selects another dataset; no AOPWiki vocabulary is embedded in
    the planner or tools. Full results are read outside the model context.
    """
    from mcp import Client as MCPClient, StdioServerParameters
    from rdfsolve.mcp.agent import ask,read_resource
    load_notebook_env()
    path=resolve_schema(schema)
    if model is None:
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider
        url,name=model_config(base_url=base_url,model_name=model_name)
        model=OpenAIChatModel(name,provider=OpenAIProvider(base_url=url,
                             api_key=os.getenv('RDFSOLVE_MODEL_API_KEY','not-needed')))
    config=launch_config(path,timeout=timeout,total_ceiling=total_ceiling)
    if data_file is not None:
        config['args']+=['--data-file',str(Path(data_file).resolve())]
    output=os.getenv('RDFSOLVE_QWEN_OUTPUT') or os.getenv('RDFSOLVE_OUTPUT')
    files={}
    if output:
        folder=Path(output);folder.mkdir(parents=True,exist_ok=True)
        stem='rdfsolve-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid4().hex[:8]
        files={'calls':str(folder/(stem+'.calls.jsonl')),'answer':str(folder/(stem+'.answer.json'))}
        config['args']+=['--log',str(folder/(stem+'.package.json'))]
    def log_call(item):
        if files:
            with Path(files['calls']).open('a',encoding='utf-8') as stream:
                stream.write(json.dumps(item,ensure_ascii=False)+'\n')
    started=perf_counter();calls=[];run=None
    answer=NotebookAnswer('', 'failed', None, [], {}, calls, str(path),diagnostic_files=files)
    # Some ipykernel versions access this after async exception groups.
    try:
        from IPython import get_ipython
        shell=get_ipython()
        if shell is not None and not hasattr(shell,'_last_traceback'):
            shell._last_traceback=[]
    except ImportError:
        pass
    try:
        async with MCPClient(StdioServerParameters(**config),read_timeout_seconds=timeout*4) as server:
            run=await ask(server,question,model=model,model_settings=generation_settings(model_settings),
                          usage_limits=usage_limits,calls=calls,on_call=log_call)
            answer.run,answer.usage,answer.messages=run,run.usage,run.messages
            answer.state,answer.text,answer.error=run.state,run.text,run.error
            answer.query_ref=run.terminal.get('query_ref')
            answer.result_ref=run.terminal.get('result_ref')
            if answer.query_ref:
                answer.query=(await read_resource(server,answer.query_ref))['sparql']
            if answer.state=='complete':
                data=await read_resource(server,answer.result_ref)
                answer.bindings=data['bindings']
                answer.execution=run.terminal.get('execution',{})
            answer.package=await read_resource(server,'diagnostics')
    except Exception as exc:
        answer.state='failed'
        answer.error={'type':type(exc).__name__,'message':str(exc),'code':'workflow_error'}
        answer.text=f'{type(exc).__name__}: {exc}'
    finally:
        answer.elapsed_seconds=perf_counter()-started
        if files:
            answer.save(files['answer'])
    return answer
