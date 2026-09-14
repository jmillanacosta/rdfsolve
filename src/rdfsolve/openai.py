"""Small notebook entry point for a running OpenAI-compatible model server.

No SLURM jobs, tunnels, model downloads, or schema mining are started here.
"""
from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from uuid import uuid4
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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


def launch_config(schema: str | Path | None = None, *, timeout: float = 900, total_ceiling: int = 2000) -> dict[str, Any]:
    """Use this kernel's Python and this exact package checkout in the subprocess."""
    path = resolve_schema(schema)
    env = dict(os.environ)
    package_parent = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [package_parent, env.get("PYTHONPATH")]))
    env["PYTHONUNBUFFERED"] = "1"
    return dict(command=sys.executable,
        args=["-m", "rdfsolve.mcp", "--schema", str(path), "--source-id", "aopwikirdf",
              "--timeout", str(timeout), "--total-ceiling", str(total_ceiling)], env=env)


def server_parameters(schema: str | Path | None = None, *, timeout: float = 900, total_ceiling: int = 2000):
    from mcp import StdioServerParameters
    return StdioServerParameters(**launch_config(schema, timeout=timeout, total_ceiling=total_ceiling))


@dataclass
class NotebookAnswer:
    """Retrieved data stays separate from the model's prose."""
    text: str
    state: str
    query: str | None
    bindings: list[dict[str, Any]]
    result_ref: str | None
    query_ref: str | None
    usage: Any
    calls: list[dict[str, Any]]
    schema: str
    run: Any
    execution: dict[str, Any] = field(default_factory=dict)
    error: dict[str, Any] | None = None
    messages: list[Any] = field(default_factory=list)
    diagnostic_files: dict[str, str] = field(default_factory=dict)

    def table(self):
        if self.state != "complete":
            raise ValueError(f"No executed result: state={self.state}. Read .text and .calls.")
        import pandas as pd
        return pd.DataFrame([{name: term["value"] for name, term in row.items()} for row in self.bindings])

    def diagnostics(self) -> dict[str, Any]:
        """Describe the recorded run, without claiming to infer its semantic cause."""
        from rdfsolve.pydantic_ai import _argument_key
        usage = self.usage() if callable(self.usage) else self.usage
        actual = [c for c in self.calls if c.get("name") != "workflow_stop"]
        keys = Counter(_argument_key(c["name"], c.get("model_arguments", c["arguments"])) for c in actual)
        transitions = [c["result"] for c in actual if c["name"] in
                       {"query_start", "query_decide", "query_finish"} and "state" in c["result"]]
        executions = [c for c in actual if c["name"] == "query_finish"
                      and c.get("sent_to_server", True)
                      and c["arguments"].get("action", {}).get("type") == "execute"]
        return {"state": self.state,
                "model_requests": usage.get("requests") if isinstance(usage, dict) else getattr(usage, "requests", None),
                "tool_calls": len(actual),
                "tools": dict(Counter(c["name"] for c in actual)),
                "server_calls": sum(c.get("sent_to_server", True) for c in actual),
                "errors": dict(Counter(c["result"]["error"].get("code", "unknown")
                                       for c in self.calls if "error" in c["result"])),
                "accepted_sessions": len({r["session"] for r in transitions if r.get("session")}),
                "execute_requests": len(executions),
                "completed_result_artifacts": len({r["result_ref"] for r in transitions if r.get("result_ref")}),
                "last_service_state": transitions[-1].get("state") if transitions else None,
                "repeated_identical_actions": sum(n-1 for n in keys.values()),
                "files": dict(self.diagnostic_files)}

    def save(self, path: str | Path) -> None:
        from pydantic_core import to_jsonable_python
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {name: value for name, value in vars(self).items() if name != "run"}
        if callable(data["usage"]):
            data["usage"] = data["usage"]()
        path.write_text(json.dumps(to_jsonable_python(data), ensure_ascii=False, indent=2), encoding="utf-8")


def generation_settings(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Model-neutral budgets; explicit settings override environment defaults."""
    settings = {"temperature": 0, "max_tokens": int(os.getenv("RDFSOLVE_MAX_TOKENS", "16384")),
                "timeout": float(os.getenv("RDFSOLVE_MODEL_TIMEOUT", "900"))}
    settings.update(overrides or {})
    if settings.get("max_tokens") is not None and settings["max_tokens"] < 1:
        raise ValueError("max_tokens must be positive")
    return settings


def ensure_kernel_error_reply() -> None:
    """Guard ipykernel's uninitialized traceback field for async exception groups.

    Does not catch errors or change their status. The native stderr traceback
    remains available even when IPython bypasses its normal traceback hook.
    """
    try:
        from IPython import get_ipython
    except ImportError:
        return
    shell = get_ipython()
    if shell is not None and not hasattr(shell, "_last_traceback"):
        shell._last_traceback = []


async def ask_aopwiki(question: str, *, model=None, base_url: str | None = None,
                     model_name: str | None = None, schema: str | Path | None = None,
                     model_settings: dict[str, Any] | None = None, usage_limits=None,
                     timeout: float = 900, total_ceiling: int = 2000,
                     mcp_timeout: float = 21600, no_progress_limit: int | None = 4,
                     diagnostics_dir: str | Path | None = None) -> NotebookAnswer:
    """Ask once using the existing local model service and the saved AOPWiki schema."""
    ensure_kernel_error_reply()
    from mcp import Client as MCPClient
    from pydantic_ai.exceptions import UnexpectedModelBehavior, UsageLimitExceeded
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider
    from rdfsolve.pydantic_ai import RepeatedToolCallError, ask, read_json_resource

    load_notebook_env()
    path = resolve_schema(schema)
    url, name = model_config(base_url=base_url, model_name=model_name)
    if model is None:
        model = OpenAIChatModel(name, provider=OpenAIProvider(base_url=url,
            api_key=os.getenv("RDFSOLVE_MODEL_API_KEY") or "not-needed"))
    if mcp_timeout <= timeout:
        raise ValueError("mcp_timeout must exceed the per-request endpoint timeout")
    files, observer = _diagnostic_writer(diagnostics_dir)
    calls = []
    trace: dict[str, Any] = {}
    model_error = None
    run = None
    # Permit server-side retries to finish before the MCP transport times out.
    async with MCPClient(server_parameters(path, timeout=timeout, total_ceiling=total_ceiling),
                         read_timeout_seconds=mcp_timeout) as server:
        try:
            run = await ask(server, question, model=model, observations=calls,
                            model_settings=model_settings, usage_limits=usage_limits, trace=trace,
                            no_progress_limit=no_progress_limit, on_observation=observer)
        except (UnexpectedModelBehavior, UsageLimitExceeded, RepeatedToolCallError) as exc:
            # Catch inside the MCP context: an expected budget failure must not
            # escape into AnyIO teardown as nested exception groups.
            errors = [c["result"]["error"] for c in calls if "error" in c["result"]]
            model_error = {"code": "model_failure", "message": f"{type(exc).__name__}: {exc}",
                           "tool_error_count": len(errors), "last_tool_error": errors[-1] if errors else None}
        transitions = [c["result"] for c in calls if c["name"] in {"query_start", "query_decide", "query_finish"}
                       and "state" in c["result"]]
        last = transitions[-1] if transitions else {}
        successes = [last] if last.get("state") == "complete" and last.get("result_ref") else []
        result_ref = query = None
        query_ref = last.get("query_ref")
        execution = await read_json_resource(server, last["execution_ref"]) if last.get("execution_ref") else {}
        bindings = []
        if successes:
            completed = successes[-1]
            result_ref = completed["result_ref"]
            data = await read_json_resource(server, result_ref)
            if data["type"] != "result" or data["row_count"] != len(data["bindings"]):
                raise RuntimeError("Invalid retained result artifact")
            bindings = data["bindings"]
            query_ref = result_ref.rsplit("/", 1)[0] + "/" + data["query_artifact_id"]
        if query_ref:
            query = (await read_json_resource(server, query_ref))["query"]
        answer = NotebookAnswer(text=run.output if run is not None else model_error["message"],
                              state="failed" if model_error else last.get("state", "failed"), query=query, bindings=bindings,
                              result_ref=result_ref, query_ref=query_ref,
                              usage=run.usage if run is not None else trace.get("usage"), calls=calls, schema=str(path), run=run,
                              execution=execution, error=model_error or last.get("error"),
                              messages=trace.get("messages", []), diagnostic_files=files)
        if files:
            try:
                answer.save(files["answer"])
            except (OSError, TypeError, ValueError) as exc:
                logging.getLogger(__name__).warning("Could not save final diagnostics: %s", exc)
            else:
                # Make the location visible before an unconditional .table() can
                # throw in a user's old cell. No fabricated empty table on failure.
                if answer.state != "complete":
                    answer.text += "\nDiagnostics: " + files["answer"]
        return answer



def _diagnostic_writer(directory=None):
    """Local append-only tool journal, outside the model's context.

    SLURM's existing output-directory environment is reused. No new shell setup
    is required. Each question gets a distinct file pair.
    """
    directory = directory or os.getenv("RDFSOLVE_TRACE_DIR") or os.getenv("RDFSOLVE_QWEN_OUTPUT")
    if not directory:
        return {}, None
    root = Path(directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    stem = "rdfsolve-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
    files = {"calls": str(root / (stem + ".calls.jsonl")), "answer": str(root / (stem + ".answer.json"))}
    # Validate that the destination is writable before invoking the model.
    Path(files["calls"]).touch(exist_ok=False)

    def write(entry):
        try:
            with open(files["calls"], "a", encoding="utf-8") as stream:
                stream.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as exc:
            logging.getLogger(__name__).warning("Could not append tool diagnostics: %s", exc)

    return files, write