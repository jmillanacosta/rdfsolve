"""Run a configured model against any saved RDF schema."""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any
from uuid import uuid4


@dataclass
class Answer:
    """Complete caller-side results and the model-visible investigation journal."""

    text: str = ""
    state: str = "failed"
    query: str | None = None
    bindings: list[dict] = field(default_factory=list)
    usage: Any = None
    calls: list[dict] = field(default_factory=list)
    error: dict | None = None
    execution: dict = field(default_factory=dict)
    package: dict = field(default_factory=dict)
    messages: list = field(default_factory=list)
    files: dict = field(default_factory=dict)
    elapsed_seconds: float = 0

    def table(self):
        """Display values while retaining RDF term metadata in bindings."""
        if self.state != "complete":
            raise ValueError(f"No completed answer: {self.error}")
        import pandas as pd

        return pd.DataFrame(
            [{name: term["value"] for name, term in row.items()} for row in self.bindings]
        )

    def diagnostics(self):
        """Report usage, strategy, recovery and failure facts."""
        from pydantic_core import to_jsonable_python

        return to_jsonable_python(
            {
                "warnings": next(
                    (
                        c["result"].get("warnings", [])
                        for c in reversed(self.calls)
                        if c["result"].get("state") in {"complete", "prepared"}
                    ),
                    [],
                ),
                "max_request_input_tokens": max(
                    (m.usage.input_tokens for m in self.messages if getattr(m, "usage", None)),
                    default=None,
                ),
                **{
                    k: v
                    for k, v in vars(self).items()
                    if k not in {"bindings", "messages", "calls", "query"}
                },
            }
        )

    def save(self, path):
        """Save exact results, costs and traces for reproduction."""
        from pydantic_core import to_jsonable_python

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(to_jsonable_python(vars(self)), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def launch_config(
    schema,
    *,
    endpoint=None,
    data_file=None,
    graph_uris=None,
    source_id="rdf",
    timeout=900,
    max_paths=100,
    mapping_file=None,
    related_registries=(),
    artifact_dir=None,
):
    """Launch this checkout with the caller's Python and explicit source settings."""
    path = Path(schema).expanduser().resolve(strict=True)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(Path(__file__).resolve().parents[2]), env.get("PYTHONPATH")])
    )
    args = [
        "-m",
        "rdfsolve.mcp",
        "--schema",
        str(path),
        "--source-id",
        source_id,
        "--timeout",
        str(timeout),
        "--max-paths",
        str(max_paths),
    ]
    for option, value in (
        ("--endpoint", endpoint),
        ("--data-file", data_file),
        ("--artifact-dir", artifact_dir),
    ):
        if value is not None:
            args += [option, str(Path(value).resolve()) if option != "--endpoint" else str(value)]
    if mapping_file:
        args += ["--mapping", str(Path(mapping_file).resolve(strict=True))]
    for path in related_registries:
        args += ["--related-registry", str(Path(path).resolve(strict=True))]
    if graph_uris is not None:
        args += ["--graphs", json.dumps(graph_uris)]
    return {"command": sys.executable, "args": args, "env": env}


async def ask_rdf(
    question: str,
    *,
    schema,
    source_id="rdf",
    endpoint=None,
    data_file=None,
    graph_uris=None,
    model=None,
    base_url=None,
    model_name=None,
    api_key=None,
    model_settings=None,
    usage_limits=None,
    timeout=900,
    max_paths=100,
    mapping_file=None,
    related_registries=(),
    output_dir=None,
) -> Answer:
    """Discover, ground, compose and execute against a caller-selected database.

    Supply a PydanticAI model or an OpenAI-compatible base URL and model name.
    Full bindings are read directly from the subprocess artifact directory.
    """
    from mcp import Client as MCPClient
    from mcp import StdioServerParameters

    from rdfsolve.mcp.agent import ask, failure

    if endpoint is not None and data_file is not None:
        raise ValueError("Choose an endpoint or a local data file.")
    if model is None:
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider

        url = base_url or os.getenv("RDFSOLVE_MODEL_BASE_URL")
        name = model_name or os.getenv("RDFSOLVE_MODEL")
        if not url or not name:
            raise ValueError("Supply model= or both base_url and model_name.")
        model = OpenAIChatModel(
            name,
            provider=OpenAIProvider(
                base_url=url, api_key=api_key or os.getenv("RDFSOLVE_MODEL_API_KEY", "not-needed")
            ),
        )
    settings = {
        "temperature": 0,
        "max_tokens": int(os.getenv("RDFSOLVE_MAX_TOKENS", "16384")),
        "timeout": float(os.getenv("RDFSOLVE_MODEL_TIMEOUT", "900")),
        **(model_settings or {}),
    }
    output = output_dir or os.getenv("RDFSOLVE_OUTPUT")
    answer = Answer()
    if output:
        folder = Path(output).expanduser().resolve()
        folder.mkdir(parents=True, exist_ok=True)
        stem = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
        answer.files = {
            "calls": str(folder / (stem + ".calls.jsonl")),
            "answer": str(folder / (stem + ".answer.json")),
        }

    def journal(item):
        if answer.files:
            with Path(answer.files["calls"]).open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(item, ensure_ascii=False) + "\n")

    started = perf_counter()
    with TemporaryDirectory(prefix="rdfsolve-results-") as artifacts:
        config = launch_config(
            schema,
            endpoint=endpoint,
            data_file=data_file,
            graph_uris=graph_uris,
            source_id=source_id,
            timeout=timeout,
            max_paths=max_paths,
            mapping_file=mapping_file,
            related_registries=related_registries,
            artifact_dir=artifacts,
        )
        if answer.files:
            config["args"] += ["--log", str(folder / (stem + ".package.json"))]
        try:
            async with MCPClient(
                StdioServerParameters(**config), read_timeout_seconds=timeout * 4
            ) as server:
                run = await ask(
                    server,
                    question,
                    model=model,
                    model_settings=settings,
                    usage_limits=usage_limits,
                    calls=answer.calls,
                    on_call=journal,
                )
                answer.state, answer.text, answer.error = run.state, run.text, run.error
                answer.usage, answer.messages = run.usage, run.messages
                answer.execution = run.terminal.get("execution", {})
                if answer.state == "complete":
                    ref = run.terminal["result_ref"]
                    if not ref.startswith("result_") or not ref.removeprefix("result_").isalnum():
                        raise ValueError("Invalid result artifact identity")
                    data = json.loads((Path(artifacts) / (ref + ".json")).read_text())
                    answer.query, answer.bindings = data["query"], data["bindings"]
                diagnostics = await server.read_resource("rdfsolve://diagnostics")
                answer.package = json.loads(diagnostics.contents[0].text)
        except Exception as exc:
            logging.getLogger(__name__).exception("RDF investigation failed")
            answer.state = "failed"
            answer.error = failure(exc, "workflow_error")
            answer.text = "The investigation failed: " + answer.error["message"]
        finally:
            answer.elapsed_seconds = perf_counter() - started
            if answer.files:
                answer.save(answer.files["answer"])
    return answer
