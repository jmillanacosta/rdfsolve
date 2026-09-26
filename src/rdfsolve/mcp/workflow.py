"""Answer a question about one RDF source with a model and the rdfsolve MCP tools."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from rdfsolve.mcp.agent import Answer

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic_ai.models import Model
    from pydantic_ai.usage import UsageLimits


def launch_config(
    schema: str | Path,
    *,
    endpoint: str | None = None,
    data_file: str | Path | None = None,
    graph_uris: list[str] | None = None,
    output_variables: Sequence[str] = (),
    source_id: str = "rdf",
    timeout: float = 900,
    ontology_grounding: bool = False,
    ontology_provider: str = "ols",
    ontology_cache: str | Path | None = None,
    ontology_offline: bool = False,
    artifact_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Give the command that starts the tool server of this checkout for one source."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(Path(__file__).resolve().parents[2]), env.get("PYTHONPATH")])
    )
    args = ["-m", "rdfsolve.mcp", "--schema", str(Path(schema).expanduser().resolve(strict=True))]
    args += ["--source-id", source_id, "--timeout", str(timeout)]
    if endpoint is not None:
        args += ["--endpoint", endpoint]
    if data_file is not None:
        args += ["--data-file", str(Path(data_file).resolve(strict=True))]
    if artifact_dir is not None:
        args += ["--artifact-dir", str(Path(artifact_dir).resolve())]
    if ontology_grounding:
        args += ["--ontology-provider", ontology_provider]
        if ontology_cache:
            args += ["--ontology-cache", str(Path(ontology_cache).resolve())]
        if ontology_offline:
            args += ["--ontology-offline"]
    if output_variables:
        args += ["--output-variables", json.dumps(list(output_variables))]
    if graph_uris is not None:
        args += ["--graphs", json.dumps(graph_uris)]
    return {"command": sys.executable, "args": args, "env": env}


def _model(model_name: str | None, base_url: str | None, api_key: str | None) -> Model:
    """Make an OpenAI-compatible model from arguments or environment variables."""
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    url = base_url or os.getenv("RDFSOLVE_MODEL_BASE_URL")
    name = model_name or os.getenv("RDFSOLVE_MODEL")
    if not url or not name:
        raise ValueError("Supply model= or both base_url and model_name.")
    key = api_key or os.getenv("RDFSOLVE_MODEL_API_KEY", "not-needed")
    return OpenAIChatModel(name, provider=OpenAIProvider(base_url=url, api_key=key))


async def ask_rdf(
    question: str,
    *,
    schema: str | Path,
    source_id: str = "rdf",
    endpoint: str | None = None,
    data_file: str | Path | None = None,
    graph_uris: list[str] | None = None,
    output_variables: Sequence[str] = (),
    model: Model | None = None,
    base_url: str | None = None,
    model_name: str | None = None,
    api_key: str | None = None,
    model_settings: dict[str, Any] | None = None,
    max_response_tokens: int | None = 4096,
    usage_limits: UsageLimits | None = None,
    timeout: float = 900,
    ontology_grounding: bool = False,
    ontology_provider: str = "ols",
    ontology_cache: str | Path | None = None,
    ontology_offline: bool = False,
    output_dir: str | Path | None = None,
) -> Answer:
    """Answer a question with the tools of one source: an endpoint or a local RDF file.

    Supply a PydanticAI model, or an OpenAI-compatible base URL and model name.
    The model sees a summary of the rows; all rows of the final query are read
    from the result file of the tool server. output_variables are the columns
    that the final query must select. max_response_tokens limits each model reply,
    reasoning included; None removes this limit.
    """
    from mcp import Client as MCPClient
    from mcp import StdioServerParameters
    from mcp.types import TextResourceContents

    from rdfsolve.mcp.agent import ask, failure

    if endpoint is not None and data_file is not None:
        raise ValueError("Choose an endpoint or a local data file.")
    model = model or _model(model_name, base_url, api_key)
    settings = {
        "temperature": 0,
        "timeout": float(os.getenv("RDFSOLVE_MODEL_TIMEOUT", "120")),
        **(model_settings or {}),
    }
    answer = Answer()
    output = output_dir or os.getenv("RDFSOLVE_OUTPUT")
    if output:
        folder = Path(output).expanduser().resolve()
        folder.mkdir(parents=True, exist_ok=True)
        stem = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
        answer.files = {
            name: str(folder / f"{stem}.{name}.{suffix}")
            for name, suffix in (("calls", "jsonl"), ("answer", "json"), ("package", "json"))
        }

    def journal(item: dict[str, Any]) -> None:
        """Add one tool call to the calls file."""
        if answer.files:
            with Path(answer.files["calls"]).open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")

    async def resource(server: Any, uri: str) -> str:
        """Read one text resource of the server."""
        content = (await server.read_resource(uri)).contents[0]
        if not isinstance(content, TextResourceContents):
            raise ValueError(f"{uri} must be text")
        return content.text

    started = perf_counter()
    with TemporaryDirectory(prefix="rdfsolve-results-") as artifacts:
        config = launch_config(
            schema,
            endpoint=endpoint,
            data_file=data_file,
            graph_uris=graph_uris,
            output_variables=output_variables,
            source_id=source_id,
            timeout=timeout,
            ontology_grounding=ontology_grounding,
            ontology_provider=ontology_provider,
            ontology_cache=ontology_cache,
            ontology_offline=ontology_offline,
            artifact_dir=artifacts,
        )
        try:
            # No time limit on one tool call: each source request has its own timeout,
            # and the final query can need many pages. The caller limits the whole run.
            async with MCPClient(StdioServerParameters(**config)) as server:
                await ask(
                    server,
                    question,
                    model=model,
                    overview=await resource(server, "rdfsolve://overview"),
                    model_settings=settings,
                    max_response_tokens=max_response_tokens,
                    usage_limits=usage_limits,
                    calls=answer.calls,
                    on_call=journal,
                    answer=answer,
                )
                answer.execution = answer.terminal.get("execution", {})
                if answer.state == "complete":
                    ref = answer.terminal["result_ref"]
                    if not ref.startswith("result_") or not ref.removeprefix("result_").isalnum():
                        raise ValueError("Invalid result file name")
                    data = json.loads((Path(artifacts) / f"{ref}.json").read_text())
                    answer.query, answer.bindings = data["query"], data["bindings"]
                answer.package = json.loads(await resource(server, "rdfsolve://diagnostics"))
        except Exception as exc:
            logging.getLogger(__name__).exception("RDF investigation failed")
            answer.state = "failed"
            answer.error = failure(exc, "workflow_error")
            answer.text = "The investigation failed: " + answer.error["message"]
        finally:
            answer.elapsed_seconds = perf_counter() - started
            if answer.files:
                Path(answer.files["package"]).write_text(
                    json.dumps(answer.package, default=str, indent=2), encoding="utf-8"
                )
                answer.save(answer.files["answer"])
    return answer
