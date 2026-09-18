"""Run an RDF investigation with a configured model and MCP client."""

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
    from collections.abc import Iterable, Sequence

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
    max_paths: int = 100,
    mapping_file: str | Path | None = None,
    related_registries: Iterable[str | Path] = (),
    ontology_grounding: bool = False,
    ontology_provider: str = "ols",
    ontology_cache: str | Path | None = None,
    ontology_offline: bool = False,
    artifact_dir: str | Path | None = None,
) -> dict[str, Any]:
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
    for registry in related_registries:
        args += ["--related-registry", str(Path(registry).resolve(strict=True))]
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
    max_paths: int = 100,
    mapping_file: str | Path | None = None,
    related_registries: Iterable[str | Path] = (),
    ontology_grounding: bool = False,
    ontology_provider: str = "ols",
    ontology_cache: str | Path | None = None,
    ontology_offline: bool = False,
    output_dir: str | Path | None = None,
) -> Answer:
    """Discover, ground, compose and execute against a caller-selected database.

    Supply a PydanticAI model or an OpenAI-compatible base URL and model name.
    Full bindings are read directly from the subprocess artifact directory.
    output_variables requires named columns in the final projection.
    max_response_tokens caps each response, including reasoning. None disables
    this ceiling; model_settings.max_tokens and provider limits still apply.
    """
    from mcp import Client as MCPClient
    from mcp import StdioServerParameters
    from mcp.types import TextResourceContents

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
        "timeout": float(os.getenv("RDFSOLVE_MODEL_TIMEOUT", "120")),
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
            "package": str(folder / (stem + ".package.json")),
        }

    def journal(item: dict[str, Any]) -> None:
        """Append one tool call to the calls journal."""
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
            output_variables=output_variables,
            source_id=source_id,
            timeout=timeout,
            max_paths=max_paths,
            mapping_file=mapping_file,
            related_registries=related_registries,
            ontology_grounding=ontology_grounding,
            ontology_provider=ontology_provider,
            ontology_cache=ontology_cache,
            ontology_offline=ontology_offline,
            artifact_dir=artifacts,
        )
        if answer.files:
            config["args"] += ["--log", answer.files["package"]]
        try:
            async with MCPClient(
                StdioServerParameters(**config), read_timeout_seconds=timeout * 4
            ) as server:
                run = await ask(
                    server,
                    question,
                    model=model,
                    model_settings=settings,
                    max_response_tokens=max_response_tokens,
                    usage_limits=usage_limits,
                    calls=answer.calls,
                    on_call=journal,
                    answer=answer,
                )
                answer.execution = run.terminal.get("execution", {})
                if answer.state == "complete":
                    ref = run.terminal["result_ref"]
                    if not ref.startswith("result_") or not ref.removeprefix("result_").isalnum():
                        raise ValueError("Invalid result artifact identity")
                    data = json.loads((Path(artifacts) / (ref + ".json")).read_text())
                    answer.query, answer.bindings = data["query"], data["bindings"]
                diagnostics = await server.read_resource("rdfsolve://diagnostics")
                content = diagnostics.contents[0]
                if not isinstance(content, TextResourceContents):
                    raise ValueError("Diagnostics resource must be JSON text")
                answer.package = json.loads(content.text)
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
