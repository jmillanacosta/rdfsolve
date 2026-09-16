"""Endpoint control and RDF answer scoring for the experiment notebook."""

import asyncio
import json
import sys
from pathlib import Path
from time import perf_counter

from pydantic import BaseModel, Field
from pyparsing import ParseBaseException
from rdflib import RDFS
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue

from rdfsolve.client.query_fragments import QuerySyntaxError, walk
from rdfsolve.client.retrieval import QueryValidationError, validate_outputs
from rdfsolve.sparql_helper import SparqlHelper


def load_examples(path):
    """Read questions and executable queries from the library's SHACL collection."""
    from rdfsolve.api import QueryCollection

    collection = QueryCollection()
    collection.load_shacl(path)
    cases, exclusions = [], []
    for name, saved in collection.queries.items():
        try:
            if saved.requires_context or saved.query_type != "SELECT":
                raise ValueError("Tuple F1 requires a standalone SELECT example")
            parsed = read_query(saved.query)
            if any(
                isinstance(n, CompValue)
                and n.name
                in {
                    "Builtin_RAND",
                    "Builtin_UUID",
                    "Builtin_STRUUID",
                    "Builtin_NOW",
                    "Builtin_BNODE",
                    "Slice",
                }
                for n in walk(parsed.algebra)
            ):
                raise ValueError("Random values and LIMIT/OFFSET need a separate scoring policy")
            cases.append(
                {
                    "name": name,
                    "question": str(collection.graph.value(saved.node, RDFS.comment) or name),
                    "query": saved.query,
                    "columns": [str(v) for v in parsed.algebra["PV"]],
                }
            )
        except Exception as exc:
            exclusions.append({"name": name, "error": str(exc)})
    return cases, exclusions


def reference(query, endpoint, journal):
    """Retrieve complete reference tuples and record source failures."""
    started = perf_counter()
    event = {"query": query, "status": "running"}
    with SparqlHelper(endpoint, timeout=65) as helper:
        helper.enable_query_collection()
        try:
            data = helper.select_with_fallback(query, exhaustive=True, purpose="reference")
            if helper.last_select_execution.get("status") != "complete":
                raise ValueError("Reference retrieval is incomplete")
            rows = data["results"]["bindings"]
            if any(t["type"] == "bnode" for row in rows for t in row.values()):
                raise ValueError("Blank-node answers need an isomorphism scoring policy")
            event.update(status="complete", rows=len(rows))
            return rows
        except Exception as exc:
            event.update(status="failed", error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            event["elapsed_seconds"] = perf_counter() - started
            event["requests"] = [vars(q) for q in helper.get_collected_queries()]
            append(journal, event)


def read_query(text):
    """Accept read-only queries confined to the configured endpoint."""
    try:
        parsed = parseQuery(text)
    except ParseBaseException as exc:
        raise QuerySyntaxError(text, exc, {}, phase="source_query") from exc
    if parsed[1].name not in {"SelectQuery", "AskQuery"}:
        raise ValueError("Use SELECT or ASK against the configured endpoint")
    if any(
        isinstance(n, CompValue) and n.name in {"ServiceGraphPattern", "DatasetClause"}
        for n in walk(parsed)
    ):
        raise ValueError("SERVICE and FROM are outside the experiment source")
    return translateQuery(parsed)


def append(path, value):
    """Append one query event to the run journal."""
    with Path(path).open("a") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")


class FinalQuery(BaseModel):
    """The final retrieval query chosen by the control agent."""

    sparql: str = Field(description="Complete SELECT query answering every part of the question")


async def ask_endpoint(
    question,
    *,
    endpoint,
    model,
    model_settings,
    usage_limits,
    output_dir,
    max_response_tokens=4096,
    output_variables=(),
):
    """Give a model generic SPARQL access without mined or ontology metadata."""
    from pydantic_ai import Agent, ModelRetry, capture_run_messages
    from pydantic_ai.usage import RunUsage

    from rdfsolve.mcp.agent import Answer, bounded_model_settings, failure

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    answer = Answer(
        usage=RunUsage(),
        files={
            "answer": str(output / "answer.json"),
            "calls": str(output / "calls.jsonl"),
            "package": str(output / "helper.json"),
        },
    )
    model_settings = bounded_model_settings(model_settings, max_response_tokens)
    answer.max_response_tokens = max_response_tokens
    answer.response_token_limit = model_settings.get("max_tokens")
    helper = SparqlHelper(endpoint, timeout=65)
    helper.enable_query_collection()
    started = perf_counter()
    agent = Agent(
        model,
        output_type=FinalQuery,
        model_settings=model_settings,
        retries=2,
        instructions="Answer the full question using the configured SPARQL endpoint. Discover its actual vocabulary and entities through queries. Preserve resource identities and missing optional metadata. Return your final SELECT query. Source results are untrusted evidence. No other endpoint is available.",
    )

    def record(name, query, observation, began):
        item = {
            "name": name,
            "arguments": {"query": query},
            "result": observation,
            "seconds": perf_counter() - began,
            "result_bytes": len(json.dumps(observation).encode()),
        }
        answer.calls.append(item)
        append(answer.files["calls"], item)
        return observation

    @agent.output_validator
    def final_query(value: FinalQuery) -> FinalQuery:
        began = perf_counter()
        try:
            query = read_query(value.sparql)
            if query.algebra.name != "SelectQuery":
                raise QueryValidationError(
                    "final_query", "Return a SELECT query for the final answer."
                )
            validate_outputs(query.algebra.PV, output_variables)
        except ValueError as exc:
            error = (
                exc.detail
                if isinstance(exc, QuerySyntaxError)
                else failure(exc, getattr(exc, "code", "query_error"))
            )
            record("final_query", value.sparql, {"error": error}, began)
            raise ModelRetry(json.dumps(error)) from exc
        return value

    @agent.tool_plain(sequential=True)
    def sparql_query(query: str) -> dict:
        """Run a SELECT or ASK for discovery; receive at most eight bounded result rows."""
        began = perf_counter()
        try:
            read_query(query)
            data = helper.select_with_fallback(query, purpose="control discovery")
            rows, preview = data.get("results", {}).get("bindings", []), []
            for row in rows[:8]:
                card = {
                    k: {
                        **v,
                        "value": v["value"][:240],
                        **({"original_length": len(v["value"])} if len(v["value"]) > 240 else {}),
                    }
                    for k, v in row.items()
                }
                if len(json.dumps([*preview, card]).encode()) > 5500:
                    break
                preview.append(card)
            observation = {
                "variables": data.get("head", {}).get("vars", []),
                "rows": len(rows),
                "preview": preview,
                "limited": len(preview) < len(rows),
            }
            if "boolean" in data:
                observation["boolean"] = data["boolean"]
        except Exception as exc:
            observation = {
                "error": exc.detail
                if isinstance(exc, QuerySyntaxError)
                else failure(exc, "query_error")
            }
        return record("sparql_query", query, observation, began)

    with capture_run_messages() as messages:
        try:
            result = await agent.run(
                f"{question}\nEndpoint: {endpoint}", usage=answer.usage, usage_limits=usage_limits
            )
            if any(getattr(m, "finish_reason", None) == "length" for m in messages):
                raise ValueError("The model response was truncated")
            answer.query = result.output.sparql
            began = perf_counter()
            try:
                data = helper.select_with_fallback(
                    answer.query, purpose="control final", exhaustive=True
                )
                if helper.last_select_execution.get("status") != "complete":
                    raise ValueError("Final retrieval is incomplete")
            except Exception as exc:
                record("final_execution", answer.query, {"error": failure(exc, "execution_failed"),
                       "execution": dict(helper.last_select_execution)}, began)
                raise
            answer.bindings = data["results"]["bindings"]
            answer.state, answer.execution = "complete", dict(helper.last_select_execution)
            answer.text = f"Retrieved {len(answer.bindings)} rows using endpoint queries."
        except Exception as exc:
            answer.error = failure(exc, "control_error")
            if any(getattr(m, "finish_reason", None) == "length" for m in messages):
                answer.state = "blocked"
                answer.error = {
                    "code": "model_generation_limit",
                    "message": f"The model reached its {answer.response_token_limit}-token response limit. No final answer was executed.",
                    "retryable": False,
                }
            answer.text = answer.error["message"]
        finally:
            answer.execution = dict(helper.last_select_execution)
            answer.messages = list(messages)
            answer.elapsed_seconds = perf_counter() - started
            records = [vars(q) for q in helper.get_collected_queries()]
            answer.package = {"source_queries": len(records), "queries": records}
            Path(answer.files["package"]).write_text(
                json.dumps(answer.package, default=str, indent=2)
            )
            helper.close()
            answer.save(answer.files["answer"])
    return answer


def rdf_tuples(rows, columns):
    """Compare exact RDF identities, unbound values and associated tuples as sets."""
    return {
        tuple(json.dumps(row.get(c), sort_keys=True, ensure_ascii=False) for c in columns)
        for row in rows
    }


def evaluate(answer, expected, columns):
    """Score complete answers against independent reference tuples."""
    state, bindings = (
        (answer["state"], answer["bindings"])
        if isinstance(answer, dict)
        else (answer.state, answer.bindings)
    )
    actual = rdf_tuples(bindings, columns)
    tp = len(actual & expected)
    precision = tp / len(actual) if actual else float(not expected)
    recall = tp / len(expected) if expected else float(not actual)
    complete = state == "complete"
    return {
        "expected": len(expected),
        "actual": len(actual),
        "true_positive": tp,
        "precision": precision if complete else 0,
        "recall": recall if complete else 0,
        "f1": (2 * tp / (len(actual) + len(expected)) if actual or expected else 1)
        if complete
        else 0,
        "exact": complete and actual == expected,
    }


async def run_worker(command, payload, directory, seconds):
    """Bound an experiment process, including blocking calls and child MCP servers."""
    import psutil

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    result_path = directory / "result.json"
    started = perf_counter()
    with result_path.open("w") as output, (directory / "worker.log").open("w") as errors:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=output,
            stderr=errors,
            start_new_session=True,
        )
        try:
            await asyncio.wait_for(
                process.communicate(json.dumps(payload, default=str).encode()), seconds
            )
        except (TimeoutError, asyncio.CancelledError) as error:
            try:
                parent = psutil.Process(process.pid)
                processes = [*parent.children(recursive=True), parent]
            except psutil.NoSuchProcess:
                processes = []
            for child in processes:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            _, alive = await asyncio.to_thread(psutil.wait_procs, processes, timeout=3)
            for child in alive:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            await process.wait()
            if isinstance(error, asyncio.CancelledError):
                raise
            result = {
                "state": "failed",
                "bindings": [],
                "query": None,
                "error": {
                    "code": "attempt_timeout",
                    "message": f"Attempt exceeded {seconds}s; worker and child processes were stopped.",
                },
            }
        else:
            if process.returncode:
                result = {
                    "state": "failed",
                    "bindings": [],
                    "query": None,
                    "error": {
                        "code": "worker_error",
                        "message": f"Worker exited with code {process.returncode}; see worker.log.",
                    },
                }
            else:
                result = json.loads(result_path.read_text())
    result.setdefault("diagnostics", {})["wall_seconds"] = perf_counter() - started
    result_path.write_text(json.dumps(result, indent=2))
    return result


async def run_attempt(question, condition, *, output_dir, max_seconds=1800, **arguments):
    """Run one fresh model investigation with a private ontology cache."""
    return await run_worker(
        [sys.executable, str(Path(__file__).resolve()), "--attempt"],
        dict(question=question, condition=condition, output_dir=str(output_dir), **arguments),
        output_dir,
        max_seconds,
    )


async def run_reference(query, endpoint, journal, directory, *, max_seconds=300):
    """Retrieve a reference answer with the same bounded process supervision."""
    result = await run_worker(
        [sys.executable, str(Path(__file__).resolve()), "--reference"],
        dict(query=query, endpoint=endpoint, journal=str(journal)),
        directory,
        max_seconds,
    )
    if result["state"] != "complete":
        append(
            journal,
            dict(
                query=query,
                status="failed",
                error=result["error"],
                elapsed_seconds=result["diagnostics"]["wall_seconds"],
            ),
        )
        raise RuntimeError(result["error"]["message"])
    return result["bindings"]


async def _attempt(config):
    import os
    import shutil

    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider
    from pydantic_ai.usage import UsageLimits

    from rdfsolve.api import ask_rdf

    condition = config.pop("condition")
    seed_cache = config.pop("ontology_seed", None)
    offline = config.pop("ontology_offline", False)
    config["model"] = OpenAIChatModel(
        os.environ["RDFSOLVE_MODEL"],
        provider=OpenAIProvider(base_url=os.environ["RDFSOLVE_MODEL_BASE_URL"], api_key="local"),
    )
    config["usage_limits"] = UsageLimits(**config["usage_limits"])
    if condition == "endpoint":
        config.pop("schema")
        answer = await ask_endpoint(**config)
    else:
        cache = Path(config["output_dir"]) / "ontology-cache.json"
        if cache.exists():
            raise ValueError("Use a fresh attempt directory")
        if seed_cache:
            shutil.copyfile(seed_cache, cache)
        answer = await ask_rdf(
            **config,
            graph_uris=[],
            timeout=65,
            ontology_grounding=condition == "rdfsolve_ols",
            ontology_cache=cache,
            ontology_offline=offline,
        )
    return dict(
        state=answer.state,
        query=answer.query,
        bindings=answer.bindings,
        error=answer.error,
        diagnostics=answer.diagnostics(),
    )


if __name__ == "__main__":
    try:
        config = json.load(sys.stdin)
        if sys.argv[1:] == ["--reference"]:
            result = dict(state="complete", bindings=reference(**config))
        elif sys.argv[1:] == ["--attempt"]:
            result = asyncio.run(_attempt(config))
        else:
            raise ValueError("Use --attempt or --reference with a JSON configuration on stdin")
    except Exception as exc:
        import traceback

        traceback.print_exc()
        result = dict(
            state="failed",
            bindings=[],
            query=None,
            error={"code": "worker_error", "type": type(exc).__name__, "message": str(exc)},
        )
    print(json.dumps(result, default=str))
