"""Compare proposed queries against withheld AOPWiki queries on one RDF snapshot."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path

import pandas as pd
from pydantic_ai import capture_run_messages
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.usage import UsageLimits
from rdflib import Graph
from rdflib.plugins.sparql.parser import parseQuery

from rdfsolve.client_api import Client
from rdfsolve.pydantic_ai import ClientTools

ROOT = Path(__file__).resolve().parent
REVISION = "e3c4fa60d5fbafec6f3cf79368d6c731e90765af"
REPOSITORY = "https://github.com/marvinm2/AOP-Wiki-Queries"
CASES = [
    ("A. Metadata/count-aops.rq", {}, ["total_AOPs"], "count"),
    ("B. AOPs/get-aop-for-ao.rq", {"ao_id": "1276"}, ["AOP", "AOPTitle"], "rows"),
    (
        "F. Chemicals/get-matching-ids-for-chems.rq",
        {"chem_name": "Phenobarbital"},
        ["CASID", "ChemicalName", "MatchingIDs"],
        "rows",
    ),
    (
        "F. Chemicals/chemicals-for-aop.rq",
        {"aop_id": "162"},
        ["aop", "aoptitle", "CASID", "chemicalname"],
        "rows",
    ),
]
DEFAULT_MODELS = {
    "OPENAI_API_KEY": ("OPENAI_MODEL", "openai:gpt-5.4-mini-2026-03-17"),
    "ANTHROPIC_API_KEY": ("ANTHROPIC_MODEL", "anthropic:claude-haiku-4-5-20251001"),
}


def configured_models():
    """Load local keys without printing or saving them."""
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env", override=False)
    return [
        os.getenv(setting, default)
        for key, (setting, default) in DEFAULT_MODELS.items()
        if os.getenv(key)
    ]


def validate_select(query):
    """Reject updates, federation, and dataset loading before execution."""
    parsed = parseQuery(query)
    if parsed[1].name != "SelectQuery":
        raise ValueError("Only SELECT is allowed")

    def visit(value):
        if getattr(value, "name", None) in {"ServiceGraphPattern", "DatasetClause"}:
            raise ValueError("SERVICE and FROM are not allowed in this local experiment")
        if isinstance(value, dict):
            for item in value.values():
                visit(item)
        elif isinstance(value, (list, tuple)) or type(value).__name__ == "ParseResults":
            for item in value:
                visit(item)

    visit(parsed)


def execute(snapshot, query):
    """Run in a disposable process with a time limit and a row limit."""
    validate_select(query)
    process = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--query", str(snapshot)],
        input=query,
        text=True,
        capture_output=True,
        timeout=45,
        check=True,
    )
    return json.loads(process.stdout)


def prepare(dump, session):
    """Freeze the data and schema; execute references outside the agent context."""
    dump, session = Path(dump).resolve(), Path(session).resolve()
    if not dump.is_file() or not session.is_file():
        raise FileNotFoundError("Supply a local RDF dump and a saved client session")
    reference = ROOT / "reference" / "AOP-Wiki-Queries"
    if not reference.exists():
        reference.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", REPOSITORY, str(reference)], check=True, capture_output=True
        )
    revision = subprocess.check_output(
        ["git", "-C", str(reference), "rev-parse", "HEAD"], text=True
    ).strip()
    if revision != REVISION:
        raise ValueError(f"Reference checkout must be at {REVISION}; found {revision}")
    if subprocess.check_output(
        ["git", "-C", str(reference), "status", "--porcelain"], text=True
    ).strip():
        raise ValueError("Reference checkout has local changes")
    run = ROOT / "runs" / time.strftime("%Y%m%d-%H%M%S")
    run.mkdir(parents=True, exist_ok=False)
    snapshot = run / "data.ttl"
    saved_session = run / "client-session.json"
    shutil.copyfile(dump, snapshot)
    shutil.copyfile(session, saved_session)
    graph = Graph().parse(snapshot, format="turtle")
    prefixes = "\n".join(f"PREFIX {prefix}: <{iri}>" for prefix, iri in graph.namespaces())
    cases = []
    for index, (relative, parameters, columns, kind) in enumerate(CASES, 1):
        raw = (reference / relative).read_text()
        description = next(
            line.removeprefix("# description: ").strip()
            for line in raw.splitlines()
            if line.startswith("# description:")
        )
        question = description + "\nParameters: " + json.dumps(parameters)
        question += "\nReturn these columns exactly: " + ", ".join(columns)
        query = raw
        for name, value in parameters.items():
            query = query.replace("{{" + name + "}}", value)
        case = {
            "id": index,
            "question": question,
            "columns": columns,
            "kind": kind,
            "source": relative,
            "reference_sha256": sha256(raw.encode()).hexdigest(),
        }
        try:
            case["expected"] = execute(snapshot, prefixes + "\n" + query)
            case["reference_status"] = "complete"
        except (ValueError, subprocess.SubprocessError) as error:
            case["reference_status"] = type(error).__name__
        cases.append(case)
    selected = {item[0] for item in CASES}
    excluded = [
        {
            "source": str(path.relative_to(reference)),
            "reason": "Outside the prespecified four-question smoke test",
        }
        for path in sorted(reference.rglob("*.rq"))
        if str(path.relative_to(reference)) not in selected
    ]
    manifest = {
        "reference_repository": REPOSITORY,
        "reference_revision": revision,
        "snapshot_sha256": sha256(snapshot.read_bytes()).hexdigest(),
        "schema_session_sha256": sha256(saved_session.read_bytes()).hexdigest(),
        "schema_source": str(session),
        "data_source": str(dump),
        "triples": len(graph),
        "scope": "Local default graph. Schema reused from the saved client session.",
        "cases": cases,
        "excluded": excluded,
    }
    (run / "reference.json").write_text(json.dumps(manifest, indent=2))
    return run


def score(expected, actual, columns, kind):
    """Compare RDF bindings, not the spelling of the queries."""
    if set(expected["head"]["vars"]) != set(columns) or set(actual["head"]["vars"]) != set(columns):
        raise ValueError("Output columns do not match the task")

    def rows(result):
        encoded = []
        for row in result["results"]["bindings"]:
            if any(value.get("type") == "bnode" for value in row.values()):
                raise ValueError("Blank node answers need graph-aware scoring")
            encoded.append(tuple(json.dumps(row.get(column), sort_keys=True) for column in columns))
        return Counter(encoded)

    wanted, found = rows(expected), rows(actual)
    exact = wanted == found
    if kind == "count":
        return {"exact": exact, "precision": None, "recall": None, "f1": None}
    hits = len(set(wanted) & set(found))
    precision = hits / len(found) if found else float(not wanted)
    recall = hits / len(wanted) if wanted else float(not found)
    return {
        "exact": exact,
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
    }


async def compare(run, models=None):
    """Run each model and question once, in sequence, with no reference feedback."""
    run = Path(run)
    models = configured_models() if models is None else models
    manifest = json.loads((run / "reference.json").read_text())
    rows = []
    for model in models:
        for case in manifest["cases"]:
            item = {
                "model": model,
                "case": case["id"],
                "status": "not_run",
                "exact": None,
                "expected_rows": len(
                    case.get("expected", {}).get("results", {}).get("bindings", [])
                ),
            }
            if case["reference_status"] != "complete":
                item["status"] = "reference_failed"
                rows.append(item)
                continue
            started = time.monotonic()
            with Client.from_session(
                run / "client-session.json",
                data_file=run / "data.ttl",
                max_subjects=500,
                max_rows=5000,
            ) as client:
                tools = ClientTools(client)
                agent = tools.agent(model, propose_query=True)
                with capture_run_messages() as messages:
                    try:
                        result = await asyncio.wait_for(
                            agent.run(
                                case["question"],
                                usage_limits=UsageLimits(
                                    request_limit=8, tool_calls_limit=12, total_tokens_limit=30000
                                ),
                            ),
                            timeout=180,
                        )
                        item["usage"] = asdict(result.usage)
                        item["proposal"] = result.output.model_dump()
                        if result.output.query is None:
                            item.update(status="unsupported", exact=False)
                        else:
                            actual = execute(run / "data.ttl", result.output.query)
                            item["result"] = actual
                            item.update(
                                score(case["expected"], actual, case["columns"], case["kind"])
                            )
                            item["status"] = "complete"
                    except Exception as error:
                        item.update(status=type(error).__name__, exact=False)
                    finally:
                        item["seconds"] = time.monotonic() - started
                        item["messages"] = json.loads(ModelMessagesTypeAdapter.dump_json(messages))
                        item["session"] = client.session_metadata()
            rows.append(item)
            (run / "results.json").write_text(json.dumps(rows, indent=2, default=str))
    columns = [
        "model",
        "case",
        "expected_rows",
        "status",
        "exact",
        "precision",
        "recall",
        "f1",
        "seconds",
    ]
    return pd.DataFrame(rows).reindex(columns=columns)


if __name__ == "__main__":
    import resource
    from itertools import islice

    resource.setrlimit(resource.RLIMIT_AS, (4 * 1024**3, 4 * 1024**3))
    query = sys.stdin.read()
    validate_select(query)
    graph = Graph().parse(sys.argv[2], format="turtle")
    result = graph.query(query)
    bindings = list(islice(result.bindings, 5001))
    if len(bindings) > 5000:
        raise ValueError("Query exceeds the 5000-row experiment limit")
    result.bindings = bindings
    print(result.serialize(format="json").decode())
