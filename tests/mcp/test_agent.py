"""A model answers through the real stdio server; the caller gets all rows of the answer."""

import asyncio
import json

from conftest import E
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import FunctionModel
from rdfsolve.api import ask_rdf

AOPS = "SELECT ?aop ?label WHERE { ?aop a ex:Pathway ; rdfs:label ?label }"


def scripted(steps, seen):
    """Give one planned tool call, or text, per model request."""

    def model(messages, info):
        seen.append(messages)
        step = steps[min(len(seen), len(steps)) - 1]
        if isinstance(step, str):
            return ModelResponse(parts=[TextPart(step)])
        return ModelResponse(parts=[ToolCallPart(step[0], step[1], tool_call_id=str(len(seen)))])

    return FunctionModel(model)


def ask(files, tmp_path, steps, seen):
    schema, data = files
    return asyncio.run(
        ask_rdf(
            "List the pathways and their names.",
            schema=schema,
            data_file=data,
            output_variables=["aop", "label"],
            model=scripted(steps, seen),
            output_dir=tmp_path / "out",
        )
    )


def test_the_answer_rows_reach_the_caller_not_the_model(files, tmp_path):
    seen = []
    steps = [("schema", {"classes": ["Adverse Outcome Pathway"]}), ("run", {"sparql": AOPS, "limit": 1}),
             ("answer", {"sparql": AOPS})]
    answer = ask(files, tmp_path, steps, seen)
    assert answer.state == "complete", answer.error
    assert {r["aop"]["value"] for r in answer.bindings} == {str(E.aop1), str(E.aop2)}
    assert answer.query.startswith("PREFIX ex:") and answer.text == "Retrieved 2 rows. Notes: Added PREFIX for ex, rdfs."
    assert len(seen) == 3 and answer.usage.requests == 3
    instructions = seen[0][0].instructions
    assert "Source summary:" in instructions and "ex:Pathway" in instructions
    assert "Do not put LIMIT in the final query" in instructions
    visible = json.dumps([str(m) for m in seen])
    assert sum(name in visible for name in ["Liver pathway", "Thyroid pathway"]) == 1, "One row only"
    assert answer.package["source_queries"] >= 2 and answer.diagnostics()["tool_calls"] == 3
    saved = sorted(p.name.split(".")[1] for p in (tmp_path / "out").iterdir())
    assert saved == ["answer", "calls", "package"]


def test_repeated_calls_and_text_endings_are_blocked(files, tmp_path):
    repeated = ask(files, tmp_path, [("run", {"sparql": AOPS})], [])
    assert (repeated.state, repeated.error["code"]) == ("blocked", "no_progress")
    assert len(repeated.calls) == 3
    ended = ask(files, tmp_path, ["I cannot answer."], [])
    assert (ended.state, ended.error["code"]) == ("blocked", "not_executed")
    assert "I cannot answer." in ended.text and ended.bindings == []
