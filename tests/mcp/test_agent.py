import asyncio
import json

from conftest import E
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel
from rdfsolve.api import ask_rdf
from rdfsolve.mcp.workflow import launch_config


def files(session, folder):
    schema, data = (folder / "schema.json", folder / "data.ttl")
    schema.write_text(json.dumps(session.client._schema.to_dict()))
    session.client.source.serialize(destination=data, format="turtle")
    return (schema, data)


def test_generic_api_and_real_stdio_preserve_data_boundary(session, tmp_path):
    schema, data = files(session, tmp_path)
    seen = []

    def model(messages, info):
        encoded = json.dumps([str(m) for m in messages])
        assert "Assay A" not in encoded and "Assay B" not in encoded
        seen.append(messages)
        step = len(seen)
        if step == 1:
            name, args = (
                "rdf_schema",
                {
                    "goals": [
                        {
                            "clause": "Return pathway identities",
                            "kind": "output",
                            "concept": "Adverse Outcome Pathway",
                            "owner": "a",
                        }
                    ],
                    "concepts": ["Adverse Outcome Pathway"],
                },
            )
        elif step == 2:
            name, args = (
                "rdf_prepare",
                {
                    "patterns": [
                        {"reference": session.catalogue.type_refs[str(E.AOP)], "bindings": ["a"]}
                    ],
                    "outputs": ["a"],
                    "grounding": {
                        "g1": {
                            "project": ["a"],
                            "evidence": [session.catalogue.type_refs[str(E.AOP)]],
                        }
                    },
                },
            )
        elif step == 3:
            results = [
                p.content
                for m in messages
                for p in m.parts
                if getattr(p, "tool_name", "") == "rdf_prepare" and hasattr(p, "content")
            ]
            name, args = ("rdf_finish", {"query_ref": results[-1]["query_ref"]})
        else:
            raise AssertionError("Unexpected model request after final execution")
        return ModelResponse(parts=[ToolCallPart(name, args, tool_call_id=str(step))])

    answer = asyncio.run(
        ask_rdf(
            "List pathways",
            schema=schema,
            data_file=data,
            source_id="independent-local-database",
            output_variables=["a"],
            model=FunctionModel(model),
            output_dir=tmp_path,
        )
    )
    assert answer.state == "complete", answer.error
    assert {r["a"]["value"] for r in answer.bindings} == {
        str(E.humanAOP),
        str(E.mouseAOP),
        str(E.noChemicalAOP),
    }
    assert len(seen) == 3 and answer.usage.requests == 3
    assert "3 rows" in answer.text
    assert answer.package["source_queries"] == 1
    assert len(list(tmp_path.glob("*.answer.json"))) == 1
    assert "aopwikirdf" not in json.dumps(launch_config(schema, source_id="other"))
