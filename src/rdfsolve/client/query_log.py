"""Render retained session queries without contacting the data source."""

from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd


class QueryLog:
    """Show named executions with their query text and returned data."""

    def __init__(self, session: dict[str, Any]) -> None:
        """Read a saved or current session without executing queries."""
        self.queries = session["queries"]
        self.tool_calls = session.get("tool_calls", [])
        self.agent = session.get("agent")
        self.names = {
            query_id: step["name"] for step in session["steps"] for query_id in step["query_ids"]
        }

    @classmethod
    def read(cls, path: str | Path) -> QueryLog:
        """Open a saved session log without contacting its source."""
        path = Path(path)
        session = json.loads(path.read_text(encoding="utf-8"))
        if name := session.get("context_file"):
            if Path(name).name != name:
                raise ValueError("Context must be beside the session log")
            session.update(json.loads(path.with_name(name).read_text(encoding="utf-8")))
        if name := session.get("queries_file"):
            if Path(name).name != name:
                raise ValueError("Query journal must be beside the session log")
            with path.with_name(name).open(encoding="utf-8") as stream:
                queries = {
                    row["id"]: row for line in stream if line.strip() for row in [json.loads(line)]
                }
            session["queries"] = [queries[row["id"]] for row in session["queries"]]
        return cls(session)

    def tools(self) -> pd.DataFrame:
        """List tool calls and the query executions used by each call."""
        return pd.DataFrame(
            [
                {
                    "Tool": call["tool"],
                    "Name": call.get("arguments", {}).get("name")
                    or call.get("arguments", {}).get("selection")
                    or call.get("arguments", {}).get("kind")
                    or call.get("arguments", {}).get("text")
                    or call["tool"],
                    "Status": call["status"],
                    "Queries": call["query_ids"],
                }
                for call in self.tool_calls
            ],
            columns=["Tool", "Name", "Status", "Queries"],
        )

    def __repr__(self) -> str:
        return f"QueryLog({len(self.queries)} executions; open in a notebook to view results)"

    def _repr_html_(self) -> str:
        sections = [
            f"<details><summary>{escape(call['tool'])} — {escape(call['status'])}</summary>"
            + "<pre>"
            + escape(json.dumps(call, indent=2, ensure_ascii=False))
            + "</pre></details>"
            for call in self.tool_calls
        ]
        for query in self.queries:
            name = self.names.get(query["id"]) or query["purpose"].replace("_", " ")
            name = name or query["query_type"]
            result = query.get("result")
            if not query["success"]:
                status, body = "Failed", escape(query.get("error") or "Request failed")
            elif not query.get("result_retained"):
                status, body = "Response not retained", ""
            elif isinstance(result, dict) and "results" in result:
                rows = result["results"]["bindings"]
                columns = result.get("head", {}).get("vars")
                table = [{key: _term(value) for key, value in row.items()} for row in rows]
                status = f"{len(rows)} rows"
                body = (
                    pd.DataFrame(table, columns=columns)
                    .fillna("")
                    .to_html(index=False, escape=True)
                )
            else:
                value = (
                    result.get("boolean")
                    if isinstance(result, dict) and "boolean" in result
                    else result
                )
                status = str(value) if isinstance(value, bool) else "Response"
                body = (
                    "<pre>"
                    + escape(value if isinstance(value, str) else json.dumps(value, indent=2))
                    + "</pre>"
                )
            fallback = " · HTTP fallback" if query.get("fallback_used") else ""
            timing = ""
            if "elapsed_seconds" in query:
                timing = (
                    f" · {query['elapsed_seconds']:.2f} s total"
                    f" ({query.get('request_seconds', 0):.2f} s HTTP,"
                    f" {query.get('wait_seconds', 0):.2f} s waiting)"
                )
            sections.append(
                f"<details><summary>{query['id']}. {escape(name)} — "
                f"{escape(status)}{fallback}{timing}</summary>"
                "<details><summary>Query text</summary><pre>"
                + escape(query["query"])
                + "</pre></details>"
                + body
                + "</details>"
            )
        return "".join(sections)


def _term(value: dict[str, Any]) -> str:
    text = str(value["value"])
    if value.get("xml:lang"):
        return text + " @" + str(value["xml:lang"])
    if value.get("datatype"):
        return text + " [" + str(value["datatype"]) + "]"
    return text
