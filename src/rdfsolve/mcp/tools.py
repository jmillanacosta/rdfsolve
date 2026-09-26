"""The tools of one investigation over one RDF source.

The language model writes SPARQL. The tools show the schema, find resources,
give routes between classes, complete and check each query, explain empty
results, and run the final query on all data.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any

from rdfsolve.client.hydration import class_iri
from rdfsolve.client.query_fragments import identifier
from rdfsolve.client.retrieval import validate_outputs
from rdfsolve.mcp.sparql import add_prefixes, diagnose, has_limit, parse, terms, with_graphs
from rdfsolve.mcp.view import SchemaView
from rdfsolve.sparql_helper import EndpointError, QueryError

if TYPE_CHECKING:
    from rdfsolve.client.api import Client

_NUMBERS = ("integer", "decimal", "double", "float", "long", "int")


class Toolbox:
    """One source, the requested output variables and the final result."""

    def __init__(
        self,
        client: Client,
        *,
        output_variables: Sequence[str] = (),
        artifact_dir: str | Path | None = None,
    ) -> None:
        """Index the schema of the client; make no source request."""
        self.client = client
        self.view = SchemaView(client._schema)
        self.output_variables = [v.lstrip("?$") for v in output_variables]
        self.artifact_dir = Path(artifact_dir).resolve() if artifact_dir else None
        self.final: dict[str, Any] | None = None
        self.lock = RLock()

    def overview(self) -> str:
        """Describe the source, its classes, its graphs and the requested outputs."""
        graphs = self.client.graph_uris
        text = (
            self.view.overview()
            + "\nGraphs: "
            + (" ".join(f"<{g}>" for g in graphs) if graphs else "all data of the source")
        )
        if self.output_variables:
            text += "\nOutput variables of the answer: " + " ".join(
                "?" + v for v in self.output_variables
            )
        return text

    def schema(self, classes: Sequence[str] = (), search: Sequence[str] = ()) -> dict[str, Any]:
        """Describe classes by name, or find classes and properties by words."""
        if not classes and not search:
            return {"text": self.view.overview()}
        parts = []
        for name in classes:
            found = self.view.match_classes(name)
            if len(found) == 1:
                parts.append(self.view.card(found[0]))
            elif found:
                names = ", ".join(self.view.named(c) for c in found)
                parts.append(f"{name!r} names more than one class: {names}.")
            else:
                similar = self.view.similar(name, self.view.classes | self.view.properties)
                parts.append(
                    f"{name!r} is not a class of the schema."
                    + (f" Similar terms: {', '.join(similar)}." if similar else "")
                )
        if search:
            parts.append(self.view.search(search))
        return {"text": "\n\n".join(parts)}

    def paths(self, source: str, target: str, max_hops: int = 3) -> dict[str, Any]:
        """Give the shortest chains of properties between two classes."""
        ends = []
        for name in (source, target):
            found = self.view.match_classes(name)
            if len(found) != 1:
                raise ValueError(
                    f"{name!r} does not name one class. Use a class CURIE from schema."
                )
            ends.append(found[0])
        groups = self.view.routes(ends[0], ends[1], max_hops)
        if not groups:
            return {"text": f"No route of {max_hops} or fewer properties links these classes."}
        head = f"?source a {self.view.curie(ends[0])}, ?target a {self.view.curie(ends[1])}:"
        return {"text": "\n".join([head, *(self.view.route_text(g) for g in groups)])}

    def find(self, text: str, in_class: str | None = None, limit: int = 10) -> dict[str, Any]:
        """Find resources by IRI, identifier, name, or words in their text."""
        kind = None
        if in_class:
            found = self.view.match_classes(in_class)
            if len(found) != 1:
                raise ValueError(
                    f"{in_class!r} does not name one class. Use a class CURIE from schema."
                )
            kind = found[0]
        lines = self._resource(text) + self._identifiers(text)
        records = list(self.client.find(text, kind=kind, allow_partial=True))
        for record in records[:limit]:
            iri = str(vars(record)["uri"])
            lines.append(
                f"{self.view.curie(iri)} a {self.view.curie(class_iri(record))}: "
                + json.dumps(self.client.title(record), ensure_ascii=False)
            )
        if not records:
            evidence = self.client.search([text], kind=kind).evidence
            for item in evidence[:limit]:
                lines.append(
                    f"{self.view.curie(item['id'])} a {self.view.curie(item['type'])}: "
                    f"{self.view.curie(item['predicate'])} {_excerpt(item['text']['value'], text)}"
                )
        if not lines:
            return {"text": f"Nothing in the source has the name or text {text!r}."}
        return {"text": "\n".join(dict.fromkeys(lines))}

    def _resource(self, text: str) -> list[str]:
        """Describe an IRI, or a CURIE of a schema or registered prefix, that is a subject."""
        from rdfsolve.client.ontology import identifier_candidates

        iri = self.view.expand(text)
        try:
            iris = [iri] if iri is not None else identifier_candidates(text.strip())[0]
        except ValueError:
            return []
        iris = [iri for iri in iris if re.fullmatch(r"[^<>\"{}|^`\\\s]+", iri)]
        if not iris:
            return []
        values = " ".join(f"<{iri}>" for iri in iris)
        rows = self.client._select(
            with_graphs(
                f"SELECT DISTINCT ?s ?type WHERE {{ VALUES ?s {{ {values} }} ?s ?p ?o "
                "OPTIONAL { ?s a ?type } } LIMIT 20",
                self.client.graph_uris,
            )
        )
        types: dict[str, list[str]] = {}
        for row in rows:
            found = types.setdefault(row["s"]["value"], [])
            if "type" in row:
                found.append(self.view.curie(row["type"]["value"]))
        return [
            f"{self.view.curie(iri)} is in the source, "
            + ("a " + ", ".join(sorted(classes)) if classes else "with no class")
            for iri, classes in types.items()
        ]

    def _identifiers(self, text: str) -> list[str]:
        """Find resources that carry a registered identifier as a value."""
        if not re.fullmatch(r"[A-Za-z][\w.-]*:\S+", text.strip()):
            return []
        try:
            found = self.client.identify([text.strip()])
        except ValueError:
            return []
        return [
            f"{self.view.curie(m.resource)} {self.view.curie(m.predicate)} "
            + (self.view.curie(m.value) if m.kind == "uri" else json.dumps(m.value))
            for m in found[:10]
        ]

    def cell(self, term: dict[str, str] | None) -> Any:
        """Write one result value in short SPARQL form; None when it is unbound."""
        if term is None:
            return None
        if term["type"] == "uri":
            return self.view.curie(term["value"])
        if term["type"] == "bnode":
            return "_:" + term["value"]
        datatype = term.get("datatype", "")
        if datatype.endswith(_NUMBERS) and "#" in datatype:
            return term["value"]
        value = " ".join(term["value"].split())
        text = json.dumps(value if len(value) <= 100 else value[:100] + "…", ensure_ascii=False)
        if term.get("xml:lang"):
            return f"{text}@{term['xml:lang']}"
        if datatype and not datatype.endswith("#string"):
            return f"{text}^^{self.view.curie(datatype)}"
        return text

    def _prepare(self, text: str) -> tuple[str, Any, list[str]]:
        """Complete the prefixes of a query, check it, and give notes on unknown terms."""
        text, added = add_prefixes(text, self.view.prefixes)
        query = parse(text)
        notes = [f"Added PREFIX for {', '.join(added)}."] if added else []
        classes, properties = terms(query)
        for iri, known, kind in (
            *((c, self.view.classes, "class") for c in sorted(classes)),
            *((p, self.view.properties, "property") for p in sorted(properties)),
        ):
            if iri not in known:
                similar = self.view.similar(self.view.curie(iri), known)
                notes.append(
                    f"{self.view.curie(iri)} is not a {kind} of the schema."
                    + (f" Similar: {', '.join(similar)}." if similar else "")
                )
        return text, query, notes

    def run(self, sparql: str, limit: int = 10) -> dict[str, Any]:
        """Run a query and give its first rows, with notes on terms and empty results."""
        text, query, notes = self._prepare(sparql)
        scoped = with_graphs(text, self.client.graph_uris)
        own_limit = has_limit(query)
        rows = self.client._select(scoped if own_limit else f"{scoped}\nLIMIT {limit + 1}")
        columns = [str(v) for v in query.algebra["PV"]]
        result: dict[str, Any] = {
            "columns": columns,
            "rows": [[self.cell(row.get(c)) for c in columns] for row in rows[:limit]],
            "more_rows": len(rows) > limit,
        }
        if not rows:
            try:
                notes += diagnose(
                    query,
                    lambda q: self.client._select(with_graphs(q, self.client.graph_uris)),
                    self._short,
                )
            except EndpointError:
                notes.append("The triple patterns could not be checked one by one: source error.")
        else:
            empty = [c for c in columns if not any(c in row for row in rows)]
            if empty:
                notes.append(
                    "No value in these rows for " + ", ".join("?" + c for c in empty) + "."
                )
        missing = [v for v in self.output_variables if v not in columns]
        if missing:
            notes.append(
                "The answer needs the variables " + ", ".join("?" + v for v in missing) + "."
            )
        if notes:
            result["notes"] = notes
        return result

    def _short(self, text: str) -> str:
        """Write the IRIs in a text as CURIEs."""
        return re.sub(r"<([^<>\s]*)>", lambda match: self.view.curie(match.group(1)), text)

    def answer(self, sparql: str) -> dict[str, Any]:
        """Run the final query on all data, keep the rows, and end the investigation."""
        if self.final is not None:
            raise ValueError("The answer was already given.")
        text, query, notes = self._prepare(sparql)
        validate_outputs(query.algebra["PV"], self.output_variables)
        scoped = with_graphs(text, self.client.graph_uris)
        try:
            rows = self.client._select(scoped, exhaustive=True)
        except QueryError:
            # Pages cannot be joined for GROUP_CONCAT, SAMPLE and random values.
            rows = self.client._select(scoped)
            notes.append("The query was run in one request, not in pages.")
        columns = [str(v) for v in query.algebra["PV"]]
        ref = identifier("result", scoped)
        self.final = {
            "query": scoped,
            "bindings": rows,
            "execution": dict(self.client.last_query_execution),
        }
        if self.artifact_dir is not None:
            self.artifact_dir.mkdir(parents=True, exist_ok=True)
            (self.artifact_dir / f"{ref}.json").write_text(
                json.dumps({"query": scoped, "bindings": rows}, ensure_ascii=False)
            )
        if not rows:
            notes.append("The final query gave no rows.")
        return {
            "state": "complete",
            "rows": len(rows),
            "columns": columns,
            "first_rows": [[self.cell(row.get(c)) for c in columns] for row in rows[:3]],
            "result_ref": ref,
            "execution": self.final["execution"],
            **({"notes": notes} if notes else {}),
        }

    def diagnostics(self) -> dict[str, Any]:
        """Report the source queries of the investigation."""
        records = [vars(r) for r in self.client._records()]
        return {
            "source_queries": len(records),
            "queries": [
                {
                    k: v
                    for k, v in r.items()
                    if k in {"query", "success", "elapsed_seconds", "error"}
                }
                for r in records
            ],
            "answered": self.final is not None,
        }


def _excerpt(value: str, text: str, width: int = 60) -> str:
    """Give the part of a value around the first match of a text."""
    value = " ".join(value.split())
    at = value.casefold().find(text.casefold())
    start = max(0, at - width) if at >= 0 else 0
    part = value[start : start + 2 * width + len(text)]
    return json.dumps(
        ("…" if start else "") + part + ("…" if start + len(part) < len(value) else ""),
        ensure_ascii=False,
    )
