"""Generate equivalent Python code snippets for rdfsolve API calls.

Each function returns a string of ready-to-run Python code that
reproduces the same operation using the ``rdfsolve`` package
directly.  The snippets are included in every API response so
users can learn how to script their workflows.
"""

from __future__ import annotations

import json
from typing import Any

# -- Formatting helpers --------------------------------------------------------


def _fmt(obj: Any, indent: int = 4) -> str:
    """Format a Python object as a readable literal."""
    raw = json.dumps(obj, indent=indent, ensure_ascii=False)
    return (
        raw.replace(": null", ": None")
        .replace(": true", ": True")
        .replace(": false", ": False")
    )


def _fmt_strings(lst: list[str]) -> str:
    """Format a list of strings as a Python literal."""
    if len(lst) <= 3:
        items = ", ".join(f'"{s}"' for s in lst)
        return f"[{items}]"
    lines = ",\n    ".join(f'"{s}"' for s in lst)
    return f"[\n    {lines},\n]"


# -- Compose -------------------------------------------------------------------


def compose_snippet(
    paths: list[dict[str, Any]],
    prefixes: dict[str, str],
    options: dict[str, Any] | None = None,
) -> str:
    """Return Python code for ``compose_query_from_paths()``."""
    opts = options or {}
    parts = [
        "from rdfsolve import compose_query_from_paths",
        "",
        f"paths = {_fmt(paths)}",
        "",
        f"prefixes = {_fmt(prefixes)}",
        "",
        f"options = {_fmt(opts)}",
        "",
        "result = compose_query_from_paths(",
        "    paths=paths,",
        "    prefixes=prefixes,",
        "    **options,",
        ")",
        'print(result["query"])',
    ]
    return "\n".join(parts)


# -- Execute SPARQL ------------------------------------------------------------


def execute_sparql_snippet(
    query: str,
    endpoint: str,
    method: str = "GET",
    timeout: int = 30,
) -> str:
    """Return Python code for ``execute_sparql()``."""
    q = query.replace('"""', r'\"\"\"')
    parts = [
        "from rdfsolve import execute_sparql",
        "",
        f'query = """\\\n{q}"""',
        "",
        f'endpoint = "{endpoint}"',
        "",
        "result = execute_sparql(",
        "    query=query,",
        "    endpoint=endpoint,",
        f'    method="{method}",',
        f"    timeout={timeout},",
        ")",
        "",
        "print(f'Rows: {result[\"row_count\"]}, Time: {result[\"duration_ms\"]}ms')",
        "for row in result['rows'][:5]:",
        "    print(row)",
    ]
    return "\n".join(parts)


# -- IRI resolution ------------------------------------------------------------


def resolve_iris_snippet(
    iris: list[str],
    endpoints: list[dict[str, Any]],
    timeout: int = 15,
) -> str:
    """Return Python code for ``resolve_iris()``."""
    parts = [
        "from rdfsolve import resolve_iris",
        "",
        f"iris = {_fmt_strings(iris)}",
        "",
        f"endpoints = {_fmt(endpoints)}",
        "",
        "result = resolve_iris(",
        "    iris=iris,",
        "    endpoints=endpoints,",
        f"    timeout={timeout},",
        ")",
        "",
        'for iri, info in result["resolved"].items():',
        "    print(f\"{iri} -> {info['types']}\")",
    ]
    return "\n".join(parts)


# -- Export --------------------------------------------------------------------


def export_query_snippet(
    query: str,
    query_type: str = "select",
    prefixes: dict[str, str] | None = None,
    endpoint: str | None = None,
) -> str:
    """Return Python code to export a query as JSON-LD."""
    q = query.replace('"""', r'\"\"\"')
    pfx = prefixes or {}
    cap = query_type.capitalize()
    parts = [
        "import json",
        "from datetime import datetime, timezone",
        "",
        f'query = """\\\n{q}"""',
        f'query_type = "{query_type}"',
        f"prefixes = {_fmt(pfx)}",
        "",
        "jsonld = {",
        '    "@context": {',
        "        **prefixes,",
        '        "sh": "http://www.w3.org/ns/shacl#",',
        "    },",
        '    "@type": [',
        '        "sh:SPARQLExecutable",',
        f'        "sh:SPARQL{cap}Executable",',
        "    ],",
        f'    "sh:{query_type}": query,',
        '    "schema:dateCreated": datetime.now(timezone.utc).isoformat(),',
        "}",
    ]
    if endpoint:
        parts += [
            "",
            'jsonld["schema:target"] = {',
            '    "@type": "sd:Service",',
            f'    "sd:endpoint": "{endpoint}",',
            "}",
        ]
    parts += ["", "print(json.dumps(jsonld, indent=2))"]
    return "\n".join(parts)
