"""Expand grounded property paths, serialize RDF terms and parse scoped SELECT queries."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pyparsing import ParseBaseException, ParseResults
from rdflib import RDF, URIRef, Variable
from rdflib.paths import Path as RDFPath
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import CompValue

from rdfsolve.client.hydration import _iri
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath

PROTECTED = re.compile(
    r"""\"\"\"(?:\\.|(?!\"\"\")[\s\S])*\"\"\"|\'\'\'(?:\\.|(?!\'\'\')[\s\S])*\'\'\'|\"(?:\\.|[^\"\\])*\"|\'(?:\\.|[^\'\\])*\'|<[^<>\s]*>|\#[^\r\n]*"""
)
MACRO = re.compile(r"\{\{\s*([A-Za-z][\w-]*)\s*((?:[?$][A-Za-z_][\w]*\s*)*)\}\}")
VAR = re.compile(r"^[?$][A-Za-z_][\w]*$")


def identifier(prefix: str, value: Any) -> str:
    """Derive a stable handle from retained content."""
    return (
        prefix
        + "_"
        + sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:12]
    )


def walk(value):
    """Traverse parsed query expressions and RDF path objects."""
    yield value
    if isinstance(value, (dict, CompValue)):
        for item in value.values():
            yield from walk(item)
    elif isinstance(value, RDFPath):
        for item in vars(value).values():
            yield from walk(item)
    elif isinstance(value, (list, tuple, set, ParseResults)):
        for item in value:
            yield from walk(item)


def path_size(path: PropertyPath) -> dict[str, int | None]:
    """Return structural edge count and minimum/maximum traversal length."""
    if path.operator == "predicate":
        return {"edges": 1, "min_hops": 1, "max_hops": 1}
    sizes = [path_size(p) for p in path.items]
    edges = sum(s["edges"] for s in sizes)
    if path.operator == "inverse":
        return sizes[0]
    if path.operator == "alternative":
        return {
            "edges": edges,
            "min_hops": min(s["min_hops"] for s in sizes),
            "max_hops": None
            if any(s["max_hops"] is None for s in sizes)
            else max(s["max_hops"] for s in sizes),
        }
    if path.operator == "sequence":
        return {
            "edges": edges,
            "min_hops": sum(s["min_hops"] for s in sizes),
            "max_hops": None
            if any(s["max_hops"] is None for s in sizes)
            else sum(s["max_hops"] for s in sizes),
        }
    return {
        "edges": edges,
        "min_hops": sizes[0]["min_hops"] if path.operator == "one_or_more" else 0,
        "max_hops": sizes[0]["max_hops"] if path.operator == "zero_or_one" else None,
    }


def linear_steps(path: PropertyPath, reverse: bool = False):
    """Expose intermediate variables only for finite sequences/inverses."""
    if path.operator == "predicate":
        return [(path.iri, reverse)]
    if path.operator == "inverse":
        return linear_steps(path.items[0], not reverse)
    if path.operator != "sequence":
        raise ValueError(
            "This path has alternatives/repetition; use two endpoints, or inspect a narrower path."
        )
    items = list(reversed(path.items)) if reverse else path.items
    return [step for p in items for step in linear_steps(p, reverse)]


@dataclass
class Fragment:
    """A retained class, field, path or exact RDF term."""

    kind: str
    label: str
    iri: str | None = None
    path: PropertyPath | None = None
    owner: str | None = None
    field_name: str | None = None

    steps: list[tuple[str, str, str, bool]] = field(default_factory=list)
    anchors: dict[int, RdfTerm | list[RdfTerm]] = field(default_factory=dict)
    endpoint_types: dict[int, str] = field(default_factory=dict)
    term_kinds: dict[int, tuple[str, str | None]] = field(default_factory=dict)
    term: RdfTerm | None = None
    basis: str = "saved schema"
    description: str | None = None

    def render(self, args: list[str], allocate: Callable[[], str]) -> str:
        """Expand bindings with fresh internal variables and exact terms."""
        if any(not VAR.fullmatch(a) for a in args):
            raise ValueError("Fragment bindings must be SPARQL variables.")
        if self.kind == "term":
            if args or self.term is None:
                raise ValueError("An RDF term insert takes no variables.")
            if self.term.kind == "bnode":
                raise ValueError(
                    "Blank node identifiers are response-scoped; probe their anchored path in one query."
                )
            return _iri(self.term.value) if self.term.kind == "uri" else self.term.to_rdf().n3()
        if self.kind == "type":
            if len(args) != 1:
                raise ValueError("A class insert takes one variable.")
            return f"{args[0]} a {_iri(self.iri)} ."
        if self.path is None or len(args) < 2:
            raise ValueError("A field/path insert takes two endpoint variables.")
        if self.steps:
            if len(args) not in {2, len(self.steps) + 1}:
                raise ValueError(
                    f"Use two endpoints or {len(self.steps) + 1} explicit step variables."
                )
            nodes = (
                args
                if len(args) > 2
                else [args[0], *[allocate() for _ in self.steps[:-1]], args[-1]]
            )
            lines = []
            for i, (s, p, o, back) in enumerate(self.steps):
                left, right = (nodes[i + 1], nodes[i]) if back else (nodes[i], nodes[i + 1])
                lines.append(f"{left} {_iri(p)} {right} .")
                if s:
                    lines.append(f"{nodes[i]} a {_iri(s)} .")
                if o:
                    lines.append(f"{nodes[i + 1]} a {_iri(o)} .")
        elif len(args) == 2:
            nodes = args
            lines = [f"{args[0]} {path_to_sparql(self.path)} {args[1]} ."]
        else:
            steps = linear_steps(self.path)
            if len(args) != len(steps) + 1:
                raise ValueError(
                    f"This sequence needs {len(steps) + 1} variables to expose its intermediates."
                )
            nodes = args
            lines = [
                f"{args[i + 1] if back else args[i]} {_iri(p)} {args[i] if back else args[i + 1]} ."
                for i, (p, back) in enumerate(steps)
            ]
        if self.kind == "field" and self.owner:
            lines.insert(0, f"{nodes[0]} a {_iri(self.owner)} .")
        for position, cls in self.endpoint_types.items():
            lines.insert(0, f"{nodes[position]} a {_iri(cls)} .")
        for position, (kind, datatype) in self.term_kinds.items():
            variable = nodes[position]
            function = {"Literal": "isLiteral", "Resource": "isIRI", "BlankNode": "isBlank"}[kind]
            test = f"{function}({variable})"
            if datatype:
                test += f" && datatype({variable}) = {_iri(datatype)}"
            lines.append(f"FILTER({test})")
        for position, term in self.anchors.items():
            terms = term if isinstance(term, list) else [term]
            if any(t.kind != "uri" for t in terms):
                raise ValueError("A resource path anchor must be an IRI.")
            lines.insert(
                0, f"VALUES {nodes[position]} {{ {' '.join(t.to_rdf().n3() for t in terms)} }}"
            )
        return "\n".join(dict.fromkeys(lines))


class QueryPattern(BaseModel):
    """A retained class, field or route attached to named result roles."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    reference: str = Field(
        description="Exact ref returned by discovery, never a label or guessed IRI."
    )
    bindings: list[str] = Field(
        min_length=1,
        max_length=7,
        description="Named roles: one for a class, source and value for a field or route. Reuse a role to join the same resource.",
    )
    optional: bool = False


def network_query(catalogue, patterns, outputs, *, values=None, text=None, distinct=True):
    """Join retained fragments and nest optional descendants under their parent."""
    patterns = [
        p if isinstance(p, QueryPattern) else QueryPattern.model_validate(p) for p in patterns
    ]
    if not patterns or not outputs or len(set(outputs)) != len(outputs):
        raise ValueError("Supply connected patterns and unique output roles")
    names = [*outputs, *(v for p in patterns for v in p.bindings)]
    if not all(VAR.fullmatch("?" + v) for v in names):
        raise ValueError("Roles must be simple column names")
    blocks, scopes = {0: []}, {}
    pending = sorted(patterns, key=lambda p: p.optional)
    while pending:
        progressed = False
        for pattern in pending[:]:
            bound = [v for v in pattern.bindings if v in scopes]
            if scopes and not bound:
                continue
            if not scopes and pattern.optional:
                raise ValueError("Start the network with a required class or route")
            ref = pattern.reference
            fragment = catalogue.fragments.get(ref)
            if fragment is None or fragment.kind not in {"type", "field", "path"}:
                raise ValueError(f"Select a retained class, field or path: {ref}")
            scope_ids = {scopes[v] for v in bound} - {0}
            if len(scope_ids) > 1:
                raise ValueError("Resolve a join between separate optional branches explicitly")
            parent = next(iter(scope_ids), 0)
            if parent and not pattern.optional:
                raise ValueError("A required relationship cannot depend on optional metadata")
            scope = len(blocks) if pattern.optional else parent
            if pattern.optional:
                if not set(pattern.bindings) - scopes.keys():
                    raise ValueError("An optional pattern must introduce a metadata role")
                blocks[scope] = []
                blocks[parent].append(scope)
            args = ["?" + v for v in pattern.bindings]
            # Render once here to validate the ports; compilation owns variable allocation.
            fragment.render(args, lambda: "?internal")
            blocks[scope].append("{{" + ref + " " + " ".join(args) + "}}")
            for v in pattern.bindings:
                scopes.setdefault(v, scope)
            pending.remove(pattern)
            progressed = True
        if not progressed:
            raise ValueError("Every required role must connect to the same network")
    if set(outputs) - scopes.keys():
        raise ValueError(
            f"Unbound output roles: {sorted(set(outputs) - scopes.keys())}. "
            f"Available roles: {sorted(scopes)}. Add field patterns binding the requested outputs."
        )
    for variable, ref in (values or {}).items():
        if variable not in scopes or scopes[variable]:
            raise ValueError("An exact restriction needs a required role")
        fragment = catalogue.fragments.get(ref)
        if fragment is None or fragment.kind != "term":
            raise ValueError("Select an exact retained RDF term for a value restriction")
        blocks[0].append("VALUES ?" + variable + " { {{" + ref + "}} }")
    from rdflib import Literal

    for variable, value in (text or {}).items():
        if variable not in scopes or scopes[variable]:
            raise ValueError("A text restriction needs a required field role")
        term = Literal(value).n3()
        blocks[0].append(
            f"FILTER(isLiteral(?{variable}) && CONTAINS(LCASE(STR(?{variable})), LCASE({term})))"
        )

    def render(scope):
        return "\n".join(
            "OPTIONAL {\n" + render(item) + "\n}" if isinstance(item, int) else item
            for item in blocks[scope]
        )

    return (
        "SELECT "
        + ("DISTINCT " if distinct else "")
        + " ".join("?" + v for v in outputs)
        + " WHERE {\n"
        + render(0)
        + "\n}"
    )


def path_query(catalogue, ref, source, target, fields):
    """Compose endpoint retrieval through the shared network builder."""
    fragment = catalogue.fragments.get(ref)
    if fragment is None or fragment.kind not in {"path", "field"}:
        raise ValueError("Select a retained path or field reference from this client")
    if source == target or set(fields) - {source, target}:
        raise ValueError("Use distinct endpoints and fields owned by their source or target column")
    owners = {
        source: fragment.owner or fragment.endpoint_types.get(0),
        target: fragment.endpoint_types.get(-1),
    }
    if fragment.steps:
        owners.update({source: fragment.steps[0][0], target: fragment.steps[-1][2]})
    if fragment.kind == "field":
        targets = catalogue.metadata[ref].get("targets", [])
        owners[target] = targets[0] if len(targets) == 1 else None
    patterns = [QueryPattern(reference=ref, bindings=[source, target])]
    outputs = [source, target]
    for role, names in fields.items():
        if not owners[role]:
            raise ValueError(f"Choose a typed route before selecting fields on {role}")
        for name in catalogue._field_names(owners[role], names):
            output = role + "_" + name
            patterns.append(
                QueryPattern(
                    reference=catalogue.field_refs[(owners[role], name)],
                    bindings=[role, output],
                    optional=True,
                )
            )
            outputs.append(output)
    return network_query(catalogue, patterns, outputs)


class QuerySyntaxError(ValueError):
    """A rejected query with JSON-safe locations and evidence-based repair hints."""

    def __init__(self, text, cause, fragments, *, phase):
        """Retain parser locations and grounded repair hints."""
        line = cause.lineno
        column = cause.col
        source_line = text.splitlines()[line - 1] if text.splitlines() else ""
        left = max(0, column - 81)
        self.detail = {
            "code": "sparql_syntax",
            "message": cause.msg,
            "phase": phase,
            "line": line,
            "column": column,
            "excerpt": source_line[left : left + 240],
            "caret_column": column - left,
            "hints": _syntax_hints(text, fragments),
            "executed": False,
        }
        super().__init__(f"{cause.msg} (line {line}, column {column})")


def _syntax_hints(text, fragments):
    visible = PROTECTED.sub(
        lambda m: m.group() if m.group().startswith("<") else re.sub(r"[^\r\n]", " ", m.group()),
        text,
    )
    prefixes = dict(re.findall(r"(?i)\bPREFIX\s+([A-Za-z_][\w-]*|):\s*<([^<>]+)>", visible))
    classes = {f.iri for f in fragments.values() if f.kind == "type"}
    pattern = r"([?$][A-Za-z_][\w]*)\s+(<[^<>\s]+>|(?:[A-Za-z_][\w-]*|):[\w-]+)\s*\.(?=\s|$|})"
    hints = []
    bare = PROTECTED.sub(lambda m: " " * len(m.group()), text)
    for match in re.finditer(r"https?://[^\s{};,()]+", bare):
        hints.append(
            {
                "code": "iri_brackets",
                "found": match.group()[:240],
                "message": "Put absolute IRIs inside <...>.",
            }
        )
    if re.search(r"(?i)(?<![\w:])strcontains\s*\(", bare):
        hints.append(
            {
                "code": "function_name",
                "message": "Use CONTAINS(string, substring); STRCONTAINS is not a SPARQL function.",
            }
        )
    stack = []
    for match in re.finditer(r"[{}]|(?i:\bLIMIT\b)", bare):
        word = match.group().upper()
        if word == "{":
            stack.append(match.end())
        elif word == "}" and stack:
            stack.pop()
        elif (
            word == "LIMIT"
            and stack
            and not re.search(r"(?i)\bSELECT\b", bare[stack[-1] : match.start()])
        ):
            hints.append(
                {
                    "code": "limit_placement",
                    "message": "Place LIMIT after the closing brace of WHERE.",
                }
            )
    hints = hints[:4]
    for match in re.finditer(pattern, visible):
        var, token = match.groups()
        iri = (
            token[1:-1]
            if token.startswith("<")
            else prefixes.get(token.split(":", 1)[0], "") + token.split(":", 1)[1]
        )
        if iri not in classes:
            continue
        hints.append(
            {
                "code": "missing_type_predicate",
                "line": text.count("\n", 0, match.start()) + 1,
                "found": match.group().strip(),
                "suggestion": f"{var} a {token} .",
                "message": "This two-term statement names a known class. A type assertion needs the predicate a (rdf:type). No repair was applied.",
            }
        )
        if len(hints) == 4:
            break
    return hints[:4]


def _parse(text, fragments, *, phase):
    try:
        return prepareQuery(text)
    except ParseBaseException as exc:
        raise QuerySyntaxError(text, exc, fragments, phase=phase) from exc
    except Exception as exc:
        if str(exc).startswith("Unknown namespace prefix"):
            raise ValueError(
                str(exc)
                + ". Declare the prefix explicitly using a discovered namespace, or use the returned fragment insert. No prefix was inferred."
            ) from exc
        raise


@dataclass
class PreparedQuery:
    """Keep a scoped query and the evidence used to construct it."""

    ref: str
    template: str
    sparql: str
    variables: list[str]
    uses: list[dict[str, Any]]
    warnings: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)


def _expand_references(template, fragments, expand):
    tokens = [
        m
        for m in re.finditer(
            MACRO.pattern + "|" + PROTECTED.pattern + r"|[?$][A-Za-z_]\w*|[A-Za-z_][\w:-]*|[^\s]",
            template,
        )
        if not m.group().startswith("#")
    ]
    values = [m.group() for m in tokens]
    chunks, end, i = [], 0, 0
    while i < len(tokens):
        value = values[i]
        macro = MACRO.fullmatch(value)
        if macro:
            rendered = expand(macro[1], macro[2].split())
            first, last = i, i
        elif value in fragments or re.fullmatch(r"[tfpe]_[a-f0-9]{12}", value):
            if value not in fragments:
                raise ValueError(
                    f"Unknown fragment {value}; discover a retained class, field or entity."
                )
            fragment = fragments[value]
            first, last, args = i, i, []
            before = values[i - 1] if i else ""
            after = values[i + 1 : i + 3]
            if fragment.kind == "term":
                rendered = expand(value, [])
            else:
                if (
                    fragment.kind == "type"
                    and i >= 2
                    and before == "a"
                    and VAR.fullmatch(values[i - 2])
                ):
                    first, args = i - 2, [values[i - 2]]
                elif after and VAR.fullmatch(after[0]) and before in {"{", ".", "}"}:
                    count = 1 if fragment.kind == "type" else 2
                    args, last = after[:count], i + count
                elif VAR.fullmatch(before) and fragment.path and after and VAR.fullmatch(after[0]):
                    first, last, args = i - 1, i + 1, [before, after[0]]
                elif before == ";" and fragment.path and after and VAR.fullmatch(after[0]):
                    start = i - 2
                    while start >= 0 and values[start] not in {"{", ".", "}"}:
                        start -= 1
                    subject = start + 1
                    if values[subject] in fragments:
                        subject += 1
                    first, last, args = i - 1, i + 1, [values[subject], after[0]]
                if not args or not all(VAR.fullmatch(a) for a in args):
                    raise ValueError(
                        f"Use {value} with its binding variables, or copy its returned SPARQL pattern."
                    )
                following = values[last + 1] if last + 1 < len(values) else ""
                if following.upper() not in {
                    ".",
                    "}",
                    ";",
                    "OPTIONAL",
                    "FILTER",
                    "VALUES",
                    "UNION",
                    "",
                }:
                    raise ValueError(
                        f"Use a complete pattern for {value}; keep path bindings explicit."
                    )
                rendered = expand(value, args)
                if values[first] == ";":
                    rendered = ". " + rendered
                if following == ";":
                    next_predicate = values[last + 2] if last + 2 < len(values) else ""
                    if next_predicate in fragments:
                        rendered = rendered.rstrip().removesuffix(".")
                    else:
                        rendered += " " + args[0] + " "
                        last += 1
                elif following == ".":
                    last += 1
        else:
            i += 1
            continue
        chunks.extend([template[end : tokens[first].start()], rendered])
        end, i = tokens[last].end(), last + 1
    return "".join([*chunks, template[end:]])


def compile_query(
    template: str, fragments: dict[str, Fragment], scope: Callable[[str], str], known_iris: set[str]
) -> PreparedQuery:
    """Expand retained fragments and parse a SELECT in its source scope."""
    if not template.strip():
        raise ValueError("Supply a SELECT query.")
    reserved = set(
        re.findall(r"[?$]([A-Za-z_][\w]*)", PROTECTED.sub(lambda m: " " * len(m.group()), template))
    )
    if any(v.startswith("__rdfsolve") for v in reserved):
        raise ValueError("Variables beginning __rdfsolve are reserved for internal path nodes.")
    counter = 0

    def allocate():
        nonlocal counter
        counter += 1
        return f"?__rdfsolve{counter}"

    uses = []

    def expand(ref, args):
        if ref not in fragments:
            raise ValueError(f"Unknown fragment {ref}; discover a retained class, field or entity.")
        text = fragments[ref].render(args, allocate)
        uses.append(
            {
                "ref": ref,
                "variables": [v[1:] for v in args],
                "pattern": text if fragments[ref].kind != "term" else "",
            }
        )
        return text

    text = _expand_references(template, fragments, expand)
    parsed = _parse(text, fragments, phase="expanded_query")
    if parsed.algebra.name != "SelectQuery":
        raise ValueError(
            "Only SELECT reads are supported. Use SELECT with a small probe for existence checks."
        )
    from rdflib.plugins.sparql.parser import parseQuery

    syntax = parseQuery(text)
    for node in walk(syntax):
        if isinstance(node, CompValue) and node.name in {
            "ServiceGraphPattern",
            "DatasetClause",
            "GraphGraphPattern",
        }:
            raise ValueError(
                "SERVICE, FROM, and explicit GRAPH are not accepted: the Client owns dataset scope."
            )
    unknown = sorted({str(n) for n in walk(parsed.algebra) if isinstance(n, URIRef)} - known_iris)

    unknown = [
        iri
        for iri in unknown
        if not iri.startswith(
            ("http://www.w3.org/2001/XMLSchema#", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        )
    ]
    if unknown:
        raise ValueError(
            f"Ungrounded IRI(s): {unknown[:8]}. Discover the actual term or inspect a record; do not guess."
        )

    masked = PROTECTED.sub(lambda m: " " * len(m.group()), text)
    start = masked.find("{")
    depth, end = 0, None
    for i in range(start, len(masked)):
        depth += (masked[i] == "{") - (masked[i] == "}")
        if depth == 0:
            end = i
            break
    if start < 0 or end is None:
        raise ValueError("Could not locate the outer query group.")
    body = text[start + 1 : end]

    if "_graph" in reserved:
        raise ValueError("?_graph is reserved for Client graph scope.")
    sparql = text[: start + 1] + "\n" + scope(body) + "\n" + text[end:]
    parsed = _parse(sparql, fragments, phase="scoped_query")
    variables = [str(v) for v in parsed.algebra.PV]
    if any(v.startswith("__rdfsolve") or v == "_graph" for v in variables):
        raise ValueError(
            "Select output variables explicitly; SELECT * would expose package-internal path/scope variables."
        )
    warnings = []

    type_variables = sorted(
        {
            str(s)
            for node in walk(parsed.algebra)
            if isinstance(node, CompValue) and node.name == "BGP"
            for s, p, o in node.triples
            if isinstance(s, Variable)
            and p == RDF.type
            and isinstance(o, URIRef)
            and not str(s).startswith("__rdfsolve")
        }
    )
    unprojected = sorted(set(type_variables) - set(variables))
    aggregate = any(
        isinstance(n, CompValue) and n.name.startswith("Aggregate_") for n in walk(parsed.algebra)
    )
    diagnostics = {
        "typed_resource_variables": type_variables,
        "unprojected_resource_variables": unprojected,
        "has_aggregates": aggregate,
    }
    return PreparedQuery(
        identifier("q", sparql), template, sparql, variables, uses, warnings, diagnostics
    )
