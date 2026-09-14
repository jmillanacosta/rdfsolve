"""Grounded SPARQL fragments. No model, MCP, or endpoint dependency.

Templates are ordinary SELECT queries with optional ``{{ref ?s ?o}}`` inserts.
SPARQL keeps responsibility for OPTIONAL/UNION/FILTER/aggregation. The package
owns paths, RDF identities, variable hygiene, parsing, and configured graph scope.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
import re
from typing import Any, Callable

from pyparsing import ParseResults
from rdflib import URIRef, Variable
from rdflib.paths import Path as RDFPath
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import CompValue

from rdfsolve.hydration import _iri
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath

# Protect comments, IRIs, and all four SPARQL string forms during interpolation.
PROTECTED = re.compile(r'''\"\"\"(?:\\.|(?!\"\"\")[\s\S])*\"\"\"|\'\'\'(?:\\.|(?!\'\'\')[\s\S])*\'\'\'|\"(?:\\.|[^\"\\])*\"|\'(?:\\.|[^\'\\])*\'|<[^<>\s]*>|\#[^\r\n]*''')
MACRO = re.compile(r"\{\{\s*([A-Za-z][\w-]*)\s*((?:[?$][A-Za-z_][\w]*\s*)*)\}\}")
VAR = re.compile(r"^[?$][A-Za-z_][\w]*$")


def identifier(prefix: str, value: Any) -> str:
    return prefix + '_' + sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:12]


def walk(value):
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
    """Structural edge count and min/max traversal length, not execution cost."""
    if path.operator == 'predicate':
        return {'edges': 1, 'min_hops': 1, 'max_hops': 1}
    sizes = [path_size(p) for p in path.items]
    edges = sum(s['edges'] for s in sizes)
    if path.operator == 'inverse':
        return sizes[0]
    if path.operator == 'alternative':
        return {'edges': edges, 'min_hops': min(s['min_hops'] for s in sizes),
                'max_hops': None if any(s['max_hops'] is None for s in sizes) else max(s['max_hops'] for s in sizes)}
    if path.operator == 'sequence':
        return {'edges': edges, 'min_hops': sum(s['min_hops'] for s in sizes),
                'max_hops': None if any(s['max_hops'] is None for s in sizes) else sum(s['max_hops'] for s in sizes)}
    return {'edges': edges, 'min_hops': sizes[0]['min_hops'] if path.operator == 'one_or_more' else 0,
            'max_hops': sizes[0]['max_hops'] if path.operator == 'zero_or_one' else None}


def linear_steps(path: PropertyPath, reverse: bool = False):
    """Expose intermediate variables only for finite sequences/inverses."""
    if path.operator == 'predicate':
        return [(path.iri, reverse)]
    if path.operator == 'inverse':
        return linear_steps(path.items[0], not reverse)
    if path.operator != 'sequence':
        raise ValueError('This path has alternatives/repetition; use two endpoints, or inspect a narrower path.')
    items = list(reversed(path.items)) if reverse else path.items
    return [step for p in items for step in linear_steps(p, reverse)]


@dataclass
class Fragment:
    kind: str
    label: str
    iri: str | None = None
    path: PropertyPath | None = None
    owner: str | None = None
    field_name: str | None = None
    # Class-route steps retain intermediate classes; fields do not imply ranges.
    steps: list[tuple[str, str, str, bool]] = field(default_factory=list)
    anchors: dict[int, RdfTerm] = field(default_factory=dict)
    endpoint_types: dict[int, str] = field(default_factory=dict)
    term: RdfTerm | None = None
    basis: str = 'saved schema'
    description: str | None = None

    def render(self, args: list[str], allocate: Callable[[], str]) -> str:
        if any(not VAR.fullmatch(a) for a in args):
            raise ValueError('Fragment bindings must be SPARQL variables.')
        if self.kind == 'term':
            if args or self.term is None:
                raise ValueError('An RDF term insert takes no variables.')
            if self.term.kind == 'bnode':
                raise ValueError('Blank node identifiers are response-scoped; probe their anchored path in one query.')
            return self.term.to_rdf().n3()
        if self.kind == 'type':
            if len(args) != 1:
                raise ValueError('A class insert takes one variable.')
            return f'{args[0]} a {_iri(self.iri)} .'
        if self.path is None or len(args) < 2:
            raise ValueError('A field/path insert takes two endpoint variables.')
        if self.steps:
            if len(args) not in {2, len(self.steps) + 1}:
                raise ValueError(f'Use two endpoints or {len(self.steps)+1} explicit step variables.')
            nodes = args if len(args) > 2 else [args[0], *[allocate() for _ in self.steps[:-1]], args[-1]]
            lines = []
            for i, (s, p, o, back) in enumerate(self.steps):
                left, right = (nodes[i+1], nodes[i]) if back else (nodes[i], nodes[i+1])
                lines.extend([f'{left} {_iri(p)} {right} .', f'{nodes[i]} a {_iri(s)} .', f'{nodes[i+1]} a {_iri(o)} .'])
        elif len(args) == 2:
            nodes = args
            lines = [f'{args[0]} {path_to_sparql(self.path)} {args[1]} .']
        else:
            steps = linear_steps(self.path)
            if len(args) != len(steps) + 1:
                raise ValueError(f'This sequence needs {len(steps)+1} variables to expose its intermediates.')
            nodes = args
            lines = [f'{args[i+1] if back else args[i]} {_iri(p)} {args[i] if back else args[i+1]} .'
                     for i, (p, back) in enumerate(steps)]
        if self.kind == 'field' and self.owner:
            lines.insert(0, f'{nodes[0]} a {_iri(self.owner)} .')
        for position, cls in self.endpoint_types.items():
            lines.insert(0, f'{nodes[position]} a {_iri(cls)} .')
        for position, term in self.anchors.items():
            if term.kind != 'uri':
                raise ValueError('A resource path anchor must be an IRI.')
            lines.insert(0, f'VALUES {nodes[position]} {{ {term.to_rdf().n3()} }}')
        return '\n'.join(dict.fromkeys(lines))


@dataclass
class PreparedQuery:
    ref: str
    template: str
    sparql: str
    variables: list[str]
    uses: list[dict[str, Any]]
    warnings: list[str] = field(default_factory=list)


def compile_query(template: str, fragments: dict[str, Fragment], scope: Callable[[str], str],
                  known_iris: set[str]) -> PreparedQuery:
    """Expand retained fragments and parse. Does not certify natural-language intent."""
    if not template.strip():
        raise ValueError('Supply a SELECT query.')
    reserved = set(re.findall(r'[?$]([A-Za-z_][\w]*)', PROTECTED.sub(lambda m:' '*len(m.group()), template)))
    if any(v.startswith('__rdfsolve') for v in reserved):
        raise ValueError('Variables beginning __rdfsolve are reserved for internal path nodes.')
    counter = 0
    def allocate():
        nonlocal counter
        counter += 1
        return f'?__rdfsolve{counter}'
    uses = []
    def expand(match):
        ref, names = match.groups()
        if ref not in fragments:
            raise ValueError(f'Unknown fragment {ref}; discover or inspect a retained fragment first.')
        args = names.split()
        text = fragments[ref].render(args, allocate)
        uses.append({'ref': ref, 'variables': [v[1:] for v in args]})
        return text
    chunks, last = [], 0
    for match in PROTECTED.finditer(template):
        chunks.extend([MACRO.sub(expand, template[last:match.start()]), match.group()])
        last = match.end()
    chunks.append(MACRO.sub(expand, template[last:]))
    text = ''.join(chunks)
    parsed = prepareQuery(text)
    if parsed.algebra.name != 'SelectQuery':
        raise ValueError('Only SELECT reads are supported. Use SELECT with a small probe for existence checks.')
    from rdflib.plugins.sparql.parser import parseQuery
    syntax = parseQuery(text)
    for node in walk(syntax):
        if isinstance(node, CompValue) and node.name in {'ServiceGraphPattern', 'DatasetClause', 'GraphGraphPattern'}:
            raise ValueError('SERVICE, FROM, and explicit GRAPH are not accepted: the Client owns dataset scope.')
    unknown = sorted({str(n) for n in walk(parsed.algebra) if isinstance(n, URIRef)} - known_iris)
    # RDF and XSD syntax terms are standards, not guessed application vocabulary.
    unknown = [iri for iri in unknown if not iri.startswith(('http://www.w3.org/2001/XMLSchema#',
                'http://www.w3.org/1999/02/22-rdf-syntax-ns#'))]
    if unknown:
        raise ValueError(f'Ungrounded IRI(s): {unknown[:8]}. Discover the actual term or inspect a record; do not guess.')
    # Locate the outer WHERE group outside protected spans. Preserve all modifiers.
    masked = PROTECTED.sub(lambda m: ' ' * len(m.group()), text)
    start = masked.find('{')
    depth, end = 0, None
    for i in range(start, len(masked)):
        depth += (masked[i] == '{') - (masked[i] == '}')
        if depth == 0:
            end = i
            break
    if start < 0 or end is None:
        raise ValueError('Could not locate the outer query group.')
    body = text[start+1:end]
    # The scope variable is controlled by the client, not the model.
    if '_graph' in reserved:
        raise ValueError('?_graph is reserved for Client graph scope.')
    sparql = text[:start+1] + '\n' + scope(body) + '\n' + text[end:]
    parsed = prepareQuery(sparql)
    variables = [str(v) for v in parsed.algebra.PV]
    if any(v.startswith('__rdfsolve') or v == '_graph' for v in variables):
        raise ValueError('Select output variables explicitly; SELECT * would expose package-internal path/scope variables.')
    warnings = ['Syntax and source vocabulary checked; interpretation of the question is not independently verified.']
    return PreparedQuery(identifier('q', sparql), template, sparql, variables, uses, warnings)
