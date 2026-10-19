"""The records of a graph as tables for SQL, following a SHACL application profile, and back.

tables() reads an RDF graph into tables of text cells (pandas DataFrames that DataFrame.to_sql
writes as they are); from_tables() writes the same graph from them; Layout.ddl() gives the SQL of
the tables. Names come from the CURIEs of the prefixes the profile declares, so a class or a field
that stays the same from one version of a profile to the next keeps its table or column:

- one table per class of the profile (each sh:targetClass of a node shape), named from the class
  (``schema_software_application``), with the columns ``record``, ``iri`` and ``type`` (the
  class), and for each single field a column of values (``schema_name``) and one of their terms
  (``schema_name__term``); its key is ``iri``. The fields of a class are the property shapes of
  its node shapes and of the node shapes they build on (sh:node, at any depth). A field is single
  when the smallest sh:maxCount of the property shapes of its class and path is 1: SHACL checks
  every shape on a path.
- for each other field of a class, a table ``<class>__<field>`` (record, iri, value, term), keyed
  by (iri, value, term), whose ``iri`` refers to the class table;
- one table ``statements`` (record, iri, property, value, term) of everything else: rdf:type
  values that are no class of the profile, properties that are no field of a class of the node,
  the statements of nodes of no class, and every value of a single field of a node that has more
  than one (the field's column then stays empty).

``record`` is the record a row belongs to: an IRI subject is its own record; a blank node belongs
to the IRI subject that reaches it through blank nodes only, at any depth (a blank node reached
from two IRI subjects, or from none, is refused). A record is read with ``WHERE record IN (...)``
and deleted with ``WHERE record = ?``, its nested nodes included.

A term says how its value is written back: ``@id`` an IRI, ``@blank`` a blank node, empty a
literal without a datatype, ``@`` and the language tag a tagged literal, else the datatype IRI.
Values are kept as the data has them, whatever kind their field's shape allows: SHACL checks
them, not these tables. A node's blank node label is kept, so a graph read back is the same
graph, blank nodes included, as rdflib.compare.isomorphic tells.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from rdflib import RDF, BNode, Graph, Literal, URIRef
from rdflib.namespace import SH

if TYPE_CHECKING:
    import pandas as pd
    from rdflib.term import Node

    from rdfsolve.schema_models.shacl_model import ShaclPropertyShape

__all__ = ["Layout", "from_tables", "layout", "tables"]

STATEMENTS = "statements"
TERM_SUFFIX = "__term"
FIELD_COLUMNS = ["record", "iri", "value", "term"]
STATEMENT_COLUMNS = ["record", "iri", "property", "value", "term"]


@dataclass
class ClassLayout:
    """The tables of one class: its name and fields (path IRI -> column name), single or not."""

    iri: str
    name: str
    single: dict[str, str] = field(default_factory=dict)
    several: dict[str, str] = field(default_factory=dict)

    def fields(self) -> dict[str, str]:
        """Return every field of the class: path IRI -> column name."""
        return {**self.single, **self.several}

    def field_table(self, column: str) -> str:
        """Return the name of the table of a field that is not single."""
        return f"{self.name}__{column}"


@dataclass
class Layout:
    """The tables of a profile: one per class, by class IRI, one per field that is not single,
    and the statements table.
    """

    classes: dict[str, ClassLayout]

    def columns(self) -> dict[str, list[str]]:
        """Return the columns of every table, by table name."""
        found: dict[str, list[str]] = {}
        for cls in self.classes.values():
            found[cls.name] = [
                "record",
                "iri",
                "type",
                *(c for name in cls.single.values() for c in (name, name + TERM_SUFFIX)),
            ]
            for name in cls.several.values():
                found[cls.field_table(name)] = list(FIELD_COLUMNS)
        found[STATEMENTS] = list(STATEMENT_COLUMNS)
        return found

    def ddl(self) -> str:
        """Return the SQL of every table, in name order (SQLite and PostgreSQL): every column
        TEXT, the key of each table, the reference of each field table to its class table, and
        an index on ``record``.
        """
        keys: dict[str, list[str]] = {STATEMENTS: ["iri", "property", "value", "term"]}
        refers: dict[str, str] = {}
        for cls in self.classes.values():
            keys[cls.name] = ["iri"]
            for name in cls.several.values():
                keys[cls.field_table(name)] = ["iri", "value", "term"]
                refers[cls.field_table(name)] = cls.name
        statements = []
        for table, columns in sorted(self.columns().items()):
            required = {"record", "iri", *keys[table]}
            lines = [f"  {_quote(c)} TEXT{' NOT NULL' if c in required else ''}" for c in columns]
            lines.append(f"  PRIMARY KEY ({', '.join(map(_quote, keys[table]))})")
            if table in refers:
                lines.append(f'  FOREIGN KEY ("iri") REFERENCES {_quote(refers[table])} ("iri")')
            statements.append(
                f"CREATE TABLE IF NOT EXISTS {_quote(table)} (\n" + ",\n".join(lines) + "\n);"
            )
            statements.append(
                f"CREATE INDEX IF NOT EXISTS {_quote(table + '__record')} "
                f'ON {_quote(table)} ("record");'
            )
        return "\n".join(statements) + "\n"


def _quote(name: str) -> str:
    """Return a table or column name quoted for SQL."""
    return '"' + name.replace('"', '""') + '"'


@lru_cache(maxsize=1)
def _default_bindings() -> frozenset[tuple[str, str]]:
    """Return the (prefix, namespace) pairs that rdflib binds in a new graph by itself."""
    return frozenset((str(p), str(ns)) for p, ns in Graph().namespaces())


def _prefixes(graph: Graph) -> dict[str, str]:
    """Return the prefix of each namespace of a profile: of the prefixes bound to a namespace,
    one the profile declares rather than one rdflib binds by itself (``dct`` rather than
    ``dcterms``), so that names do not depend on how the profile was parsed.
    """
    defaults = _default_bindings()
    by_namespace: dict[str, list[tuple[bool, str]]] = defaultdict(list)
    for prefix, namespace in graph.namespaces():
        if str(prefix):
            by_namespace[str(namespace)].append(
                ((str(prefix), str(namespace)) in defaults, str(prefix))
            )
    return {ns: min(found)[1] for ns, found in by_namespace.items()}


def _name(iri: str, prefixes: Mapping[str, str]) -> str:
    """Return the table or column name of a term: its CURIE in the profile's prefixes, as
    ``prefix_local_name`` (camelCase split into words); a term of no prefix is refused.
    """
    from rdfsolve.schema_models.exporters.pydantic import _identifier

    namespace = max(
        (ns for ns in prefixes if iri.startswith(ns) and ns != iri), key=len, default=None
    )
    if namespace is None:
        raise ValueError(f"No prefix of the profile names {iri}: declare one")
    words = _identifier(iri[len(namespace) :])
    return re.sub(r"[^a-z0-9_]", "_", f"{prefixes[namespace]}_{words}".lower())


@lru_cache(maxsize=32)
def _parsed(text: str) -> Graph:
    """Return a profile given as Turtle text as a graph, parsed once for a text."""
    return Graph(bind_namespaces="none").parse(data=text, format="turtle")


def _profile_graph(profile: str | Graph) -> Graph:
    """Return the profile as a graph."""
    return profile if isinstance(profile, Graph) else _parsed(profile)


@lru_cache(maxsize=32)
def _layout_of_text(text: str) -> Layout:
    """Return the layout of a profile given as Turtle text, made once for a text."""
    return _layout(_parsed(text))


def layout(profile: str | Graph) -> Layout:
    """Return the tables of a SHACL profile (Turtle text or a graph): one per sh:targetClass of a
    node shape, its fields by property shape with an IRI path (also those of the node shapes it
    builds on with sh:node), single when the smallest sh:maxCount of the shapes of the class and
    path is 1.
    """
    return _layout_of_text(profile) if isinstance(profile, str) else _layout(profile)


def _layout(graph: Graph) -> Layout:
    """Return the layout of a profile graph (layout)."""
    from rdfsolve.schema_models.shacl_model import ShaclNodeShape

    prefixes = _prefixes(graph)
    maxima: dict[str, dict[str, set[int | None]]] = defaultdict(lambda: defaultdict(set))
    for node in set(graph.subjects(SH.targetClass, None)):
        shape = ShaclNodeShape.from_rdf(graph, node)
        fields = shape.property_shapes + _built_on(graph, node)
        for cls in shape.target_classes:
            maxima.setdefault(cls, defaultdict(set))
            for prop in fields:
                if isinstance(prop.path, str) and prop.path:
                    maxima[cls][prop.path].add(prop.max_count)
    classes: dict[str, ClassLayout] = {}
    columns: dict[str, str] = {}
    tables: dict[str, str] = {STATEMENTS: STATEMENTS}
    for cls, paths in sorted(maxima.items()):
        found = ClassLayout(iri=cls, name=_name(cls, prefixes))
        if tables.setdefault(found.name, cls) != cls:
            raise ValueError(
                f"{cls} and {tables[found.name]} have the same table name {found.name}"
            )
        for path, counts in sorted(paths.items()):
            name = _name(path, prefixes)
            if columns.setdefault(name, path) != path:
                raise ValueError(f"{path} and {columns[name]} have the same column name {name}")
            bounds = [c for c in counts if c is not None]
            if bounds and min(bounds) <= 1:
                found.single[path] = name
            else:
                found.several[path] = name
                table = found.field_table(name)
                if tables.setdefault(table, path) != path:
                    raise ValueError(f"{path} and {tables[table]} have the same table name {table}")
        classes[cls] = found
    return Layout(classes)


def _built_on(graph: Graph, shape: Node) -> list[ShaclPropertyShape]:
    """Return the property shapes of the node shapes that a node shape builds on: its sh:node
    values, theirs, and so on. SHACL checks a focus node against them too.
    """
    from rdfsolve.schema_models.shacl_model import ShaclPropertyShape

    found = []
    seen = {shape}
    todo = list(graph.objects(shape, SH.node))
    while todo:
        other = todo.pop()
        if other in seen:
            continue
        seen.add(other)
        for prop in graph.objects(other, SH.property):
            found.append(ShaclPropertyShape.from_rdf(graph, prop))
        todo.extend(graph.objects(other, SH.node))
    return found


def _cell(term: Node) -> tuple[str, str]:
    """Return a term as (value, term code)."""
    if isinstance(term, URIRef):
        return str(term), "@id"
    if isinstance(term, BNode):
        return str(term), "@blank"
    if not isinstance(term, Literal):
        raise TypeError(f"Not an RDF term: {term!r}")
    if term.language:
        return str(term), "@" + term.language
    return str(term), str(term.datatype) if term.datatype else ""


def _term(value: str, code: str) -> Node:
    """Return the term of a cell (value, term code)."""
    if code == "@id":
        return URIRef(value)
    if code == "@blank":
        return BNode(value)
    if code.startswith("@"):
        return Literal(value, lang=code[1:])
    return Literal(value, datatype=URIRef(code)) if code else Literal(value)


def _subject(value: str) -> Node:
    """Return the node of an ``iri`` cell: a blank node as _:label, else an IRI."""
    return BNode(value[2:]) if value.startswith("_:") else URIRef(value)


def _iri_cell(node: Node) -> str:
    """Return the ``iri`` cell of a node."""
    return f"_:{node}" if isinstance(node, BNode) else str(node)


def _records(data: Graph) -> dict[Node, str]:
    """Return the record of each blank node subject: the IRI subject that reaches it through
    blank nodes only; a blank node reached from two IRI subjects, or from none, is refused.
    """
    incoming: dict[Node, set[Node]] = defaultdict(set)
    for subject, _, value in data:
        if isinstance(value, BNode):
            incoming[value].add(subject)
    found: dict[Node, str] = {}
    for node in {s for s in data.subjects() if isinstance(s, BNode)}:
        owners: set[Node] = set()
        seen = {node}
        todo = [node]
        while todo:
            for parent in incoming.get(todo.pop(), ()):
                if isinstance(parent, BNode):
                    if parent not in seen:
                        seen.add(parent)
                        todo.append(parent)
                else:
                    owners.add(parent)
        if len(owners) != 1:
            reached = "no IRI subject" if not owners else f"{len(owners)} IRI subjects"
            raise ValueError(f"The blank node _:{node} is reached from {reached}: it has no record")
        found[node] = str(next(iter(owners)))
    return found


def _frame(rows: list[list[str | None]], columns: list[str]) -> pd.DataFrame:
    """Return a table of text cells (pandas' string dtype; missing cells are NA)."""
    import pandas as pd

    frame = pd.DataFrame(rows, columns=columns, dtype="string")
    return frame.sort_values(columns).reset_index(drop=True) if rows else frame


def tables(data: Graph, profile: str | Graph) -> dict[str, pd.DataFrame]:
    """Return the tables of the nodes of *data* that *profile* gives (see the module).

    Without data, the tables are empty, with their columns.
    """
    shapes = layout(profile)
    columns = shapes.columns()
    owners = _records(data)

    def record(node: Node) -> str:
        """Return the record of a subject."""
        return owners[node] if isinstance(node, BNode) else str(node)

    rows: dict[str, list[list[str | None]]] = {name: [] for name in columns}
    statements: set[tuple[str, str, str, str, str]] = set()
    covered: dict[Node, set[str]] = defaultdict(set)
    for cls in shapes.classes.values():
        fields = cls.fields()
        for node in sorted(set(data.subjects(RDF.type, URIRef(cls.iri))), key=str):
            owner, subject = record(node), _iri_cell(node)
            covered[node] |= set(fields) | {str(RDF.type)}
            values: dict[str, list[Node]] = defaultdict(list)
            for predicate, value in data.predicate_objects(node):
                if str(predicate) in fields:
                    values[str(predicate)].append(value)
            row: list[str | None] = [owner, subject, cls.iri]
            for path in cls.single:
                found = values.get(path, [])
                if len(found) == 1:
                    row += list(_cell(found[0]))
                else:
                    row += [None, None]
                    # More than one value of a single field: every one in the statements.
                    statements |= {(owner, subject, path, *_cell(value)) for value in found}
            rows[cls.name].append(row)
            for path, name in cls.several.items():
                for value in values.get(path, []):
                    rows[cls.field_table(name)].append([owner, subject, *_cell(value)])
    classes = {URIRef(c) for c in shapes.classes}
    for node, predicate, value in data:
        done = covered.get(node, set())
        if str(predicate) in done and not (predicate == RDF.type and value not in classes):
            continue
        statements.add((record(node), _iri_cell(node), str(predicate), *_cell(value)))
    rows[STATEMENTS] = [list(row) for row in statements]
    return {name: _frame(rows[name], columns[name]) for name in columns}


def from_tables(given: Mapping[str, pd.DataFrame], profile: str | Graph) -> Graph:
    """Return the graph of the tables that tables() gives (also read back from SQL, or only the
    rows of some records): every value as its term says, whatever kind its field's shape allows.
    """
    import pandas as pd

    shapes = layout(profile)
    source = _profile_graph(profile)
    graph = Graph(bind_namespaces="none")
    for namespace, prefix in _prefixes(source).items():
        graph.bind(prefix, namespace, override=False)

    def text(cell: object) -> str | None:
        """Return a cell as text, or None when it is missing (None, NA or NaN, as SQL gives it)."""
        missing = cell is None or cell is pd.NA or (isinstance(cell, float) and math.isnan(cell))
        return None if missing else str(cell)

    def add(node: Node, predicate: URIRef, record: Mapping[Any, object]) -> None:
        """Add the statement of a row (value, term) of *node*."""
        graph.add((node, predicate, _term(str(record["value"]), text(record["term"]) or "")))

    for cls in shapes.classes.values():
        table = given.get(cls.name)
        if table is not None:
            for row in table.to_dict("records"):
                node = _subject(str(row["iri"]))
                graph.add((node, RDF.type, URIRef(cls.iri)))
                for path, name in cls.single.items():
                    value = text(row.get(name))
                    if value is not None:
                        code = text(row.get(name + TERM_SUFFIX)) or ""
                        graph.add((node, URIRef(path), _term(value, code)))
        for path, name in cls.several.items():
            values = given.get(cls.field_table(name))
            if values is not None:
                for row in values.to_dict("records"):
                    add(_subject(str(row["iri"])), URIRef(path), row)
    rest = given.get(STATEMENTS)
    if rest is not None:
        for row in rest.to_dict("records"):
            add(_subject(str(row["iri"])), URIRef(str(row["property"])), row)
    return graph
