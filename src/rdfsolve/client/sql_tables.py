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
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from rdflib import RDF, RDFS, XSD, BNode, Graph, Literal, URIRef
from rdflib.namespace import SH

if TYPE_CHECKING:
    import pandas as pd
    from rdflib.term import Node
    from sqlalchemy import Connection, Select

    from rdfsolve.schema_models.shacl_model import ShaclPropertyShape
    from rdfsolve.schema_models.void_model import VoidClassPartition, VoidPropertyPartition

__all__ = ["Layout", "Merged", "context", "from_tables", "layout", "merge", "tables"]

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

    def within(self, of_class: str, hierarchy: Graph) -> list[str]:
        """Return the classes of the layout whose records are of a class too: the class, and
        those below it in *hierarchy* (rdfs:subClassOf statements, at any depth), in IRI order.
        """
        below = {str(c) for c in hierarchy.transitive_subjects(RDFS.subClassOf, URIRef(of_class))}
        found = []
        if of_class in self.classes:
            found.append(of_class)
        for cls in self.classes:
            if cls != of_class and cls in below:
                found.append(cls)
        return found

    def void(
        self,
        connection: Connection,
        records: Select[Any],
        *,
        name: Callable[..., str],
        left_out: Collection[str] = (),
    ) -> list[VoidClassPartition]:
        """Return the VoID class partitions of the rows of some records, counted in SQL.

        A partition per class with rows, with its entities (its nodes with an IRI) and a
        property partition per property of its nodes: its triples, the classes of the nodes its
        values are (class partitions) and the datatypes of its literals (datatype partitions;
        rdf:langString for a tagged literal, xsd:string for a plain one). rdf:type and the
        properties in *left_out* are not counted. *records* is a SQLAlchemy select of the
        records' IRIs (``record`` cells), and *connection* a SQLAlchemy connection to the
        tables. *name* gives the IRI of a partition from the IRIs it partitions by: the class,
        then the property, then the class or datatype of the values.
        """
        from sqlalchemy import column, func, select, table, union_all

        from rdfsolve.schema_models.void_model import (
            VoidClassPartition,
            VoidDatatypePartition,
            VoidPropertyPartition,
        )

        if not self.classes:
            return []
        tables = {}
        for table_name, columns in self.columns().items():
            tables[table_name] = table(table_name, *[column(c) for c in columns])
        # The class of each node, to partition the values that are nodes by their class.
        typed = [select(tables[c.name].c.iri, tables[c.name].c.type) for c in self.classes.values()]
        classes: dict[str, str] = {}
        for iri, cls_iri in connection.execute(union_all(*typed)):
            classes[iri] = cls_iri
        rest = tables[STATEMENTS]
        found = []
        for cls in sorted(self.classes.values(), key=lambda c: c.iri):
            own = tables[cls.name]
            total, entities = connection.execute(
                select(func.count(), func.count().filter(~own.c.iri.startswith("_:"))).where(
                    own.c.record.in_(records)
                )
            ).one()
            if not total:
                continue
            counted: dict[str, list[tuple[str | None, str, int]]] = defaultdict(list)
            for path, held in cls.single.items():
                value, term = own.c[held], own.c[held + TERM_SUFFIX]
                statement = (
                    select(term, value, func.count())
                    .where(own.c.record.in_(records), value.is_not(None))
                    .group_by(term, value)
                )
                for row in connection.execute(statement):
                    counted[path].append((row[0], row[1], row[2]))
            for path, held in cls.several.items():
                values = tables[cls.field_table(held)]
                statement = (
                    select(values.c.term, values.c.value, func.count())
                    .where(values.c.record.in_(records))
                    .group_by(values.c.term, values.c.value)
                )
                for row in connection.execute(statement):
                    counted[path].append((row[0], row[1], row[2]))
            # The statements of the class's nodes outside its fields, and the values of a single
            # field of a node that has more than one.
            statement = (
                select(rest.c.property, rest.c.term, rest.c.value, func.count())
                .select_from(rest.join(own, own.c.iri == rest.c.iri))
                .where(rest.c.record.in_(records), rest.c.property != str(RDF.type))
                .group_by(rest.c.property, rest.c.term, rest.c.value)
            )
            for row in connection.execute(statement):
                counted[row[0]].append((row[1], row[2], row[3]))
            properties = []
            for path, rows in sorted(counted.items()):
                if path in left_out or not rows:
                    continue
                properties.append(_property_partition(cls.iri, path, rows, classes, name))
            found.append(
                VoidClassPartition(
                    uri=name(cls.iri),
                    class_uri=cls.iri,
                    entities=entities,
                    property_partitions=properties,
                )
            )
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


def _property_partition(
    cls: str,
    path: str,
    rows: list[tuple[str | None, str, int]],
    classes: Mapping[str, str],
    name: Callable[..., str],
) -> VoidPropertyPartition:
    """Return the VoID property partition of a property of a class from its counted values
    (term, value, count) and the class of each node (Layout.void).
    """
    from rdfsolve.schema_models.void_model import (
        VoidClassPartition,
        VoidDatatypePartition,
        VoidPropertyPartition,
    )

    linked: dict[str, int] = defaultdict(int)
    datatypes: dict[str, int] = defaultdict(int)
    triples = 0
    for term, value, count in rows:
        triples += count
        kind = term or ""
        if kind == "@id":
            of_value = classes.get(value)
            if of_value:
                linked[of_value] += count
        elif kind == "@blank":
            of_value = classes.get("_:" + value)
            if of_value:
                linked[of_value] += count
        elif kind.startswith("@"):
            datatypes[str(RDF.langString)] += count
        else:
            datatypes[kind or str(XSD.string)] += count
    class_partitions = []
    for of_value, count in sorted(linked.items()):
        class_partitions.append(
            VoidClassPartition(uri=name(cls, path, of_value), class_uri=of_value, triples=count)
        )
    datatype_partitions = []
    for datatype, count in sorted(datatypes.items()):
        datatype_partitions.append(
            VoidDatatypePartition(
                uri=name(cls, path, datatype), datatype_uri=datatype, triples=count
            )
        )
    return VoidPropertyPartition(
        uri=name(cls, path),
        property_uri=path,
        triples=triples,
        class_partitions=class_partitions,
        datatype_partitions=datatype_partitions,
    )


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


def context(profile: str | Graph, of_class: str, term: Callable[[str], str]) -> dict[str, Any]:
    """Return the JSON-LD term definitions of the records of a class: the keywords @id and @type
    as ``id`` and ``type``, and each field of the profile named by *term* (a property IRI to its
    name), with ``@container: @set`` when it takes several values. The class's own fields count
    as the class takes them; a field of other classes only (of the records a record links to) is
    a set unless every class takes one value. No value is coerced to an IRI, so a text where an
    IRI is expected stays a text.
    """
    classes = layout(profile).classes
    single: set[str] = set()
    several: set[str] = set()
    for cls in classes.values():
        single.update(cls.single)
        several.update(cls.several)
    own = classes.get(of_class)
    terms: dict[str, Any] = {"id": "@id", "type": "@type"}
    for iri in sorted(single | several):
        if own is not None and iri in own.fields():
            listed = iri in own.several
        else:
            listed = iri in several
        definition = {"@id": iri}
        if listed:
            definition["@container"] = "@set"
        terms[term(iri)] = definition
    return terms


@dataclass
class Merged:
    """A record merged with another source's statements (merge): the merged graph, the
    statements added to it, and the values a record kept where the other source offers another
    (subject, property, kept, offered).
    """

    graph: Graph
    added: list[tuple[Node, Node, Node]] = field(default_factory=list)
    conflicts: list[tuple[str, str, str, str]] = field(default_factory=list)


def _part_key(graph: Graph, node: Node, seen: frozenset[Node] = frozenset()) -> str:
    """Return a value as merge compares it: a blank node by the values it holds, at any depth."""
    if not isinstance(node, BNode):
        return str(node)
    if node in seen:
        return "part"
    held = []
    for predicate, value in graph.predicate_objects(node):
        held.append(str(predicate) + " " + _part_key(graph, value, seen | {node}))
    held.sort()
    return "part " + " ".join(held)


def _described(graph: Graph, node: BNode) -> list[tuple[Node, Node, Node]]:
    """Return the statements of a blank node, and of the blank nodes it holds, at any depth."""
    found = []
    todo: list[Node] = [node]
    seen: set[Node] = set()
    while todo:
        current = todo.pop()
        if current in seen:
            continue
        seen.add(current)
        for statement in graph.triples((current, None, None)):
            found.append(statement)
            if isinstance(statement[2], BNode):
                todo.append(statement[2])
    return found


def merge(current: Graph, added: Graph, profile: str | Graph) -> Merged:
    """Return *current* with the statements of *added* about IRI subjects, as the profile allows.

    A field that takes one value (single, for any class of the subject: its classes in
    *current*, else in *added*) keeps the value it has, and a different value offered is a
    conflict. A blank node is added with its statements, unless the subject already holds one
    with the same values on that property, so that merging twice adds nothing. The type of a
    subject that *current* types is kept.
    """
    classes = layout(profile).classes
    merged = Merged(Graph())
    merged.graph += current
    parts = set()
    for subject, predicate, value in current:
        if isinstance(value, BNode):
            parts.add((subject, predicate, _part_key(current, value)))
    for subject, predicate, value in sorted(added):
        if not isinstance(subject, URIRef):
            continue
        if predicate == RDF.type and (subject, RDF.type, None) in current:
            continue
        if isinstance(value, BNode):
            if (subject, predicate, _part_key(added, value)) not in parts:
                for statement in [(subject, predicate, value), *_described(added, value)]:
                    merged.graph.add(statement)
                    merged.added.append(statement)
            continue
        kinds = set(current.objects(subject, RDF.type)) or set(added.objects(subject, RDF.type))
        single = False
        for kind in kinds:
            if str(kind) in classes and str(predicate) in classes[str(kind)].single:
                single = True
        given = sorted(current.objects(subject, predicate), key=str)
        if single and given:
            if value not in given:
                merged.conflicts.append((str(subject), str(predicate), str(given[0]), str(value)))
            continue
        if (subject, predicate, value) not in merged.graph:
            merged.graph.add((subject, predicate, value))
            merged.added.append((subject, predicate, value))
    return merged
