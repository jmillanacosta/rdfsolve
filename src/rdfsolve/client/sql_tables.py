"""The records of a graph as tables for SQL, following a SHACL application profile, and back.

tables() reads an RDF graph into tables of text cells (pandas DataFrames that DataFrame.to_sql
writes as they are); from_tables() writes the same graph from them. Names come from the CURIEs
of the profile's prefixes, so a class or a field that stays the same from one version of a
profile to the next keeps its table or column:

- one table per class of the profile (a node shape with sh:targetClass), named from the class
  (``schema_software_application``), with the columns ``iri`` and ``type`` (the class), and for
  each field that takes one value (sh:maxCount 1) a column of values (``schema_name``) and one of
  their terms (``schema_name__term``);
- for each class, a table ``<class>__values`` (iri, field, value, term) of the fields that take
  several values, and of the values of a single field for a node that has more than one;
- one table ``statements`` (iri, property, value, term) of every other statement: rdf:type values
  that are no class of the profile, properties that are no field of a class of the node, and the
  statements of nodes of no class of the profile.

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
from typing import TYPE_CHECKING

from rdflib import RDF, BNode, Graph, Literal, URIRef
from rdflib.namespace import SH

if TYPE_CHECKING:
    import pandas as pd
    from rdflib.term import Node

__all__ = ["Layout", "from_tables", "layout", "tables"]

STATEMENTS = "statements"
VALUES_SUFFIX = "__values"
TERM_SUFFIX = "__term"
LONG_COLUMNS = ["iri", "field", "value", "term"]
STATEMENT_COLUMNS = ["iri", "property", "value", "term"]


@dataclass
class ClassLayout:
    """The table of one class: its name and fields (path IRI -> column name, single or not)."""

    iri: str
    name: str
    single: dict[str, str] = field(default_factory=dict)
    several: dict[str, str] = field(default_factory=dict)

    def fields(self) -> dict[str, str]:
        """Return every field of the class: path IRI -> column name."""
        return {**self.single, **self.several}


@dataclass
class Layout:
    """The tables of a profile: one per class, by class IRI."""

    classes: dict[str, ClassLayout]

    def columns(self) -> dict[str, list[str]]:
        """Return the columns of every table, by table name."""
        found: dict[str, list[str]] = {}
        for cls in self.classes.values():
            found[cls.name] = [
                "iri",
                "type",
                *(c for name in cls.single.values() for c in (name, name + TERM_SUFFIX)),
            ]
            found[cls.name + VALUES_SUFFIX] = list(LONG_COLUMNS)
        found[STATEMENTS] = list(STATEMENT_COLUMNS)
        return found


def _name(iri: str, graph: Graph) -> str:
    """Return the table or column name of a term: its CURIE in the graph's prefixes, as
    ``prefix_local_name`` (camelCase split into words); a term of no prefix is refused.
    """
    from rdfsolve.schema_models.exporters.pydantic import _identifier

    try:
        prefix, _, local = graph.namespace_manager.compute_qname_strict(iri, generate=False)
    except (KeyError, ValueError) as error:
        raise ValueError(f"No prefix of the profile names {iri}: declare one") from error
    words = _identifier(local)
    return re.sub(r"[^a-z0-9_]", "_", f"{prefix}_{words}".lower())


def _profile_graph(profile: str | Graph) -> Graph:
    """Return the profile as a graph whose prefixes are only those the profile declares."""
    if isinstance(profile, Graph):
        return profile
    return Graph(bind_namespaces="none").parse(data=profile, format="turtle")


def layout(profile: str | Graph) -> Layout:
    """Return the tables of a SHACL profile (Turtle text or a graph): one per class of a node
    shape with sh:targetClass, and its fields by property shape with an IRI path; a field
    whose every shape for the class says sh:maxCount 1 takes one value.
    """
    from rdfsolve.schema_models.shacl_model import ShaclNodeShape

    graph = _profile_graph(profile)
    maxima: dict[str, dict[str, set[int | None]]] = defaultdict(lambda: defaultdict(set))
    for node in set(graph.subjects(SH.targetClass, None)):
        shape = ShaclNodeShape.from_rdf(graph, node)
        if shape.target_class is None:
            continue
        maxima.setdefault(shape.target_class, defaultdict(set))
        for prop in shape.property_shapes:
            if isinstance(prop.path, str) and prop.path:
                maxima[shape.target_class][prop.path].add(prop.max_count)
    classes: dict[str, ClassLayout] = {}
    taken: dict[str, str] = {}
    for cls, paths in sorted(maxima.items()):
        found = ClassLayout(iri=cls, name=_name(cls, graph))
        for path, counts in sorted(paths.items()):
            name = _name(path, graph)
            if taken.setdefault(name, path) != path:
                raise ValueError(f"{path} and {taken[name]} have the same column name {name}")
            target = found.single if counts == {1} else found.several
            target[path] = name
        if taken.setdefault(found.name, cls) != cls:
            raise ValueError(f"{cls} and {taken[found.name]} have the same table name {found.name}")
        classes[cls] = found
    return Layout(classes)


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
    rows: dict[str, list[list[str | None]]] = {name: [] for name in columns}
    covered: dict[Node, set[str]] = defaultdict(set)
    for cls in shapes.classes.values():
        fields = cls.fields()
        for node in sorted(set(data.subjects(RDF.type, URIRef(cls.iri))), key=str):
            subject = _iri_cell(node)
            covered[node] |= set(fields) | {str(RDF.type)}
            values: dict[str, list[Node]] = defaultdict(list)
            for predicate, value in data.predicate_objects(node):
                if str(predicate) in fields:
                    values[str(predicate)].append(value)
            row: list[str | None] = [subject, cls.iri]
            for path, name in cls.single.items():
                found = values.get(path, [])
                if len(found) == 1:
                    row += list(_cell(found[0]))
                else:
                    row += [None, None]
                    # More than one value of a single field: all of them in the long table.
                    for value in found:
                        rows[cls.name + VALUES_SUFFIX].append([subject, name, *_cell(value)])
            rows[cls.name].append(row)
            for path, name in cls.several.items():
                for value in values.get(path, []):
                    rows[cls.name + VALUES_SUFFIX].append([subject, name, *_cell(value)])
    classes = {URIRef(c) for c in shapes.classes}
    for node, predicate, value in data:
        done = covered.get(node, set())
        if str(predicate) in done and not (predicate == RDF.type and value not in classes):
            continue
        rows[STATEMENTS].append([_iri_cell(node), str(predicate), *_cell(value)])
    return {name: _frame(rows[name], columns[name]) for name in columns}


def from_tables(given: Mapping[str, pd.DataFrame], profile: str | Graph) -> Graph:
    """Return the graph of the tables that tables() gives (also read back from SQL): every value
    as its term says, whatever kind its field's shape allows.
    """
    import pandas as pd

    shapes = layout(profile)
    graph = Graph(bind_namespaces="none")
    source = _profile_graph(profile)
    for prefix, namespace in source.namespaces():
        graph.bind(prefix, namespace, override=False)

    def text(cell: object) -> str | None:
        """Return a cell as text, or None when it is missing (None, NA or NaN, as SQL gives it)."""
        missing = cell is None or cell is pd.NA or (isinstance(cell, float) and math.isnan(cell))
        return None if missing else str(cell)

    for cls in shapes.classes.values():
        paths = {name: URIRef(path) for path, name in cls.fields().items()}
        table = given.get(cls.name)
        if table is not None:
            for record in table.to_dict("records"):
                node = _subject(str(record["iri"]))
                graph.add((node, RDF.type, URIRef(cls.iri)))
                for name in cls.single.values():
                    value = text(record.get(name))
                    if value is not None:
                        code = text(record.get(name + TERM_SUFFIX)) or ""
                        graph.add((node, paths[name], _term(value, code)))
        long = given.get(cls.name + VALUES_SUFFIX)
        if long is not None:
            for record in long.to_dict("records"):
                node = _subject(str(record["iri"]))
                graph.add(
                    (
                        node,
                        paths[str(record["field"])],
                        _term(str(record["value"]), text(record["term"]) or ""),
                    )
                )
    statements = given.get(STATEMENTS)
    if statements is not None:
        for record in statements.to_dict("records"):
            graph.add(
                (
                    _subject(str(record["iri"])),
                    URIRef(str(record["property"])),
                    _term(str(record["value"]), text(record["term"]) or ""),
                )
            )
    return graph
