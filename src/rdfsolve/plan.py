"""A conversion plan: what a source holds, what a target model can say, and how one becomes the other.

The plan is proposed from evidence, not written by hand:

- a kind of record (a source class) is proposed a target class from the identifiers of its
  records: the target classes that list each identifier namespace as theirs (the target's
  preferred identifier prefixes); the name of the kind breaks ties;
- a kind whose records have no identifiers of their own and link other records of the scope
  through two links is a relation: an edge from one end to the other, or a process node with
  inputs and outputs when other relations point at its records; its target predicate or class
  is proposed from the names and definitions of the target's terms;
- the link that puts records in the scope (their container) is proposed a target predicate the
  same way; a kind's naming property (a label or a title) is proposed the target's name slot.

What the evidence does not decide is an open choice. Each proposal lists its options with
their consequences: how many records each would convert, an example statement from the data,
and what the target model says of the option. Choices are made with objects (Kind, Link and
Term), never with free text, and each is checked against both schemas when it is made.
"""

from __future__ import annotations

import keyword
import logging
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from rdfsolve.client.api import Client, Results
    from rdfsolve.target_model import Model

logger = logging.getLogger(__name__)

__all__ = [
    "Choices",
    "Identities",
    "Kind",
    "Link",
    "Names",
    "Network",
    "Plan",
    "Proposal",
    "Target",
    "Term",
    "kinds_of",
]

# Properties whose values name a record, in order of preference.
NAMING_PROPERTIES = (
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
    "http://purl.org/dc/terms/title",
    "http://purl.org/dc/elements/1.1/title",
    "https://schema.org/name",
    "http://schema.org/name",
)
# Words that tell which end of a relation a link is.
FROM_WORDS = ("source", "subject", "from", "input", "left", "regulator", "agent")
TO_WORDS = ("target", "object", "to", "output", "right", "regulated", "patient")


def _attribute(text: str, snake: bool = False) -> str:
    """Return a Python attribute name for a label, safe to type: CamelCase, or snake_case."""
    words = re.findall(r"[A-Za-z0-9]+", text)
    if snake:
        name = "_".join(w.lower() for w in words) or "_"
    else:
        name = (
            "".join(w[:1].upper() + w[1:] for w in words) if len(words) > 1 else (words or ["_"])[0]
        )
    if name[0].isdigit() or keyword.iskeyword(name):
        name = "_" + name
    return name


def _tokens(text: str) -> set[str]:
    """Return the lower-case word stems of a text (the first five letters of each word)."""
    from rdfsolve.naming import words

    return {w.lower()[:5] for part in re.findall(r"[A-Za-z]+", text) for w in words(part)}


def _clip(value: Any, width: int = 60) -> Any:
    """Return a cell shortened to *width* characters (text only)."""
    if not isinstance(value, str) or len(value) <= width:
        return value
    return value[: width - 1] + "…"


def _text_table(frame: Any, width: int = 60) -> str:
    """Return a DataFrame as pandas prints it (without the index), text left-aligned, long cells
    shortened, never wrapped into blocks.
    """
    import pandas as pd

    if frame.empty:
        return "  (none)"
    # Every cell and header padded to its column's width (text left, numbers right), so that
    # pandas has nothing left to align.
    frame = frame.map(_clip, width=width)
    names = {}
    for column in frame.columns:
        numeric = frame[column].dtype.kind in "iuf"
        cells = frame[column].fillna("").astype(str)
        size = max(int(cells.str.len().max()), len(str(column)))
        frame[column] = cells.str.rjust(size) if numeric else cells.str.ljust(size)
        names[column] = str(column).rjust(size) if numeric else str(column).ljust(size)
    frame = frame.rename(columns=names)
    with pd.option_context(
        "display.max_colwidth",
        None,
        "display.expand_frame_repr",
        False,
        "display.max_columns",
        None,
        "display.max_rows",
        None,
    ):
        return str(frame.to_string(index=False, justify="left"))


def _in_notebook() -> bool:
    """Return whether this runs in a notebook kernel (which shows HTML and Mermaid)."""
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    shell = get_ipython()
    return shell is not None and type(shell).__name__ == "ZMQInteractiveShell"


class _Shown:
    """An object that prints as text lines and tables: plain text in a terminal or a log, HTML in
    a notebook. Subclasses give _parts(): a list of lines (str) and tables (DataFrame).
    """

    def _parts(self) -> list[Any]:
        raise NotImplementedError

    def __repr__(self) -> str:
        """Print the lines, and each table as aligned text."""
        return "\n".join(
            part if isinstance(part, str) else _text_table(part) for part in self._parts()
        )

    def _repr_html_(self) -> str:
        """Show the lines, and each table as pandas shows a DataFrame (notebooks)."""
        import html

        out = []
        for part in self._parts():
            if isinstance(part, str):
                out.append(f"<div style='white-space:pre-wrap'>{html.escape(part.strip())}</div>")
            else:
                out.append(part.to_html(index=False))
        return "\n".join(out)

    def show(self) -> None:
        """Display the lines and each table as a pandas DataFrame (in a terminal, print them)."""
        if not _in_notebook():
            sys.stdout.write(f"{self!r}\n")
            return
        from IPython.display import Markdown, display

        for part in self._parts():
            display(Markdown(part.strip()) if isinstance(part, str) else part)


class Table(_Shown):
    """A titled table that prints as the plan's other tables do (.frame is the DataFrame)."""

    def __init__(self, title: str, frame: Any, note: str = "") -> None:
        """Keep the title, the DataFrame and a closing note."""
        self.title, self.frame, self.note = title, frame, note

    def _parts(self) -> list[Any]:
        return [self.title, self.frame, *([self.note] if self.note else [])]


class Sparql(str):
    """SPARQL text that prints as itself (not as a quoted string)."""

    def __repr__(self) -> str:
        """Print the query as it is written."""
        return str(self)


class Names(_Shown):
    """Objects reachable by attribute, with completion and a printout of what is there."""

    def __init__(self, title: str, items: Mapping[str, Any], *, snake: bool = False) -> None:
        """Keep *items* under attribute names made from their keys (snake_case with *snake*)."""
        self._title = title
        self._items: dict[str, Any] = {}
        for key, value in items.items():
            name, n = _attribute(key, snake), 2
            base = name
            while name in self._items:
                name, n = f"{base}_{n}", n + 1
            self._items[name] = value

    def __getattr__(self, name: str) -> Any:
        """Return the object named *name*; an unknown name lists the near ones."""
        items = self.__dict__.get("_items", {})
        if name in items:
            return items[name]
        near = sorted(k for k in items if _tokens(k) & _tokens(name))[:8]
        raise AttributeError(
            f"{self._title} has no {name!r}" + (f"; near: {', '.join(near)}" if near else "")
        )

    def __dir__(self) -> list[str]:
        """Offer every name for completion."""
        return sorted(self._items)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the objects."""
        return iter(self._items.values())

    def __len__(self) -> int:
        """Return how many objects there are."""
        return len(self._items)

    def table(self) -> Any:
        """Return the objects as a DataFrame: the attribute to use, then what each one is."""
        import pandas as pd

        return pd.DataFrame(
            [
                {"attribute": f".{name}", **value.row()}
                for name, value in sorted(self._items.items())
            ]
        )

    def _parts(self) -> list[Any]:
        return [f"{self._title} ({len(self._items)})", self.table()]


@dataclass(eq=False)
class Link:
    """A link of a source kind: its property, label, and the kinds or values it reaches."""

    kind: Kind
    name: str
    label: str
    property: str
    targets: tuple[str, ...]
    values: tuple[str, ...]

    def line(self) -> str:
        """Return one line describing the link."""
        reach = ", ".join(self.targets) or ", ".join(self.values) or "?"
        return f"{self.label} → {reach}"

    def row(self) -> dict[str, Any]:
        """Return the link as a table row."""
        row: dict[str, Any] = {
            "link": self.label,
            "reaches": ", ".join(self.targets) or ", ".join(self.values) or "?",
        }
        share = self.kind.client.link_support(self.kind.iri, self.property)
        if share is not None:  # tested paths: the share of the kind's records that follow it
            row["followed by"] = f"{share:.0%}"
        return row | {"property": self.property}

    def __repr__(self) -> str:
        """Describe the link."""
        return f"Link {self.kind.label}.{self.name}: {self.line()}  <{self.property}>"


@dataclass(eq=False)
class Kind:
    """A kind of record of a source (one of its classes), with its links."""

    client: Client
    model: Any
    label: str
    iri: str

    @property
    def links(self) -> Names:
        """Return the links of this kind, by name; what a link reaches is named by kind labels."""
        if "_links" not in self.__dict__:
            table = self.client.fields(self.model)
            names = {
                str(getattr(m, "rdf_class_iri", "")): self.client.type_name(m)
                for m in self.client.models.values()
            }
            self.__dict__["_links"] = Names(
                f"Links of {self.label}",
                {
                    str(row.label): Link(
                        self,
                        _attribute(str(row.label)),
                        str(row.label),
                        str(row.property),
                        tuple(names.get(str(x), str(x)) for x in cast("list[Any]", row.targets)),
                        tuple(cast("list[str]", row.values)),
                    )
                    for row in table.itertuples()
                },
            )
        return cast("Names", self.__dict__["_links"])

    def __getattr__(self, name: str) -> Link:
        """Return a link by name: kind.source."""
        if name.startswith("_") or name in {"client", "model", "label", "iri", "links", "paths"}:
            raise AttributeError(name)
        return cast("Link", getattr(self.links, name))

    def __dir__(self) -> list[str]:
        """Offer the links for completion."""
        return [*dir(self.links), "links"]

    def row(self) -> dict[str, Any]:
        """Return the kind as a table row."""
        return {"kind": self.label, "class": self.iri}

    @property
    def paths(self) -> Table:
        """Return the paths tested on the source that start at this kind (Client.add_paths), most
        followed first: each link they follow, and how many of the kind's records follow them.
        """
        import pandas as pd

        def short(iri: str) -> str:
            """Return the local name of an IRI."""
            return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]

        rows = [
            {
                "path": " → ".join([self.label, *(short(s.property_uri) for s in r.steps)]),
                "records": r.source_count,
                "follow": r.matched_sources,
                "share": round((r.matched_sources or 0) / r.source_count, 2)
                if r.source_count
                else None,
            }
            for r in self.client.tested_paths(self.iri)
        ]
        frame = pd.DataFrame(rows, columns=["path", "records", "follow", "share"])
        frame = frame.sort_values("share", ascending=False).reset_index(drop=True)
        return Table(f"Tested paths from {self.label} ({len(frame)})", frame)

    def __repr__(self) -> str:
        """Describe the kind and its links."""
        return f"Kind {self.label} <{self.iri}>\n{self.links!r}"

    def _repr_html_(self) -> str:
        """Show the kind and its links (notebooks)."""
        return f"<p>Kind {self.label} &lt;{self.iri}&gt;</p>" + self.links._repr_html_()


def kinds_of(client: Client) -> Names:
    """Return the kinds of a source's mined schema, by name (client.kinds)."""
    out = {}
    for model in client.models.values():
        label = client.type_name(model)
        out[label] = Kind(client, model, label, str(getattr(model, "rdf_class_iri", "")))
    return Names(f"Kinds of {client.schema.about.dataset_name or 'the source'}", out)


@dataclass(eq=False)
class Term:
    """A term of a target model: a class, a predicate (a slot) or a value of an enumeration."""

    target: Target
    name: str
    iri: str
    role: str  # "class", "predicate" or "value"
    definition: str = ""
    domain: str | None = None
    range: str | None = None
    parent: str | None = None
    id_prefixes: tuple[str, ...] = ()
    deprecated: bool = False
    enumeration: str | None = None

    def row(self) -> dict[str, Any]:
        """Return the term as a table row."""
        row: dict[str, Any] = {"term": self.name, "role": self.role}
        if self.role == "predicate":
            row |= {"from": self.domain or "any", "to": self.range or "any"}
        elif self.role == "class":
            row |= {"is a": self.parent or "", "identifiers": ", ".join(self.id_prefixes[:4])}
        else:
            row |= {"in": self.enumeration or ""}
        return row | ({"deprecated": "yes"} if self.deprecated else {})

    def __repr__(self) -> str:
        """Describe the term."""
        lines = [f"{self.role.capitalize()} {self.name}  <{self.iri}>"]
        if self.parent:
            lines.append(f"  is a: {self.parent}")
        if self.role == "predicate":
            lines.append(f"  from: {self.domain or 'any'}   to: {self.range or 'any'}")
        if self.id_prefixes:
            lines.append(f"  identifiers: {', '.join(self.id_prefixes)}")
        if self.definition:
            lines.append(f"  {self.definition.strip()[:300]}")
        if self.deprecated:
            lines.append("  deprecated in the target model")
        return "\n".join(lines)


class Target(_Shown):
    """A target model as objects: kinds, relations and values (Tab completes their names).

    *model* is any TargetModel (a LinkML schema, a property-graph metagraph, …), or a path to a
    LinkML schema. What the model's format does not state is listed in the printout, and the
    plan says what it cannot propose without it.
    """

    def __init__(self, model: Any) -> None:
        """Read the model's terms."""
        from rdfsolve.target_model import Model, TargetModel

        self.model = model if isinstance(model, TargetModel) else Model.read(model)
        m = self.model
        self._kinds = {k.name: k for k in m.kinds()}
        self._relations = {r.name: r for r in m.relations()}
        classes = {
            k.name: Term(
                self,
                k.name,
                k.iri or k.name,
                "class",
                k.definition,
                parent=(k.parents or (None,))[0],
                id_prefixes=k.identifiers,
                deprecated=k.deprecated,
            )
            for k in self._kinds.values()
        }
        predicates = {
            r.name: Term(
                self,
                r.name,
                r.iri or r.name,
                "predicate",
                r.definition,
                domain=r.domain,
                range=r.range,
                parent=(r.parents or (None,))[0],
                deprecated=r.deprecated,
            )
            for r in self._relations.values()
        }
        values = {
            v.name: Term(self, v.name, v.name, "value", enumeration=v.enumeration)
            for v in m.values()
        }
        label = f"{m.name or 'target'} {m.version}".strip()
        self.kinds = Names(f"Kinds of {label}", classes)
        self.relations = Names(f"Relations of {label}", predicates, snake=True)
        self.values = Names(f"Values of {label}", values, snake=True)

    @property
    def lacks(self) -> list[str]:
        """Return the capabilities the model does not have."""
        from rdfsolve.target_model import CAPABILITIES

        return [c for c in CAPABILITIES if c not in self.model.capabilities]

    def term(self, iri_or_name: str) -> Term | None:
        """Return the term of an IRI, a name or an attribute name (spaces or underscores), or None."""
        wanted = iri_or_name.replace("_", " ")
        for names in (self.kinds, self.relations, self.values):
            for t in names:
                if iri_or_name in (t.iri, t.name) or wanted == t.name:
                    return cast("Term", t)
        return None

    def named(self, name: str, role: str | None = None) -> Term:
        """Return the term of this name (spaces, underscores and capitals do not matter), or say
        which names are near; a name that is both a kind and a relation must be given as an object.
        """
        from rdfsolve.naming import words as split

        def key(text: str) -> str:
            """Return a name as lower-case words (camel case, spaces and underscores alike)."""
            return " ".join(w.lower() for w in split(text.replace("_", " ")))

        wanted = key(name)
        groups = {"class": self.kinds, "predicate": self.relations, "value": self.values}
        found = [
            t
            for r, names in groups.items()
            if role is None or r == role
            for t in names
            if key(t.name) == wanted or t.iri == name
        ]
        if len(found) == 1:
            return cast("Term", found[0])
        if len(found) > 1:
            raise ValueError(
                f"{name!r} names both a {found[0].role} and a {found[1].role} of {self.model.name}: "
                "give it as an object (plan.target.kinds.<Name> or plan.target.relations.<name>)"
            )
        words = _tokens(name)
        near = sorted(
            {
                t.name
                for r, names in groups.items()
                if role is None or r == role
                for t in names
                if words & _tokens(t.name)
            }
        )[:8]
        raise ValueError(
            f"{self.model.name} has no {role or 'term'} named {name!r}"
            + (f"; near: {', '.join(near)}" if near else "")
        )

    def require(self, iri_or_name: str) -> Term:
        """Return the term of an IRI or a name; a term the model does not have is an error."""
        found = self.term(iri_or_name)
        if found is None:
            raise KeyError(f"The target model has no term {iri_or_name!r}")
        return found

    def is_abstract(self, name: str) -> bool:
        """Return whether a kind or a relation is not used on its own (a mixin, an abstract one)."""
        info = self._kinds.get(name) or self._relations.get(name)
        return bool(info and info.abstract)

    def is_qualifier(self, relation: str) -> bool:
        """Return whether a relation is a qualifier of a statement."""
        info = self._relations.get(relation)
        return bool(info and info.qualifier)

    def depth(self, kind: str) -> int:
        """Return how many parents a kind has along its first parent (0 without a hierarchy)."""
        n, seen = 0, set()
        info = self._kinds.get(kind)
        while info and info.parents and info.name not in seen:
            seen.add(info.name)
            n += 1
            info = self._kinds.get(info.parents[0])
        return n

    def descends(self, cls: str | None, ancestor: str | None) -> bool:
        """Return whether kind *cls* is *ancestor* or below it; any when either is unknown."""
        if not ancestor or not cls:
            return True
        return cls == ancestor or ancestor in self.model.kind_ancestors(cls)

    def fits(self, relation: str, source: str | None, target: str | None) -> bool:
        """Return whether a relation may go from kind *source* to kind *target*."""
        info = self._relations.get(relation)
        if info is None:
            return False
        if info.pairs:
            return any(
                (source is None or self.descends(source, a))
                and (target is None or self.descends(target, b))
                for a, b in info.pairs
            )
        return self.descends(source, info.domain) and self.descends(target, info.range)

    def _parts(self) -> list[Any]:
        import pandas as pd

        m = self.model
        parts: list[Any] = [
            f"Target model {m.name} {m.version}".strip(),
            pd.DataFrame(
                [
                    {"terms": ".kinds", "how many": len(self.kinds), "what": "kinds of node"},
                    {
                        "terms": ".relations",
                        "how many": len(self.relations),
                        "what": "relations (edges)",
                    },
                    {
                        "terms": ".values",
                        "how many": len(self.values),
                        "what": "values of value lists",
                    },
                ]
            ),
        ]
        if self.lacks:
            parts.append("not stated by this model: " + ", ".join(self.lacks))
        return parts


@dataclass(eq=False)
class Option:
    """One option of a proposal: a target term, the evidence for it, and what it would do."""

    term: Term
    support: int
    why: str
    reading: str = ""  # "node", "edge" or "process" when it reads the kind otherwise than proposed
    ends: tuple[Link | None, Link | None] = (None, None)  # an edge reading's start and end links


@dataclass(eq=False, repr=False)
class Proposal(_Shown):
    """What one source kind (or its container link) becomes in the target."""

    plan: Plan
    kind: Kind
    role: str  # "node", "edge", "process" or "container"
    records: int
    options: list[Option]
    chosen: Option | None = None
    status: str = "proposed"  # "proposed", "chosen", "choose" or "left out"
    ends: tuple[Link | None, Link | None] = (None, None)
    qualifiers: dict[str, Term] = field(default_factory=dict)
    only_this_kind: bool = False
    note: str = ""
    ends_kinds: dict[str, tuple[str, ...]] = field(default_factory=dict)
    name: str = ""  # what the plan lists it as (the kind's label, or the container link's)
    iris: set[str] | None = None  # its records, as its rules select them (None: not known)

    def __post_init__(self) -> None:
        """List the proposal under its kind's label unless it has a name of its own."""
        self.name = self.name or self.kind.label

    def use(self, term: Term | str, **qualifiers: Term | str) -> Proposal:
        """Choose *term* for this kind, with qualifier values (slot name: value term).

        A term is an object of the plan's target (plan.target.kinds.<Name>,
        plan.target.relations.<name>, plan.target.values.<name>) or its name as text
        ("in complex with", "in_complex_with" and "InComplexWith" are one name).
        """
        p = self.plan
        if isinstance(term, str):
            term = p.target.named(term)
        values: dict[str, Term] = {
            slot: p.target.named(value, role="value") if isinstance(value, str) else value
            for slot, value in qualifiers.items()
        }
        if not isinstance(term, Term) or term.target is not p.target:
            raise TypeError(
                "Choose a term of the plan's target, as an object: plan.target.kinds.<Name> for a "
                "node, plan.target.relations.<name> for an edge (Tab completes the names)"
            )
        wanted = "class" if self.role in ("node", "process") else "predicate"  # edge, container
        offered = next((o for o in self.options if o.term is term), None)
        if offered is not None and offered.reading == "pairs" and self.role != "pairs":
            self.as_pairs(offered.ends[0])  # type: ignore[arg-type]
        elif term.role == "predicate" and self.role in ("node", "process"):
            # A relation: the records become edges, between the option's ends or the kind's.
            ends = offered.ends if offered and offered.ends[0] else self.edge_links()
            group = self.plan._group_link(self) if ends is None else None
            if ends is not None:
                self.as_edge(ends[0], ends[1])
            elif group is not None:
                self.as_pairs(group)  # each record relates the records its link reaches, in pairs
            else:
                name = f"plan.{_attribute(self.name)}"
                links = ", ".join(
                    f"'{x.label}' ({', '.join(x.targets[:2])})" for x in self.links_out()
                )
                raise ValueError(
                    f"{self.name} has no start and end link, and no link that reaches several "
                    f"records per record, to read its records as edges. Its links to other kinds: "
                    f"{links or 'none'}. Name them: {name}.as_edge('<start>', '<end>') or "
                    f"{name}.as_pairs('<link>'), then .use(<relation>); or use a kind for nodes."
                )
        elif term.role == "class" and self.role in ("edge", "pairs"):
            self.as_node()  # a class: the records become nodes
        elif term.role != wanted:
            raise ValueError(f"{self.name} is a {self.role}: choose a {wanted}, not a {term.role}")
        for slot, value in values.items():
            known = p.target.term(slot)
            if known is None or known.role != "predicate":
                raise ValueError(f"{slot!r} is not a slot of the target")
            if (
                not isinstance(value, Term)
                or value.role != "value"
                or (known.range and value.enumeration != known.range)
            ):
                raise ValueError(f"{slot} takes a value of {known.range}; {value!r} is not one")
        self.chosen = next(
            (o for o in self.options if o.term is term), Option(term, self.records, "chosen")
        )
        self.qualifiers = values
        self.status, self.note = "chosen", ""
        logger.info(
            "%s → %s%s: %s",
            self.name,
            term.name,
            "".join(f", {k} = {v.name}" for k, v in values.items()),
            p.consequence(self, term),
        )
        if self.role == "node":
            p._repropose_relations()
        p._hint()
        return self

    def as_edge(self, start: str | Link | None = None, end: str | Link | None = None) -> Proposal:
        """Convert this kind's records as edges between two of their links' ends.

        *start* and *end* are links of the kind (names as in kind.links, or Link objects); by
        default the two ends the plan found. Then choose the relation: .use(predicate).
        """
        ends = list(self.ends)
        for i, given in enumerate((start, end)):
            if given is None:
                continue
            if isinstance(given, Link):
                ends[i] = given
                continue
            found = [
                x
                for x in self.kind.links
                if _attribute(x.label).lower() == _attribute(given).lower()
            ]
            if not found:
                raise ValueError(
                    f"{self.name} has no link {given!r}; its links: "
                    + ", ".join(x.label for x in self.kind.links)
                )
            ends[i] = found[0]
        if ends[0] is None or ends[1] is None:
            raise ValueError(
                f"{self.name} has no two ends the plan could find; name them: "
                f"plan.{_attribute(self.name)}.as_edge(start='<link>', end='<link>') "
                f"(its links: {', '.join(x.label for x in self.kind.links)})"
            )
        self.ends = (ends[0], ends[1])
        self.ends_kinds = {link.label: link.targets for link in self.ends if link is not None}
        self.role, self.chosen, self.qualifiers = "edge", None, {}
        self.plan._repropose(self)
        logger.info(
            "%s will be converted as edges <%s> → <%s>; choose the relation with "
            "plan.%s.use(plan.target.relations.<name>).",
            self.name,
            ends[0].label,
            ends[1].label,
            _attribute(self.name),
        )
        self.plan._hint()
        return self

    def as_pairs(self, link: str | Link) -> Proposal:
        """Convert each record as edges between each pair of the records *link* reaches (members
        of one group, each related to the others). Then choose the relation: .use(predicate).
        """
        if isinstance(link, Link):
            chosen_link = link
        else:
            found = [
                x
                for x in self.kind.links
                if _attribute(x.label).lower() == _attribute(link).lower()
            ]
            if not found:
                raise ValueError(
                    f"{self.name} has no link {link!r}; its links: "
                    + ", ".join(x.label for x in self.kind.links)
                )
            chosen_link = found[0]
        self.role, self.chosen, self.qualifiers = "pairs", None, {}
        self.ends = (chosen_link, chosen_link)
        self.ends_kinds = {chosen_link.label: chosen_link.targets}
        self.plan._repropose(self)
        logger.info(
            "%s will be converted as edges between each pair of the records its %s reaches; choose "
            "the relation with plan.%s.use(plan.target.relations.<name>).",
            self.name,
            chosen_link.label,
            _attribute(self.name),
        )
        self.plan._hint()
        return self

    def as_node(self) -> Proposal:
        """Convert this kind's records as nodes (with a class) instead of as edges."""
        plan = self.plan
        own = plan._iris_by_kind().get(self.kind.label, set())
        edges = [o for o in self.options if (o.reading or self.role) == "edge"]
        ends = self.ends
        self.role, self.chosen, self.qualifiers, self.ends = "node", None, {}, (None, None)
        self.options = plan._class_options(own, self.kind.label) + [
            Option(o.term, o.support, o.why, "edge", o.ends if o.ends[0] else ends) for o in edges
        ]
        self.status = "choose"
        plan._settle(self)
        logger.info(
            "%s will be converted as nodes; choose the class with plan.%s.use(plan.target.kinds.<Name>).",
            self.name,
            _attribute(self.name),
        )
        plan._hint()
        return self

    def leave_out(self) -> Proposal:
        """Leave this kind out of the conversion."""
        self.status, self.chosen = "left out", None
        logger.info("%s left out: its %d records are not converted.", self.name, self.records)
        self.plan._hint()
        return self

    @property
    def target(self) -> Term | None:
        """Return the target term in use (chosen or proposed), or None."""
        return self.chosen.term if self.chosen else None

    def options_table(self) -> Any:
        """Return the options as a DataFrame: the term, the evidence, and what using it would do."""
        import pandas as pd

        return pd.DataFrame(
            [
                {
                    "#": i,
                    "as": o.reading or self.role,
                    "option": o.term.name,
                    "evidence": o.why,
                    "what it would do": self.plan.consequence(self, o.term, o.ends),
                }
                for i, o in enumerate(self.options[:8], 1)
            ]
        )

    @property
    def query(self) -> Sparql:
        """Return the SPARQL CONSTRUCT that plan.run() would write and run for this kind, as it is
        decided now (nothing is written or run).
        """
        from rdfsolve.conversion import write_query

        if self.status not in ("proposed", "chosen") or self.target is None:
            return Sparql(f"# {self.name} is {self.status}: no query until it is decided")
        key = next(k for k, p in self.plan.proposals.items() if p is self)
        for name, title, rules, focus in self.plan._rules():
            if name == _attribute(key, snake=True):
                return Sparql(
                    write_query(
                        rules,
                        title=title,
                        source=self.plan.client,
                        target=self.plan.target.model,
                        focus_as=focus,
                    )
                )
        return Sparql(f"# {self.name}: no query (its ends are not known)")

    def reading_options(self, reading: str) -> list[Option]:
        """Return the options that read this kind as *reading* ("node", "edge" or "process")."""
        return [o for o in self.options if (o.reading or self.role) == reading]

    def why_open(self) -> str:
        """Return, in words, why the evidence does not decide this kind."""
        if not self.options:
            return "no term of the target fits; choose any, or leave it out"
        own = self.reading_options(self.role)
        if not own:
            return "no term fits its own reading; another reading is offered"
        best = own[0]
        second = own[1] if len(own) > 1 else None
        if self.role in ("node", "process"):
            if best.support == 0 and "identifiers" in self.plan.target.model.capabilities:
                return "matched by name only; its records have no identifiers the target names kinds by"
            if second is not None and second.why == best.why:
                return f"{best.term.name} and {second.term.name} fit equally"
            return "the evidence is weak"
        if best.why.startswith("name") and second is not None and second.why == best.why:
            return f"{best.term.name} and {second.term.name} match its name equally"
        return "no relation's name matches its name; the most general that fit its ends are offered"

    def ends_evidence(self, ends: tuple[Link | None, Link | None]) -> str:
        """Return how many of this kind's records follow each end in the tested paths, if read."""
        shares = [self.plan.path_support(self.kind, e) if e else None for e in ends]
        if all(s is None for s in shares):
            return ""
        return (
            "; "
            + " and ".join("?" if s is None else f"{s:.0%}" for s in shares)
            + " of records have them"
        )

    def links_out(self) -> list[Link]:
        """Return the links of this kind to other kinds of the plan: the ends it could have."""
        kinds = set(self.plan.proposals)
        contents = self.plan.contents
        return [
            link
            for link in self.kind.links
            if set(link.targets) & kinds
            and not (contents is not None and link.property == contents.property)
        ]

    def ends_text(self) -> str:
        """Return the ends of an edge or a process (start → end); for a node, how to make it an edge."""
        if self.role == "pairs" and self.ends[0] is not None:
            return f"pairs over {self.ends[0].label}"
        if self.role in ("edge", "process") and self.ends != (None, None):
            return " → ".join(e.label if e else "?" for e in self.ends)
        line = self.edge_line()
        return "as edges: " + line.removeprefix(f"plan.{_attribute(self.name)}") if line else ""

    def edge_links(self) -> tuple[Link, Link] | None:
        """Return the two links this kind's records would be edges between, or None.

        The ends the data in scope supports (Plan._ends: links most records follow to other
        records of the plan) come first; then a link to another kind of the plan named as a
        start and one named as an end; else the two links most records of the whole source
        follow (the tested paths, plan.add_paths). The evidence of each is in its options.
        """
        found = self.plan._ends(self.kind, set(self.plan.proposals))
        if found[0] is not None and found[1] is not None:
            return found[0], found[1]
        links = self.links_out()
        if len(links) < 2:
            return None

        def side(link: Link, words: tuple[str, ...]) -> bool:
            """Return whether a link's name says it is this end."""
            return bool(set(_tokens(link.label)) & {w[:5] for w in words})

        first = next((x for x in links if side(x, FROM_WORDS)), None)
        second = next((x for x in links if side(x, TO_WORDS) and x is not first), None)
        if first is not None and second is not None:
            return first, second
        # No link named as a start and an end: the two most records of the whole source
        # follow (tested paths), when most follow both.
        followed = sorted(
            (x for x in links if (self.plan.path_support(self.kind, x) or 0) >= 0.5),
            key=lambda x: -(self.plan.path_support(self.kind, x) or 0),
        )
        return (followed[0], followed[1]) if len(followed) >= 2 else None

    def edge_line(self) -> str:
        """Return the one-liner that converts this node kind as edges, when it has edge links."""
        found = self.edge_links() if self.role == "node" else None
        if found is None:
            return ""
        return f"plan.{_attribute(self.name)}.as_edge('{found[0].label}', '{found[1].label}').use(<predicate>)"

    def _parts(self) -> list[Any]:
        lines = [f"{self.name} ({self.role}, {self.records} records): {self.status}"]
        if self.ends != (None, None):
            lines.append(
                f"  from: {self.ends[0].label if self.ends[0] else '?'}"
                f"   to: {self.ends[1].label if self.ends[1] else '?'}"
            )
        if self.only_this_kind:
            lines.append("  only records of no more specific kind of the source")
        if self.chosen:
            lines.append(f"  uses: {self.chosen.term.name}  ({self.chosen.why})")
            for slot, value in self.qualifiers.items():
                lines.append(f"    with {slot} = {value.name}")
        parts: list[Any] = ["\n".join(lines)]
        lines = []
        if self.options and self.status != "chosen":
            parts += ["options:", self.options_table()]
        if self.status == "choose":
            name = f"plan.{_attribute(self.name)}"
            lines.append(f"  open: {self.why_open()}; its records are not converted until decided")
            ends = self.edge_links() if self.role == "node" else self.ends
            ways = [f"{name}.use(plan.target.kinds.<Name>) as nodes"]
            if ends is not None and ends[0] is not None and ends[1] is not None:
                ways.append(
                    f"{name}.use(plan.target.relations.<name>) as edges {ends[0].label} → "
                    f"{ends[1].label}{self.ends_evidence(ends)}"
                )
            group = self.reading_options("pairs")
            if group and group[0].ends[0] is not None:
                ways.append(
                    f"{name}.use(plan.target.relations.<name>) as edges between each pair of its "
                    f"{group[0].ends[0].label}"
                )
            lines.append("  decide: " + "; or ".join(ways) + f"; or {name}.leave_out()")
            if self.role == "process" and not self.plan.target.model.process_kinds():
                lines.append(
                    "  the target has no process kinds: plan."
                    f"{_attribute(self.name)}.as_edge() converts each record as an edge "
                    "from its start to its end, or .leave_out()"
                )
        if self.note:
            lines.append(f"  note: {self.note}")
        return parts + (["\n".join(lines)] if lines else [])


class Choices(_Shown, list):  # type: ignore[type-arg]
    """The open choices of a plan: a list of proposals that prints as one table."""

    def _parts(self) -> list[Any]:
        import pandas as pd

        if not self:
            return ["Nothing open: plan.run() converts the records."]

        def names(options: list[Option], n: int = 2) -> str:
            """Return the first options' terms."""
            shown = ", ".join(o.term.name for o in options[:n])
            return shown + (f" (+{len(options) - n})" if len(options) > n else "")

        rows = []
        for p in self:
            nodes = p.reading_options("node") + p.reading_options("process")
            edges = p.reading_options("edge")
            pairs = p.reading_options("pairs")
            ends = edges[0].ends if edges and edges[0].ends[0] else p.ends
            edge_text = ""
            if edges and ends[0] is not None and ends[1] is not None:
                edge_text = (
                    f"{ends[0].label} → {ends[1].label}{p.ends_evidence(ends)}: {names(edges)}"
                )
            elif pairs and pairs[0].ends[0] is not None:
                edge_text = f"pairs over {pairs[0].ends[0].label}: {names(pairs)}"
            rows.append(
                {
                    "choose": f"plan.{_attribute(p.name)}",
                    "records": p.records,
                    "as nodes": names(nodes) if nodes else "",
                    "as edges": edge_text,
                    "why it is open": p.why_open(),
                }
            )
        first = self[0]
        name = f"plan.{_attribute(first.name)}"
        return [
            f"{len(self)} choices open; records of these kinds are not converted until decided.",
            pd.DataFrame(rows),
            (
                f"{name} lists every option with what it would do. Decide with "
                f"{name}.use(plan.target.kinds.<Name>) for nodes, {name}.use(plan.target.relations.<name>) "
                f"for edges, or {name}.leave_out()."
            ),
        ]


class Plan(_Shown):
    """The proposed conversion of the records in a scope into a target model."""

    def __init__(
        self,
        scope: Results,
        into: Target,
        *,
        contents: str | Link | None = None,
    ) -> None:
        """Propose a conversion for *scope* (records) and, through *contents*, what they contain.

        *contents* is the link between the scope and what it contains, written as in
        Results.related: "name" for a link of the scope's records to their contents, "^name"
        for a link of the contents to the scope's records (each record that is part of one of
        them). A Link object works too, from either side. Without it, only the scope's records
        are converted, and the plan lists the links that could give their contents.
        """
        self.client = scope.client
        self.target = into
        self.scope = scope
        if scope.coverage.get("status") == "partial":
            logger.warning(
                "The scope is partial: the search that found it stopped at the client's limit "
                "(max_subjects=%d, max_rows=%d). Records past the limit are not in the plan; open "
                "the client with a larger limit, or narrow the search.",
                self.client.max_subjects,
                self.client.max_rows,
            )
        self.kinds = kinds_of(self.client)
        self.contents, self.contents_incoming = self._contents_link(contents)
        records = list(scope.records)
        if self.contents is not None:
            from rdfsolve.client.hydration import HydrationLimitError

            try:
                inner = scope.related(
                    via=self.contents.label, incoming=self.contents_incoming, depth=1
                )
            except HydrationLimitError as error:
                raise ValueError(
                    f"{self._contents_text()} reaches more records of one kind than the client "
                    f"reads in one call (max_subjects={self.client.max_subjects}). Open the client "
                    "with a larger max_subjects, or start from fewer records."
                ) from error
            records += list(inner.records)
        self.records = records
        self.proposals: dict[str, Proposal] = {}
        self.identity: Identities | None = None
        self._propose()
        self._hint()

    def path_support(self, kind: Kind, link: Link) -> float | None:
        """Return the share of a kind's records (source-wide) that follow *link* in the tested
        paths the client has (Client.add_paths), or None.
        """
        return self.client.link_support(kind.iri, link.property)

    def add_paths(self, paths: str | Path) -> Plan:
        """Add tested paths to the client (Client.add_paths) and propose again the other readings
        of every kind with them; log the kinds whose options changed.
        """
        before = {
            k: [(o.reading, o.term.name, o.why) for o in p.options]
            for k, p in self.proposals.items()
        }
        self.client.add_paths(paths)
        iris = self._iris_by_kind()
        for p in self.proposals.values():
            p.options = [o for o in p.options if not o.reading]
            self._add_readings(p, iris)
        changed = [
            k
            for k, p in self.proposals.items()
            if [(o.reading, o.term.name, o.why) for o in p.options] != before[k]
        ]
        logger.info(
            "Options changed with the tested paths: %s",
            ", ".join(f"plan.{_attribute(k)}" for k in changed) or "none",
        )
        self._hint()
        return self

    def _scope_kinds(self) -> set[str]:
        """Return the record types of the scope."""
        return {self.client.type_name(type(r)) for r in self.scope.records}

    def _contents_text(self) -> str:
        """Return the contents link as written in Results.related ("^name" when it points in)."""
        if self.contents is None:
            return "no contents"
        return f'contents="{"^" if self.contents_incoming else ""}{self.contents.label}"'

    def _container_links(self) -> list[tuple[str, str]]:
        """Return the links that could give the scope's contents: (via text, kinds that have it)."""
        scope = self._scope_kinds()
        found: dict[str, set[str]] = defaultdict(set)
        for kind in self.kinds:
            for link in kind.links:
                if kind.label in scope and set(link.targets) - scope:
                    found[link.label].add(kind.label)
                elif kind.label not in scope and set(link.targets) & scope:
                    found["^" + link.label].add(kind.label)
        return sorted(((via, ", ".join(sorted(k))) for via, k in found.items()), key=lambda x: x[0])

    def _contents_link(self, contents: str | Link | None) -> tuple[Link | None, bool]:
        """Return the contents link and whether it points to the scope (from the contents)."""
        if contents is None:
            return None, False
        scope = self._scope_kinds()
        if isinstance(contents, Link):
            return contents, contents.kind.label not in scope
        incoming = contents.startswith("^")
        label = contents.lstrip("^")
        for kind in self.kinds:
            if (kind.label in scope) == incoming:
                continue
            for link in kind.links:
                if _attribute(link.label) == _attribute(label) and (
                    not incoming or set(link.targets) & scope
                ):
                    return link, incoming
        choices = "\n".join(
            f'  contents="{via}"   ({kinds})' for via, kinds in self._container_links()
        )
        raise ValueError(
            f"No link {contents!r} between the scope ({', '.join(sorted(scope))}) and other "
            f'records. The links that could give its contents ("^" points to the scope):\n{choices}'
        )

    def _hint(self) -> None:
        """Log what the plan holds and the next step, with the objects to use."""
        counts = Counter(p.status for p in self.proposals.values())
        logger.info(
            "Plan: %d kinds (%s). Print plan for the table; plan.<Kind> for one kind; "
            "plan.show(diagram=True) to draw it.",
            len(self.proposals),
            ", ".join(f"{n} {s}" for s, n in sorted(counts.items())),
        )
        if self.open:
            first = _attribute(self.open[0].kind.label)
            logger.info(
                "%d choices open: %s. Print one to see its options and their consequences, then "
                "plan.%s.use(plan.target.kinds.<Name> or plan.target.relations.<name>); "
                "plan.%s.leave_out() to leave it out.",
                len(self.open),
                ", ".join(f"plan.{_attribute(p.name)}" for p in self.open),
                first,
                first,
            )
        else:
            logger.info("Nothing open. Next: plan.run() converts the records.")

    # -- evidence ------------------------------------------------------------------------------

    def _iris_by_kind(self) -> dict[str, set[str]]:
        """Return the IRIs of the records of each kind in the plan."""
        out: dict[str, set[str]] = defaultdict(set)
        for r in self.records:
            out[self.client.type_name(type(r))].add(str(vars(r)["uri"]))
        return out

    def _children(self) -> dict[str, set[str]]:
        """Return the kinds below each kind, as the source's data relates their members."""
        ext = self.client.schema.class_extensions
        label = {k.iri: k.label for k in self.kinds}
        out: dict[str, set[str]] = defaultdict(set)
        for child, parents in ((ext.contained_in if ext else None) or {}).items():
            for parent in parents:
                if child in label and parent in label:
                    out[label[parent]].add(label[child])
        return out

    def _class_options(self, iris: set[str], label: str) -> list[Option]:
        """Return target kinds for records with these IRIs, best first.

        With identifier namespaces in the target: by how many records' identifiers a kind takes,
        then how many are in the namespace it prefers (listed first), then the name, then the
        broadest kind. Without them: by the words of the name; when none match, every kind is an
        option, for the user to choose.
        """
        from rdfsolve.identifiers import parse

        prefixes = Counter(found.prefix for i in iris if (found := parse(i)) is not None)
        name_words = _tokens(label)
        with_ids = "identifiers" in self.target.model.capabilities
        scored = []
        for term in self.target.kinds:
            if term.deprecated or self.target.is_abstract(term.name):
                continue
            listed = list(term.id_prefixes)
            support = sum(n for p, n in prefixes.items() if p in listed) if with_ids else 0
            preferred = prefixes.get(listed[0], 0) if with_ids and listed else 0
            words = _tokens(term.name) & name_words
            if not (support or words):
                continue
            reason = "; ".join(
                x
                for x in (
                    "ids: "
                    + ", ".join(f"{p} {n}" for p, n in prefixes.most_common() if p in listed)
                    if support
                    else "",
                    f"preferred {listed[0]}" if preferred else "",
                    "name" if words else "",
                )
                if x
            )
            scored.append(
                (support, preferred, len(words), -self.target.depth(term.name), term, reason)
            )
        scored.sort(key=lambda s: s[:4], reverse=True)
        if scored:
            return [Option(t, n, r) for n, _, _, _, t, r in scored]
        return [
            Option(t, 0, "a kind of the target")
            for t in self.target.kinds
            if not t.deprecated and not self.target.is_abstract(t.name)
        ]

    def _predicate_options(
        self, label: str, definition: str, domain: str | None, range_: str | None
    ) -> list[Option]:
        """Return target relations for a source relation, best first.

        Only relations that may go between the ends' target kinds count (Target.fits). They are
        ranked by the words of the source relation's name in the relation's name, then in its
        definition (when the target has definitions); when no word matches, the fitting
        relations are offered most general first (the most relations of the target descend from them).
        """
        words = _tokens(label) | {w for w in _tokens(definition) if len(w) > 3}
        fitting = [
            term
            for term in self.target.relations
            if not term.deprecated
            and not self.target.is_qualifier(term.name)
            and self.target.fits(term.name, domain, range_)
        ]
        scored = []
        for term in fitting:
            hits = _tokens(term.name) & _tokens(label)
            loose = _tokens(f"{term.name} {term.definition}") & words
            if hits or len(loose) >= 2:
                why = (
                    f"name: {', '.join(sorted(hits))}"
                    if hits
                    else f"definition: {', '.join(sorted(loose))[:60]}"
                )
                extra = len(_tokens(term.name) - _tokens(label))
                scored.append((len(hits), -extra, len(loose), term, why))
        scored.sort(key=lambda s: s[:3], reverse=True)
        if scored:
            return [Option(t, h, r) for h, _, _, t, r in scored[:8]]
        info = self.target._relations
        # With no word to go by, the most general relations are offered first: those that the most
        # relations of the target descend from (without a hierarchy, those with stated ends).
        below = Counter(a for t in self.target.relations for a in self._slot_ancestors(t.name))
        usable = [t for t in fitting if not self.target.is_abstract(t.name)]
        ranked = sorted(
            usable, key=lambda t: (-below[t.name], len(self._slot_ancestors(t.name)), t.name)
        )
        if not below:
            ranked = [
                t for t in ranked if info[t.name].domain or info[t.name].range or info[t.name].pairs
            ]
        return [Option(t, 0, "a general relation that fits the ends") for t in ranked[:8]]

    def _slot_ancestors(self, name: str) -> list[str]:
        """Return a predicate's ancestors (none without a hierarchy)."""
        return self.target.model.relation_ancestors(name)

    def _reached(self, label: str, link: Link) -> dict[str, set[str]]:
        """Return, for each record of kind *label*, the records of the plan its *link* reaches."""
        from rdfsolve.client.api import Results

        if (label, link.property) not in self._reach:
            records = [r for r in self.records if self.client.type_name(type(r)) == label]
            found: dict[str, set[str]] = {}
            try:
                values = Results(self.client, records)._links(link.label)
            except Exception as error:  # a link the records cannot be read by is not an end
                logger.debug("Link %s of %s not read: %s", link.label, label, error)
                values = {}
            for record, targets in values.items():
                found[str(record)] = {str(t) for t in targets} & self._all_iris
            self._reach[(label, link.property)] = found
        return self._reach[(label, link.property)]

    def _ends(self, kind: Kind, present: set[str]) -> tuple[Link | None, Link | None]:
        """Return the two links by which the records of a relation kind reach other records of
        the plan: those most of its records have, the one named as a start first.
        """
        records = (
            len(
                {
                    str(vars(r)["uri"])
                    for r in self.records
                    if self.client.type_name(type(r)) == kind.label
                }
            )
            or 1
        )
        reaching = []
        for link in kind.links:
            if not set(link.targets) & present or (
                self.contents is not None and link.property == self.contents.property
            ):
                continue
            covered = sum(1 for targets in self._reached(kind.label, link).values() if targets)
            if covered * 2 >= records:
                reaching.append((covered, link))
        reaching.sort(key=lambda x: x[0], reverse=True)
        links = [link for _, link in reaching[:4]]

        def side(link: Link, words: tuple[str, ...]) -> bool:
            """Return whether a link's name says it is this end."""
            return bool(_tokens(link.label) & {w[:5] for w in words})

        first = next((x for x in links if side(x, FROM_WORDS)), None)
        second = next((x for x in links if side(x, TO_WORDS) and x is not first), None)
        if first is None or second is None:
            rest = [x for x in links if x not in (first, second)]
            first = first or (rest.pop(0) if rest else None)
            second = second or (rest.pop(0) if rest else None)
        return first, second

    # -- proposals -----------------------------------------------------------------------------

    def _propose(self) -> None:
        """Make a proposal for every kind of record in the plan."""
        from rdfsolve.identifiers import parse

        iris = self._iris_by_kind()
        self._all_iris = set().union(*iris.values()) if iris else set()
        self._reach: dict[tuple[str, str], dict[str, set[str]]] = {}
        children = self._children()
        present = set(iris)
        identified = {k: any(parse(i) is not None for i in v) for k, v in iris.items()}
        candidates = [k for k in present if not identified[k]]
        ends = {k: self._ends(getattr(self.kinds, _attribute(k)), present) for k in candidates}
        relations = {k for k, (a, b) in ends.items() if a is not None and b is not None}
        # A relation is a process (a node with inputs and outputs) when another relation's
        # records point at its records, in the data.
        pointed = set()
        for k in relations:
            for link in ends[k]:
                if link is None:
                    continue
                for targets in self._reached(k, link).values():
                    pointed |= targets
        # Nodes first, so that a relation's ends have their target classes.
        for label in sorted(present, key=lambda k: (k in relations, k)):
            kind = getattr(self.kinds, _attribute(label))
            below = set().union(*(iris.get(c, set()) for c in children.get(label, ())))
            generic = bool(below)
            # Records of exactly this kind, as the rules select them (Rule exact_kind): those
            # that are not also of a kind at the same level or below it.
            own = iris[label] - (self._exact_excluded(kind, iris) if generic else below)
            count = len(own)
            if label in relations and not (own & pointed):
                domain = self._kind_target_of(ends[label][0])
                range_ = self._kind_target_of(ends[label][1])
                options = self._predicate_options(label, "", domain, range_)
                p = Proposal(
                    self, kind, "edge", count, options, ends=ends[label], only_this_kind=generic
                )
            elif label in relations:
                processes = self.target.model.process_kinds()
                options = [
                    o
                    for o in self._class_options(own, label)
                    if o.term.name in processes and o.support
                ]
                broadest = sorted(
                    (n for n in processes if self.target.term(n)), key=self.target.depth
                )
                options = options or [
                    Option(
                        self.target.require(n),
                        0,
                        "the broadest process kind of the target"
                        if i == 0
                        else "a narrower process kind",
                    )
                    for i, n in enumerate(broadest[:6])
                ]
                p = Proposal(
                    self, kind, "process", count, options, ends=ends[label], only_this_kind=generic
                )
            else:
                options = self._class_options(own, label)
                p = Proposal(self, kind, "node", count, options, only_this_kind=generic)
                if not identified[label]:
                    p.note = "its records have no identifiers of a registered namespace: proposed by name only"
            p.iris = own
            self.proposals[label] = p
            if count == 0:
                p.status, p.note = "left out", "every record is of a more specific kind"
            else:
                self._settle(p)
            p.ends_kinds = {link.label: link.targets for link in p.ends if link is not None}
        for p in self.proposals.values():
            self._add_readings(p, iris)
        if self.contents is not None:
            self._propose_container(present)

    def _group_link(self, p: Proposal) -> Link | None:
        """Return the link by which most of a kind's records in scope reach two or more records
        of the plan (members of a group), the one that reaches most; or None.
        """
        best: tuple[float, Link | None] = (0.0, None)
        for link in p.links_out():
            reached = [
                v
                for r, v in self._reached(p.kind.label, link).items()
                if p.iris is None or r in p.iris
            ]
            several = sum(1 for v in reached if len(v) >= 2)
            if reached and several * 2 >= len(reached):
                mean = sum(len(v) for v in reached) / len(reached)
                if mean > best[0]:
                    best = (mean, link)
        return best[1]

    def _exact_excluded(self, kind: Kind, iris: Mapping[str, set[str]]) -> set[str]:
        """Return the records of the kinds a record of exactly *kind* is not (same level or below)."""
        from rdfsolve.conversion import _other_kinds

        labels = {k.iri: k.label for k in self.kinds}
        return set().union(
            *(iris.get(labels.get(c, ""), set()) for c in _other_kinds(self.client, kind.iri))
        )

    def _add_readings(self, p: Proposal, iris: Mapping[str, set[str]]) -> None:
        """Add the other readings of a kind to its options, after its own: a node kind with two
        links to other kinds can be edges between them; an edge or process kind can be nodes.
        """
        if p.status == "left out":
            return
        if p.role == "node" and p.edge_links() is None:
            group = self._group_link(p)
            if group is not None:
                target = self._kind_target_of(group)
                p.options += [
                    Option(
                        o.term,
                        o.support,
                        f"pairs over {group.label}; {o.why}",
                        "pairs",
                        (group, group),
                    )
                    for o in self._predicate_options(p.name, "", target, target)[:4]
                ]
            return
        if p.role == "node":
            ends = p.edge_links()
            if ends is None:
                return
            supported = self._ends(p.kind, set(self.proposals))
            shares = [self.path_support(p.kind, x) for x in ends]
            how = (
                "the data in scope"
                if supported[0] is not None and supported[1] is not None
                else "the schema"
            )
            if any(s is not None for s in shares):  # how many of its records follow each end
                how += (
                    "; followed by "
                    + " and ".join("?" if s is None else f"{s:.0%}" for s in shares)
                    + " of its records"
                )
            domain, range_ = self._kind_target_of(ends[0]), self._kind_target_of(ends[1])
            p.options += [
                Option(
                    o.term,
                    o.support,
                    f"edges {ends[0].label} → {ends[1].label} ({how}); {o.why}",
                    "edge",
                    ends,
                )
                for o in self._predicate_options(p.name, "", domain, range_)[:4]
            ]
        else:
            p.options += [
                Option(o.term, o.support, f"nodes; {o.why}", "node")
                for o in self._class_options(iris.get(p.kind.label, set()), p.kind.label)[:3]
                if o.support or o.why.startswith("name")  # evidence for nodes, not any kind
            ]

    def _propose_container(self, present: set[str]) -> None:
        """Propose the relation that says what the records are part of (the contents link)."""
        link = self.contents
        if link is None:
            return
        holders = {self.client.type_name(type(r)) for r in self.scope.records}
        holder = next(iter(holders)) if len(holders) == 1 else None
        kind = getattr(self.kinds, _attribute(holder)) if holder else link.kind
        held = self.proposals.get(holder) if holder else None
        range_ = held.target.name if held is not None and held.target is not None else None
        options = self._predicate_options(link.label, "", None, range_)
        p = Proposal(
            self,
            kind,
            "container",
            len(self.records) - len(self.scope.records),
            options,
            ends=(None, link),
            name=link.label,
        )
        p.note = f"each record {link.label} a {holder or 'record of the scope'}"
        self._settle(p)
        self.proposals[link.label] = p

    def _repropose(self, p: Proposal) -> None:
        """Make the options of a relation again, from its ends' current target kinds."""
        domain, range_ = self._kind_target_of(p.ends[0]), self._kind_target_of(p.ends[1])
        others = [o for o in p.options if o.reading and o.reading != "edge"]
        p.options = self._predicate_options(p.name, "", domain, range_) + others
        p.chosen, p.status = None, "choose"
        self._settle(p)

    def _repropose_relations(self) -> None:
        """Make the options of every undecided relation again (after a node's kind was chosen)."""
        for p in self.proposals.values():
            if p.role == "edge" and p.status != "chosen":
                self._repropose(p)

    def _kind_target_of(self, link: Link | None) -> str | None:
        """Return the target class of the records a link reaches, when they are of one class."""
        if link is None:
            return None
        found = {
            term.name
            for k in link.targets
            if k in self.proposals
            and (term := self.proposals[k].target) is not None
            and self.proposals[k].role in ("node", "process")
        }  # a kind converted as edges has no node kind
        return next(iter(found)) if len(found) == 1 else None

    def _kind_target(self, link: Link | None) -> str | None:
        """Return the target class proposed for the kind a link reaches, when one kind is reached."""
        if link is None or len(link.targets) != 1:
            return None
        p = self.proposals.get(link.targets[0])
        return p.target.name if p and p.target and p.role in ("node", "process") else None

    @staticmethod
    def _closer(best: Option, second: Option, p: Proposal) -> bool:
        """Return whether *best*'s name has only words of the source's name and *second*'s has more."""
        words = _tokens(p.name)
        return not (_tokens(best.term.name) - words) and bool(_tokens(second.term.name) - words)

    def _settle(self, p: Proposal) -> None:
        """Propose the best option when the evidence decides it, else leave the choice open.

        A node's kind is decided by its records' identifiers (when the target states identifier
        namespaces), else by its name; a name alone does not decide where the target states
        identifiers and the records have none. A relation is decided only by a word of its name.
        Ties are left open, with the tied options first.
        """
        if not p.options:
            p.status = "choose" if p.role in ("edge", "process") else "left out"
            p.note = "no target term fits the evidence; choose one, or leave it out"
            return
        best = p.options[0]
        second = p.options[1] if len(p.options) > 1 else None
        if p.role in ("node", "process"):
            by_ids = best.support > 0
            by_name = "name" in best.why and "identifiers" not in self.target.model.capabilities
            if best.why.startswith("the broadest process kind"):
                # A process with no other evidence is the broadest process kind: it claims least.
                p.chosen, p.status = best, "proposed"
                return
            tied = second is not None and second.support == best.support and second.why == best.why
            if (by_ids or by_name) and not (by_name and tied):
                p.chosen, p.status = best, "proposed"
            else:
                p.status = "choose"
        elif best.why.startswith("name") and not (
            second and second.why == best.why and not self._closer(best, second, p)
        ):
            p.chosen, p.status = best, "proposed"
        else:
            p.status = "choose"

    # -- the user's view -----------------------------------------------------------------------

    def __getattr__(self, name: str) -> Proposal:
        """Return the proposal of a kind: plan.Inhibition."""
        proposals = self.__dict__.get("proposals", {})
        for label, p in proposals.items():
            if _attribute(label) == name:
                return cast("Proposal", p)
        raise AttributeError(
            f"The plan has no kind {name!r}; kinds: {', '.join(_attribute(k) for k in proposals)}"
        )

    def __dir__(self) -> list[str]:
        """Offer the kinds for completion."""
        return sorted(_attribute(k) for k in self.proposals)

    def consequence(
        self, p: Proposal, term: Term, ends: tuple[Link | None, Link | None] = (None, None)
    ) -> str:
        """Return what using *term* for *p* would do: statements written, and what the target says."""
        if term.role == "class":
            text = f"{p.records} records typed {term.name}"
            if term.id_prefixes:
                text += f"; the target names them by {', '.join(term.id_prefixes[:3])}"
            return text
        if ends == (None, None):
            ends = p.ends if p.ends != (None, None) else (p.edge_links() or (None, None))
        a, b = (e.label if e else "?" for e in ends)
        if ends[0] is not None and ends[0] is ends[1]:  # pairs of what one link reaches
            reached = self._reached(p.kind.label, ends[0])
            pairs = sum(
                len(v) * (len(v) - 1) // 2
                for r, v in reached.items()
                if p.iris is None or r in p.iris
            )
            return f"{pairs} edges, each pair of the records {a} reaches related by {term.name}" + (
                f"; between {term.domain}s" if term.domain and term.domain == term.range else ""
            )
        edges = p.records
        if ends[0] is not None and ends[1] is not None:  # records in scope that have both ends
            starts, stops = (
                self._reached(p.kind.label, ends[0]),
                self._reached(p.kind.label, ends[1]),
            )
            edges = sum(
                1
                for r, found in starts.items()
                if found and stops.get(r) and (p.iris is None or r in p.iris)
            )
        fit = []
        if term.domain:
            fit.append(f"from must be {term.domain}")
        if term.range:
            fit.append(f"to must be {term.range}")
        missing = p.records - edges
        text = f"{edges} edges <{a}> {term.name} <{b}>"
        if missing:
            text += f" ({missing} of {p.records} records lack an end in scope)"
        return text + (f"; {', '.join(fit)}" if fit else "")

    @property
    def open(self) -> Choices:
        """Return the proposals that wait for a choice (a list that prints as a table)."""
        return Choices(p for p in self.proposals.values() if p.status == "choose")

    def table(self) -> Any:
        """Return the plan as a DataFrame: every kind, its role, records, status, target and why."""
        import pandas as pd

        rows = []
        for label, p in sorted(self.proposals.items()):
            why = p.chosen.why if p.chosen else (p.options[0].why if p.options else p.note)
            rows.append(
                {
                    "kind": label,
                    "role": p.role,
                    "records": p.records,
                    "status": p.status,
                    "becomes": p.target.name if p.target else "",
                    "options": len(p.options),
                    "why": why,
                }
            )
        return pd.DataFrame(rows)

    def _parts(self) -> list[Any]:
        import pandas as pd

        source = self.client.schema.about.dataset_name or "the source"
        target = f"{self.target.model.name} {self.target.model.version}".strip()
        parts: list[Any] = [f"Plan: {len(self.records)} records of {source} → {target}"]
        if self.target.lacks:
            parts.append(
                f"{target} does not state: {', '.join(self.target.lacks)}; proposals use only what it states."
            )
        decided = [
            (k, p) for k, p in sorted(self.proposals.items()) if p.status in ("proposed", "chosen")
        ]
        if decided:
            parts.append(f"\nDecided ({len(decided)}):")
            parts.append(
                pd.DataFrame(
                    [
                        {
                            "kind": k,
                            "role": p.role,
                            "records": p.records,
                            "becomes": p.target.name if p.target else "",
                            "ends": p.ends_text() if p.role != "node" else "",
                            "how": p.status,
                            "why": _clip(p.chosen.why if p.chosen else "", 44),
                        }
                        for k, p in decided
                    ]
                )
            )
        if self.open:
            parts.append(f"\nTo choose ({len(self.open)}): not converted until decided")
            parts.append(
                pd.DataFrame(
                    [
                        {
                            "kind": p.name,
                            "role": p.role,
                            "records": p.records,
                            "options": _clip(
                                f"{len(p.options)}: "
                                + ", ".join(o.term.name for o in p.options[:3])
                                + (", …" if len(p.options) > 3 else ""),
                                50,
                            )
                            if p.options
                            else "none fits",
                            "ends": _clip(p.ends_text(), 70),
                        }
                        for p in sorted(self.open, key=lambda p: (p.role, p.name))
                    ]
                )
            )
        left = [k for k, p in sorted(self.proposals.items()) if p.status == "left out"]
        if left:
            parts.append(f"\nLeft out ({len(left)}): " + ", ".join(left))
        first = (
            _attribute(self.open[0].name)
            if self.open
            else _attribute(next(iter(sorted(self.proposals)), "Kind"))
        )
        parts.append(
            f"\nplan.{first} shows a kind's options and what each would do; "
            f"plan.{first}.use(<term>) or .leave_out() decides it. Kinds are named without spaces."
        )
        return parts

    # -- identity ------------------------------------------------------------------------------

    def identify(self, using: Iterable[Any] = (), exact: bool = True) -> Identities:
        """Decide which records name one entity, for each node kind the target names by identifiers.

        For each kind, the namespace is the first the target lists for its proposed kind that
        the records' identifiers use (else the first it lists). The records' own statements are read (Claims.of), then each source in *using*
        is asked which of its entries carry these identifiers (Claims.ask). The source that
        issues the namespace decides, then the others in order. *exact* takes the
        cross-references into that namespace as exact matches (an assumption, recorded with
        each decision); without it only declared identities join records.
        """
        from rdfsolve.identifiers import parse
        from rdfsolve.mappings.claims import Claims

        by_kind = self._iris_by_kind()
        decided: dict[str, Any] = {}
        for label, p in sorted(self.proposals.items()):
            if (
                p.role != "node"
                or p.target is None
                or not p.target.id_prefixes
                or p.status == "left out"
            ):
                continue
            iris = sorted(by_kind.get(p.kind.label, ()))
            used = {found.prefix for i in iris if (found := parse(i)) is not None}
            namespace = next(
                (n for n in p.target.id_prefixes if n in used), p.target.id_prefixes[0]
            )
            records = self.client.from_table(p.kind.iri, iris).load()
            claims = Claims.of(self.client, records=[records])
            for other in using:
                claims = claims.ask(client=other, through=True)
            if not len(claims):
                continue
            decided[label] = (
                namespace,
                claims.decide(namespaces=[namespace], exact=[namespace] if exact else []),
            )
        self.identity = Identities(decided, exact)
        logger.info("%r", self.identity)
        return self.identity

    # -- running the plan ----------------------------------------------------------------------

    def _rules(self) -> list[tuple[str, str, list[Any], str]]:
        """Return the rules of every decided proposal: (name, title, rules, focus variable)."""
        from rdfsolve.conversion import Rule

        model, client = self.target.model, self.client
        naming = model.naming_relation()
        naming_term = self.target.term(naming) if naming else None
        statement = model.statement_kind()
        to_input, to_output = model.process_relations()
        out: list[tuple[str, str, list[Any], str]] = []
        for label, p in sorted(self.proposals.items()):
            if p.status not in ("proposed", "chosen") or p.target is None:
                continue
            exact = p.only_this_kind
            if p.role == "container":
                if p.ends[1] is None:
                    continue
                holder = p.kind.iri
                rules = [
                    Rule.between(
                        client,
                        focus=holder,
                        predicate=p.target.iri,
                        subject=("^" if self.contents_incoming else "") + p.ends[1].label,
                        subject_as="member",
                    )
                ]
                out.append(
                    (
                        _attribute(label, snake=True),
                        f"{p.ends[1].label} becomes {p.target.name}",
                        rules,
                        _attribute(p.kind.label, snake=True),
                    )
                )
                continue
            name = _attribute(label, snake=True)
            if p.role in ("node", "process"):
                rules = [
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate="a",
                        value=p.target.iri,
                        exact_kind=exact,
                    )
                ]
                link = next((x for x in p.kind.links if x.property in NAMING_PROPERTIES), None)
                if naming_term is not None and link is not None:
                    rules.append(
                        Rule.between(
                            client,
                            focus=p.kind.iri,
                            predicate=naming_term.iri,
                            object=link.label,
                            object_as="name",
                            exact_kind=exact,
                        )
                    )
                if p.role == "process":
                    for relation, end, as_ in (
                        (to_input, p.ends[0], "input"),
                        (to_output, p.ends[1], "output"),
                    ):
                        term = self.target.term(relation) if relation else None
                        if term is not None and end is not None:
                            rules.append(
                                Rule.between(
                                    client,
                                    focus=p.kind.iri,
                                    predicate=term.iri,
                                    object=end.label,
                                    object_as=as_,
                                    exact_kind=exact,
                                )
                            )
                out.append((name, f"{label} becomes {p.target.name}", rules, name))
                continue
            a, b = p.ends
            if a is None or b is None:
                continue
            if p.role == "pairs":
                rules = [
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate=p.target.iri,
                        subject=a.label,
                        subject_as="member",
                        pairs=True,
                        exact_kind=exact,
                    )
                ]
                out.append(
                    (name, f"{label} becomes {p.target.name} between its {a.label}", rules, name)
                )
                continue
            if p.qualifiers and statement:
                kind_term = self.target.require(statement)
                slot = {s: self.target.require(s) for s in model.STATEMENT_SLOTS}  # type: ignore[attr-defined]
                rules = [
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate="a",
                        value=kind_term.iri,
                        exact_kind=exact,
                    ),
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate=slot["subject"].iri,
                        object=a.label,
                        object_as="subject",
                        exact_kind=exact,
                    ),
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate=slot["predicate"].iri,
                        value=p.target.iri,
                        exact_kind=exact,
                    ),
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate=slot["object"].iri,
                        object=b.label,
                        object_as="object",
                        exact_kind=exact,
                    ),
                ] + [
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate=self.target.require(q).iri,
                        value=v.name,
                        literal=True,
                        exact_kind=exact,
                    )
                    for q, v in p.qualifiers.items()
                ]
            else:
                rules = [
                    Rule.between(
                        client,
                        focus=p.kind.iri,
                        predicate=p.target.iri,
                        subject=a.label,
                        subject_as="start",
                        object=b.label,
                        object_as="end",
                        exact_kind=exact,
                    )
                ]
            out.append((name, f"{label} becomes {p.target.name}", rules, name))
        return out

    def run(self, folder: str | Path = "queries") -> Network:
        """Convert the scope by the decided proposals; return the network in the target model.

        The rules are written as SPARQL CONSTRUCT files in *folder* (one per kind, for anyone to
        read or run again; "queries" in the current directory by default; earlier .rq files
        there are replaced), run on the source within the scope, and followed by what the
        target model's conventions imply. Open choices are not converted, and are listed.
        """
        from rdfsolve.conversion import Profile, derive_conversions, within, write_query

        if self.open:
            logger.warning(
                "Not converted, still open: %s. Decide them first for a complete conversion.",
                ", ".join(f"plan.{_attribute(p.name)}" for p in self.open),
            )
        folder = Path(folder).expanduser().resolve()
        try:
            folder.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ValueError(
                f"The queries cannot be written to {folder} ({error.strerror}); give a folder you "
                "can write to: plan.run('<folder>')"
            ) from error
        for old in folder.glob("*.rq"):
            old.unlink()
        paths = []
        for name, title, rules, focus in self._rules():
            path = folder / f"{name}.rq"
            path.write_text(
                write_query(
                    rules, title=title, source=self.client, target=self.target.model, focus_as=focus
                )
            )
            paths.append(path)
        profile = Profile.from_queries(paths, client=self.client)
        scope = (
            within(
                self.client,
                records=self.scope,
                via=self.contents.label,
                inverse=not self.contents_incoming,
            )
            if self.contents
            else ""
        )
        statements = profile.run(self.client, scope=scope)
        implied = derive_conversions(self.client, statements, cast("Model", self.target.model))
        network = Network(self, statements, paths, implied, profile)
        logger.info("Wrote %d queries to %s.", len(paths), folder)
        logger.info("%r", network)
        logger.info(
            "Next: network.graph() builds the property graph; network.save(folder) writes RDF, "
            "the property graph and the rules."
        )
        return network

    def show(self, diagram: bool = False) -> Any:
        """Display the plan; with *diagram*, also as a diagram.

        In a notebook the diagram is drawn by Mermaid; in a terminal as a text tree.
        """
        super().show()
        if not diagram:
            return None
        if _in_notebook():
            from IPython.display import Markdown, display

            display(Markdown(self.diagram()))
            return None
        sys.stdout.write(f"{self.diagram()!r}\n")
        return None

    def diagram(self) -> str:
        """Return the plan as a Mermaid diagram: source kinds on the left, target terms on the right."""
        from rdfsolve.client.diagram import Diagram

        lines = ["flowchart LR"]
        targets: dict[str, str] = {}  # one box per target term, shared by the kinds that become it
        for i, (label, p) in enumerate(sorted(self.proposals.items())):
            style = {"choose": ":::open", "left out": ":::out"}.get(p.status, "")
            if p.target is not None:
                node = targets.setdefault(p.target.name, f"T{len(targets)}")
                box = f'{node}["{p.target.name}"]'
            else:
                box = f'U{i}["{"choose" if p.status == "choose" else "left out"}"]{style}'
            lines.append(f'  S{i}["{label}"]{style} -->|"{p.role}"| {box}')
        lines.append("  classDef open fill:#fff3cd,stroke:#b8860b")
        lines.append("  classDef out fill:#eeeeee,stroke:#999999,color:#777777")
        return Diagram("```mermaid\n" + "\n".join(lines) + "\n```")


class Identities(_Shown):
    """The identity decisions of a plan: per kind, the namespace and what was accepted."""

    def __init__(self, decided: Mapping[str, tuple[str, Any]], exact: bool) -> None:
        """Keep each kind's namespace and resolution (Claims.decide)."""
        self.decided = dict(decided)
        self.exact = exact

    def pairs(self) -> list[tuple[str, str]]:
        """Return the pairs of IRIs decided to name one entity, of every kind."""
        return sorted({pair for _, r in self.decided.values() for pair in r.pairs()})

    def table(self) -> Any:
        """Return every decision (kind, record, namespace, outcome, rule) as a DataFrame."""
        import pandas as pd

        frames = [r.table().assign(kind=k) for k, (_, r) in self.decided.items() if r.groups]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def summary(self) -> Any:
        """Return, per kind, the namespace, how many records were decided, left ambiguous, joined."""
        import pandas as pd

        rows = []
        for kind, (namespace, r) in self.decided.items():
            outcomes = Counter(g["outcome"].split(":")[0] for g in r.groups)
            rows.append(
                {
                    "kind": kind,
                    "namespace": namespace,
                    "decided": outcomes.get("accepted", 0),
                    "ambiguous": outcomes.get("ambiguous", 0),
                    "pairs joined": len(r.pairs()),
                }
            )
        return pd.DataFrame(rows)

    def _parts(self) -> list[Any]:
        lines = [
            "assumption: "
            + (
                "a cross-reference into the namespace is an exact match "
                "(plan.identify(exact=False) joins only declared identities)"
                if self.exact
                else "only declared identities join records "
                "(plan.identify(exact=True) also takes cross-references as exact)"
            )
        ]
        lines.append(
            "next: identity.table() lists every decision; plan.run() uses them to join nodes"
        )
        return [
            "Identity: which records name one entity, in the namespace the target lists for their kind",
            self.summary(),
            *lines,
        ]


class Network(_Shown):
    """The converted statements of a plan, in the target model, with how they were made."""

    def __init__(
        self,
        plan: Plan,
        statements: Any,
        rules: list[Path],
        implied: Mapping[str, int],
        profile: Any,
    ) -> None:
        """Keep the statements, the rule files that made them, and what the model implied."""
        self.plan = plan
        self.statements = statements
        self.rules = rules
        self.implied = dict(implied)
        self.profile = profile

    def save(
        self, folder: str | Path, same: Iterable[tuple[str, str]] | None = None
    ) -> dict[str, Path]:
        """Write the network to *folder*; return what was written, by name.

        - statements.nq: the statements in the target model (RDF).
        - mappings.sssom.tsv: what each source term became, from the rules.
        - queries/: the rules, as the SPARQL files that made the statements.
        - identity/: each kind's identity decisions (Plan.identify), as SSSOM.
        - the property graph (*same* merges IRIs decided to name one entity): KGX TSV when the
          target reifies statements, else a JSON node and edge list.
        """
        import shutil

        import pyoxigraph as ox

        folder = Path(folder).expanduser().resolve()
        try:
            (folder / "queries").mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ValueError(
                f"The network cannot be saved to {folder} ({error.strerror}); give a folder you "
                "can write to: network.save('<folder>')"
            ) from error
        out = {"statements": folder / "statements.nq", "mappings": folder / "mappings.sssom.tsv"}
        ox.serialize(self.statements, str(out["statements"]), format=ox.RdfFormat.N_QUADS)
        from sssom.writers import write_table

        with out["mappings"].open("w") as handle:
            write_table(self.profile.to_sssom(), handle)
        for path in self.rules:
            shutil.copy(path, folder / "queries" / path.name)
        out["queries"] = folder / "queries"
        graph = self.graph(same)
        identity = getattr(self.plan, "identity", None)
        if identity is not None and identity.decided:
            out["identity"] = folder / "identity"
            out["identity"].mkdir(exist_ok=True)
            for kind, (_, resolution) in identity.decided.items():
                name = _attribute(kind, snake=True)
                with (out["identity"] / f"{name}.sssom.tsv").open("w") as handle:
                    write_table(resolution.to_sssom(name=name), handle)
        model = self.plan.target.model
        if model.statement_kind() is not None and hasattr(model, "statement_classes"):
            from rdfsolve.conversion import to_kgx

            name = self.plan.client.schema.about.dataset_name or "source"
            out["nodes"], out["edges"] = to_kgx(
                graph, cast("Model", model), folder, knowledge_source=f"infores:{name}"
            )
        else:
            import json

            out["graph"] = folder / "graph.json"
            out["graph"].write_text(json.dumps(graph.to_json(), indent=1))
        logger.info("Saved to %s: %s", folder, ", ".join(p.name for p in out.values()))
        return out

    def counts(self) -> dict[str, Counter[str]]:
        """Return how many nodes of each kind and statements of each relation there are."""
        rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        kinds: Counter[str] = Counter()
        relations: Counter[str] = Counter()
        for q in self.statements:
            if q.predicate.value == rdf_type:
                term = self.plan.target.term(q.object.value)
                kinds[term.name if term else q.object.value] += 1
            else:
                term = self.plan.target.term(q.predicate.value)
                relations[term.name if term else q.predicate.value] += 1
        return {"kinds": kinds, "relations": relations}

    def graph(self, same: Iterable[tuple[str, str]] | None = None) -> Any:
        """Return the statements as a property graph: reified statements become edges.

        *same* are pairs of IRIs decided to name one entity, merged into one node; by default
        the plan's identity decisions (Plan.identify).
        """
        if same is None:
            identity = getattr(self.plan, "identity", None)
            same = identity.pairs() if identity is not None else ()
        from rdfsolve.conversion import derive_associations
        from rdfsolve.property_graph import Identity, PropertyGraph

        model = self.plan.target.model
        hierarchy = model.hierarchy() if hasattr(model, "hierarchy") else {}
        graph = PropertyGraph.from_rdf(
            graph=self.statements, identity=Identity(same=list(same)), hierarchy=hierarchy
        )
        if hasattr(model, "statement_classes"):
            derive_associations(graph=graph, biolink=cast("Model", model))
        return graph

    def _parts(self) -> list[Any]:
        import pandas as pd

        c = self.counts()
        parts: list[Any] = [
            f"Network: {len(self.statements)} statements from {len(self.rules)} rule files",
            pd.DataFrame(c["kinds"].most_common(), columns=["node kind", "nodes"]),
            pd.DataFrame(c["relations"].most_common(), columns=["relation", "statements"]),
        ]
        if self.implied:
            parts.append(
                "implied by the target model: "
                + " · ".join(f"{k} {n}" for k, n in self.implied.items())
            )
        parts.append("next: network.graph() · network.save(folder)")
        return parts
