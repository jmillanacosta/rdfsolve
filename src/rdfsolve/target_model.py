"""Target models: the schema a conversion writes into, read from LinkML.

A target model is any LinkML schema: its classes (with is_a, mixins and the identifier prefixes
they take), its slots (with domain, range and is_a) and its enumerations. Terms are named by the
schema's default prefix: a class by its name in CamelCase, a slot by its name in snake_case.

A model can carry conventions of its own that the general schema does not state, such as how
it reifies a statement or which statements its process classes imply. Those live in a subclass
registered for the schema's id (Model.read picks it); the general model has none, and every
operation that needs one says so.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

__all__ = [
    "KindInfo",
    "Model",
    "RelationInfo",
    "TargetModel",
    "ValueInfo",
    "read_model",
    "register",
]

# Capabilities a target model can declare; the plan uses only those its target has.
CAPABILITIES = (
    "hierarchy",  # kinds and relations have parents
    "identifiers",  # kinds state the identifier namespaces that name their records
    "iris",  # terms have IRIs, so the converted data has an RDF view
    "definitions",  # terms have definitions, used to rank options by their words
    "qualifiers",  # relations take qualifiers with value lists
    "reification",  # statements can be nodes (with subject, predicate and object)
    "processes",  # kinds for processes, with inputs and outputs
    "directed",  # relations have a direction
)


@dataclass(frozen=True)
class KindInfo:
    """A kind (class, label) of a target model."""

    name: str
    iri: str | None = None
    parents: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()  # Bioregistry prefixes, in the model's order of preference
    definition: str = ""
    abstract: bool = False  # a mixin or an abstract kind: not given to a node on its own
    deprecated: bool = False


@dataclass(frozen=True)
class RelationInfo:
    """A relation (predicate, edge type, slot) of a target model."""

    name: str
    iri: str | None = None
    parents: tuple[str, ...] = ()
    definition: str = ""
    domain: str | None = None  # the kind its edges start from (stated or inherited), when one
    range: str | None = None
    pairs: tuple[
        tuple[str, str], ...
    ] = ()  # the (from kind, to kind) it is allowed between, when listed
    directed: bool = True
    qualifier: bool = False  # a qualifier of a statement, not a relation of its own
    enumeration: str | None = None  # the value list a qualifier takes
    deprecated: bool = False
    abstract: bool = False  # a grouping of relations (abstract or mixin), not stated on its own


@dataclass(frozen=True)
class ValueInfo:
    """A value of a value list (enumeration) of a target model."""

    name: str
    enumeration: str


class TargetModel:
    """What a conversion needs from a target model; each adapter fills what its format states.

    *capabilities* says which optional parts the model has (CAPABILITIES); the plan reads only
    those, and says which ones the model lacks.
    """

    # Each adapter sets name, version and capabilities (a frozenset of CAPABILITIES).
    capabilities = frozenset()  # type: ignore[var-annotated]
    if TYPE_CHECKING:
        name: str
        version: str
        prefix: str
        base: str

    def kinds(self) -> list[KindInfo]:
        """Return the kinds of the model."""
        raise NotImplementedError

    def relations(self) -> list[RelationInfo]:
        """Return the relations of the model."""
        raise NotImplementedError

    def values(self) -> list[ValueInfo]:
        """Return the values of the model's value lists (none when it has none)."""
        return []

    def kind_ancestors(self, name: str) -> list[str]:
        """Return a kind's ancestors, nearest first (none without a hierarchy)."""
        return []

    def relation_ancestors(self, name: str) -> list[str]:
        """Return a relation's ancestors, nearest first (none without a hierarchy)."""
        return []

    def process_kinds(self) -> list[str]:
        """Return the kinds of processes, which have inputs and outputs (none when it has none)."""
        return []

    def naming_relation(self) -> str | None:
        """Return the relation that gives a node its name (None when the model has none)."""
        return None

    def statement_kind(self) -> str | None:
        """Return the kind that reifies a statement (None when the model has none)."""
        return None

    def process_relations(self) -> tuple[str | None, str | None]:
        """Return the relations from a process to its inputs and to its outputs, when the model has them."""
        return None, None


_MODELS: dict[str, type[Model]] = {}


def read_model(source: Any) -> TargetModel:
    """Return the target model in a file, by its kind: LinkML YAML (.yaml, .yml), a metagraph
    (.json with metanode_kinds), or a SHACL shapes graph (any other RDF file, or an rdflib Graph).
    """
    import json

    from rdfsolve.plan import PlanError

    if hasattr(source, "triples"):  # an rdflib graph of shapes
        from rdfsolve.targets.shacl import Shacl

        return Shacl(source)
    path = Path(source)
    if not path.exists():
        raise PlanError(
            f"No target model file at {path}",
            code="no_such_file",
            hint="Target('<a LinkML .yaml, a metagraph .json or a SHACL file>')",
        )
    suffix = "".join(path.suffixes[-2:]).lower()
    if suffix.endswith((".yaml", ".yml")):
        return Model.read(path)
    if suffix.endswith(".json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        if "metanode_kinds" in data:
            from rdfsolve.targets.metagraph import Metagraph

            return Metagraph.read(path)
        raise PlanError(
            f"{path.name} is JSON but not a metagraph (no metanode_kinds)",
            code="unknown_model_format",
            hint="a LinkML .yaml, a metagraph .json or a SHACL file",
        )
    from rdfsolve.targets.shacl import Shacl

    return Shacl.read(path)


def registered_prefixes() -> dict[str, str]:
    """Return the prefix and namespace of each registered model's own terms (PREFIX, NAMESPACE)."""
    import rdfsolve.targets  # registers the models with conventions of their own

    return {cls.PREFIX: cls.NAMESPACE for cls in _MODELS.values() if cls.PREFIX and cls.NAMESPACE}


def register(schema_id: str) -> Any:
    """Register a Model subclass for the LinkML schemas whose id is *schema_id*."""

    def wrap(cls: type[Model]) -> type[Model]:
        """Record *cls* as the reader of the schemas with that id."""
        _MODELS[schema_id] = cls
        return cls

    return wrap


@dataclass
class Model(TargetModel):
    """A LinkML schema at one version: classes, slots, enums and its prefixes."""

    version: str
    classes: dict[str, dict[str, Any]]
    slots: dict[str, dict[str, Any]]
    enums: dict[str, dict[str, Any]] = field(default_factory=dict)
    prefix: str = ""
    base: str = ""
    name: str = ""
    prefixes: dict[str, str] = field(default_factory=dict)

    # The slots of a class that state one edge (a reified statement), when the model has them.
    STATEMENT_SLOTS: ClassVar[tuple[str, str, str]] = ("subject", "predicate", "object")
    # A registered model's own prefix and namespace, for CURIEs of its terms (registered_prefixes).
    PREFIX: ClassVar[str] = ""
    NAMESPACE: ClassVar[str] = ""

    @classmethod
    def read(cls, path: str | Path) -> Model:
        """Read a LinkML YAML schema (a pinned copy); a registered subclass reads its own id."""
        import yaml

        data = yaml.safe_load(Path(path).read_text())
        prefixes = {
            k: (v if isinstance(v, str) else v.get("prefix_reference", ""))
            for k, v in (data.get("prefixes") or {}).items()
        }
        prefix = str(data.get("default_prefix") or "")
        base = prefixes.get(prefix, "") or str(data.get("id") or "")
        import rdfsolve.targets  # registers the models with conventions of their own

        kind = _MODELS.get(str(data.get("id") or ""), cls) if cls is Model else cls
        return kind(
            str(data.get("version", "")),
            data.get("classes", {}) or {},
            data.get("slots", {}) or {},
            data.get("enums", {}) or {},
            prefix,
            base,
            str(data.get("name") or ""),
            prefixes,
        )

    @property
    def BASE(self) -> str:  # noqa: N802 (kept for callers that read it as a constant)
        """Return the namespace of the model's own terms."""
        return self.base

    def class_iri(self, name: str) -> str:
        """Return the IRI of a class, from its name in the model."""
        return self.base + "".join(w[:1].upper() + w[1:] for w in name.split(" "))

    def slot_iri(self, name: str) -> str:
        """Return the IRI of a slot, from its name in the model."""
        return self.base + name.replace(" ", "_")

    def term(self, iri: str) -> str:
        """Return a term of the model as a CURIE with its prefix, any other IRI as given."""
        return (
            f"{self.prefix}:{iri[len(self.base) :]}"
            if self.base and iri.startswith(self.base)
            else iri
        )

    def ancestors(self, name: str, mixins: bool = False) -> list[str]:
        """Return a class's ancestors along is_a, nearest first.

        With *mixins*, also the mixins of the class and of its ancestors, with their own
        parents: the classes the model says a node should carry.
        """
        out: list[str] = []
        todo = [name]
        while todo:
            current = self.classes.get(todo.pop(0), {})
            parents = [current.get("is_a"), *((current.get("mixins") or []) if mixins else [])]
            for parent in parents:
                if parent and parent != name and parent not in out:
                    out.append(parent)
                    todo.append(parent)
        return out

    def below(self, name: str) -> list[str]:
        """Return *name* and the classes below it (along is_a)."""
        return [n for n in self.classes if n == name or name in self.ancestors(n)]

    def name_of(self, term: str) -> str:
        """Return the model's name of a term given as a CURIE, an IRI or a name."""
        local = term.rsplit(":", 1)[-1].rsplit("/", 1)[-1]
        for name in (*self.classes, *self.slots):
            if (
                name == local
                or self.class_iri(name).endswith("/" + local)
                or self.slot_iri(name).endswith("/" + local)
            ):
                return name
        raise ValueError(
            f"{term!r} is not a class or slot of {self.name or 'the model'} {self.version}"
        )

    def root(self) -> str | None:
        """Return the class that every non-mixin class is below, when there is one."""
        tops = [n for n, c in self.classes.items() if not c.get("is_a") and not c.get("mixin")]
        counts = Counter(t for n in self.classes for t in [*self.ancestors(n), n] if t in tops)
        return counts.most_common(1)[0][0] if counts else None

    def statement_classes(self) -> list[str]:
        """Return the classes that reify a statement (they have the STATEMENT_SLOTS), with those below."""
        found = [
            n
            for n, c in self.classes.items()
            if set(self.STATEMENT_SLOTS) <= set(c.get("slots") or [])
        ]
        return sorted({b for n in found for b in self.below(n)})

    def diagram(self, *names: str, terms: Sequence[str] = (), fenced: bool = True) -> str:
        """Draw the part of the model that *terms* use (Mermaid): a class with its parents (is_a),
        its mixins and the identifier prefixes it takes; a slot as an edge from its domain to
        its range, with the slot it specializes.
        """
        from rdfsolve.client.diagram import flowchart

        ids: dict[str, str] = {}
        nodes: list[tuple[str, str, str, str]] = []
        edges: list[tuple[str, str, str, str]] = []

        def node(name: str, detail: str = "", style: str = "") -> str:
            """Return the box of a class, drawn once."""
            if name not in ids:
                ids[name] = f"B{len(ids)}"
                nodes.append((ids[name], name, detail, style))
            return ids[name]

        def edge(a: str, label: str, b: str, line: str = "") -> None:
            """Add an arrow once."""
            if (a, label, b, line) not in edges:
                edges.append((a, label, b, line))

        for term in [*names, *terms]:
            name = self.name_of(term)
            if name in self.classes:
                c = self.classes[name]
                prefixes = ", ".join((c.get("id_prefixes") or [])[:6])
                child = node(name, f"ids: {prefixes}" if prefixes else "", "focus")
                current = name
                for parent in self.ancestors(name)[:2]:
                    edge(ids[current], "is a", node(parent))
                    current = parent
                for mixin in c.get("mixins") or []:
                    edge(child, "mixin", node(mixin, "mixin", "mixin"), "dashed")
                    current = mixin
                    for parent in self.ancestors(mixin)[:2]:
                        edge(ids[current], "is a", node(parent))
                        current = parent
            else:
                slot = self.slots[name]
                parent = f" (is a {slot['is_a']})" if slot.get("is_a") else ""
                edge(
                    node(slot.get("domain") or "any class"),
                    name + parent,
                    node(slot.get("range") or "any class"),
                )
        return flowchart(nodes, edges, fenced=fenced)

    def _normalized_prefixes(self) -> dict[str, str]:
        """Return the model's identifier prefixes by their Bioregistry prefix."""
        import bioregistry

        if not hasattr(self, "_prefix_spelling"):
            self._prefix_spelling = {
                (bioregistry.normalize_prefix(p) or p.lower()): p
                for c in self.classes.values()
                for p in c.get("id_prefixes") or []
            }
        return self._prefix_spelling

    def curie(self, iri: str) -> str | None:
        """Return an identifier as a CURIE with the prefix the model writes (its spelling of the
        prefix, as its classes list them in id_prefixes), compared as Bioregistry prefixes.
        None when the model takes no prefix for it.
        """
        from rdfsolve.identifiers import parse

        found = parse(iri)
        if found is None:
            return None
        prefix = self._normalized_prefixes().get(found.prefix)
        return f"{prefix}:{found.local}" if prefix else None

    def id_prefixes(self, term: str) -> list[str]:
        """Return the identifier prefixes a class takes, in the model's order of preference, as
        Bioregistry prefixes; a class that lists none takes those of its nearest ancestor that
        does. The first one a node has names it.
        """
        import bioregistry

        for name in [self.name_of(term), *self.ancestors(self.name_of(term))]:
            listed = self.classes.get(name, {}).get("id_prefixes") or []
            if listed:
                return [bioregistry.normalize_prefix(p) or p.lower() for p in listed]
        return []

    def hierarchy(self) -> dict[str, set[str]]:
        """Return the ancestors (IRIs) of each class IRI, for the most specific class."""
        return {
            self.class_iri(n): {self.class_iri(a) for a in self.ancestors(n)} for n in self.classes
        }

    def categories(self, prefix: str) -> list[str]:
        """Return the classes (IRIs, not mixins) where an identifier prefix belongs: those that
        list it (compared as Bioregistry prefixes) while none of their ancestors does. A model
        can repeat a prefix on narrower classes; the prefix belongs to the broadest class that
        lists it.
        """
        import bioregistry

        wanted = bioregistry.normalize_prefix(prefix) or prefix.lower()
        found = [
            name
            for name, c in self.classes.items()
            if not c.get("mixin")
            and any(
                (bioregistry.normalize_prefix(p) or p.lower()) == wanted
                for p in c.get("id_prefixes") or []
            )
        ]
        listed = set(found)
        return sorted(self.class_iri(n) for n in found if not listed & set(self.ancestors(n)))

    @property
    def capabilities(self) -> frozenset[str]:  # type: ignore[override]
        """Return what a LinkML schema states: all but processes, unless the model names them."""
        found = {"hierarchy", "iris", "definitions", "directed"}
        if any(c.get("id_prefixes") for c in self.classes.values()):
            found.add("identifiers")
        if self.enums:
            found.add("qualifiers")
        if self.statement_classes():
            found.add("reification")
        if self.process_kinds():
            found.add("processes")
        return frozenset(found)

    def kinds(self) -> list[KindInfo]:
        """Return the classes of the schema."""
        import bioregistry

        return [
            KindInfo(
                name,
                self.class_iri(name),
                tuple(p for p in [c.get("is_a"), *(c.get("mixins") or [])] if p),
                tuple(
                    bioregistry.normalize_prefix(p) or p.lower() for p in c.get("id_prefixes") or []
                ),
                str(c.get("description") or ""),
                bool(c.get("mixin") or c.get("abstract")),
                bool(c.get("deprecated")),
            )
            for name, c in self.classes.items()
        ]

    def _slot_value(self, slot: str, key: str) -> Any:
        """Return a slot's value of *key*, from its nearest ancestor that states one."""
        seen = set()
        current: str | None = slot
        while current and current not in seen:
            seen.add(current)
            s = self.slots.get(current) or {}
            if s.get(key):
                return s[key]
            current = s.get("is_a")
        return None

    def relations(self) -> list[RelationInfo]:
        """Return the slots of the schema; a slot whose range is a value list is a qualifier."""
        out = []
        for name, s in self.slots.items():
            range_ = self._slot_value(name, "range")
            out.append(
                RelationInfo(
                    name,
                    self.slot_iri(name),
                    tuple(p for p in [s.get("is_a"), *(s.get("mixins") or [])] if p),
                    str(s.get("description") or ""),
                    self._slot_value(name, "domain"),
                    range_,
                    qualifier=name.endswith("qualifier") or (range_ in self.enums),
                    enumeration=range_ if range_ in self.enums else None,
                    deprecated=bool(s.get("deprecated")),
                    abstract=bool(s.get("abstract") or s.get("mixin")),
                )
            )
        return out

    def values(self) -> list[ValueInfo]:
        """Return the permissible values of the schema's enums."""
        return [
            ValueInfo(str(v), enum)
            for enum, e in self.enums.items()
            for v in ((e or {}).get("permissible_values") or {})
        ]

    def kind_ancestors(self, name: str) -> list[str]:
        """Return a class's ancestors with its mixins, nearest first."""
        return self.ancestors(name, mixins=True)

    def relation_ancestors(self, name: str) -> list[str]:
        """Return a slot's ancestors along is_a, nearest first."""
        out, slot = [], self.slots.get(name, {}).get("is_a")
        while slot and slot not in out:
            out.append(slot)
            slot = self.slots.get(slot, {}).get("is_a")
        return out

    NAMING_IRIS: ClassVar[tuple[str, ...]] = (
        "rdfs:label",
        "schema:name",
        "skos:prefLabel",
        "http://www.w3.org/2000/01/rdf-schema#label",
        "http://schema.org/name",
    )

    def naming_relation(self) -> str | None:
        """Return the slot mapped to a naming property (its slot_uri), when the schema has one."""
        for name, s in self.slots.items():
            if str(s.get("slot_uri") or "") in self.NAMING_IRIS:
                return name
        return None

    def statement_kind(self) -> str | None:
        """Return the broadest class with the STATEMENT_SLOTS, when the schema has one."""
        found = self.statement_classes()
        tops = [n for n in found if not set(self.ancestors(n)) & set(found)]
        return max(tops, key=lambda n: len(self.below(n))) if tops else None

    def process_relations(self) -> tuple[str | None, str | None]:
        """Return the slots from a process class to its inputs and outputs, by their names."""
        processes = set(self.process_kinds())
        found: dict[str, str] = {}
        for name in self.slots:
            domain = self._slot_value(name, "domain")
            if domain in processes:
                for side in ("input", "output"):
                    if side in name.split() and side not in found:
                        found[side] = name
        return found.get("input"), found.get("output")

    def implied(self) -> list[str]:
        """Return the CONSTRUCT queries of the statements that the model's conventions imply
        from converted data (none for a general model).
        """
        return []

    def by_prefix(self, prefixes: Iterable[str]) -> dict[str, list[str]]:
        """Return the classes (names) that list each identifier prefix, broadest and narrower."""
        import bioregistry

        out: dict[str, list[str]] = defaultdict(list)
        wanted = {bioregistry.normalize_prefix(p) or p.lower() for p in prefixes}
        for name, c in self.classes.items():
            for p in c.get("id_prefixes") or []:
                key = bioregistry.normalize_prefix(p) or p.lower()
                if key in wanted:
                    out[key].append(name)
        return dict(out)
