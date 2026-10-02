"""Identifier signatures of mined schemas, and the links between datasets that they imply.

A schema example value is typed by Bioregistry when it is an IRI in a registered URI format,
or a literal CURIE of a registered prefix with a valid local identifier. A class is typed by
its example subjects in the same way. Two datasets are linked by a join when values of one
carry the identifier type of subjects of the other, and by a shared reference when values of
both carry the same identifier type. A link is a candidate: its evidence is the schema
examples, and sampled lookups in the target decide whether it holds.
"""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from rdfsolve.schema_models.core import MinedSchema

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from rdfsolve.client.api import Client

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
# Bioregistry prefixes of vocabularies: their terms describe data, they do not identify entities.
VOCABULARIES = frozenset(
    {
        "rdf",
        "rdfs",
        "owl",
        "xsd",
        "sd",
        "skos",
        "dcterms",
        "dc",
        "foaf",
        "void",
        "sh",
        "prov",
        "schema",
        "dcat",
        "idot",
        "shex",
        "vann",
        "bibo",
        "oa",
        "biolink",
    }
)


@dataclass
class Signatures:
    """Identifier types of one schema: of property values, and of class subjects."""

    values: dict[str, set[tuple[str, str]]] = field(default_factory=dict)
    subjects: dict[str, set[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class Link:
    """A candidate link from a (class, property) of one dataset to another dataset."""

    kind: Literal["join", "shared"]
    source: str
    source_class: str
    property: str
    identifier_type: str
    target: str
    target_class: str | None = None  # the class whose subjects the values name (joins)
    target_property: str | None = None  # the property with the same identifier type (shared)


def identifier_type(value: str, ignore: Iterable[str] = VOCABULARIES) -> str | None:
    """Return the Bioregistry prefix of a valid identifier IRI or CURIE, or None.

    Read by rdfsolve.identifiers.parse; an identifier that fails the pattern of its prefix
    (purl.uniprot.org/uniprot/P53_HUMAN, an entry name) has no type.
    """
    from rdfsolve.identifiers import parse

    found = parse(value)
    if found is None or found.valid is False or found.prefix in set(ignore):
        return None
    return found.prefix


def signatures(schema: MinedSchema, ignore: Iterable[str] = VOCABULARIES) -> Signatures:
    """Type the example values and subjects of a schema; rdf:type values are classes."""
    ignored = frozenset(ignore)
    values: dict[str, set[tuple[str, str]]] = defaultdict(set)
    subjects: dict[str, set[str]] = defaultdict(set)
    for example in schema.enrichment.examples if schema.enrichment else []:
        if example.property_uri == RDF_TYPE:
            continue
        if example.subject.kind == "uri" and (
            kind := identifier_type(example.subject.value, ignored)
        ):
            subjects[kind].add(example.subject_class)
        if kind := identifier_type(example.value.value, ignored):
            values[kind].add((example.subject_class, example.property_uri))
    return Signatures(dict(values), dict(subjects))


def infer_links(
    schemas: Mapping[str, MinedSchema], ignore: Iterable[str] = VOCABULARIES
) -> list[Link]:
    """Return the candidate joins and shared references between the datasets."""
    found = {name: signatures(schema, ignore) for name, schema in schemas.items()}
    links: set[Link] = set()
    for source, mine in found.items():
        for kind, pairs in mine.values.items():
            for target, theirs in found.items():
                if target == source:
                    continue
                for cls, prop in pairs:
                    for target_class in theirs.subjects.get(kind, ()):
                        links.add(
                            Link("join", source, cls, prop, kind, target, target_class=target_class)
                        )
                    if target > source and not theirs.subjects.get(kind):
                        for target_class, target_property in theirs.values.get(kind, ()):
                            links.add(
                                Link(
                                    "shared",
                                    source,
                                    cls,
                                    prop,
                                    kind,
                                    target,
                                    target_class,
                                    target_property,
                                )
                            )
    return sorted(
        links,
        key=lambda link: (
            link.kind,
            link.source,
            link.target,
            link.identifier_type,
            link.source_class,
            link.property,
        ),
    )


@dataclass
class LinkEvidence:
    """Sampled values of a link's source property, and how many the target has.

    target_forms counts how the target writes the identifiers that it has ({id} marks the
    local identifier): this is the rewrite that applies the link. replaced counts the sampled
    identifiers that were looked up through a replacement (see read_replacements). population
    is the number of distinct values of the property on the class, of any kind; complete says
    that every value was read, so that the share is exact and not an estimate.
    """

    link: Link
    sampled: int
    found: int
    target_forms: dict[str, int]
    examples: list[tuple[str, str]]
    replaced: int = 0
    population: int | None = None
    complete: bool = False
    # For a link over an identity property: the failed checks of the statements (see
    # rdfsolve.mappings.identity) with their counts, and the number of statements checked.
    flags: dict[str, int] = field(default_factory=dict)
    flag_checked: int = 0

    @property
    def flagged(self) -> bool:
        """Tell whether a checked statement of the link failed an identity check."""
        return bool(self.flags)

    @property
    def share(self) -> float | None:
        """Return the share of sampled values that the target has."""
        return self.found / self.sampled if self.sampled else None

    def interval(self, z: float = 1.96) -> tuple[float, float] | None:
        """Return the share itself when every value was read, else its Wilson interval."""
        if not self.sampled:
            return None
        p, n = self.found / self.sampled, self.sampled
        if self.complete:
            return p, p
        centre = (p + z * z / (2 * n)) / (1 + z * z / n)
        half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / (1 + z * z / n)
        return max(0.0, centre - half), min(1.0, centre + half)


def _local(value: str) -> tuple[str, str] | None:
    """Return the Bioregistry prefix and the standard local identifier of an IRI or CURIE.

    A local identifier that is not valid for the prefix gives None: a namespace can also name
    other things (purl.uniprot.org/uniprot/P53_HUMAN is an entry name, not an accession).
    """
    from rdfsolve.identifiers import parse

    found = parse(value)
    if found is None or found.valid is False:
        return None
    return found.prefix, found.local


class LookupStoppedError(Exception):
    """A lookup was stopped before its next query (the time budget of the caller ended)."""


def _lookup(
    link: Link,
    target: Client,
    keys: Iterable[str],
    replacements: Mapping[str, str],
    *,
    in_target_class: bool = False,
    extra: str = "",
    stop: Callable[[], bool] | None = None,
) -> dict[str, list[tuple[str, str]]]:
    """Look up identifiers (Bioregistry CURIEs) in the target, in every spelling.

    Return, for each identifier found, the target terms and their target subjects: the term
    itself for a join, the subject that has the term as value of the target property for a
    shared reference (with *in_target_class*, only subjects of the target class). *extra* is
    a pattern that the target subject ?x must also match (the path of a route). *stop* is
    asked before each query; when it answers True, LookupStoppedError is raised.
    """
    from rdflib import URIRef

    from rdfsolve.client.identify import spellings

    if link.kind == "join":
        pattern = f"?t a {URIRef(link.target_class or '').n3()} . BIND(?t AS ?x)"
        terms = {
            key: [t for t in spellings(replacements.get(key, key)) if isinstance(t, URIRef)]
            for key in keys
        }
    else:
        pattern = f"?x {URIRef(link.target_property or '').n3()} ?t ."
        if in_target_class and link.target_class:
            pattern += f" ?x a {URIRef(link.target_class).n3()} ."
        terms = {key: spellings(replacements.get(key, key)) for key in keys}
    pattern += extra
    found: dict[str, list[tuple[str, str]]] = defaultdict(list)
    pairs = [(key, term) for key, forms in terms.items() for term in forms]
    for start in range(0, len(pairs), 200):
        if stop is not None and stop():
            raise LookupStoppedError(f"stopped after {start} of {len(pairs)} spellings")
        values = " ".join(f'("{key}" {term.n3()})' for key, term in pairs[start : start + 200])
        scoped = target._scope(f"VALUES (?key ?t) {{ {values} }} {pattern}")
        for row in target._select(f"SELECT DISTINCT ?key ?t ?x WHERE {{ {scoped} }}"):
            found[row["key"]["value"]].append((row["t"]["value"], row["x"]["value"]))
    return dict(found)


# Most statements of an identity link that are checked.
FLAG_SAMPLE = 1000


def _identity_flags(link: Link, source: Client, body: str) -> tuple[dict[str, int], int]:
    """Check the statements of a link over an identity property, as declared identities are.

    Return the failed checks with their counts, and the number of statements checked. A
    statement is checked when its subject and its value are identifiers with a known prefix.
    Other properties state no identity.
    """
    from rdfsolve.identifiers import parse
    from rdfsolve.mappings.declared import IDENTITY_PROPERTIES
    from rdfsolve.mappings.identity import identity_flags

    if link.property not in IDENTITY_PROPERTIES:
        return {}, 0
    rows = source._select(
        f"SELECT DISTINCT ?s ?v WHERE {{ {body} FILTER(isIRI(?s)) }} LIMIT {FLAG_SAMPLE}"
    )
    counts: dict[str, int] = defaultdict(int)
    checked = 0
    for row in rows:
        # As written, not as sampled: an identifier that is not valid for its namespace is
        # what the check finds (the sample leaves it out).
        subject, value = parse(row["s"]["value"]), parse(row["v"]["value"])
        if subject is None or value is None:
            continue
        checked += 1
        flags = identity_flags(subject.curie, value.curie)
        for flag in {"namespace" if f.startswith("namespace:") else f for f in flags}:
            counts[flag] += 1
    return dict(counts), checked


# Most distinct target terms that verify(read_target=True) reads at once; above, lookups.
TARGET_READ_LIMIT = 2_000_000
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"


def _term_key(
    kind: str, value: str, datatype: str | None = None, lang: str | None = None
) -> tuple[str | None, ...]:
    """Identify an RDF term as a VALUES join does; a plain literal is an xsd:string (RDF 1.1)."""
    if kind == "uri":
        return ("uri", value)
    if kind == "bnode":
        return ("bnode", value)
    return ("literal", value, None if lang else (datatype or XSD_STRING), lang or None)


def _read_all(client: Client, query: str, expected: int | None) -> list[dict[str, Any]]:
    """Read a whole result in one response when its rows match the count, else page to the end.

    Paging sorts the whole result for every page (a local target of 45,000 terms: about 350 s);
    a local QLever index answers in one response.
    """
    rows = client._select(query)
    if expected is None or len(rows) != expected:
        rows = client._select(query, exhaustive=True)
    return rows


def _read_target(
    link: Link,
    target: Client,
    keys: Iterable[str],
    replacements: Mapping[str, str],
    *,
    in_target_class: bool = False,
    extra: str = "",
) -> dict[str, list[tuple[str, str]]] | None:
    """Match identifiers against every term of the target, read once, as _lookup does per spelling.

    Return None when the target has more than TARGET_READ_LIMIT distinct terms.
    The spellings of an identifier are tried in their order, so the first match is fixed.
    """
    from rdflib import Literal, URIRef

    from rdfsolve.client.identify import spellings

    if link.kind == "join":
        pattern = f"?t a {URIRef(link.target_class or '').n3()} . BIND(?t AS ?x)"
    else:
        pattern = f"?x {URIRef(link.target_property or '').n3()} ?t ."
        if in_target_class and link.target_class:
            pattern += f" ?x a {URIRef(link.target_class).n3()} ."
    # Only the terms are read: verify() keeps the matched term, and a blank node (a subject, or a
    # value) cannot be paged and equals no spelling of an identifier.
    scoped = target._scope(pattern + extra) + " FILTER(!isBlank(?t))"
    from rdfsolve.sparql_helper import SparqlHelperError

    try:
        counted = target._select(f"SELECT (COUNT(DISTINCT ?t) AS ?n) WHERE {{ {scoped} }}")
        if not counted or int(counted[0]["n"]["value"]) > TARGET_READ_LIMIT:
            return None
        rows = _read_all(
            target, f"SELECT DISTINCT ?t WHERE {{ {scoped} }}", int(counted[0]["n"]["value"])
        )
    except SparqlHelperError as error:
        # A target that the engine cannot read at once (its query memory) is looked up.
        logger.warning("The terms of %s were not read at once: %s", link.target, str(error)[:200])
        return None
    held = {
        _term_key(r["t"]["type"], r["t"]["value"], r["t"].get("datatype"), r["t"].get("xml:lang"))
        for r in rows
    }
    found: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for key in keys:
        for term in spellings(replacements.get(key, key)):
            if link.kind == "join" and not isinstance(term, URIRef):
                continue
            if isinstance(term, Literal):
                datatype = str(term.datatype) if term.datatype else None
                identity = _term_key("literal", str(term), datatype, term.language)
            else:
                identity = _term_key("uri", str(term))
            if identity in held:
                found[key].append((str(term), str(term)))
    return {key: matches for key, matches in found.items() if matches}


def verify(
    link: Link,
    source: Client,
    target: Client,
    *,
    sample: int | None = 50,
    replacements: Mapping[str, str] | None = None,
    read_target: bool = False,
) -> LinkEvidence:
    """Look up up to *sample* values of the link's source property in the target.

    With *sample* None every value is read and looked up, and the share is exact; otherwise
    the number of distinct values is counted, so that the sample can be put in proportion.

    A join looks for the identifiers as subjects of the target class; a shared reference
    looks for them as values of the target property. Every spelling of an identifier is tried.
    An identifier in *replacements* (Bioregistry CURIEs, secondary to primary) is looked up
    by its replacement. With *read_target* (a local index), the terms of the target are read
    once and matched in Python, with the term equality of the lookup (_read_target).
    """
    replacements = replacements or {}
    from rdflib import URIRef

    def _iri(value: str) -> str:
        return URIRef(value).n3()

    body = source._scope(f"?s a {_iri(link.source_class)} ; {_iri(link.property)} ?v .")
    population: int | None
    if sample is None:
        counted = source._select(f"SELECT (COUNT(DISTINCT ?v) AS ?n) WHERE {{ {body} }}")
        expected = int(counted[0]["n"]["value"]) if counted else None
        rows = _read_all(source, f"SELECT DISTINCT ?v WHERE {{ {body} }}", expected)
        population = len(rows)
    else:
        rows = source._select(f"SELECT DISTINCT ?v WHERE {{ {body} }} LIMIT {sample}")
        counted = source._select(f"SELECT (COUNT(DISTINCT ?v) AS ?n) WHERE {{ {body} }}")
        population = int(counted[0]["n"]["value"]) if counted else None
    keys: dict[str, str] = {}
    for row in rows:
        parsed = _local(row["v"]["value"])
        if parsed and parsed[0] == link.identifier_type:
            keys[f"{parsed[0]}:{parsed[1]}"] = row["v"]["value"]
    matched = _read_target(link, target, keys, replacements) if read_target else None
    if matched is None:
        matched = _lookup(link, target, keys, replacements)
    found: dict[str, str] = {key: matches[0][0] for key, matches in matched.items()}
    forms: dict[str, int] = defaultdict(int)
    for key, term in found.items():
        local = replacements.get(key, key).split(":", 1)[1]
        forms[term.replace(local, "{id}") if term.endswith(local) else "literal"] += 1
    flags, checked = _identity_flags(link, source, body)
    return LinkEvidence(
        link,
        len(keys),
        len(found),
        dict(forms),
        sorted((keys[key], term) for key, term in found.items()),
        sum(key in replacements for key in keys),
        population,
        sample is None,
        flags,
        checked,
    )


TERM_REPLACED_BY = "http://purl.obolibrary.org/obo/IAO_0100001"
NO_TERM_FOUND = "https://w3id.org/sssom/NoTermFound"


def describe_replacement_sets(paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    """Describe the identifier replacement sets used by link verification, for the release.

    Each set is given with its file name, checksum, and the mapping_set_id and
    mapping_set_version of its SSSOM header. The citation is a placeholder: it stays empty until
    the sets (pysec2pri) are deposited with a persistent identifier.
    """
    import hashlib
    import re

    described = []
    for path in map(Path, paths):
        data = path.read_bytes()
        meta: dict[str, str] = {}
        for line in data.decode("utf-8").splitlines():
            if not line.startswith("#"):
                break
            found = re.match(r"#\s*(mapping_set_id|mapping_set_version):\s*(.+?)\s*$", line)
            if found:
                meta[found.group(1)] = found.group(2).strip("\"'")
        described.append(
            {
                "file": path.name,
                "sha256": hashlib.sha256(data).hexdigest(),
                "mapping_set_id": meta.get("mapping_set_id"),
                "mapping_set_version": meta.get("mapping_set_version"),
                "citation": None,
            }
        )
    return described


def read_replacements(path: str | Path) -> dict[str, str]:
    """Read the identifier replacements of an SSSOM mapping set, such as pysec2pri writes.

    A row with the predicate "term replaced by" (IAO:0100001) maps a secondary identifier
    (subject) to its primary identifier (object). Identifiers are returned as Bioregistry
    CURIEs. A withdrawn identifier (no object identifier) and a split (more than one
    object) have no single replacement and are left out.
    """
    from sssom.parsers import parse_sssom_table

    mapping_set = parse_sssom_table(path)
    expand = mapping_set.converter.expand
    targets: dict[str, set[str]] = defaultdict(set)
    for row in mapping_set.df.itertuples():
        if expand(str(row.predicate_id)) != TERM_REPLACED_BY:
            continue
        object_iri = expand(str(row.object_id)) or ""
        old = _local(expand(str(row.subject_id)) or "")
        new = None if object_iri == NO_TERM_FOUND else _local(object_iri)
        if old:
            targets[f"{old[0]}:{old[1]}"].add(f"{new[0]}:{new[1]}" if new else "")
    return {old: next(iter(new)) for old, new in targets.items() if len(new) == 1 and "" not in new}


LINK_FIELDS = (
    "kind",
    "source",
    "source_class",
    "property",
    "identifier_type",
    "target",
    "target_class",
    "target_property",
    "sampled",
    "found",
    "share",
    "target_forms",
    "examples",
    "replaced",
    "population",
    "complete",
    "flags",
    "flag_checked",
)


def write_links(path: str | Path, links: Iterable[LinkEvidence]) -> None:
    """Write verified links as a table (tab-separated, one link per row).

    target_forms and examples are JSON; share is written for the reader and is not read back.
    """
    with Path(path).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, LINK_FIELDS, delimiter="\t")
        writer.writeheader()
        for evidence in links:
            link = evidence.link
            writer.writerow(
                {
                    **{name: getattr(link, name) or "" for name in LINK_FIELDS[:8]},
                    "sampled": evidence.sampled,
                    "found": evidence.found,
                    "share": "" if evidence.share is None else evidence.share,
                    "target_forms": json.dumps(evidence.target_forms),
                    "examples": json.dumps(evidence.examples),
                    "replaced": evidence.replaced,
                    "population": "" if evidence.population is None else evidence.population,
                    "complete": evidence.complete,
                    "flags": json.dumps(evidence.flags),
                    "flag_checked": evidence.flag_checked,
                }
            )


def read_links(path: str | Path) -> list[LinkEvidence]:
    """Read a table of verified links. A row without a sample (a failed lookup) is skipped.

    The examples of an exact link hold every found pair, which can exceed the default field
    limit of the csv module (HGNC to AOP-Wiki: 136,477 characters).
    """
    csv.field_size_limit(max(csv.field_size_limit(), 1 << 30))
    with Path(path).open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle, delimiter="\t") if row["sampled"]]
    return [
        LinkEvidence(
            Link(
                row["kind"],  # type: ignore[arg-type]
                row["source"],
                row["source_class"],
                row["property"],
                row["identifier_type"],
                row["target"],
                row["target_class"] or None,
                row["target_property"] or None,
            ),
            int(row["sampled"]),
            int(row["found"]),
            json.loads(row["target_forms"]),
            [tuple(pair) for pair in json.loads(row["examples"])],
            int(row["replaced"]),
            int(row["population"]) if row.get("population") else None,
            row.get("complete") == "True",
            json.loads(row.get("flags") or "{}"),
            int(row.get("flag_checked") or 0),
        )
        for row in rows
    ]


@dataclass
class ClassAssociation:
    """The entity pairs that a verified link joins, and the members of each class that take part.

    A source subject takes part when one of its values of the link property is found in the
    target; a target subject when it is the target of such a value. Coverage is relative to all
    members of the class in the scope of each client.
    """

    link: Link
    pairs: list[tuple[str, str]]
    source_subjects: int
    source_members: int
    target_subjects: int
    target_members: int

    @property
    def source_coverage(self) -> float | None:
        """Return the share of source class members that take part."""
        return self.source_subjects / self.source_members if self.source_members else None

    @property
    def target_coverage(self) -> float | None:
        """Return the share of target class members that take part."""
        return self.target_subjects / self.target_members if self.target_members else None


def class_association(
    link: Link,
    source: Client,
    target: Client,
    *,
    replacements: Mapping[str, str] | None = None,
) -> ClassAssociation:
    """Read every value of the link along the link, and return the association of its classes."""
    from rdflib import URIRef

    replacements = replacements or {}
    cls, prop = URIRef(link.source_class).n3(), URIRef(link.property).n3()
    rows = source._select(
        f"SELECT DISTINCT ?s ?v WHERE {{ {source._scope(f'?s a {cls} ; {prop} ?v .')} }}",
        exhaustive=True,
    )
    subjects_of: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        parsed = _local(row["v"]["value"])
        if parsed and parsed[0] == link.identifier_type:
            subjects_of[f"{parsed[0]}:{parsed[1]}"].add(row["s"]["value"])
    found = _lookup(link, target, subjects_of, replacements, in_target_class=True)
    pairs = sorted(
        {(s, x) for key, matches in found.items() for _, x in matches for s in subjects_of[key]}
    )

    def members(client: Client, iri: str | None) -> int:
        """Count the members of a class in the scope of the client."""
        if not iri:
            return 0
        body = client._scope(f"?m a {URIRef(iri).n3()} .")
        rows = client._select(f"SELECT (COUNT(DISTINCT ?m) AS ?n) WHERE {{ {body} }}")
        return int(rows[0]["n"]["value"]) if rows else 0

    return ClassAssociation(
        link,
        pairs,
        len({s for s, _ in pairs}),
        members(source, link.source_class),
        len({x for _, x in pairs}),
        members(target, link.target_class),
    )


ASSOCIATION_FIELDS = (
    *LINK_FIELDS[:8],
    "pairs",
    "source_subjects",
    "source_members",
    "source_coverage",
    "target_subjects",
    "target_members",
    "target_coverage",
)


def association_row(association: ClassAssociation) -> dict[str, object]:
    """Return the table row of a class association (ASSOCIATION_FIELDS), without the pairs."""
    link = association.link
    return {
        **{name: getattr(link, name) or "" for name in LINK_FIELDS[:8]},
        "pairs": len(association.pairs),
        "source_subjects": association.source_subjects,
        "source_members": association.source_members,
        "source_coverage": _blank(association.source_coverage),
        "target_subjects": association.target_subjects,
        "target_members": association.target_members,
        "target_coverage": _blank(association.target_coverage),
    }


def write_associations(path: str | Path, associations: Iterable[ClassAssociation]) -> None:
    """Write class associations as a table (tab-separated, one link per row), without the pairs."""
    with Path(path).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, ASSOCIATION_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(association_row(a) for a in associations)


def _blank(value: float | None) -> float | str:
    """Return the value, or an empty cell for None."""
    return "" if value is None else value
