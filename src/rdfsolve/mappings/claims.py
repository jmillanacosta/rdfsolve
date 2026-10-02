"""Mappings that sources state, compared and resolved, as SSSOM records.

A claim is one source stating a mapping: WikiPathways (BridgeDb) says the LIPID MAPS id of a
metabolite is ChEBI 89488 and 91146; ChEBI, which issues ChEBI ids, says (with a
cross-reference) that it is 91146 only. Sources disagree, and BridgeDb links depend on the
BridgeDb release without saying which. So claims keep their source, are compared, and a
resolution says which are accepted and why.

Records follow SSSOM, the Simple Standard for Sharing Ontological Mappings (Matentzoglu et al.
2022; https://w3id.org/sssom), as sssom-py's Mapping: the stated cross-reference is
``oboinowl:hasDbXref`` (a declared identity, ``skos:exactMatch``) with justification
``semapv:UnspecifiedMatching``; the source that states it is ``mapping_provider``; the IRIs as
the source wrote them and its property are kept in ``other``, so nothing is lost. Prefixes are
written as Bioregistry normalizes them. An identifier that fails the pattern of its namespace
(a UniProt proteome IRI with a fragment) is no SSSOM record; the claims list it as invalid.

The resolution follows the assembly and prioritization of SeMRA, the Semantic Mapping Reasoning
Assembler (Hoyt et al. 2025; https://github.com/biopragmatics/semra), and adds the authority of
the source that issues a namespace:

- the source that issues the target identifiers (ChEBI for ChEBI ids) is the authority for
  mappings to them; when it states targets, the other sources' different targets are
  overruled;
- several targets of one subject narrow to those the issuer prefers, when it marks some
  (UniProt's reviewed entries: BridgeDb links an Ensembl gene to its Swiss-Prot entry and to
  TrEMBL fragments);
- several targets of one subject are accepted together only when they are variants of one
  entity that a source states (L-serine and its zwitterion are tautomers, in ChEBI);
- otherwise several targets are ambiguous: none is accepted, and the report says so.

The resolution is written as SSSOM too (:meth:`Resolution.to_sssom`): accepted mappings as
``skos:exactMatch`` with justification ``semapv:MappingReview`` and the rule that decided them,
overruled ones as negative mappings (``predicate_modifier: Not``). SSSOM tools such as SeMRA,
which removes negative mappings before grouping, then group the identifiers as resolved.

A claim can also be checked through a third identifier: the BridgeDb link from an Ensembl
gene to a UniProt accession agrees with UniProt when UniProt gives the accession the same
NCBI Gene id that BridgeDb gives the gene.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rdfsolve.identifiers import parse

if TYPE_CHECKING:
    import pyoxigraph as ox
    from sssom import Mapping, MappingSetDataFrame

    from rdfsolve.client.api import Client

__all__ = ["Claims", "Resolution", "claim"]

STATED = "semapv:UnspecifiedMatching"
REVIEWED = "semapv:MappingReview"
CROSS_REFERENCE = "oboinowl:hasDbXref"  # prefixes as Bioregistry normalizes them
EXACT = "skos:exactMatch"


def claim(subject: str, object: str, source: str, predicate: str) -> Mapping:
    """Return the SSSOM record of *source* stating, with *predicate*, that *subject* is *object*.

    *subject* and *object* are IRIs (or CURIEs) of registered identifiers, as the source wrote
    them; the record keeps them, the source and the predicate in ``other``.
    """
    from sssom import Mapping

    from rdfsolve.config import mint
    from rdfsolve.mappings.declared import IDENTITY_PROPERTIES

    s, o = parse(subject), parse(object)
    if s is None or o is None:
        raise ValueError(f"Not registered identifiers: {subject}, {object}")
    return Mapping(
        subject_id=s.curie,
        predicate_id=EXACT if predicate in IDENTITY_PROPERTIES else CROSS_REFERENCE,
        object_id=o.curie,
        mapping_justification=STATED,
        mapping_provider=mint("dataset", source),
        other=json.dumps(
            {"subject": subject, "object": object, "source": source, "property": predicate},
            sort_keys=True,
        ),
    )


def _valid(*identifiers: str) -> bool:
    """Return whether each identifier is registered and matches the pattern of its namespace."""
    return all((found := parse(i)) is not None and found.valid is not False for i in identifiers)


def _other(record: Mapping) -> dict[str, str]:
    return dict(json.loads(record.other or "{}"))


def subject_of(record: Mapping) -> str:
    """Return the subject IRI as the source wrote it."""
    return _other(record).get("subject") or str(record.subject_id)


def object_of(record: Mapping) -> str:
    """Return the object IRI as the source wrote it."""
    return _other(record).get("object") or str(record.object_id)


def source_of(record: Mapping) -> str:
    """Return the name of the source that states the mapping."""
    return _other(record).get("source") or str(record.mapping_provider)


def property_of(record: Mapping) -> str:
    """Return the property the source states the mapping with."""
    return _other(record).get("property") or str(record.predicate_id)


def _key(iri: str) -> str:
    """Return the CURIE of a registered identifier, else the IRI."""
    found = parse(iri)
    return found.curie if found else iri


def _prefix(iri: str) -> str:
    found = parse(iri)
    return found.prefix if found else "-"


def _issuer(source: str) -> str | None:
    """Return the registered prefix that a source issues (its Bioregistry name), if any."""
    import bioregistry

    return bioregistry.normalize_prefix(source)


@dataclass
class Resolution:
    """The accepted claims, the variant pairs accepted with them, and every group's outcome."""

    accepted: list[Mapping]
    variants: list[tuple[str, str]]
    groups: list[dict[str, Any]] = field(default_factory=list)
    overruled: list[tuple[str, str, str]] = field(default_factory=list)

    def targets(self, namespace: str) -> list[str]:
        """Return the accepted IRIs in *namespace*: the records worth fetching."""
        return sorted({object_of(c) for c in self.accepted if _prefix(object_of(c)) == namespace})

    def pairs(self) -> list[tuple[str, str]]:
        """Return the accepted (subject, object) pairs, as the sources wrote them."""
        return sorted({(subject_of(c), object_of(c)) for c in self.accepted})

    def table(self) -> Any:
        """Return the outcome of each (subject, target namespace) group as a DataFrame."""
        import pandas as pd

        return pd.DataFrame(self.groups)

    def to_sssom(self, name: str = "resolution") -> MappingSetDataFrame:
        """Return the resolution as an SSSOM mapping set.

        Accepted mappings are ``skos:exactMatch`` with justification ``semapv:MappingReview``
        and the rule that decided them; overruled ones are negative mappings (``Not``), with
        the source whose statement overruled them.
        """
        from sssom import Mapping

        from rdfsolve.config import mint
        from rdfsolve.mappings.sssom import converter_for, create_sssom_mappings

        rule = {(g["subject"], t): g for g in self.groups for t in g["targets"].split(", ") if t}
        records = []
        for subject, target in sorted({(_key(s), _key(o)) for s, o in self.pairs()}):
            group = rule.get((subject, target), {})
            decided = (
                f"{group.get('outcome', 'accepted')}; decided by "
                f"{group.get('decided by', '?')} ({group.get('basis', '')})"
            )
            records.append(
                Mapping(
                    subject_id=subject,
                    predicate_id=EXACT,
                    object_id=target,
                    mapping_justification=REVIEWED,
                    curation_rule_text=[decided],
                )
            )
        for subject, target, by in sorted(set(self.overruled)):
            records.append(
                Mapping(
                    subject_id=subject,
                    predicate_id=EXACT,
                    predicate_modifier="Not",
                    object_id=target,
                    mapping_justification=REVIEWED,
                    curation_rule_text=[
                        f"overruled: {by}, which issues these identifiers, states other targets"
                    ],
                )
            )
        converter = converter_for(
            c for r in records for c in (r.subject_id, r.predicate_id, r.object_id)
        )
        return create_sssom_mappings(records, mint("mappings", name), converter=converter)


class Claims:
    """A set of claims (SSSOM records) from several sources."""

    def __init__(
        self, claims: Iterable[Mapping] = (), invalid: Iterable[tuple[str, str, str]] = ()
    ) -> None:
        """Keep the claims, each once, and the statements whose identifiers are invalid."""
        seen: dict[tuple[str, str, str, str], Mapping] = {}
        for c in claims:
            seen.setdefault((subject_of(c), object_of(c), source_of(c), property_of(c)), c)
        self.claims: list[Mapping] = list(seen.values())
        self.invalid: list[tuple[str, str, str]] = sorted(set(invalid))

    def __add__(self, other: Claims) -> Claims:
        """Return the claims of both."""
        return Claims([*self.claims, *other.claims], [*self.invalid, *other.invalid])

    def __len__(self) -> int:
        """Return the number of claims."""
        return len(self.claims)

    @classmethod
    def stated(
        cls,
        rdf: ox.Dataset | Iterable[ox.Quad],
        predicates: Iterable[str],
        source: str,
        *,
        objects: str | None = None,
    ) -> Claims:
        """Return the mappings that *source* states in RDF with *predicates*.

        Only links between registered identifiers are claims; *objects* keeps the claims whose
        object has this prefix (UniProt's rdfs:seeAlso to NCBI Gene, not to InterPro).
        """
        import pyoxigraph as ox

        wanted = set(predicates)
        found = []
        invalid: list[tuple[str, str, str]] = []
        for quad in rdf:
            if quad.predicate.value not in wanted or not isinstance(quad.object, ox.NamedNode):
                continue
            if not isinstance(quad.subject, ox.NamedNode):
                continue
            subject, obj = quad.subject.value, quad.object.value
            target = parse(obj)
            if parse(subject) is None or target is None:
                continue
            if objects is not None and target.prefix != objects:
                continue
            if not _valid(subject, obj):
                invalid.append((subject, obj, source))
                continue
            found.append(claim(subject, obj, source, quad.predicate.value))
        return cls(found, invalid)

    @classmethod
    def of(cls, client: Client, *results: Any, citing: Iterable[str] = ()) -> Claims:
        """Return what *client* states in these records about the identity of their identifiers.

        The predicates are read from the records: a declared identity (owl:sameAs,
        skos:exactMatch, Bio2RDF cross-references), or a predicate whose values are all
        identifiers of one namespace that this source does not issue, some of them only cited
        here, not described (WikiPathways' BridgeDb links; not dcterms:isPartOf, whose values
        are WikiPathways' own, nor wp:source, whose enzymes the records describe), and that the
        schema of the source lists as a cross-reference (Client.cross_references: not
        dcterms:references, whose publications WikiPathways describes). *citing* adds every
        link to identifiers of these namespaces, as evidence for check (UniProt's NCBI Gene id
        of an accession).
        """
        import pyoxigraph as ox

        from rdfsolve.mappings.declared import is_declared_property

        records = client.to_oxigraph(*results)
        name = client._schema.about.dataset_name or "source"
        own = set(client.issued_kinds())
        described = {
            q.subject.value
            for q in records
            if q.predicate.value == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        }
        spaces: dict[str, set[str]] = defaultdict(set)
        cited: set[str] = set()
        for quad in records:
            if not isinstance(quad.object, ox.NamedNode):
                continue
            found = parse(quad.object.value)
            spaces[quad.predicate.value].add(found.prefix if found else "-")
            if quad.object.value not in described:
                cited.add(quad.predicate.value)
        # The schema says which of these the source describes itself (its publications).
        schema = set(client.cross_references()) if hasattr(client, "cross_references") else None
        chosen = {
            predicate
            for predicate, found in spaces.items()
            if is_declared_property(predicate)
            or (
                len(found) == 1
                and not found & (own | {"-"})
                and predicate in cited
                and (schema is None or predicate in schema)
            )
        }
        wanted = set(citing)
        claims = []
        invalid: list[tuple[str, str, str]] = []
        for quad in records:
            if not isinstance(quad.object, ox.NamedNode):
                continue
            subject, target = parse(quad.subject.value), parse(quad.object.value)
            if subject is None or target is None:
                continue
            # A node drawn with its ChEBI id that names it again (bdbChEBI to itself) is a claim,
            # so that the ChEBI record is fetched; two ids of one namespace are two entities.
            if subject.prefix == target.prefix and subject.curie != target.curie:
                continue
            if quad.predicate.value in chosen or target.prefix in wanted:
                if not _valid(quad.subject.value, quad.object.value):
                    invalid.append((quad.subject.value, quad.object.value, name))
                    continue
                claims.append(
                    claim(quad.subject.value, quad.object.value, name, quad.predicate.value)
                )
        return cls(claims, invalid)

    def targets(self, namespace: str) -> list[str]:
        """Return the IRIs that the claims map to in *namespace* (a Bioregistry prefix)."""
        return sorted({object_of(c) for c in self.claims if _prefix(object_of(c)) == namespace})

    def ask(
        self,
        client: Client,
        identifiers: Iterable[str] | None = None,
        *,
        source: str | None = None,
    ) -> Claims:
        """Return these claims and those of *client*: the resources that carry each identifier.

        The source states the mapping with a cross-reference (ChEBI's hasDbXref
        'lipidmaps:LMSP0501AA04'); Client.identify finds them, in batches. Only a match written
        with the prefix of the identifier, or as an IRI, is a cross-reference (a bare number is
        not), and identifiers of the client's own prefix are not asked. By default the
        identifiers asked are the subjects of the claims into the namespace *client* issues.
        """
        from rdfsolve.identifiers import curie

        name = source or (client._schema.about.dataset_name or "source")
        issued = set(client.issued_kinds())
        if identifiers is None:
            identifiers = sorted(
                {subject_of(c) for c in self.claims if _prefix(object_of(c)) in issued}
            )
        # The issuer is not asked about its own identifiers: a ChEBI id is its own class, and
        # matching its bare number found other classes (any value "15377").
        written = {
            curie(i): i for i in identifiers if (read := parse(i)) and read.prefix not in issued
        }
        matches = [
            (written.get(x.identifier, x.identifier), x.resource, x.predicate)
            for x in client.identify(list(written))
            if x.kind == "uri"
            or x.value.lower().startswith(x.identifier.split(":", 1)[0].lower() + ":")
        ]
        found = [claim(s, o, name, p) for s, o, p in matches if _valid(s, o)]
        invalid = [(s, o, name) for s, o, _ in matches if not _valid(s, o)]
        return self + Claims(found, invalid)

    def table(self) -> Any:
        """Return the claims as a DataFrame (subject and object as CURIEs)."""
        import pandas as pd

        return pd.DataFrame(
            [
                {
                    "subject": str(c.subject_id),
                    "object": str(c.object_id),
                    "source": source_of(c),
                    "predicate": property_of(c),
                }
                for c in self.claims
            ],
            columns=["subject", "object", "source", "predicate"],
        )

    def to_sssom(self, name: str = "claims") -> MappingSetDataFrame:
        """Return the claims as an SSSOM mapping set (each with its source and property)."""
        from rdfsolve.config import mint
        from rdfsolve.mappings.sssom import converter_for, create_sssom_mappings

        converter = converter_for(
            c for r in self.claims for c in (r.subject_id, r.predicate_id, r.object_id)
        )
        return create_sssom_mappings(self.claims, mint("mappings", name), converter=converter)

    def _groups(self) -> dict[tuple[str, str], dict[str, set[str]]]:
        """Return the targets of each (subject, target namespace), by source."""
        groups: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for c in self.claims:
            groups[(_key(subject_of(c)), _prefix(object_of(c)))][source_of(c)].add(
                _key(object_of(c))
            )
        return groups

    def compare(self) -> Any:
        """Return, for each subject and target namespace, what each source states and whether
        they agree (one source; agree: the same targets; disagree: different targets).
        """
        import pandas as pd

        rows = []
        for (subject, namespace), by_source in sorted(self._groups().items()):
            stated = {s: sorted(t) for s, t in sorted(by_source.items())}
            distinct = {tuple(t) for t in stated.values()}
            status = (
                "one source" if len(stated) == 1 else "agree" if len(distinct) == 1 else "disagree"
            )
            rows.append({"subject": subject, "namespace": namespace, "status": status, **stated})
        return pd.DataFrame(rows)

    def check(self, through: str, *, among: Sequence[str] | None = None) -> Any:
        """Check each claim through a third identifier namespace (*through*, e.g. ncbigene).

        A claim subject → object agrees when the *through* targets that any source gives the
        subject and those it gives the object share one; it disagrees when both have some and
        share none; it is unknown otherwise. *among* keeps the claims of these sources.
        """
        import pandas as pd

        third: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for c in self.claims:
            if _prefix(object_of(c)) == through:
                third[_key(subject_of(c))][source_of(c)].add(_key(object_of(c)))

        def described(key: str, found: set[str]) -> str:
            """Return the third identifiers of *key*, each with the sources that give it."""
            return ", ".join(
                f"{t} ({', '.join(sorted(s for s, ts in third[key].items() if t in ts))})"
                for t in sorted(found)
            )

        rows = []
        for c in self.claims:
            if _prefix(object_of(c)) == through or (
                among is not None and source_of(c) not in among
            ):
                continue
            subject, target = _key(subject_of(c)), _key(object_of(c))
            left = {t for ts in third[subject].values() for t in ts}
            right = {t for ts in third[target].values() for t in ts}
            status = "unknown" if not left or not right else "agree" if left & right else "disagree"
            rows.append(
                {
                    "subject": subject,
                    "object": target,
                    "source": source_of(c),
                    "status": status,
                    f"{through} of subject": described(subject, left),
                    f"{through} of object": described(target, right),
                }
            )
        return pd.DataFrame(rows)

    def decide(
        self,
        *,
        authority: Sequence[str] | None = None,
        variants: Callable[[list[str]], Iterable[tuple[str, str]]] | Iterable[tuple[str, str]] = (),
        namespaces: Iterable[str] | None = None,
        prefer: Iterable[Any] = (),
    ) -> Resolution:
        """Decide which claims are accepted, for each subject and target namespace.

        The authority is the source that issues the target namespace (ChEBI for ChEBI ids),
        then *authority* in order, then any source. The targets of the first source that states
        some are taken; the other sources' different targets are overruled. One target is
        accepted; several are accepted only when *variants* (pairs, or a function of the
        target IRIs, such as Ontologies.variants) joins them into one group; otherwise the
        group is ambiguous and nothing is accepted. *namespaces* keeps these target namespaces;
        by default those that a source of the claims issues (where the issuer can be heard).
        *prefer* are identifiers (IRIs, or Results of a client) that the issuer marks as its
        preferred entries (UniProt's reviewed ones): several targets narrow to those preferred,
        when some are.
        """
        order = list(authority or [])
        preferred = {_key(iri) for iri in _iris(prefer)}
        if namespaces is None:
            namespaces = {i for s in {source_of(c) for c in self.claims} if (i := _issuer(s))}
        kept = set(namespaces)
        by_group: dict[tuple[str, str], list[Mapping]] = defaultdict(list)
        for c in self.claims:
            by_group[(_key(subject_of(c)), _prefix(object_of(c)))].append(c)
        accepted: list[Mapping] = []
        variant_pairs: list[tuple[str, str]] = []
        groups: list[dict[str, Any]] = []
        overruled_by: list[tuple[str, str, str]] = []
        for (subject, namespace), claims in sorted(by_group.items()):
            if namespace not in kept:
                continue
            sources = sorted({source_of(c) for c in claims})
            rank = sorted(
                sources,
                key=lambda s: (
                    _issuer(s) != namespace,
                    order.index(s) if s in order else len(order),
                    s,
                ),
            )
            chosen = rank[0]
            mine = [c for c in claims if source_of(c) == chosen]
            # One IRI per identifier: a source can write one target twice (bdbUniprot to
            # identifiers.org/uniprot/Q13510, owl:sameAs to purl.uniprot.org/uniprot/Q13510).
            targets = sorted(
                {_key(object_of(c)): object_of(c) for c in sorted(mine, key=object_of)}.values()
            )
            overruled = sorted(
                {_key(object_of(c)) for c in claims if source_of(c) != chosen}
                - {_key(t) for t in targets}
            )
            if _issuer(chosen) == namespace:
                overruled_by += [(subject, t, chosen) for t in overruled]
            outcome = "accepted"
            pairs: list[tuple[str, str]] = []
            narrowed = [t for t in targets if _key(t) in preferred]
            if len(targets) > 1 and narrowed:
                outcome = f"accepted: preferred by the issuer, {len(targets) - len(narrowed)} not"
                targets = narrowed
            if len(targets) > 1:
                found = variants(targets) if callable(variants) else variants
                pairs = [(a, b) for a, b in found if a in targets and b in targets]
                if not _connected(targets, pairs):
                    outcome = "ambiguous: several targets, not variants of one entity"
                else:
                    outcome = "accepted: variants of one entity"
            set_aside: set[str] = {_key(object_of(c)) for c in mine} - {_key(t) for t in targets}
            if outcome.startswith("accepted"):
                keys = {_key(t) for t in targets}
                accepted += [c for c in mine if _key(object_of(c)) in keys]
                variant_pairs += pairs
            groups.append(
                {
                    "subject": subject,
                    "namespace": namespace,
                    "decided by": chosen,
                    "basis": "issuer of the namespace"
                    if _issuer(chosen) == namespace
                    else "authority order"
                    if chosen in order
                    else "only or first source",
                    "targets": ", ".join(_key(t) for t in targets),
                    "outcome": outcome,
                    "overruled": ", ".join(overruled),
                    "not preferred": ", ".join(sorted(set_aside)),
                    "sources": ", ".join(sources),
                }
            )
        return Resolution(accepted, sorted(set(variant_pairs)), groups, overruled_by)


def _iris(items: Any) -> list[str]:
    """Return the IRIs of identifiers given as IRIs, as client Results, or as a list of both."""
    if isinstance(items, str):
        return [items]
    if hasattr(items, "records"):
        return [str(vars(r)["uri"]) for r in items.records if vars(r).get("uri")]
    return [iri for item in items for iri in _iris(item)]


def _connected(items: list[str], pairs: Iterable[tuple[str, str]]) -> bool:
    """Return whether *pairs* join every item into one group."""
    root = {i: i for i in items}

    def find(i: str) -> str:
        """Return the root of an item."""
        while root[i] != i:
            i = root[i]
        return i

    for a, b in pairs:
        root[find(a)] = find(b)
    return len({find(i) for i in items}) == 1
