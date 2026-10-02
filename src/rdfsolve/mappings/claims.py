"""Claims that two identifiers name one entity, who makes each claim, and a decision among them.

A claim is one source stating a mapping: WikiPathways (BridgeDb) says the LIPID MAPS id of a
metabolite is ChEBI 89488 and 91146; ChEBI, which issues ChEBI ids, says (with a
cross-reference) that it is 91146 only. Sources disagree, and BridgeDb links depend on the
BridgeDb release without saying which. So claims keep their source, are compared, and a
decision says which are accepted and why:

- the source that issues the target identifiers (ChEBI for ChEBI ids) is the authority for
  mappings to them; when it states targets, the other sources' different targets are
  overruled;
- several targets of one subject are accepted together only when they are variants of one
  entity that a source states (L-serine and its zwitterion are tautomers, in ChEBI);
- otherwise several targets are ambiguous: none is accepted, and the report says so.

A claim can also be checked through a third identifier: the BridgeDb link from an Ensembl
gene to a UniProt accession agrees with UniProt when UniProt gives the accession the same
NCBI Gene id that BridgeDb gives the gene.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rdfsolve.identifiers import Identifier, parse

if TYPE_CHECKING:
    import pyoxigraph as ox

    from rdfsolve.client.api import Client

__all__ = ["Claim", "Claims", "Decision"]


@dataclass(frozen=True)
class Claim:
    """A source stating that *subject* maps to *object* (both IRIs, as written)."""

    subject: str
    object: str
    source: str
    predicate: str

    @property
    def subject_id(self) -> Identifier | None:
        """Return the subject read as a registered identifier."""
        return parse(self.subject)

    @property
    def object_id(self) -> Identifier | None:
        """Return the object read as a registered identifier."""
        return parse(self.object)


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
class Decision:
    """The accepted claims, the variant pairs accepted with them, and every group's outcome."""

    accepted: list[Claim]
    variants: list[tuple[str, str]]
    groups: list[dict[str, Any]] = field(default_factory=list)

    def pairs(self) -> list[tuple[str, str]]:
        """Return the accepted (subject, object) pairs."""
        return sorted({(c.subject, c.object) for c in self.accepted})

    def table(self) -> Any:
        """Return the outcome of each (subject, target namespace) group as a DataFrame."""
        import pandas as pd

        return pd.DataFrame(self.groups)


class Claims:
    """A set of claims from several sources."""

    def __init__(self, claims: Iterable[Claim] = ()) -> None:
        """Keep the claims, each once."""
        self.claims: list[Claim] = list(dict.fromkeys(claims))

    def __add__(self, other: Claims) -> Claims:
        """Return the claims of both."""
        return Claims([*self.claims, *other.claims])

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
        wanted = set(predicates)
        found = []
        import pyoxigraph as ox

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
            found.append(Claim(subject, obj, source, quad.predicate.value))
        return cls(found)

    def ask(
        self, client: Client, identifiers: Iterable[str], *, source: str | None = None
    ) -> Claims:
        """Return these claims and those of *client*: the resources that carry each identifier.

        The source states the mapping with a cross-reference (ChEBI's hasDbXref
        'lipidmaps:LMSP0501AA04'); Client.identify finds them, in batches. Only a match written
        with the prefix of the identifier, or as an IRI, is a cross-reference (a bare number is
        not), and identifiers of the client's own prefix are not asked.
        """
        from rdfsolve.identifiers import curie

        name = source or (client._schema.about.dataset_name or "source")
        issued = set(client.issued_kinds())
        # The issuer is not asked about its own identifiers: a ChEBI id is its own class, and
        # matching its bare number found other classes (any value "15377").
        written = {
            curie(i): i for i in identifiers if (read := parse(i)) and read.prefix not in issued
        }
        found = [
            Claim(written.get(x.identifier, x.identifier), x.resource, name, x.predicate)
            for x in client.identify(list(written))
            if x.kind == "uri"
            or x.value.lower().startswith(x.identifier.split(":", 1)[0].lower() + ":")
        ]
        return self + Claims(found)

    def table(self) -> Any:
        """Return the claims as a DataFrame (subject and object as CURIEs)."""
        import pandas as pd

        return pd.DataFrame(
            [
                {
                    "subject": _key(c.subject),
                    "object": _key(c.object),
                    "source": c.source,
                    "predicate": c.predicate,
                }
                for c in self.claims
            ]
        )

    def _groups(self) -> dict[tuple[str, str], dict[str, set[str]]]:
        """Return the targets of each (subject, target namespace), by source."""
        groups: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for c in self.claims:
            groups[(_key(c.subject), _prefix(c.object))][c.source].add(_key(c.object))
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
            if _prefix(c.object) == through:
                third[_key(c.subject)][c.source].add(_key(c.object))
        rows = []
        for c in self.claims:
            if _prefix(c.object) == through or (among is not None and c.source not in among):
                continue
            left = {t for ts in third[_key(c.subject)].values() for t in ts}
            right = {t for ts in third[_key(c.object)].values() for t in ts}
            status = "unknown" if not left or not right else "agree" if left & right else "disagree"
            rows.append(
                {
                    "subject": _key(c.subject),
                    "object": _key(c.object),
                    "source": c.source,
                    "status": status,
                    f"{through} of subject": ", ".join(
                        f"{t} ({', '.join(sorted(s for s, ts in third[_key(c.subject)].items() if t in ts))})"
                        for t in sorted(left)
                    ),
                    f"{through} of object": ", ".join(
                        f"{t} ({', '.join(sorted(s for s, ts in third[_key(c.object)].items() if t in ts))})"
                        for t in sorted(right)
                    ),
                }
            )
        return pd.DataFrame(rows)

    def decide(
        self,
        *,
        authority: Sequence[str] | None = None,
        variants: Callable[[list[str]], Iterable[tuple[str, str]]] | Iterable[tuple[str, str]] = (),
        namespaces: Iterable[str] | None = None,
    ) -> Decision:
        """Decide which claims are accepted, for each subject and target namespace.

        The authority is the source that issues the target namespace (ChEBI for ChEBI ids),
        then *authority* in order, then any source. The targets of the first source that states
        some are taken; the other sources' different targets are overruled. One target is
        accepted; several are accepted only when *variants* (pairs, or a function of the
        target IRIs, such as Ontologies.variants) joins them into one group; otherwise the
        group is ambiguous and nothing is accepted. *namespaces* keeps these target namespaces.
        """
        order = list(authority or [])
        kept = set(namespaces) if namespaces is not None else None
        by_group: dict[tuple[str, str], list[Claim]] = defaultdict(list)
        for c in self.claims:
            by_group[(_key(c.subject), _prefix(c.object))].append(c)
        accepted: list[Claim] = []
        variant_pairs: list[tuple[str, str]] = []
        groups: list[dict[str, Any]] = []
        for (subject, namespace), claims in sorted(by_group.items()):
            if kept is not None and namespace not in kept:
                continue
            sources = sorted({c.source for c in claims})
            rank = sorted(
                sources,
                key=lambda s: (
                    _issuer(s) != namespace,
                    order.index(s) if s in order else len(order),
                    s,
                ),
            )
            chosen = rank[0]
            mine = [c for c in claims if c.source == chosen]
            targets = sorted({c.object for c in mine})
            overruled = sorted(
                {_key(c.object) for c in claims if c.source != chosen} - {_key(t) for t in targets}
            )
            outcome = "accepted"
            pairs: list[tuple[str, str]] = []
            if len(targets) > 1:
                found = variants(targets) if callable(variants) else variants
                pairs = [(a, b) for a, b in found if a in targets and b in targets]
                if not _connected(targets, pairs):
                    outcome = "ambiguous: several targets, not variants of one entity"
                else:
                    outcome = "accepted: variants of one entity"
            if outcome.startswith("accepted"):
                accepted += [c for c in mine if c.object in targets]
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
                    "sources": ", ".join(sources),
                }
            )
        return Decision(accepted, sorted(set(variant_pairs)), groups)


def _connected(items: list[str], pairs: Iterable[tuple[str, str]]) -> bool:
    """Return whether *pairs* join every item into one group."""
    root = {i: i for i in items}

    def find(i: str) -> str:
        while root[i] != i:
            i = root[i]
        return i

    for a, b in pairs:
        root[find(a)] = find(b)
    return len({find(i) for i in items}) == 1
