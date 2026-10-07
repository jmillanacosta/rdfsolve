"""Checks of a conversion into a target model, for any RDF source (docs: conversion workflow).

Each step of a conversion writes a table, and a run fails or lists each case, so that no problem
is found later by looking at the graph:

- :func:`inventory` (step 1): the identifier namespaces of the nodes of each class, with counts,
  the identifiers that look versioned (a local id that is another id of its namespace with a
  version suffix) and the nodes with no registered identifier (contextual nodes, which exist
  only within one part of the source);
- :class:`IdentityPolicy` (step 2): one authority namespace per kind, the namespaces kept as
  their own nodes with the reason, and the policy for contextual nodes;
- :func:`coverage` (step 3): for each kind and namespace, the share of identifiers that a
  resolution maps to the authority, and those it does not;
- :func:`cluster_checks` (step 5): the merged nodes, each checked for at most one authority
  identifier (but declared variants), one kind, and only registered identifiers (no merge
  through a name or a local id); the merges identity refused; the singletons;
- :func:`audit` (step 8): nodes of one kind that share a normalized name under different ids,
  each explained or listed as unexplained, and the nodes with no authority identifier;
- :func:`regressions` (step 9): fixed cases, each a set of identifiers that must be one node;
- :func:`run_report` (step 10): per kind, nodes before and after, coverage, conflicts and
  unexplained duplicates (which must be 0).

The graph is an rdfsolve PropertyGraph; the statements are RDF (an Oxigraph dataset or quads);
a resolution is rdfsolve.mappings.claims.Resolution. Nothing here is specific to a source: kinds
are class IRIs, namespaces are Bioregistry prefixes.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rdfsolve.identifiers import parse

if TYPE_CHECKING:
    import pandas as pd

    from rdfsolve.mappings.claims import Resolution
    from rdfsolve.property_graph import PGNode, PropertyGraph

__all__ = [
    "IdentityPolicy",
    "audit",
    "cluster_checks",
    "coverage",
    "inventory",
    "regressions",
    "run_report",
]

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
NAMES = (
    "https://w3id.org/biolink/vocab/name",
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
)
LOCAL = "-"  # the namespace of a node with no registered identifier
# A version suffix: _r<n>, .<n>, _v<n> or -v<n> after the identifier.
_VERSION = re.compile(r"^(?P<base>.+?)(?:_r|_v|\.|-v)\d+$")


@dataclass
class IdentityPolicy:
    """Which identifier names each kind of entity, decided before converting.

    *authority* maps a kind (a class IRI) to its authority namespace (a Bioregistry prefix),
    the namespace whose issuer decides identity for that kind. *keep* names namespaces whose identifiers stay
    their own nodes, each with the reason; every other namespace of a kind is mapped to the
    authority, and an identifier without a mapping is flagged. *contextual* is the policy for
    nodes with no registered identifier: "per context" (kept per source context, never merged)
    or "by members". Versions of one identifier are one node (a pair given to Identity.same).
    """

    authority: Mapping[str, str]
    keep: Mapping[str, str] = field(default_factory=dict)
    contextual: str = "per context"

    def kind_of(self, node: PGNode) -> str | None:
        """Return the kind of a node: the first of its classes that the policy names."""
        return next((k for k in self.authority if k in node.labels), None)

    def to_dict(self) -> dict[str, Any]:
        """Return the policy as plain data, to store with the run."""
        return {
            "authority": dict(self.authority),
            "keep": dict(self.keep),
            "contextual": self.contextual,
        }


def _identifiers(node: PGNode) -> list[str]:
    """Return the registered identifiers of a node (CURIEs): its IRIs and those decided for it."""
    iris = [*(node.members or [node.id]), *getattr(node, "identifiers", [])]
    return list(dict.fromkeys(found.curie for iri in iris if (found := parse(iri))))


def _namespace(iri: str) -> str:
    """Return the namespace of an IRI: its Bioregistry prefix, else LOCAL."""
    found = parse(iri)
    return found.prefix if found else LOCAL


def normalize(name: str) -> str:
    """Return a name for comparison: lower case, without punctuation, spaces collapsed."""
    return re.sub(r"[\W_]+", " ", name.lower()).strip()


def inventory(statements: Iterable[Any], classes: Iterable[str] | None = None) -> pd.DataFrame:
    """Return, for each class and identifier namespace, how many typed nodes use it (step 1).

    Columns: class, namespace (LOCAL for nodes with no registered identifier: contextual nodes),
    nodes, versioned (identifiers whose local id is another identifier of the namespace with a
    version suffix that is also valid in it), example. *classes* keeps these classes.
    """
    import bioregistry
    import pandas as pd

    wanted = set(classes) if classes is not None else None
    typed: dict[str, set[str]] = defaultdict(set)
    for quad in statements:
        if quad.predicate.value != RDF_TYPE or quad.subject.value.startswith("_:"):
            continue
        if wanted is None or quad.object.value in wanted:
            typed[quad.object.value].add(quad.subject.value)
    rows = []
    for cls, nodes in sorted(typed.items()):
        by_space: dict[str, list[str]] = defaultdict(list)
        for node in sorted(nodes):
            by_space[_namespace(node)].append(node)
        for space, members in sorted(by_space.items()):
            versioned = 0
            if space != LOCAL:
                resource = bioregistry.get_resource(space)
                for iri in members:
                    found = parse(iri)
                    match = _VERSION.match(found.local) if found else None
                    if match and resource and resource.is_valid_identifier(match["base"]):
                        versioned += 1
            rows.append(
                {
                    "class": cls,
                    "namespace": space,
                    "nodes": len(members),
                    "versioned": versioned,
                    "example": members[0],
                }
            )
    return pd.DataFrame(rows, columns=["class", "namespace", "nodes", "versioned", "example"])


def _exact_targets(resolution: Resolution | None, authority: str) -> dict[str, set[str]]:
    """Return the authority identifiers that the resolution's exact pairs give each identifier."""
    found: dict[str, set[str]] = defaultdict(set)
    for subject, target in resolution.pairs() if resolution is not None else ():
        s, t = parse(subject), parse(target)
        if s and t and t.prefix == authority:
            found[s.curie].add(t.curie)
    return found


def coverage(
    statements: Iterable[Any], policy: IdentityPolicy, resolution: Resolution | None = None
) -> pd.DataFrame:
    """Return, for each kind and namespace, how many identifiers reach the authority (step 3).

    An identifier reaches it when it is in the authority namespace, or when an accepted exact
    pair of *resolution* maps it there. Namespaces the policy keeps are listed with their
    reason. Columns: kind, namespace, identifiers, mapped, share, treatment, missing.
    """
    import pandas as pd

    quads = list(statements)
    rows = []
    for kind, authority in policy.authority.items():
        targets = _exact_targets(resolution, authority)
        nodes = sorted(
            {
                q.subject.value
                for q in quads
                if q.predicate.value == RDF_TYPE and q.object.value == kind
            }
        )
        by_space: dict[str, list[str]] = defaultdict(list)
        for node in nodes:
            found = parse(node)
            by_space[found.prefix if found else LOCAL].append(found.curie if found else node)
        for space, ids in sorted(by_space.items()):
            mapped = [i for i in ids if space == authority or targets.get(i)]
            missing = sorted(set(ids) - set(mapped))
            treatment = (
                "authority"
                if space == authority
                else f"kept: {policy.keep[space]}"
                if space in policy.keep
                else f"contextual: {policy.contextual}"
                if space == LOCAL
                else "mapped to the authority"
            )
            rows.append(
                {
                    "kind": kind,
                    "namespace": space,
                    "identifiers": len(ids),
                    "mapped": len(mapped),
                    "share": round(len(mapped) / len(ids), 3) if ids else 0.0,
                    "treatment": treatment,
                    "missing": ", ".join(missing) if treatment.startswith("mapped") else "",
                }
            )
    return pd.DataFrame(
        rows,
        columns=["kind", "namespace", "identifiers", "mapped", "share", "treatment", "missing"],
    )


def _variant_keys(variants: Iterable[tuple[str, str]]) -> set[frozenset[str]]:
    return {
        frozenset((a.curie, b.curie)) for x, y in variants if (a := parse(x)) and (b := parse(y))
    }


def _versions(keys: Iterable[str]) -> set[frozenset[str]]:
    """Return the pairs of an identifier and a version of it (_VERSION): one entity,
    by the policy (a version is a property of one node).
    """
    present = set(keys)
    found = set()
    for key in present:
        prefix, _, local = key.partition(":")
        match = _VERSION.match(local)
        if match and f"{prefix}:{match['base']}" in present:
            found.add(frozenset((key, f"{prefix}:{match['base']}")))
    return found


def _one_group(keys: list[str], pairs: set[frozenset[str]]) -> bool:
    """Return whether *pairs*, or versions of one identifier, join all *keys* into one group."""
    from rdfsolve.property_graph import _joined

    return len(keys) < 2 or _joined(keys, pairs | _versions(keys))


def cluster_checks(
    graph: PropertyGraph,
    policy: IdentityPolicy,
    *,
    variants: Iterable[tuple[str, str]] = (),
    hierarchy: Mapping[str, set[str]] | None = None,
) -> pd.DataFrame:
    """Check each merged node (step 5): at most one authority identifier (but *variants*), one
    kind (classes of which one is under the other, by *hierarchy*: class → ancestors), and only
    registered identifiers among its members, so no merge went through a name or a local id.
    The merges that identity refused (report()["identity"]["refused"]) are conflicts too.

    Rows: node, kind, members, status ("merged", "conflict" or "refused"), reason.
    """
    import pandas as pd

    allowed = _variant_keys(variants)
    above = hierarchy or {}
    rows = []
    for node in graph.nodes.values():
        if len(node.members) + len(getattr(node, "identifiers", [])) < 2:
            continue
        kind = policy.kind_of(node)
        ids = _identifiers(node)
        reasons = []
        authority = policy.authority.get(kind or "", "")
        own = [i for i in ids if i.split(":", 1)[0] == authority]
        if not _one_group(own, allowed):
            reasons.append(f"{len(own)} {authority} identifiers, not variants: {', '.join(own)}")
        classes = sorted(set(node.labels))
        apart = [
            (a, b)
            for i, a in enumerate(classes)
            for b in classes[i + 1 :]
            if a not in above.get(b, set()) and b not in above.get(a, set())
        ]
        if hierarchy is not None and apart:
            reasons.append("two kinds: " + ", ".join(f"{a} and {b}" for a, b in apart))
        local = [m for m in node.members if parse(m) is None]
        if local:
            reasons.append("merged with no registered identifier: " + ", ".join(local))
        rows.append(
            {
                "node": node.id,
                "kind": kind or "",
                "members": " ".join(ids + local),
                "status": "conflict" if reasons else "merged",
                "reason": "; ".join(reasons),
            }
        )
    for refused in graph.report()["identity"].get("refused", []):
        first = graph.nodes.get(refused["nodes"][0])
        rows.append(
            {
                "node": " ".join(refused["nodes"]),
                "kind": (policy.kind_of(first) if first is not None else None) or "",
                "members": " ".join(refused["identifiers"]),
                "status": "refused",
                "reason": refused["reason"],
            }
        )
    return pd.DataFrame(rows, columns=["node", "kind", "members", "status", "reason"])


def _names(node: PGNode, keys: Sequence[str]) -> set[str]:
    return {
        normal
        for key in keys
        for value in node.properties.get(key, [])
        if (normal := normalize(str(value.lexical)))
    }


def audit(
    graph: PropertyGraph,
    policy: IdentityPolicy,
    resolution: Resolution | None = None,
    *,
    variants: Iterable[tuple[str, str]] = (),
    names: Sequence[str] = NAMES,
) -> dict[str, pd.DataFrame]:
    """Explain every name that nodes of one kind share under different ids (step 8).

    Each group of nodes of one kind with one normalized name is:

    - "contextual": a node has no registered identifier (policy.contextual);
    - "ambiguous claim": a node's identifier has an ambiguous decision in *resolution*;
    - "missing claim": a node has no authority identifier (the namespace is named);
    - "variants kept apart": their authority identifiers are variants of one entity (*variants*,
      a pair given to Identity.same joins them);
    - "different entities": each has its own authority identifier, and they differ;
    - "unexplained" otherwise.

    Returns {"duplicates": one row per group, "missing_authority": nodes per kind and namespace
    with no authority identifier, "passed": a one-row table, True when nothing is unexplained}.
    """
    import pandas as pd

    allowed = _variant_keys(variants)
    ambiguous = set()
    if resolution is not None:
        for group in resolution.groups:
            if group["outcome"].startswith("ambiguous"):
                ambiguous.add(group["subject"])
    groups: dict[tuple[str, str], list[PGNode]] = defaultdict(list)
    missing: Counter[tuple[str, str]] = Counter()
    for node in graph.nodes.values():
        kind = policy.kind_of(node)
        if kind is None:
            continue
        authority = policy.authority[kind]
        ids = _identifiers(node)
        if not any(i.split(":", 1)[0] == authority for i in ids):
            missing[(kind, ids[0].split(":", 1)[0] if ids else LOCAL)] += 1
        for name in _names(node, names):
            groups[(kind, name)].append(node)
    rows = []
    for (kind, name), nodes in sorted(groups.items()):
        if len(nodes) < 2:
            continue
        authority = policy.authority[kind]
        by_node = {n.id: _identifiers(n) for n in nodes}
        own = {
            n: [i for i in found if i.split(":", 1)[0] == authority] for n, found in by_node.items()
        }
        local = [n for n, found in by_node.items() if not found]
        unclear = sorted({i for found in by_node.values() for i in found} & ambiguous)
        without = sorted(
            {found[0].split(":", 1)[0] for n, found in by_node.items() if found and not own[n]}
        )
        keys = sorted({i for found in own.values() for i in found})
        if local:
            explanation, detail = "contextual", f"policy: {policy.contextual}"
        elif unclear:
            explanation, detail = "ambiguous claim", ", ".join(unclear)
        elif without:
            kept = [s for s in without if s in policy.keep]
            explanation = (
                "kept by policy" if kept and len(kept) == len(without) else "missing claim"
            )
            detail = ", ".join(f"{s}: {policy.keep[s]}" if s in policy.keep else s for s in without)
        elif _one_group(keys, allowed) and len(keys) > 1:
            explanation, detail = "variants kept apart", ", ".join(keys)
        elif len(keys) == len(nodes):
            explanation, detail = "different entities", f"different {authority}: {', '.join(keys)}"
        else:
            explanation, detail = "unexplained", ", ".join(keys)
        rows.append(
            {
                "kind": kind,
                "name": name,
                "nodes": " ".join(sorted(ids)),
                "explanation": explanation,
                "detail": detail,
            }
        )
    duplicates = pd.DataFrame(rows, columns=["kind", "name", "nodes", "explanation", "detail"])
    absent = pd.DataFrame(
        [{"kind": k, "namespace": s, "nodes": n} for (k, s), n in sorted(missing.items())],
        columns=["kind", "namespace", "nodes"],
    )
    unexplained = int((duplicates["explanation"] == "unexplained").sum())
    return {
        "duplicates": duplicates,
        "missing_authority": absent,
        "passed": pd.DataFrame([{"unexplained": unexplained, "passed": unexplained == 0}]),
    }


def regressions(graph: PropertyGraph, cases: Mapping[str, Iterable[str]]) -> pd.DataFrame:
    """Check fixed cases (step 9): each names identifiers (IRIs or CURIEs) that must be one node.

    Rows: case, nodes (the nodes the identifiers are on), missing (identifiers on no node),
    passed (one node, and every identifier found).
    """
    import pandas as pd

    where: dict[str, str] = {}
    for node in graph.nodes.values():
        for key in _identifiers(node):
            where.setdefault(key, node.id)
        for iri in node.members or [node.id]:
            where.setdefault(iri, node.id)
    rows = []
    for case, identifiers in cases.items():
        keys = [found.curie if (found := parse(i)) else i for i in identifiers]
        nodes = sorted({where[k] for k in keys if k in where})
        missing = [k for k in keys if k not in where]
        rows.append(
            {
                "case": case,
                "nodes": " ".join(nodes),
                "missing": " ".join(missing),
                "passed": len(nodes) == 1 and not missing,
            }
        )
    return pd.DataFrame(rows, columns=["case", "nodes", "missing", "passed"])


def run_report(
    before: PropertyGraph,
    after: PropertyGraph,
    policy: IdentityPolicy,
    *,
    covered: pd.DataFrame | None = None,
    clusters: pd.DataFrame | None = None,
    audited: Mapping[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Return per kind: nodes before and after merging, authority coverage, conflicts and
    unexplained duplicates (step 10).
    """
    import pandas as pd

    def count(graph: PropertyGraph) -> Counter[str]:
        """Return the number of nodes of each kind in *graph*."""
        return Counter(k for n in graph.nodes.values() if (k := policy.kind_of(n)))

    nodes_before, nodes_after = count(before), count(after)
    rows = []
    for kind in policy.authority:
        row: dict[str, Any] = {
            "kind": kind,
            "nodes before": nodes_before.get(kind, 0),
            "nodes after": nodes_after.get(kind, 0),
        }
        if covered is not None:
            mine = covered[covered["kind"] == kind]
            total = int(mine["identifiers"].sum())
            row["authority coverage"] = (
                round(int(mine["mapped"].sum()) / total, 3) if total else 0.0
            )
        if clusters is not None:
            row["conflicts"] = int(
                ((clusters["kind"] == kind) & (clusters["status"] != "merged")).sum()
            )
        if audited is not None:
            duplicates = audited["duplicates"]
            row["unexplained duplicates"] = int(
                ((duplicates["kind"] == kind) & (duplicates["explanation"] == "unexplained")).sum()
            )
        rows.append(row)
    return pd.DataFrame(rows)
