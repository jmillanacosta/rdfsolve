"""Retain an investigation and render bounded model observations."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from threading import RLock
from urllib.parse import urlsplit

from rdfsolve.catalogue import Catalogue, score, words
from rdfsolve.hydration import _term
from rdfsolve.query_fragments import Fragment, identifier, path_size
from rdfsolve.retrieval import QueryValidationError, Requirement, verify_query
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql


def short(text, limit=180):
    """Bound a display excerpt while retaining its original value."""
    text = str(text or "")
    return text if len(text) <= limit else text[:limit] + "…"


def profile(result):
    """Summarize RDF term kinds and population of each column."""
    columns = {}
    for name in result.variables:
        values = [row[name] for row in result.rows if name in row]
        columns[name] = {
            "kinds": dict(Counter(v.type for v in values)),
            "unbound": result.row_count - len(values),
            "distinct_terms": len({(v.type, v.value, v.lang, v.datatype) for v in values}),
        }
    return {"rows": result.row_count, "columns": columns}


class Session:
    """One source, immutable question clauses and retained query artifacts."""

    def __init__(
        self,
        client,
        source_id="rdf",
        *,
        artifact_dir=None,
        max_paths=100,
        class_mappings=(),
        related_registries=(),
    ):
        """Attach the source and initialize one investigation."""
        self.client = client
        self.catalogue = Catalogue(
            client, source_id, class_mappings=class_mappings, related_registries=related_registries
        )
        self.lock = RLock()
        self.question = ""
        self.goals = {}
        self.requirements = {}
        self.interpretation_warnings = []
        self.grounding = {}
        self.prepared = {}
        self.results = {}
        self.executions = {}
        self.records = {}
        self.cache = {}
        self.max_paths = max_paths
        self.artifact_dir = Path(artifact_dir).resolve() if artifact_dir else None
        self.events = []

    def _card(self, ref):
        c = self.catalogue
        f = c.fragments[ref]
        card = {"ref": ref, "label": short(f.label), "kind": f.kind}
        if f.description:
            card["description"] = short(f.description)
        if f.iri:
            card["iri"] = f.iri
        if f.path:
            card.update(
                owner=c.type_refs.get(f.owner),
                field=f.field_name,
                path=path_to_sparql(f.path),
                complexity=path_size(f.path),
            )
            m = c.metadata.get(ref, {})
            card["targets"] = [c.type_refs.get(t, t) for t in m.get("targets", [])]
            card["value_kinds"] = m.get("node_kinds", [])
        if f.term:
            card["identity"] = ref
            card["term_kind"] = f.term.kind
            if f.term.kind == "uri":
                card["iri"] = f.term.value
        arguments = " ?s" if f.kind == "type" else " ?s ?o" if f.path else ""
        card["insert"] = "{{" + ref + arguments + "}}"
        return {k: v for k, v in card.items() if v not in (None, [], "")}

    def _page(self, refs, offset=0, budget=5500, limit=None):
        cards = []
        for ref in refs[offset:]:
            card = self._card(ref)
            if (limit is not None and len(cards) >= limit) or len(
                json.dumps([*cards, card]).encode()
            ) > budget:
                break
            cards.append(card)
        return {"items": cards, "more": offset + len(cards) < len(refs), "retained": len(refs)}

    def schema(
        self, concepts=None, owners=None, targets=None, *, question="", goals=None, offset=0
    ):
        """Declare clauses and retrieve their connected schema region locally."""
        if goals is not None:
            if not question.strip() or not goals:
                raise ValueError(
                    "Supply the original question and each requested output or restriction as a clause."
                )
            requirements = {
                f"g{i}": Requirement.model_validate(g)
                for i, g in enumerate(goals, 1)
                if isinstance(g, dict)
            }
            about = self.client._schema.about
            source_names = [
                self.catalogue.registry.source_id,
                about.dataset_name or "",
                about.title or "",
            ]
            source_names += [
                part
                for url in [about.endpoint, about.homepage]
                if url
                for part in (urlsplit(url).hostname or "").split(".")
            ]

            def normalize(value):
                return re.sub(r"[^a-z0-9]", "", value.casefold())

            for key, requirement in requirements.items():
                if requirement.kind == "scope" or (
                    requirement.concept.casefold() in {"database", "dataset", "source"}
                    and normalize(requirement.value) in {normalize(n) for n in source_names if n}
                ):
                    if normalize(requirement.value or requirement.concept) not in {
                        normalize(n) for n in source_names if n
                    }:
                        raise ValueError(
                            "The requested dataset differs from the configured source. Configure another source outside the model."
                        )
                    requirements[key] = requirement.model_copy(update={"kind": "scope"})
            requested = {
                f"g{i}": clause if isinstance(clause, str) else clause["clause"]
                for i, clause in enumerate(goals, 1)
            }
            if self.goals and (self.question != question or self.goals != requested):
                raise ValueError(
                    "Keep the original question and clause texts. Their grounding can be corrected before preparing a query."
                )
            if self.requirements and self.requirements != requirements:
                if self.prepared:
                    raise ValueError(
                        "Prepared requirements are retained across probes and query repairs. A data result cannot weaken them."
                    )
                for key, previous in self.requirements.items():
                    if previous != requirements[key]:
                        self.interpretation_warnings.append(
                            f"Interpretation revised for '{previous.clause}': {previous.kind} to {requirements[key].kind}."
                        )
            self.question, self.goals = question, requested
            self.requirements = requirements
        c = self.catalogue
        owner_ids = {c._type(o) for o in owners or []}
        target_ids = {c._type(t) for t in targets or []}
        refs = list(c.field_refs.values()) if owner_ids else list(c.schema_documents)
        refs = [
            r
            for r in refs
            if (not owner_ids or c.fragments[r].owner in owner_ids)
            and (
                not target_ids or target_ids.intersection(c.metadata.get(r, {}).get("targets", []))
            )
        ]
        groups, seeds = [], set(owner_ids) | target_ids
        for concept in concepts or [""]:
            ranked = sorted(
                (r for r in refs if score(c.schema_documents[r][0], concept)),
                key=lambda r: (
                    -score(c.fragments[r].label, concept),
                    -score(c.schema_documents[r][0], concept),
                    r,
                ),
            )
            exact = [
                r
                for r in ranked
                if c.fragments[r].kind == "type" and words(concept) == words(c.fragments[r].label)
            ]
            if exact:
                ranked = exact
                seeds.update(c.fragments[r].iri for r in exact)
            groups.append(
                {
                    "concept": concept,
                    **self._page(ranked, offset, budget=5000 // max(1, len(concepts or []))),
                }
            )
        connections = [
            r
            for r in c.field_refs.values()
            if c.fragments[r].owner in seeds
            and seeds.intersection(c.metadata[r].get("targets", []))
            and (len(seeds) > 1 or target_ids)
        ]
        return {
            "types": self._page([c.type_refs[cls] for cls in sorted(seeds)], budget=1800),
            "matches": groups,
            "connections": self._page(connections, budget=2500),
            "goals": self.goals,
            "requirements": {g: r.model_dump() for g, r in self.requirements.items()},
            "source": c.registry.source_id,
            "scope": c.registry.binding,
            "mapping_evidence": c.mapping_status,
        }

    def find(self, text, kind, *, fields=None):
        """Keep typed candidate records; expose only distinguishing identities."""
        c = self.catalogue
        cls = c._type(kind)
        names = c._field_names(cls, fields or [])
        key = identifier("find", [text, cls, names])
        if key not in self.cache:
            result = (
                self.client.search([text], kind=cls, fields=names)
                if names
                else self.client.find(text, kind=cls)
            )
            refs = []
            for record in result:
                term = RdfTerm(kind="uri", value=str(record.uri))
                labels = [
                    str(x)
                    for name in ("label", "title")
                    for x in (getattr(record, name, None) or [])
                ]
                ref = c._put(
                    Fragment(
                        "term",
                        short("; ".join(labels) or str(record.uri)),
                        term=term,
                        basis="typed entity retrieval",
                    ),
                    [cls, term.value],
                )
                self.records[ref] = record
                refs.append(ref)
            self.cache[key] = refs
        return self._page(self.cache[key], budget=3000, limit=4)

    def paths(self, source, target, *, max_hops=3, offset=0):
        """Delegate path generation to the core catalogue and client."""
        c = self.catalogue
        src = self.records[source].rdf_class_iri if source in self.records else source
        dst = self.records[target].rdf_class_iri if target in self.records else target
        key = identifier("paths", [source, target, max_hops])
        if key not in self.cache:
            refs, limited = c.paths(src, dst, max_hops, self.max_paths)
            anchored = []
            for ref in refs:
                f = c.fragments[ref]
                anchors = {
                    i: c.fragments[r].term
                    for i, r in ((0, source), (-1, target))
                    if r in self.records
                }
                if anchors:
                    f = Fragment(**{**vars(f), "anchors": anchors})
                    ref = c._put(f, [ref, source, target])
                anchored.append(ref)
            self.cache[key] = anchored, limited
        refs, limited = self.cache[key]
        return {
            **self._page(refs, offset, limit=4),
            "search_limited": limited,
            "basis": "Retained shape paths and mined class routes; instance existence is checked by probes.",
        }

    def inspect(self, ref, *, text=""):
        """Return definitions or a profile of a targeted value lookup."""
        c = self.catalogue
        if ref in self.prepared:
            p = self.prepared[ref]
            return {
                "query_ref": ref,
                "variables": p.variables,
                "checks": p.diagnostics,
                "warnings": p.warnings,
            }
        if ref in self.results:
            return {"result_ref": ref, "profile": profile(self.results[ref])}
        if ref not in c.fragments:
            raise ValueError("Unknown retained reference. Use a reference returned by discovery.")
        card = self._card(ref)
        f = c.fragments[ref]
        if text and f.kind == "field":
            key = identifier("values", [ref, text])
            if key not in self.cache:
                self.cache[key] = self.client.field_values(
                    f.owner, f.field_name, text=text, limit=6
                )
            rows = self.cache[key]
            candidates = []
            for row in rows[:4]:
                term = _term(row["value"])
                term_ref = c._put(
                    Fragment(
                        "term", short(term.value, 100), term=term, basis="observed field value"
                    ),
                    [key, term.model_dump()],
                )
                candidates.append(
                    {"ref": term_ref, "label": short(term.value, 100), "kind": term.kind}
                )
            card.update(candidates=candidates, limited=len(rows) > 4)
        return card

    def prepare(self, sparql, grounding):
        """Expand and verify a SELECT against retained semantic commitments."""
        active = {
            g
            for g in self.goals
            if g not in self.requirements or self.requirements[g].kind != "scope"
        }
        if not self.goals or set(grounding) != active:
            raise QueryValidationError(
                "unresolved_goals",
                f"Ground every active clause: {sorted(active)}. Source-scope clauses are already enforced by the Client.",
            )
        query = verify_query(sparql, self.requirements, grounding, self.catalogue)
        query.warnings = [*self.interpretation_warnings, *query.warnings]
        self.grounding = grounding
        self.prepared = {query.ref: query}
        return {
            "state": "prepared",
            "query_ref": query.ref,
            "variables": query.variables,
            "goals": self.goals,
            "checks": query.diagnostics,
            "warnings": query.warnings,
            "interpretation": "Declared semantic commitments were checked against their selected schema evidence. The initial interpretation remains a model decision.",
        }

    def probe(self, query_ref, *, limit=5):
        """Execute a bounded query and report its RDF term profile."""
        query = self.prepared[query_ref]
        key = identifier("probe", [query_ref, limit])
        if key not in self.results:
            self.results[key] = self.client.select(query.sparql + f"\nLIMIT {limit + 1}")
        result = self.results[key]
        return {
            "state": "probed",
            "query_ref": query_ref,
            "result_ref": key,
            "profile": profile(result),
            "limited": result.row_count > limit,
            "warning": "A sample establishes observed matches; it does not establish completeness or meaning.",
        }

    def finish(self, query_ref):
        """Execute the verified artifact once and retain its full RDF bindings."""
        if query_ref in self.executions:
            return self.executions[query_ref]
        query = self.prepared[query_ref]
        try:
            result = self.client.select(query.sparql)
        except Exception as exc:
            return {
                "state": "failed",
                "query_ref": query_ref,
                "error": {"code": "execution_failed", "message": str(exc)},
            }
        ref = identifier("result", query_ref)
        self.results[ref] = result
        receipt = {
            "state": "complete",
            "query_ref": query_ref,
            "result_ref": ref,
            "rows": result.row_count,
            "variables": result.variables,
            "goals": self.goals,
            "warnings": query.warnings,
            "strategy": "Expanded retained paths, checked selected goal witnesses and binding scopes, and retrieved RDF through the shared query helper.",
            "execution": self.client.last_query_execution,
        }
        self.executions[query_ref] = receipt
        if self.artifact_dir:
            self.artifact_dir.mkdir(parents=True, exist_ok=True)
            path = self.artifact_dir / (ref + ".json")
            path.write_text(json.dumps(self.export(ref), ensure_ascii=False), encoding="utf-8")
            receipt["artifact"] = str(path)
        return receipt

    def export(self, ref):
        """Read complete artifacts from Python."""
        if ref in self.results:
            result = self.results[ref]
            return {
                "query": result.query,
                "variables": result.variables,
                "bindings": [
                    {
                        name: {
                            "type": v.type,
                            "value": v.value,
                            **({"datatype": v.datatype} if v.datatype else {}),
                            **({"xml:lang": v.lang} if v.lang else {}),
                        }
                        for name, v in row.items()
                    }
                    for row in result.rows
                ],
            }
        return asdict(self.prepared[ref])

    def diagnostics(self):
        """Report query counts and correlated operation outcomes."""
        return {
            "source_queries": len(self.client.queries),
            "steps": self.events,
            "schema_revision": self.catalogue.registry.revision,
        }
