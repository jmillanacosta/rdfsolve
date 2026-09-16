"""Retain an investigation and render bounded model observations."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from threading import RLock
from urllib.parse import urlsplit

from rdfsolve.client.catalogue import score, words
from rdfsolve.client.hydration import _term
from rdfsolve.client.query_fragments import Fragment, identifier, path_size
from rdfsolve.client.retrieval import QueryValidationError, Requirement
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
        *,
        artifact_dir=None,
        log_path=None,
        max_paths=100,
        output_variables=(),
    ):
        """Attach the source and initialize one investigation."""
        self.client = client
        self.catalogue = client.catalogue
        self.lock = RLock()
        self.question = ""
        self.goals = {}
        self.requirements = {}
        self.interpretation_warnings = []
        self.prepared = {}
        self.results = {}
        self.executions = {}
        self.records = self.catalogue.records
        self.selections = {}
        self.cache = {}
        self.max_paths = max_paths
        self.output_variables = tuple(output_variables)
        self.artifact_dir = Path(artifact_dir).resolve() if artifact_dir else None
        self.log_path = Path(log_path).resolve() if log_path else None

    def _card(self, ref):
        c = self.catalogue
        f = c.fragments[ref]
        card = {"ref": ref, "label": short(f.label), "kind": f.kind}
        if f.description:
            card["description"] = short(f.description)
        if f.iri:
            card["iri"] = f.iri
        if f.kind in {"type", "field", "path"}:
            card["binding_count"] = 1 if f.kind == "type" else 2
            if f.steps:
                card["explicit_path_ports"] = len(f.steps) + 1
        matches = c.metadata.get(ref, {}).get("name_matches", [])
        if matches:
            card["name_matches"] = [{**n, "text": short(n["text"])} for n in matches[:2]]
        if f.path:
            card.update(
                owner=c.type_refs.get(f.owner),
                field=f.field_name,
                path=path_to_sparql(f.path),
                complexity=path_size(f.path),
            )
            m = c.metadata.get(ref, {})
            card["targets"] = [
                {"ref": c.type_refs[t], "label": short(c.fragments[c.type_refs[t]].label)}
                for t in m.get("targets", [])
                if t in c.type_refs
            ]
            card["value_kinds"] = m.get("node_kinds", [])
        if f.kind == "path":
            card["evidence"] = c.metadata.get(ref, {"status": "schema_only"})
        if f.term:
            card["identity"] = ref
            card["term_kind"] = f.term.kind
            if f.term.kind == "uri":
                card["iri"] = f.term.value
        hints = c.schema_documents.get(ref, ("", []))[1]
        if hints:
            card["mapped_context"] = [
                {
                    "ref": h,
                    "label": short(c.metadata[h]["label"]),
                    "source": c.metadata[h]["related_source"],
                }
                for h in hints[:2]
            ]
        evidence = c.metadata.get(ref, {}).get("ontology", [])
        if evidence:
            card["ontology_evidence"] = [
                {
                    "local_iri": e["local_iri"],
                    "label": short(e["label"]),
                    "description": short(" ".join(e["description"])),
                    "source": e["source"],
                    "match": e["match"],
                    "parents": [short(p["label"]) for p in e["parents"][:2]],
                }
                for e in evidence[:2]
            ]
        return {k: v for k, v in card.items() if v not in (None, [], "")}

    def _page(self, refs, offset=0, budget=5500, limit=None, *, brief=False):
        cards = []
        for ref in refs[offset:]:
            card = self._card(ref)
            if brief:
                card = {k: card[k] for k in ("ref", "label", "targets") if k in card}
            if not cards and len(json.dumps([card]).encode()) > budget:
                card = {k: card[k] for k in ("ref", "label", "kind", "owner") if k in card}
                card["label"] = short(card["label"], 80)
            if (limit is not None and len(cards) >= limit) or len(
                json.dumps([*cards, card]).encode()
            ) > budget:
                break
            cards.append(card)
        return {
            "items": cards,
            "more": offset + len(cards) < len(refs),
            "retained": len(refs),
            "next_offset": offset + len(cards),
        }

    def schema(
        self,
        concepts=None,
        owners=None,
        targets=None,
        *,
        question="",
        goals=None,
        corrections=None,
        offset=0,
    ):
        """Declare clauses and retrieve their connected schema region locally."""
        corrections = corrections or {}
        if goals is not None or corrections:
            question = question or self.question
            if not question.strip() or (not goals and not corrections):
                raise ValueError(
                    "Supply the original question and each requested output or restriction as a clause."
                )
            requirements = dict(self.requirements)
            warnings = []
            for item in goals or []:
                goal = self.catalogue.requirement(item, self.output_variables)
                key = next(
                    (k for k, r in requirements.items() if r.clause == goal.clause),
                    f"g{len(requirements) + 1}",
                )
                if corrections and key in self.requirements:
                    if goal != self.requirements[key]:
                        warnings.append(
                            f"Kept retained {key}; applied only its explicit corrections."
                        )
                    continue
                requirements[key] = goal
            for key, changes in corrections.items():
                if key not in self.requirements:
                    raise ValueError(f"Unknown goal {key}; use a retained requirement ID")
                changes = {name: value for name, value in changes.items() if value is not None}
                if set(changes) - {"concept", "owner", "kind", "required"}:
                    raise ValueError("Corrections retain the original clause and value restriction")
                previous = self.requirements[key]
                kind = changes.get("kind", previous.kind)
                if previous.kind != kind and (previous.kind, kind) != (
                    "text_filter",
                    "entity_filter",
                ):
                    raise ValueError(
                        f"Preserve the {previous.kind} role of {key} ('{previous.clause}'). Correct only its concept or owner."
                    )
                revised = Requirement.model_validate({**previous.model_dump(), **changes})
                if previous.concept != revised.concept:
                    self.catalogue.validate_correction(previous, revised)
                requirements[key] = revised

            def normalize(value):
                return re.sub(r"[^a-z0-9]", "", value.casefold())

            about = self.client._schema.about
            source_names = [
                self.catalogue.registry.source_id,
                about.dataset_name or "",
                about.title or "",
            ]
            source_names += [
                part
                for url in (about.endpoint, about.homepage)
                if url
                for part in (urlsplit(url).hostname or "").split(".")
            ]
            source_names = {normalize(name) for name in source_names if name}
            for key, goal in requirements.items():
                if goal.kind == "scope" or (
                    goal.concept.casefold() in {"database", "dataset", "source"}
                    and normalize(goal.value) in source_names
                ):
                    if normalize(goal.value or goal.concept) not in source_names:
                        raise ValueError("The requested dataset differs from the configured source")
                    requirements[key] = goal.model_copy(update={"kind": "scope"})
            requested = {key: goal.clause for key, goal in requirements.items()}
            if self.goals and self.question != question:
                raise ValueError("Keep the original question across repairs")
            if self.requirements and self.requirements != requirements:
                if (
                    self.prepared
                    and not corrections
                    and any(requirements[k] != r for k, r in self.requirements.items())
                ):
                    raise ValueError(
                        "Prepared requirements are retained across probes and query repairs"
                    )
                for key, previous in self.requirements.items():
                    revised = requirements[key]
                    if previous == revised:
                        continue
                    if previous.concept != revised.concept and key not in corrections:
                        raise ValueError(
                            f"Preserve the requested concept in goals; use corrections['{key}'] to replace a vocabulary guess with a discovered concept."
                        )
                    if previous.kind in {"entity_filter", "text_filter"} and (
                        previous.value != revised.value
                        or revised.kind not in {"entity_filter", "text_filter"}
                        or (previous.kind == "entity_filter" and revised.kind != previous.kind)
                    ):
                        raise ValueError(
                            f"Preserve the value restriction '{previous.clause}' and its relationship"
                        )
                    changes = ", ".join(
                        f"{name}: {getattr(previous, name)} → {getattr(revised, name)}"
                        for name in ("concept", "owner", "kind", "required")
                        if getattr(previous, name) != getattr(revised, name)
                    )
                    warnings.append(f"Grounding corrected for '{previous.clause}' ({changes}).")
            if self.requirements != requirements:
                self.prepared.clear()
            self.interpretation_warnings.extend(warnings)
            self.question, self.goals, self.requirements = question, requested, requirements
        c = self.catalogue
        unresolved = []

        def resolve_types(values):
            resolved = set()
            for value in values or []:
                try:
                    if value in self.selections:
                        resolved.update(str(r.rdf_class_iri) for r in self.selections[value])
                    elif value in self.records:
                        resolved.add(str(self.records[value].rdf_class_iri))
                    else:
                        resolved.add(c._type(value))
                except ValueError:
                    unresolved.append(value)
            return resolved

        owner_ids, target_ids = resolve_types(owners), resolve_types(targets)
        groups, seeds, fallback = [], set(owner_ids) | target_ids, {}
        searches = dict.fromkeys(((concept, "") for concept in concepts or []), None)
        for key, requirement in self.requirements.items():
            if requirement.kind != "scope" and (
                goals is not None or key in corrections or requirement.concept in (concepts or [])
            ):
                searches.pop((requirement.concept, ""), None)
                searches[(requirement.concept, requirement.owner)] = None
        if not searches:
            searches[("", "")] = None
        for concept, requested_owner in searches:
            cls = next(iter(owner_ids)) if len(owner_ids) == 1 else None
            if requested_owner:
                try:
                    cls = c._type(requested_owner)
                except ValueError:
                    cls = None
            ranked = c.search(
                concept,
                owners=[cls] if cls else owner_ids,
                targets=target_ids,
            )
            exact = [
                r
                for r in ranked
                if c.fragments[r].kind == "type" and words(concept) == words(c.fragments[r].label)
            ]
            if exact:
                ranked = exact
                seeds.update(c.fragments[r].iri for r in exact)
            if cls and not any(c.relevance(r, concept) >= 1 for r in ranked):
                fallback[cls] = [r for r in c.field_refs.values() if c.fragments[r].owner == cls]
            groups.append(
                {
                    "concept": concept,
                    "owner": requested_owner
                    or (next(iter(owner_ids)) if len(owner_ids) == 1 else ""),
                    **self._page(ranked, offset, budget=5000 // len(searches)),
                }
            )
        connections = (
            c.search("", owners=owner_ids, targets=target_ids)
            if target_ids
            else [
                r
                for r in c.field_refs.values()
                if c.fragments[r].owner in seeds
                and seeds.intersection(c.metadata[r].get("targets", []))
                and len(seeds) > 1
            ]
        )
        return {
            "types": self._page([c.type_refs[cls] for cls in sorted(seeds)], budget=1800),
            "matches": groups,
            "connections": self._page(connections, budget=2500),
            "requirements": {g: r.model_dump() for g, r in self.requirements.items()},
            "source": c.registry.source_id,
            "scope": c.registry.binding,
            "mapping_evidence": c.mapping_status,
            "ontology": self.client.trace()["ontology"],
            "warnings": [
                "Some ontology evidence was unavailable. Unresolved meanings still need evidence."
            ]
            if self.client.ontology and self.client.ontology.diagnostics()["unavailable"]
            else [],
            "unresolved_types": unresolved,
            "unmatched_field_indexes": {
                c.type_refs[cls]: {
                    "basis": "No recorded meaning matched the requested concept. Select or clarify a retained field on this owner.",
                    **self._page(fields, offset, 4000 // len(fallback), brief=True),
                }
                for cls, fields in fallback.items()
            },
            "next_step": "Use rdf_find for named members of a discovered class."
            if unresolved
            else None,
        }

    def _retain(self, result, key, offset=0):
        """Keep a typed Results set and present a few candidate identities."""
        self.selections[key] = result
        refs = result.references
        summary = result.summary()
        return {"selection": key, **summary, **self._page(refs, offset, budget=3000, limit=4)}

    def find(self, text, kind=None, *, fields=None, target=None, max_hops=2, offset=0):
        """Use typed search and optionally evaluate connections for the whole result set."""
        c = self.catalogue
        cls = c._type(kind) if kind else None
        names = c._field_names(cls, fields or []) if cls else fields or []
        key = identifier("selection", [text, cls, names])
        if key not in self.selections:
            self.selections[key] = (
                self.client.search([text], kind=cls, fields=names)
                if names
                else self.client.find(text, kind=cls, allow_partial=True)
            )
        result = self._retain(self.selections[key], key, offset)
        if self.selections[key].coverage.get("status") == "partial":
            result["warning"] = (
                "Search budget reached. This selection covers only retained matches. Refine the name or class to resolve an individual."
            )
        if cls:
            result["searched_class"] = {
                "ref": c.type_refs[cls],
                "label": c.fragments[c.type_refs[cls]].label,
            }
        if target:
            result["connections"] = self.paths(key, target, max_hops=max_hops)
        if not result["records"]:
            result["finding"] = (
                "No name matched in the searched fields. A documented alias or a targeted field search can provide further evidence."
            )
        return result

    def _endpoint(self, ref):
        if ref in self.selections:
            return self.selections[ref]
        if ref in self.records:
            return self.records[ref]
        return self.catalogue._type(ref)

    def paths(self, source, target, *, max_hops=3, offset=0):
        """Evaluate selected records with Client.paths_between; retain schema alternatives."""
        key = identifier("paths", [source, target, max_hops])
        if key not in self.cache:
            table = self.client.paths_between(
                self._endpoint(source),
                self._endpoint(target),
                max_hops=max_hops,
                max_paths=self.max_paths,
                allow_partial=True,
                allow_repeated_classes=True,
            )
            refs = list(table.attrs["references"])
            limited = bool(table.attrs.get("truncated"))
            refs.sort(key=lambda r: self.catalogue.metadata[r].get("status") != "matched")
            self.cache[key] = refs, limited, table.attrs
        refs, limited, about = self.cache[key]
        return {
            **self._page(refs, offset, limit=4),
            "search_limited": limited,
            "basis": about["basis"],
            "warnings": about.get("warnings", [])[:3],
            "scope": "All identities in each selected set; paths stay in the configured graph scope.",
        }

    def follow(self, source, target, *, via=None, value=None, offset=0):
        """Follow a selected field through Results.related without disclosing its records."""
        from rdfsolve.client.api import Results

        selected = self._endpoint(source)
        if not isinstance(selected, Results):
            if source not in self.records:
                raise ValueError("Follow requires a retained selection or entity")
            selected = Results(self.client, [selected])
        field = self.catalogue.fragments.get(via)
        if field:
            if field.kind != "field" or any(r.rdf_class_iri != field.owner for r in selected):
                raise ValueError("Select a field owned by the source records")
            via = field.field_name
        cls = self.catalogue._type(target)
        key = identifier("selection", [source, cls, via, value])
        if key not in self.selections:
            self.selections[key] = selected.related(cls, via=via, value=value)
        return self._retain(self.selections[key], key, offset)

    def inspect(self, ref, *, text=""):
        """Return definitions or a profile of a targeted value lookup."""
        c = self.catalogue
        if ref in self.selections:
            return {"selection": ref, **self.selections[ref].summary()}
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
        if ref in c.metadata and ref.startswith("map_"):
            evidence = dict(c.metadata[ref])
            evidence["description"] = short(evidence.get("description"))
            return evidence
        if ref not in c.fragments:
            raise ValueError("Unknown retained reference. Use a reference returned by discovery.")
        c.explain(ref)
        card = self._card(ref)
        f = c.fragments[ref]
        if text and f.kind == "type":
            return self.schema([text], owners=[ref])
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

    def prepare(
        self,
        sparql=None,
        grounding=None,
        *,
        route=None,
        source="source",
        target="target",
        fields=None,
        patterns=None,
        values=None,
        text=None,
        distinct=True,
        outputs=(),
    ):
        """Expand and verify a SELECT against retained semantic commitments."""
        self.prepared.clear()
        active = {
            g
            for g in self.goals
            if g not in self.requirements or self.requirements[g].kind != "scope"
        }
        if not self.goals or (grounding is not None and not set(grounding) <= active):
            raise QueryValidationError(
                "unresolved_goals",
                f"Ground every active clause: {sorted(active)}. Source-scope clauses are already enforced by the Client.",
            )
        options = {"requirements": self.requirements, "grounding": grounding}
        if patterns:
            from rdfsolve.client.retrieval import validate_outputs

            selected = (
                outputs
                or self.output_variables
                or list(dict.fromkeys(v for p in patterns for v in p["bindings"]))
            )
            validate_outputs(selected, self.output_variables)
            query = self.client.prepare_network(
                patterns, outputs=selected, values=values, text=text, distinct=distinct, **options
            )
        elif route:
            query = self.client.prepare_path(
                route,
                source=source,
                target=target,
                fields=fields,
                output_variables=self.output_variables,
                **options,
            )
        else:
            query = self.client.prepare(sparql, output_variables=self.output_variables, **options)
        query.warnings = list(dict.fromkeys([*self.interpretation_warnings, *query.warnings]))
        self.prepared = {query.ref: query}
        return {
            "state": "prepared",
            "query_ref": query.ref,
            "variables": query.variables,
            "checks": query.diagnostics,
            "warnings": query.warnings,
            "interpretation": "Declared semantic commitments were checked against their selected schema evidence. The initial interpretation remains a model decision.",
        }

    def _query(self, ref):
        if ref not in self.prepared:
            raise ValueError(
                "Unknown or superseded query. Prepare the current query before probing or finishing."
            )
        return self.prepared[ref]

    def probe(self, query_ref, *, limit=5):
        """Execute a bounded query and report its RDF term profile."""
        query = self._query(query_ref)
        key = identifier("probe", [query_ref, limit])
        if key not in self.results:
            self.results[key] = self.client.select(query, limit=limit + 1)
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
        query = self._query(query_ref)
        try:
            result = self.client.select(query, exhaustive=True)
        except Exception as exc:
            return {
                "state": "failed",
                "query_ref": query_ref,
                "error": {"code": "execution_failed", "message": str(exc)},
            }
        ref = identifier("result", query_ref)
        self.results[ref] = result
        warnings = list(query.warnings)
        for name, column in profile(result)["columns"].items():
            if len(column["kinds"]) > 1:
                warnings.append(
                    f"{name} contains mixed RDF value kinds ({', '.join(column['kinds'])}); raw values were preserved for inspection."
                )
        steps = [s["name"] for s in self.client._steps]
        actions = [
            label
            for prefix, label in (
                ("Find ", "typed resource search"),
                ("Search ", "field search"),
                ("Evaluate generated", "evaluated paths"),
                ("Read related", "typed field traversal"),
            )
            if any(s.startswith(prefix) for s in steps)
        ]
        restrictions = [
            r for r in self.requirements.values() if r.kind in {"entity_filter", "text_filter"}
        ]
        receipt = {
            "state": "complete",
            "query_ref": query_ref,
            "result_ref": ref,
            "rows": result.row_count,
            "variables": result.variables,
            "warnings": warnings,
            "strategy": "Used "
            + ", ".join(["generated schema", *actions])
            + "; checked bindings and source scope before retrieval. "
            + f"Recorded value restrictions: {len(restrictions)}.",
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
        return asdict(self._query(ref))

    def diagnostics(self):
        """Report query counts and correlated operation outcomes."""
        return {
            **self.client.trace(),
            "schema_revision": self.catalogue.registry.revision,
            "log": str(self.log_path) if self.log_path else None,
        }
