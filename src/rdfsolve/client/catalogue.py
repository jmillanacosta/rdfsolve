"""Index generated RDF classes, full property paths and mapping evidence."""

from __future__ import annotations

import re

from rdfsolve.client.query_fragments import Fragment, identifier
from rdfsolve.schema_models.enrichment import NAME_PREDICATES, SYNONYM_PREDICATES, RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath


def words(text: str) -> set[str]:
    """Normalize schema labels for deterministic lexical retrieval."""
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    return {word.lower().rstrip("s") for word in re.findall(r"[A-Za-z0-9]+", text)}


def score(text: str, query: str) -> float:
    """Rank label and description overlap with a concept."""
    terms = words(query)
    if not terms:
        return 1
    found = words(text)
    initials = "".join(word[0] for word in re.findall(r"[A-Za-z0-9]+", text)).casefold()
    if len(initials) > 1 and query.casefold() == initials:
        return 4
    overlap = len(terms & found) / len(terms)
    phrase = query.casefold() in text.casefold()
    return overlap + 2 * phrase if terms <= found else max(overlap, 0.25 * phrase)


class Catalogue:
    """A source-scoped index built from the generated models."""

    def __init__(self, client, source_id="rdf", *, class_mappings=(), related_registries=()):
        """Index a generated registry and supplied mapping evidence."""
        self.client = client
        self.registry = client.registry(source_id=source_id)
        self.fragments: dict[str, Fragment] = {}
        self.records = {}
        self.metadata: dict[str, dict] = {}
        self.known_iris: set[str] = set()
        self.type_refs, self.field_refs = {}, {}
        for item in self.registry.types:
            ref = self._put(
                Fragment("type", item.label, iri=item.id, description=item.description), item.id
            )
            self.type_refs[item.id] = ref
            self.metadata[ref] = {"description": item.description, "fields": []}
            for f in item.fields:
                path = PropertyPath.model_validate(f.binding["path"])
                fref = self._put(
                    Fragment(
                        "field",
                        f.label,
                        owner=item.id,
                        field_name=f.name,
                        path=path,
                        description=f.description,
                    ),
                    [item.id, f.name, f.binding],
                )
                self.field_refs[(item.id, f.name)] = fref
                self.metadata[ref]["fields"].append(fref)
                self.metadata[fref] = f.model_dump(mode="json")

        self.retrieval_hints = self._mapping_hints(class_mappings, related_registries)
        self.schema_documents = {
            ref: self._schema_document(ref)
            for ref in (*self.type_refs.values(), *self.field_refs.values())
        }

    def _mapping_hints(self, mappings, registries):
        """Index ClassPair or MappingEdge evidence from supplied registries."""
        lookup = {self.registry.source_id: self.registry}
        for registry in registries:
            if registry.source_id in lookup:
                raise ValueError("Related registry IDs must be distinct from the active source")
            lookup[registry.source_id] = registry
        hints = {}
        self.mapping_status = {
            "supplied_pairs": 0,
            "indexed_links": 0,
            "missing_related_metadata": 0,
        }
        for pair in getattr(mappings, "edges", mappings):
            self.mapping_status["supplied_pairs"] += 1

            for local, remote, local_ds, remote_ds in (
                (pair.source_class, pair.target_class, pair.source_dataset, pair.target_dataset),
                (pair.target_class, pair.source_class, pair.target_dataset, pair.source_dataset),
            ):
                if local_ds != self.registry.source_id or local not in self.type_refs:
                    continue
                registry = lookup.get(remote_ds)
                other = (
                    next((t for t in registry.types if t.id == remote), None) if registry else None
                )
                if other is None:
                    self.mapping_status["missing_related_metadata"] += 1
                    continue
                evidence = {
                    "local_class": local,
                    "related_class": remote,
                    "related_source": remote_ds,
                    "label": other.label,
                    "description": other.description,
                    "mapping": {
                        "source_class": pair.source_class,
                        "source_dataset": pair.source_dataset,
                        "target_class": pair.target_class,
                        "target_dataset": pair.target_dataset,
                        "predicate": pair.predicate,
                    },
                    "use": "class retrieval",
                }
                if hasattr(pair, "instance_count"):
                    evidence["support"] = {
                        "mapped_pairs": pair.instance_count,
                        "distinct_source_entities": len(pair.source_entities),
                        "distinct_target_entities": len(pair.target_entities),
                    }
                elif getattr(pair, "mapping_justification", None):
                    evidence["justification"] = pair.mapping_justification
                hint_ref = identifier("map", evidence)
                self.metadata[hint_ref] = evidence
                hints.setdefault(local, {})[hint_ref] = evidence
        self.mapping_status["indexed_links"] = sum(len(v) for v in hints.values())
        return hints

    def _schema_document(self, ref):
        f = self.fragments[ref]
        text = f"{f.label} {f.iri or ''} {f.description or ''} {f.field_name or ''}"
        if f.path:
            text += " " + path_to_sparql(f.path)
        classes = [f.iri] if f.kind == "type" else self.metadata[ref].get("targets", [])
        hint_refs = []
        for cls in classes:
            target = self.fragments.get(self.type_refs.get(cls))
            if f.kind == "field" and target:
                text += f" {target.label} {target.description or ''}"
            for hint_ref, hint in self.retrieval_hints.get(cls, {}).items():
                text += f" {hint['label']} {hint['description'] or ''}"
                hint_refs.append(hint_ref)
        iris = [f.iri] if f.iri else [p.iri for p in self._paths(f.path) if p.iri]
        if f.path and f.path.iri in NAME_PREDICATES:
            text += " name " + self.fragments[self.type_refs[f.owner]].label
        names = [
            {
                "text": item.text.value,
                "predicate": item.predicate,
                "scope": SYNONYM_PREDICATES.get(item.predicate, "label"),
            }
            for item in self.client._schema.enrichment.labels
            if item.term_iri in iris
        ]
        text += " " + " ".join(
            n["text"] for n in names if n["scope"] in {"label", "exact", "alternative"}
        )
        evidence = [
            self.client.vocabulary_evidence[i]
            for i in dict.fromkeys([*iris, *classes])
            if self.client.vocabulary_evidence.get(i)
        ]
        self.metadata[ref]["ontology"] = evidence
        for item in evidence:
            names.extend(item.get("synonym_evidence", []))
            text += " " + " ".join(
                [
                    item["iri"],
                    item["label"],
                    *item["description"],
                    *item["synonyms"],
                    *item["namespace"],
                ]
            )
        self.metadata[ref]["names"] = names
        return text, hint_refs

    def explain(self, ref):
        """Resolve a selected vocabulary term through the client's evidence overlay."""
        f = self.fragments[ref]
        if f.kind not in {"type", "field"} or not self.client.ontology or f.description:
            return
        iris = [f.iri] if f.iri else [p.iri for p in self._paths(f.path) if p.iri]
        for iri in [*iris, *self.metadata[ref].get("targets", [])]:
            self.client.vocabulary(iri)
        self.schema_documents[ref] = self._schema_document(ref)

    def search(self, concept="", *, owners=(), targets=()):
        """Find generated classes or fields by meaning and structural endpoints."""
        owners = {self._type(o) for o in owners}
        targets = {self._type(t) for t in targets}
        refs = [
            r
            for r in (self.field_refs.values() if owners else self.schema_documents)
            if (not owners or self.fragments[r].owner in owners)
            and (not targets or targets.intersection(self.metadata.get(r, {}).get("targets", [])))
        ]
        if not refs:
            return []
        if concept and self.client.ontology:
            from rdfsolve.client.ontology import term_key

            iris = set()
            for ref in refs:
                f = self.fragments[ref]
                iris.update([f.iri] if f.iri else [p.iri for p in self._paths(f.path) if p.iri])
                iris.update(self.metadata[ref].get("targets", []))
            for iri in sorted(iris):
                if self.client.ontology.cached(iri):
                    self.client.vocabulary(iri)
            self.schema_documents.update({r: self._schema_document(r) for r in refs})
            if not any(self.relevance(r, concept) >= 1 for r in refs):
                matches = {term_key(t["iri"]) for t in self.client.ontology.search(concept)}
                for iri in sorted(iris):
                    if term_key(iri) in matches:
                        self.client.vocabulary(iri)
                self.schema_documents.update({r: self._schema_document(r) for r in refs})
        refs = [r for r in refs if self.relevance(r, concept)]
        for ref in refs:
            self.metadata[ref]["name_matches"] = [
                n
                for n in self.metadata[ref].get("names", [])
                if concept and score(n["text"], concept) >= 1
            ]
        return sorted(
            refs,
            key=lambda r: (
                -score(self.fragments[r].label, concept),
                -self.relevance(r, concept),
                r,
            ),
        )

    def relevance(self, ref, concept):
        """Rank a concept using local metadata and exact ontology names."""
        fragment = self.fragments[ref]
        document = self.schema_documents.get(ref, (fragment.label, []))[0]
        rank = score(document, concept)
        if fragment.owner:
            owner = self.fragments[self.type_refs[fragment.owner]]
            qualification = words(owner.label)
            remainder = words(concept) - qualification
            if qualification and qualification <= words(concept) and remainder:
                rank = max(rank, score(document, " ".join(sorted(remainder))))
        names = [fragment.label]
        for evidence in self.metadata.get(ref, {}).get("ontology", []):
            names.extend([evidence["label"], *evidence["synonyms"]])
        if any(len(words(name)) >= 2 and words(name) <= words(concept) for name in names):
            rank = max(rank, 1)
        related = [
            n
            for n in self.metadata.get(ref, {}).get("names", [])
            if n["scope"] in {"broad", "narrow", "related"}
        ]
        if related:
            rank = max(rank, max(min(score(n["text"], concept), 1) * 0.5 for n in related))
        return rank

    def requirement(self, value, output_variables=()):
        """Separate requested output bindings from their semantic clauses."""
        from rdfsolve.client.retrieval import Requirement

        goal = Requirement.model_validate(value)
        aliases = {v.lstrip("?$") for v in output_variables}
        if goal.kind == "output" and goal.concept.lstrip("?$") in aliases:
            goal = goal.model_copy(
                update={
                    "concept": goal.concept
                    if words(goal.concept) <= words(goal.clause)
                    else goal.clause,
                    "binding": goal.binding or goal.concept.lstrip("?$"),
                }
            )
        return goal

    def validate_correction(self, previous, revised):
        """Keep known vocabulary evidence when correcting an initial search term."""
        c = self
        concept = (
            " ".join(sorted(words(previous.concept) - words(previous.value))) or previous.concept
        )
        known = {r for r in c.schema_documents if c.relevance(r, concept) >= 1}
        selected = {r for r in c.schema_documents if c.relevance(r, revised.concept) >= 1}
        for phrase, candidates in ((concept, known), (revised.concept, selected)):
            exact = {r for r in candidates if words(c.fragments[r].label) == words(phrase)}
            if exact:
                candidates.intersection_update(exact)
        if not selected:
            raise ValueError("Use a discovered class or field label in the correction")
        if previous.kind == "output":
            return
        if previous.kind == "entity_filter" and revised.owner:
            try:
                owner = c._type(revised.owner)
                if c._type(previous.concept) == owner and any(
                    c.fragments[r].kind == "field" and c.fragments[r].owner == owner
                    for r in selected
                ):
                    return
            except ValueError:
                pass
        if known and not known.intersection(selected):
            raise ValueError(
                f"The correction conflicts with retained evidence for '{previous.concept}'"
            )

    def _put(self, fragment: Fragment, key):
        prefix = {"type": "t", "field": "f", "path": "p", "term": "e"}[fragment.kind]
        ref = identifier(prefix, key)
        self.fragments.setdefault(ref, fragment)
        if fragment.iri:
            self.known_iris.add(fragment.iri)
        if fragment.path:
            self.known_iris.update(x.iri for x in self._paths(fragment.path) if x.iri)
        self.known_iris.update(fragment.endpoint_types.values())
        self.known_iris.update(
            t.value
            for anchor in fragment.anchors.values()
            for t in (anchor if isinstance(anchor, list) else [anchor])
            if t.kind == "uri"
        )
        if fragment.term and fragment.term.kind == "uri":
            self.known_iris.add(fragment.term.value)
        return ref

    @staticmethod
    def _paths(path):
        yield path
        for child in path.items:
            yield from Catalogue._paths(child)

    def _type(self, value):
        if value in self.fragments and self.fragments[value].kind == "type":
            return self.fragments[value].iri
        fragment = self.fragments.get(value)
        if fragment and fragment.path:
            classes = [fragment.owner, *self.metadata.get(value, {}).get("targets", [])]
            choices = {
                self.type_refs[c]: self.fragments[self.type_refs[c]].label
                for c in classes
                if c in self.type_refs
            }
            raise ValueError(
                f"{value} already describes a path. Use its insert, or select a class endpoint for further search: {choices}."
            )
        return str(self.client.model(value).rdf_class_iri)

    def _field_names(self, owner, fields):
        """Resolve references, names and unique exact labels within one owner."""
        names = []
        for value in fields:
            fragment = self.fragments.get(value)
            if fragment:
                if fragment.kind != "field" or fragment.owner != owner:
                    raise ValueError("Field reference belongs to a different subject type")
                value = fragment.field_name
            name = self.client.field_name(self.client.model(owner), value)
            if name not in names:
                names.append(name)
        return names

    def paths(self, source: str, target: str, max_hops: int = 3, max_paths: int = 50):
        """Build fragments from retained shapes and bounded class paths."""
        table = self.client.paths_between(
            self._type(source),
            self._type(target),
            max_hops=max_hops,
            max_paths=max_paths,
            allow_partial=True,
            allow_repeated_classes=True,
        )
        return table.attrs["references"]

    def retain_records(self, result):
        """Retain typed identities and the names that actually matched their lookup."""
        refs = []
        for record in result:
            term = RdfTerm(kind="uri", value=str(record.uri))
            ref = self._put(
                Fragment(
                    "term", self.client.title(record), term=term, basis="typed entity retrieval"
                ),
                [record.rdf_class_iri, term.value],
            )
            self.records[ref] = record
            lookups = result.coverage.get("terms", [result.coverage.get("text", "")])
            names = [
                {
                    "text": e["text"]["value"],
                    "predicate": e["predicate"],
                    "scope": e.get("name_scope") or "label",
                }
                for e in result.evidence
                if e.get("id") == str(record.uri) and "text" in e
            ]
            self.metadata.setdefault(ref, {})["name_matches"] = names
            if names and all(n["scope"] in {"broad", "narrow", "related"} for n in names):
                lookups = []
            self.metadata[ref].setdefault("lookups", set()).update(
                t.casefold() for t in lookups if t
            )
            refs.append(ref)
        return refs

    def retain_paths(self, table):
        """Index the Client's executable paths without reconstructing them."""
        refs = []
        for index, fragment in enumerate(table.attrs["fragments"]):
            ref = self._put(fragment, vars(fragment))
            self.metadata[ref] = (
                table.attrs["observations"][index]
                if "observations" in table.attrs
                else {"status": "schema_only"}
            )
            refs.append(ref)
        table.attrs["references"] = refs
        candidates = (
            [r["candidate"] for r in table.attrs["routes"]]
            if table.attrs["routes"] and isinstance(table.attrs["routes"][0], dict)
            else range(len(refs))
        )
        table["Reference"] = table["Path"].map({i + 1: refs[j] for i, j in enumerate(candidates)})
        return refs, bool(table.attrs.get("truncated"))
