"""Index generated RDF classes, full property paths and mapping evidence."""

from __future__ import annotations

import re

from rdfsolve.query_fragments import Fragment, identifier
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath


def words(text: str) -> set[str]:
    """Normalize schema labels for deterministic lexical retrieval."""
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
    return len(terms & found) / len(terms) + 2 * (query.casefold() in text.casefold())


class Catalogue:
    """A source-scoped index built from the generated models."""

    def __init__(self, client, source_id="rdf", *, class_mappings=(), related_registries=()):
        """Index a generated registry and supplied mapping evidence."""
        self.client = client
        self.registry = client.registry(source_id=source_id)
        self.fragments: dict[str, Fragment] = {}
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
        return text, hint_refs

    def search(self, concept="", *, owners=(), targets=()):
        """Find generated classes or fields by meaning and structural endpoints."""
        owners = {self._type(o) for o in owners}
        targets = {self._type(t) for t in targets}
        refs = self.field_refs.values() if owners else self.schema_documents
        refs = [
            r
            for r in refs
            if (not owners or self.fragments[r].owner in owners)
            and (not targets or targets.intersection(self.metadata.get(r, {}).get("targets", [])))
            and score(self.schema_documents[r][0], concept)
        ]
        return sorted(
            refs,
            key=lambda r: (
                -score(self.fragments[r].label, concept),
                -score(self.schema_documents[r][0], concept),
                r,
            ),
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
        return self.retain_paths(table)

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
        return refs, bool(table.attrs.get("truncated"))
