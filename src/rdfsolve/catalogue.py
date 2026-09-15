"""Index generated RDF classes, full property paths and mapping evidence."""

from __future__ import annotations

import re

from rdfsolve.query_fragments import Fragment, identifier, path_size
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

            for shape in getattr(client.model(item.id), "rdf_shapes", []):
                for prop in shape.get("property_shapes", []):
                    if prop.get("deactivated"):
                        continue
                    p = prop["path"]
                    pp = (
                        PropertyPath(operator="predicate", iri=p)
                        if isinstance(p, str)
                        else PropertyPath.model_validate(p)
                    )
                    for fr in self.metadata[ref]["fields"]:
                        if self.fragments[fr].path == pp:
                            m = self.metadata[fr]
                            target = prop.get("class_constraint") or (
                                prop.get("qualified_shape") or {}
                            ).get("class_constraint")
                            if target:
                                m["targets"] = sorted(set(m["targets"] + [target]))
                                m["target_basis"] = "SHACL target hint"
                            if prop.get("name"):
                                self.fragments[fr].label = prop["name"]
                            if prop.get("description"):
                                self.fragments[fr].description = prop["description"]

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

    def _put(self, fragment: Fragment, key):
        prefix = {"type": "t", "field": "f", "path": "p", "term": "e"}[fragment.kind]
        ref = identifier(prefix, key)
        self.fragments.setdefault(ref, fragment)
        if fragment.iri:
            self.known_iris.add(fragment.iri)
        if fragment.path:
            self.known_iris.update(x.iri for x in self._paths(fragment.path) if x.iri)
        self.known_iris.update(fragment.endpoint_types.values())
        self.known_iris.update(t.value for t in fragment.anchors.values() if t.kind == "uri")
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
        matches = [
            t.id
            for t in self.registry.types
            if t.label.casefold() == str(value).casefold()
            or t.id == value
            or score(t.label, str(value)) == 4
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError("Ambiguous class label; use the retained type reference")
        try:
            return str(self.client.model(value).rdf_class_iri)
        except ValueError as exc:
            raise ValueError(
                "Unknown or ambiguous class. Use a type reference from schema discovery."
            ) from exc

    def _field_names(self, owner, fields):
        """Resolve references, names and unique exact labels within one owner."""
        available = {name: ref for (cls, name), ref in self.field_refs.items() if cls == owner}
        names = []
        for value in fields:
            fragment = self.fragments.get(value)
            if fragment is not None:
                if fragment.kind != "field" or fragment.owner != owner:
                    raise ValueError("Field reference belongs to a different subject type")
                name = fragment.field_name
            elif value in available:
                name = value
            else:
                candidates = [
                    name
                    for name, ref in available.items()
                    if self.fragments[ref].path.operator == "predicate"
                    and self.fragments[ref].path.iri == value
                ]
                if not candidates:
                    candidates = [
                        name
                        for name, ref in available.items()
                        if self.fragments[ref].label.casefold() == value.casefold()
                    ]
                if len(candidates) != 1:
                    qualifier = "Ambiguous" if candidates else "Unknown"
                    raise ValueError(
                        f"{qualifier} field {value!r} for {owner}. "
                        "Use an exact field name, IRI, or reference from this owner."
                    )
                name = candidates[0]
            if name not in names:
                names.append(name)
        return names

    def paths(self, source: str, target: str, max_hops: int = 3, max_paths: int = 50):
        """Build fragments from retained shapes and bounded class paths."""
        first, last = self._type(source), self._type(target)
        found = []
        for ref in self.field_refs.values():
            fragment, metadata = self.fragments[ref], self.metadata[ref]
            reverse = fragment.owner == last and first in metadata.get("targets", [])
            if not ((fragment.owner == first and last in metadata.get("targets", [])) or reverse):
                continue
            size = path_size(fragment.path)
            if size["max_hops"] is None or size["max_hops"] > max_hops:
                continue
            path = (
                PropertyPath(operator="inverse", items=[fragment.path])
                if reverse
                else fragment.path
            )
            item = Fragment(
                "path",
                ("inverse " if reverse else "") + fragment.label,
                path=path,
                endpoint_types={0: first, -1: last},
                description=fragment.description,
                basis="retained shape",
            )
            found.append(self._put(item, [first, last, path.model_dump()]))
        table = self.client.paths_between(
            first,
            last,
            max_hops=max_hops,
            max_paths=max_paths,
            allow_partial=True,
            allow_repeated_classes=True,
        )
        for route in table.attrs["routes"]:
            parts = [PropertyPath(operator="predicate", iri=p) for _, p, _, _ in route]
            parts = [
                PropertyPath(operator="inverse", items=[p]) if edge[3] else p
                for edge, p in zip(route, parts, strict=True)
            ]
            path = parts[0] if len(parts) == 1 else PropertyPath(operator="sequence", items=parts)
            if any(self.fragments[ref].path == path for ref in found):
                continue
            labels = [
                next(
                    (
                        a.text.value
                        for a in self.client._schema.enrichment.labels
                        if a.term_iri == p
                    ),
                    p.rsplit("/", 1)[-1].rsplit("#", 1)[-1],
                )
                for _, p, _, _ in route
            ]
            found.append(
                self._put(
                    Fragment(
                        "path",
                        " / ".join(labels),
                        path=path,
                        steps=route,
                        basis="mined class route",
                    ),
                    [first, last, route],
                )
            )
        return found, bool(table.attrs.get("truncated"))
